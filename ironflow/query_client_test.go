package ironflow

// ProjectionClient.ExecuteSQL against the shape the server actually emits
// (#1972 step 10).
//
// Replaces TestProjectionClient_ExecuteSQL, deleted from projection_client_test.go
// in the same commit. That test drove a mock which invented its response —
// `rows` as a list of objects, plus a `count` field — and the server emits
// neither. store.QuerySQL returns ([]string, [][]string) on both backends and
// the handler wrote those rows positionally under `total_rows`, so the shipped
// method returned a decode error for every non-empty result set while its only
// test stayed green. A fixture that agrees with the client instead of with the
// server cannot fail.
//
// So the assertions here are about what ExecuteSQL RETURNS, never how it
// fetched it, and the fixture is the generated QueryService handler rather than
// a hand-written response. Both properties are what make the test able to fail.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

// stubQueryService serves QueryService/ExecuteSQL with a fixed result, the way
// internal/server/connect.QueryHandler does over the real store.
type stubQueryService struct {
	ironflowv1connect.UnimplementedQueryServiceHandler
	columns []string
	rows    [][]string
	gotReq  *ironflowv1.ExecuteSQLRequest
	err     error
}

func (s *stubQueryService) ExecuteSQL(
	_ context.Context,
	req *connect.Request[ironflowv1.ExecuteSQLRequest],
) (*connect.Response[ironflowv1.ExecuteSQLResponse], error) {
	s.gotReq = req.Msg
	if s.err != nil {
		return nil, s.err
	}
	pbRows := make([]*ironflowv1.SQLRow, 0, len(s.rows))
	for _, r := range s.rows {
		pbRows = append(pbRows, &ironflowv1.SQLRow{Values: r})
	}
	return connect.NewResponse(&ironflowv1.ExecuteSQLResponse{
		Columns:   s.columns,
		Rows:      pbRows,
		TotalRows: int32(len(s.rows)),
	}), nil
}

func setupQueryServer(t *testing.T, svc *stubQueryService) (*Client, func()) {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(ironflowv1connect.NewQueryServiceHandler(svc))
	server := httptest.NewServer(mux)
	client := &Client{
		serverURL:   server.URL,
		httpClient:  server.Client(),
		retryConfig: &ClientRetryConfig{MaxAttempts: 1},
		logger:      NewNoopLogger(),
	}
	return client, server.Close
}

func TestProjectionClient_ExecuteSQL_ServerShape(t *testing.T) {
	t.Run("zips positional row values against the column list", func(t *testing.T) {
		svc := &stubQueryService{
			columns: []string{"id", "status", "total"},
			rows: [][]string{
				{"ord_1", "completed", "99"},
				{"ord_2", "pending", "42"},
			},
		}
		client, cleanup := setupQueryServer(t, svc)
		defer cleanup()

		result, err := client.Projections().ExecuteSQL(context.Background(), "SELECT * FROM orders LIMIT 10")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if svc.gotReq.GetQuery() != "SELECT * FROM orders LIMIT 10" {
			t.Errorf("query not forwarded: got %q", svc.gotReq.GetQuery())
		}
		if len(result.Columns) != 3 || result.Columns[0] != "id" {
			t.Fatalf("columns = %v", result.Columns)
		}
		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d (%v)", len(result.Rows), result.Rows)
		}
		// The declared type is []map[string]any and always has been. Before
		// this change nothing populated it: the server sends positional values
		// and json.Unmarshal refused them, so every caller got a decode error
		// and an empty map. The values are strings because store.QuerySQL
		// returns [][]string on both backends — REST never carried typed cells
		// either.
		if got := result.Rows[0]["id"]; got != "ord_1" {
			t.Errorf("Rows[0][id] = %v, want ord_1", got)
		}
		if got := result.Rows[0]["status"]; got != "completed" {
			t.Errorf("Rows[0][status] = %v, want completed", got)
		}
		if got := result.Rows[1]["total"]; got != "42" {
			t.Errorf("Rows[1][total] = %v, want 42", got)
		}
		// Count was pinned at zero: the server's field is total_rows and the
		// struct tag reads count.
		if result.Count != 2 {
			t.Errorf("Count = %d, want 2", result.Count)
		}
	})

	t.Run("empty result set is not an error", func(t *testing.T) {
		client, cleanup := setupQueryServer(t, &stubQueryService{columns: []string{"id"}})
		defer cleanup()

		result, err := client.Projections().ExecuteSQL(context.Background(), "SELECT id FROM orders WHERE 1=0")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(result.Rows) != 0 || result.Count != 0 {
			t.Errorf("expected an empty result, got %+v", result)
		}
	})

	t.Run("a row shorter than the column list keeps the columns it has", func(t *testing.T) {
		// The server never sends this. The zip must not panic if it ever does.
		svc := &stubQueryService{
			columns: []string{"a", "b", "c"},
			rows:    [][]string{{"1", "2"}},
		}
		client, cleanup := setupQueryServer(t, svc)
		defer cleanup()

		result, err := client.Projections().ExecuteSQL(context.Background(), "SELECT a, b, c FROM t")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(result.Rows[0]) != 2 {
			t.Errorf("expected 2 populated cells, got %v", result.Rows[0])
		}
	})

	t.Run("server error surfaces to the caller", func(t *testing.T) {
		svc := &stubQueryService{err: connect.NewError(connect.CodeInvalidArgument, errors.New("only SELECT is permitted"))}
		client, cleanup := setupQueryServer(t, svc)
		defer cleanup()

		if _, err := client.Projections().ExecuteSQL(context.Background(), "DROP TABLE runs"); err == nil {
			t.Fatal("expected an error")
		}
	})
}

// TestProjectionClient_ExecuteSQL_ErrorContract pins the error shape across the
// transport move (#1972 step 10). This is the half a "did it error?" assertion
// misses.
//
// restRequest returned an *IronflowError carrying a code, a Retryable flag and
// a sentinel Cause. A bare *connect.Error carries none of them, and the gap is
// not cosmetic: IsRetryable defaults to TRUE for an error type it does not
// recognize, so an invalid_argument SQL rejection — the one error this endpoint
// exists to produce — would tell every caller to retry it forever.
func TestProjectionClient_ExecuteSQL_ErrorContract(t *testing.T) {
	cases := []struct {
		name          string
		code          connect.Code
		wantCode      string
		wantRetryable bool
		wantSentinel  error
	}{
		{"rejected query is not retryable", connect.CodeInvalidArgument, "INVALID_ARGUMENT", false, nil},
		{"server fault is retryable", connect.CodeInternal, "INTERNAL", true, nil},
		{"unavailable is retryable", connect.CodeUnavailable, "UNAVAILABLE", true, nil},
		{"unauthenticated carries the sentinel", connect.CodeUnauthenticated, "UNAUTHENTICATED", false, ErrUnauthorized},
		{"permission denied carries the sentinel", connect.CodePermissionDenied, "PERMISSION_DENIED", false, ErrForbidden},
		{"lost CAS race is ErrContended", connect.CodeAborted, "ABORTED", false, ErrContended},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &stubQueryService{err: connect.NewError(tc.code, errors.New("boom"))}
			client, cleanup := setupQueryServer(t, svc)
			defer cleanup()

			_, err := client.Projections().ExecuteSQL(context.Background(), "SELECT 1")
			if err == nil {
				t.Fatal("expected an error")
			}

			var ifErr *IronflowError
			if !errors.As(err, &ifErr) {
				t.Fatalf("error is %T, want *IronflowError", err)
			}
			if ifErr.Code != tc.wantCode {
				t.Errorf("Code = %q, want %q", ifErr.Code, tc.wantCode)
			}
			if ifErr.Retryable != tc.wantRetryable {
				t.Errorf("Retryable = %v, want %v", ifErr.Retryable, tc.wantRetryable)
			}
			if IsRetryable(err) != tc.wantRetryable {
				t.Errorf("IsRetryable = %v, want %v", IsRetryable(err), tc.wantRetryable)
			}
			if tc.wantSentinel != nil && !errors.Is(err, tc.wantSentinel) {
				t.Errorf("error does not match sentinel %v", tc.wantSentinel)
			}
			if !strings.Contains(ifErr.Message, "boom") {
				t.Errorf("server message lost: %q", ifErr.Message)
			}
		})
	}
}
