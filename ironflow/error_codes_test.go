package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

// One condition, one code, on every transport (#3054): the lowercase Connect
// code. A caller that checks ie.Code == "not_found" must not miss a route
// because of the transport it rides.

func codeOf(t *testing.T, err error) string {
	t.Helper()
	var ie *IronflowError
	if !errors.As(err, &ie) {
		t.Fatalf("error = %T %v, want *IronflowError", err, err)
	}
	return ie.Code
}

func TestConnectErrorCodeIsTheConnectCode(t *testing.T) {
	for _, code := range []connect.Code{
		connect.CodeNotFound, connect.CodeInvalidArgument, connect.CodeAlreadyExists,
		connect.CodeInternal, connect.CodeUnavailable, connect.CodePermissionDenied,
		connect.CodeUnauthenticated, connect.CodeFailedPrecondition,
	} {
		t.Run(code.String(), func(t *testing.T) {
			if got := codeOf(t, connectError(connect.NewError(code, errors.New("boom")))); got != code.String() {
				t.Fatalf("Code = %q, want %q", got, code.String())
			}
		})
	}
}

func TestConnectGoNotFoundAnswersNotFound(t *testing.T) {
	_, handler := ironflowv1connect.NewProjectionServiceHandler(projectionClientService{})
	client, cleanup := setupMockProjectionServer(t, handler)
	defer cleanup()

	_, err := client.Projections().Get(context.Background(), "missing")
	if got := codeOf(t, err); got != "not_found" {
		t.Fatalf("Projections().Get Code = %q, want not_found", got)
	}
}

func TestRESTNotFoundAnswersNotFound(t *testing.T) {
	client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"projection not found","message":"projection not found","code":"not_found","retryable":false}`))
	}))
	defer cleanup()

	err := client.Projections().Delete(context.Background(), "gone")
	if got := codeOf(t, err); got != "not_found" {
		t.Fatalf("Projections().Delete Code = %q, want not_found", got)
	}
}

func TestAuthErrorCodes(t *testing.T) {
	for status, want := range map[int]string{
		http.StatusUnauthorized: "unauthenticated",
		http.StatusForbidden:    "permission_denied",
	} {
		rec := httptest.NewRecorder()
		rec.WriteHeader(status)
		if got := codeOf(t, authError(rec.Result(), "subscribe")); got != want {
			t.Errorf("authError(%d).Code = %q, want %q", status, got, want)
		}
	}
}

// A REST body with no code (an older server, a proxy) still gets the Connect
// code its status maps to, instead of an invented HTTP_<status>.
func TestRESTErrorWithoutCodeGetsTheConnectCodeOfItsStatus(t *testing.T) {
	for status, want := range map[int]string{
		http.StatusUnauthorized:        "unauthenticated",
		http.StatusForbidden:           "permission_denied",
		http.StatusNotFound:            "not_found",
		http.StatusConflict:            "already_exists",
		http.StatusPreconditionFailed:  "failed_precondition",
		http.StatusBadRequest:          "invalid_argument",
		http.StatusTooManyRequests:     "resource_exhausted",
		http.StatusInternalServerError: "internal",
		http.StatusServiceUnavailable:  "unavailable",
		http.StatusPaymentRequired:     "failed_precondition",
		http.StatusRequestTimeout:      "deadline_exceeded",
		http.StatusNotImplemented:      "unimplemented",
		http.StatusGatewayTimeout:      "deadline_exceeded",
		http.StatusInsufficientStorage: "failed_precondition",
	} {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"nope"}`))
		}))
		for name, call := range map[string]func() error{
			"restRequest": func() error { return client.Projections().Delete(context.Background(), "x") },
			"kv put": func() error {
				_, err := client.KV().Bucket("b").Put(context.Background(), "k", []byte("v"))
				return err
			},
		} {
			if got := codeOf(t, call()); got != want {
				t.Errorf("%s: status %d Code = %q, want %q", name, status, got, want)
			}
		}
		cleanup()
	}
}

// A server released before #3054 sends capitals for the generic codes; the
// SDK answers the same lowercase code, and a domain code keeps its name.
func TestLegacyUppercaseGenericCodesAreNormalised(t *testing.T) {
	for body, want := range map[string]string{
		"NOT_FOUND": "not_found", "CONFLICT": "already_exists", "FORBIDDEN": "permission_denied",
		"INVALID": "invalid_argument", "INTERNAL": "internal", "BUCKET_NOT_EMPTY": "BUCKET_NOT_EMPTY",
		"FUNCTION_NOT_FOUND": "FUNCTION_NOT_FOUND",
	} {
		if got := restErrorCode(body, http.StatusConflict); got != want {
			t.Errorf("restErrorCode(%q) = %q, want %q", body, got, want)
		}
	}
}

func TestCapabilitiesAndWorkersErrorsCarryTheBodyCode(t *testing.T) {
	client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"no","message":"no","code":"permission_denied"}`))
	}))
	defer cleanup()
	for name, call := range map[string]func() error{
		"GetCapabilities": func() error { _, err := client.GetCapabilities(context.Background()); return err },
		"ListWorkers":     func() error { _, err := client.ListWorkers(context.Background()); return err },
	} {
		err := call()
		var ie *IronflowError
		if !errors.As(err, &ie) || ie.Code != "permission_denied" || ie.Details["http_status"] != http.StatusForbidden {
			t.Errorf("%s = %v, want permission_denied with http_status 403", name, err)
		}
	}
}

// The KV write throttle's 507 also answers failed_precondition; only a 412 is a lost claim race.
func TestIsHTTPStatusMatchesTheStatusNotTheCode(t *testing.T) {
	lost := &IronflowError{Code: "failed_precondition", Details: map[string]any{"http_status": http.StatusPreconditionFailed}}
	diskCap := &IronflowError{Code: "failed_precondition", Details: map[string]any{"http_status": http.StatusInsufficientStorage}}
	if !isHTTPStatus(lost, http.StatusPreconditionFailed) || isHTTPStatus(diskCap, http.StatusPreconditionFailed) {
		t.Fatal("isHTTPStatus must tell a 412 from a 507 that shares the code")
	}
}

func TestKVAbortedBodyCarriesTheContendedSentinel(t *testing.T) {
	client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"lost","code":"aborted"}`))
	}))
	defer cleanup()
	err := client.Projections().Delete(context.Background(), "x")
	if !errors.Is(err, ErrContended) || codeOf(t, err) != "aborted" {
		t.Fatalf("err = %v, want aborted + ErrContended", err)
	}
}
