package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/gorilla/websocket"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type headerRecorder struct {
	mu   sync.Mutex
	seen []string
}

func (h *headerRecorder) add(r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = append(h.seen, r.URL.Path+"="+r.Header.Get("X-Ironflow-SDK"))
}

func (h *headerRecorder) all() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.seen...)
}

func wantSDKHeader() string { return "go/" + SDKVersion }

func requireHeader(t *testing.T, rec *headerRecorder, wantPaths ...string) {
	t.Helper()
	got := rec.all()
	for _, p := range wantPaths {
		want := p + "=" + wantSDKHeader()
		found := false
		for _, g := range got {
			found = found || g == want
		}
		if !found {
			t.Errorf("no request %q; saw %v", want, got)
		}
	}
}

func TestSDKHeaderOnRESTAndConnect(t *testing.T) {
	rec := &headerRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.add(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{ServerURL: srv.URL, Retry: &ClientRetryConfig{}})
	_, _ = c.Emit(context.Background(), "x.y", map[string]any{})
	_, _ = c.GetRunSteps(context.Background(), "run_1") // Connect unary
	requireHeader(t, rec, "/ironflow.v1.PubSubService/Emit", "/ironflow.v1.IronflowService/GetRunSteps")
}

func TestSDKHeaderKeepsCustomHTTPClient(t *testing.T) {
	rec := &headerRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.add(r)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	custom := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		r.Header.Set("X-Custom", "1")
		return http.DefaultTransport.RoundTrip(r)
	})}
	c := NewClient(ClientConfig{ServerURL: srv.URL, HTTPClient: custom, Retry: &ClientRetryConfig{}})
	_, _ = c.Emit(context.Background(), "x.y", map[string]any{})
	requireHeader(t, rec, "/ironflow.v1.PubSubService/Emit")
	if _, ok := custom.Transport.(roundTripFunc); !ok {
		t.Error("the caller's http.Client was mutated")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSDKHeaderOnH2CAndWebSocket(t *testing.T) {
	rec := &headerRecorder{}
	up := websocket.Upgrader{}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.add(r)
		if r.URL.Path == "/ws" {
			if conn, err := up.Upgrade(w, r, nil); err == nil {
				_ = conn.Close()
			}
			return
		}
		_, _ = w.Write([]byte(`{}`))
	})
	srv := httptest.NewServer(h2c.NewHandler(h, &http2.Server{}))
	defer srv.Close()

	resp, err := newH2CClient(srv.URL).Get(srv.URL + "/h2c")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	sc := NewSubscriptionClient(SubscriptionClientConfig{WSURL: "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sc.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	sc.Close()
	requireHeader(t, rec, "/h2c", "/ws")
}

// A transport built without the wrapper would drop the header on that path.
func TestNoUnwrappedHTTPClientsInSource(t *testing.T) {
	raw := regexp.MustCompile(`http\.DefaultClient|&http\.Client\{|websocket\.DefaultDialer`)
	files, _ := filepath.Glob("*.go")
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") || f == "skew.go" {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			if raw.MatchString(line) && !strings.Contains(line, "withSDKHeader(") {
				t.Errorf("%s:%d builds an HTTP client or dials without the SDK header: %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
}

func TestServerTooOld(t *testing.T) {
	err := serverTooOld(connect.NewError(connect.CodeUnimplemented, nil))
	var ie *IronflowError
	if !asIE(err, &ie) || ie.Code != ErrCodeServerTooOld {
		t.Fatalf("got %v, want code %s", err, ErrCodeServerTooOld)
	}
	if !strings.Contains(err.Error(), "0.48.0") || !strings.Contains(err.Error(), "SubscribeBidirectional") {
		t.Errorf("message must name the rpc and minimum version: %v", err)
	}
	if IsRetryable(err) {
		t.Error("SERVER_TOO_OLD must not be retryable")
	}
	other := connect.NewError(connect.CodeUnavailable, nil)
	if serverTooOld(other) != other {
		t.Error("a non-unimplemented error must pass through unchanged")
	}
}

func TestJoinGRPCOnOldServerNamesMinVersion(t *testing.T) {
	for name, body := range map[string]string{"plain 404": "", "connect unimplemented": `{"code":"unimplemented","message":"no such method"}`} {
		t.Run(name, func(t *testing.T) {
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(body))
			})
			srv := httptest.NewServer(h2c.NewHandler(h, &http2.Server{}))
			defer srv.Close()
			c := NewClient(ClientConfig{ServerURL: srv.URL, Retry: &ClientRetryConfig{}})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := c.JoinConsumerGroup(ctx, "g", WithJoinTransport("grpc"))
			var ie *IronflowError
			if !asIE(err, &ie) || ie.Code != ErrCodeServerTooOld || !strings.Contains(err.Error(), "0.48.0") {
				t.Fatalf("got %v, want SERVER_TOO_OLD naming 0.48.0", err)
			}
		})
	}
}

func TestUnknownEnumPassesThrough(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/ironflow.v1.IronflowService/GetRun":
			_, _ = w.Write([]byte(`{"id":"r1","status":"RUN_STATUS_ZZ_FUTURE"}`))
		case "/ironflow.v1.IronflowService/ListRuns":
			_, _ = w.Write([]byte(`{"runs":[{"id":"r1","status":"RUN_STATUS_ZZ_FUTURE"},{"id":"r2","status":"RUN_STATUS_COMPLETED"}],"total":2}`))
		case "/ironflow.v1.IronflowService/GetRunSteps":
			_, _ = w.Write([]byte(`{"steps":[{"id":"s1","stepType":"STEP_TYPE_ZZ_FUTURE","status":"STEP_STATUS_ZZ_FUTURE"},{"id":"s2"}]}`))
		}
	}))
	defer srv.Close()
	c := NewClient(ClientConfig{ServerURL: srv.URL, Retry: &ClientRetryConfig{}})
	ctx := context.Background()

	run, err := c.GetRun(ctx, "r1")
	if err != nil || run.Status != RunStatus("zz_future") {
		t.Fatalf("GetRun = %+v, %v; want status zz_future", run, err)
	}
	list, err := c.ListRuns(ctx, nil)
	if err != nil || len(list.Runs) != 2 || list.Runs[1].Status != RunStatusCompleted {
		b, _ := json.Marshal(list)
		t.Fatalf("ListRuns = %s, %v; want both runs", b, err)
	}
	steps, err := c.GetRunSteps(ctx, "r1")
	if err != nil || len(steps.Steps) != 2 {
		t.Fatalf("GetRunSteps = %+v, %v; want both steps", steps, err)
	}
}

func TestConsumerGroupUnknownEnumPassesThrough(t *testing.T) {
	g := mapConsumerGroupResponse(&consumerGroupResponse{
		AckMode:      "ACK_MODE_ZZ_FUTURE",
		Backpressure: "BACKPRESSURE_MODE_ZZ_FUTURE",
		Status:       "CONSUMER_GROUP_STATUS_ZZ_FUTURE",
	})
	if g.AckMode != AckMode("zz_future") || g.Backpressure != BackpressureMode("zz_future") || g.Status != ConsumerGroupStatus("zz_future") {
		t.Fatalf("got ack=%q backpressure=%q status=%q; want zz_future for each", g.AckMode, g.Backpressure, g.Status)
	}
	// The known wire names keep mapping to the constants.
	k := mapConsumerGroupResponse(&consumerGroupResponse{
		AckMode:      "ACK_MODE_MANUAL",
		Backpressure: "BACKPRESSURE_MODE_DROP",
		Status:       "CONSUMER_GROUP_STATUS_PAUSED",
	})
	if k.AckMode != AckModeManual || k.Backpressure != BackpressureDrop || k.Status != ConsumerGroupStatusPaused {
		t.Fatalf("known = %q %q %q", k.AckMode, k.Backpressure, k.Status)
	}
	a := mapConsumerGroupResponse(&consumerGroupResponse{
		AckMode:      "ACK_MODE_AUTO",
		Backpressure: "BACKPRESSURE_MODE_BUFFER",
		Status:       "CONSUMER_GROUP_STATUS_ACTIVE",
	})
	if a.AckMode != AckModeAuto || a.Backpressure != BackpressureBuffer || a.Status != ConsumerGroupStatusActive {
		t.Fatalf("known defaults = %q %q %q", a.AckMode, a.Backpressure, a.Status)
	}
	// An absent or UNSPECIFIED value keeps the documented default.
	d := mapConsumerGroupResponse(&consumerGroupResponse{Status: "CONSUMER_GROUP_STATUS_UNSPECIFIED"})
	if d.AckMode != AckModeAuto || d.Backpressure != BackpressureBuffer || d.Status != ConsumerGroupStatusActive {
		t.Fatalf("defaults = %q %q %q", d.AckMode, d.Backpressure, d.Status)
	}
}

// A value read from a newer server and written back must not be replaced by a default.
func TestConsumerGroupUnknownEnumRoundTripsOnWrite(t *testing.T) {
	if got := mapConsumerGroupStatusToProto(ConsumerGroupStatus("zz_future")); got != "CONSUMER_GROUP_STATUS_ZZ_FUTURE" {
		t.Errorf("status = %q", got)
	}
	if got := mapAckModeToProto(AckMode("zz_future")); got != "ACK_MODE_ZZ_FUTURE" {
		t.Errorf("ack mode = %q", got)
	}
	if got := mapBackpressureToProto(BackpressureMode("zz_future")); got != "BACKPRESSURE_MODE_ZZ_FUTURE" {
		t.Errorf("backpressure = %q", got)
	}
	if got := mapConsumerGroupStatusToProto(""); got != "CONSUMER_GROUP_STATUS_ACTIVE" {
		t.Errorf("empty status = %q, want the default", got)
	}
}

func asIE(err error, target **IronflowError) bool { return errors.As(err, target) }

type closeSpy struct{ closed int }

func (c *closeSpy) RoundTrip(*http.Request) (*http.Response, error) { return nil, errors.New("unused") }
func (c *closeSpy) CloseIdleConnections()                           { c.closed++ }

// http.Client.CloseIdleConnections only reaches a transport that has the method;
// the h2c stream clients rely on it to drop their HTTP/2 connection.
func TestSDKTransportForwardsCloseIdleConnections(t *testing.T) {
	spy := &closeSpy{}
	withSDKHeader(&http.Client{Transport: spy}).CloseIdleConnections()
	if spy.closed != 1 {
		t.Errorf("inner CloseIdleConnections called %d times, want 1", spy.closed)
	}
	withSDKHeader(&http.Client{}).CloseIdleConnections() // nil inner: must not panic
}
