package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTimeFromProto(t *testing.T) {
	if got := timeFromProto(nil); !got.IsZero() {
		t.Errorf("timeFromProto(nil) = %s, want the zero time, not the epoch", got)
	}
	if got := optionalTimeFromProto(nil); got != nil {
		t.Errorf("optionalTimeFromProto(nil) = %s, want nil", got)
	}
	want := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC)
	if got := timeFromProto(timestamppb.New(want)); !got.Equal(want) {
		t.Errorf("timeFromProto = %s, want %s", got, want)
	}
	if got := optionalTimeFromProto(timestamppb.New(want)); got == nil || !got.Equal(want) {
		t.Errorf("optionalTimeFromProto = %v, want %s", got, want)
	}
}

func TestRunResponseTimestamps(t *testing.T) {
	var resp runResponse
	if err := json.Unmarshal([]byte(`{"status":"RUN_STATUS_RUNNING","createdAt":"2026-03-04T05:06:07.123456789Z","updatedAt":"2026-03-04T05:06:08Z"}`), &resp); err != nil {
		t.Fatal(err)
	}
	run, err := mapRunResponse(&resp)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC); !run.CreatedAt.Equal(want) {
		t.Errorf("CreatedAt = %s, want %s", run.CreatedAt, want)
	}
	if run.StartedAt != nil || run.EndedAt != nil {
		t.Errorf("StartedAt = %v, EndedAt = %v; want nil when the server omits them", run.StartedAt, run.EndedAt)
	}

	if err := json.Unmarshal([]byte(`{"createdAt":"yesterday"}`), &resp); err == nil {
		t.Error("an unreadable createdAt decoded without an error")
	}
}

func TestPollEventTime(t *testing.T) {
	want := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC)
	if got := (pollEvent{Timestamp: "2026-03-04T05:06:07.123456789Z"}).time(); !got.Equal(want) {
		t.Errorf("time() = %s, want %s", got, want)
	}
	for _, wire := range []string{"", "yesterday"} {
		if got := (pollEvent{Timestamp: wire}).time(); !got.IsZero() {
			t.Errorf("time() for %q = %s, want the zero time", wire, got)
		}
	}
}

func TestPausedAndTimeTravelTimestampsAreOptional(t *testing.T) {
	var snap TimeTravelRunStateSnapshot
	if err := json.Unmarshal([]byte(`{"runId":"r","timestamp":"2026-03-04T05:06:07Z","steps":[{"stepId":"s"}]}`), &snap); err != nil {
		t.Fatal(err)
	}
	if snap.CreatedAt != nil || snap.Steps[0].StartedAt != nil || snap.Steps[0].CompletedAt != nil {
		t.Errorf("snapshot = %+v; want nil for omitted timestamps", snap)
	}
}

func TestWebhookSourceTimestamps(t *testing.T) {
	var source WebhookSource
	if err := json.Unmarshal([]byte(`{"id":"wh_1","created_at":"2026-03-04T05:06:07.123456789Z","updated_at":"2026-03-04T05:06:08Z"}`), &source); err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC); !source.CreatedAt.Equal(want) {
		t.Errorf("CreatedAt = %s, want %s", source.CreatedAt, want)
	}
	if source.VerifySecretPrevExpiresAt != nil {
		t.Errorf("VerifySecretPrevExpiresAt = %s, want nil when the server omits it", source.VerifySecretPrevExpiresAt)
	}

	out, err := json.Marshal(WebhookSource{ID: "wh_1"})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(out, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["created_at"]; ok {
		t.Errorf("a zero CreatedAt marshaled: %s", out)
	}
}

func mustTime(t testing.TB, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestEventMetaUnreadableTimestampIsNil(t *testing.T) {
	for _, frame := range []string{`{"sequence":7}`, `{"sequence":7,"timestamp":""}`, `{"sequence":7,"timestamp":"yesterday"}`, `{"sequence":7,"timestamp":12}`} {
		var meta EventMeta
		if err := json.Unmarshal([]byte(frame), &meta); err != nil {
			t.Errorf("%s: %v", frame, err)
		}
		if meta.Timestamp != nil || meta.Sequence != 7 {
			t.Errorf("%s = %+v, want a nil timestamp and sequence 7", frame, meta)
		}
	}
	var meta EventMeta
	if err := json.Unmarshal([]byte(`{"timestamp":"2026-03-04T05:06:07.123456789Z"}`), &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Timestamp == nil || !meta.Timestamp.Equal(mustTime(t, "2026-03-04T05:06:07.123456789Z")) {
		t.Errorf("Timestamp = %v", meta.Timestamp)
	}
}

func TestListEventsKeepsSubSecondFilters(t *testing.T) {
	var query url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query()
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client := NewClient(ClientConfig{ServerURL: server.URL})

	until := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC)
	if _, err := client.ListEvents(context.Background(), ListEventsOptions{Since: &until, Until: &until}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"since", "until"} {
		if got := query.Get(name); got != "2026-03-04T05:06:07.123456789Z" {
			t.Errorf("%s = %q, want nanoseconds kept", name, got)
		}
	}
}

func TestAuditFiltersKeepSubSecondPrecision(t *testing.T) {
	var query url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query()
		_, _ = w.Write([]byte(`{"events":[]}`))
	}))
	defer server.Close()
	client := NewClient(ClientConfig{ServerURL: server.URL})

	at := time.Date(2026, 3, 4, 7, 6, 7, 123456789, time.FixedZone("elsewhere", 2*60*60))
	if _, err := client.ListAuditEvents(context.Background(), ListAuditEventsOpts{FromTimestamp: &at, ToTimestamp: &at}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"from", "to"} {
		if got := query.Get(name); got != "2026-03-04T05:06:07.123456789Z" {
			t.Errorf("%s = %q, want UTC with nanoseconds", name, got)
		}
	}

	query = nil
	if _, err := client.ListAuditEvents(context.Background(), ListAuditEventsOpts{}); err != nil {
		t.Fatal(err)
	}
	if query.Has("from") || query.Has("to") {
		t.Errorf("nil filters sent %v", query)
	}
}

func TestEventSchemaCreatedAtIsOptional(t *testing.T) {
	var schema EventSchema
	if err := json.Unmarshal([]byte(`{"event_name":"a","version":1,"schema_json":"{}"}`), &schema); err != nil {
		t.Fatal(err)
	}
	if schema.CreatedAt != nil {
		t.Errorf("CreatedAt = %s, want nil", schema.CreatedAt)
	}
	if err := json.Unmarshal([]byte(`{"event_name":"a","version":1,"created_at":"2026-03-04T05:06:07Z"}`), &schema); err != nil {
		t.Fatal(err)
	}
	if schema.CreatedAt == nil || !schema.CreatedAt.Equal(mustTime(t, "2026-03-04T05:06:07Z")) {
		t.Errorf("CreatedAt = %v", schema.CreatedAt)
	}
}

var fixtureTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestRequireTimestamps(t *testing.T) {
	type inner struct {
		At time.Time
	}
	type outer struct {
		At       time.Time
		Optional *time.Time
		Skipped  time.Time `json:"-"`
		hidden   time.Time
		Children []inner
		ByName   map[string]inner
		Payload  any
		Raw      json.RawMessage
	}
	now := fixtureTime
	valid := outer{At: now, hidden: time.Time{}, Children: []inner{{At: now}}, ByName: map[string]inner{"a": {At: now}}, Payload: inner{}, Raw: json.RawMessage(`{}`)}
	if err := requireTimestamps(&valid); err != nil {
		t.Errorf("valid = %v", err)
	}
	zero := time.Time{}
	if err := requireTimestamps(outer{At: now, Optional: &zero}); err != nil {
		t.Errorf("a set pointer to the zero time = %v, want it skipped", err)
	}
	if err := requireTimestamps(nil); err != nil {
		t.Errorf("nil = %v", err)
	}
	if err := requireTimestamps((*outer)(nil)); err != nil {
		t.Errorf("nil pointer = %v", err)
	}
	if err := requireTimestamps(&json.RawMessage{}); err != nil {
		t.Errorf("raw message = %v", err)
	}

	for name, tc := range map[string]struct {
		value any
		path  string
	}{
		"top level": {outer{}, "outer.At"},
		"slice":     {outer{At: now, Children: []inner{{At: now}, {}}}, "outer.Children[1].At"},
		"map":       {outer{At: now, ByName: map[string]inner{"a": {}}}, "outer.ByName[a].At"},
		"pointer":   {&outer{}, "outer.At"},
		"top slice": {[]inner{{}}, "inner[0].At"},
	} {
		err := requireTimestamps(tc.value)
		var ifErr *IronflowError
		if !errors.As(err, &ifErr) || ifErr.Code != "INVALID_RESPONSE" || !strings.Contains(err.Error(), tc.path) {
			t.Errorf("%s = %v, want INVALID_RESPONSE naming %s", name, err, tc.path)
		}
	}
}

// A missing timestamp must fail the call on every decode path, not read as the
// zero time: a zero ExpectedUpdatedAt would turn the webhook guard off.
func TestMissingRequiredTimestampFailsTheCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/projects":
			_, _ = w.Write([]byte(`[{"id":"p"}]`))
		case "/api/v1/secrets":
			_, _ = w.Write([]byte(`[{"name":"s"}]`))
		case "/api/v1/workers":
			_, _ = w.Write([]byte(`{"workers":[{"id":"w"}]}`))
		default:
			_, _ = w.Write([]byte(`{"id":"wh_1"}`))
		}
	}))
	defer server.Close()
	client := NewClient(ClientConfig{ServerURL: server.URL})
	ctx := context.Background()

	calls := map[string]func() error{
		"rest":        func() error { _, err := client.Webhooks().GetSource(ctx, "wh_1"); return err },
		"list":        func() error { _, err := client.Projects().List(ctx); return err },
		"secret list": func() error { _, err := client.Secrets().List(ctx); return err },
		"workers":     func() error { _, err := client.ListWorkers(ctx); return err },
	}
	for name, call := range calls {
		if err := call(); err == nil || !strings.Contains(err.Error(), "missing required timestamp") {
			t.Errorf("%s = %v, want a missing timestamp error", name, err)
		}
	}
}
