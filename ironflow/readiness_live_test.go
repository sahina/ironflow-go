package ironflow

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// readinessSeed mirrors the file tests/readiness-live/seed writes (#2942).
type readinessSeed struct {
	FunctionID    string         `json:"functionId"`
	PausedVersion int64          `json:"pausedVersion"`
	RunID         string         `json:"runId"`
	StepID        string         `json:"stepId"`
	EntityID      string         `json:"entityId"`
	BeforeRun     string         `json:"beforeRun"`
	AfterStep     string         `json:"afterStep"`
	StepName      string         `json:"stepName"`
	StepOutput    map[string]any `json:"stepOutput"`

	PartitionProjection string   `json:"partitionProjection"`
	Partitions          []string `json:"partitions"`
	SchemaEvent         string   `json:"schemaEvent"`
}

// sameJSON reports whether raw decodes to want.
func sameJSON(raw json.RawMessage, want map[string]any) bool {
	var got map[string]any
	return json.Unmarshal(raw, &got) == nil && reflect.DeepEqual(got, want)
}

// TestLiveReadiness reads one real seeded run back through the Go helpers.
// It skips unless IRONFLOW_READINESS_SEED is set; `make test-readiness-live` sets it.
func TestLiveReadiness(t *testing.T) {
	seedPath := os.Getenv("IRONFLOW_READINESS_SEED")
	if seedPath == "" {
		t.Skip("IRONFLOW_READINESS_SEED not set; run `make test-readiness-live`")
	}
	raw, err := os.ReadFile(seedPath) // #nosec G304 -- path comes from the make target
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	var seed readinessSeed
	if err := json.Unmarshal(raw, &seed); err != nil {
		t.Fatalf("decode seed: %v", err)
	}
	client := NewClient(ClientConfig{ServerURL: os.Getenv("IRONFLOW_TEST_SERVER"), APIKey: os.Getenv("IRONFLOW_TEST_API_KEY")})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("event-emit-batch and event-names", func(t *testing.T) {
		suffix := fmt.Sprintf("%d", time.Now().UnixNano())
		a, b := "gosdk.readiness.a."+suffix, "gosdk.readiness.b."+suffix
		results, err := client.TriggerBatch(ctx, []TriggerBatchEvent{{Event: a, Data: map[string]any{"i": 1}}, {Event: b, Data: map[string]any{"i": 2}}})
		if err != nil {
			t.Fatalf("TriggerBatch: %v", err)
		}
		if len(results) != 2 || results[0].EventID == "" || results[0].EventID == results[1].EventID {
			t.Fatalf("TriggerBatch results = %+v, want two distinct event IDs", results)
		}
		names, err := client.ListEventNames(ctx, ListEventNamesOptions{})
		if err != nil {
			t.Fatalf("ListEventNames: %v", err)
		}
		counts := map[string]int64{}
		for _, n := range names.Names {
			counts[n.Name] = n.Count
		}
		if counts[a] != 1 || counts[b] != 1 {
			t.Fatalf("ListEventNames counts for batch events = %d, %d, want 1, 1", counts[a], counts[b])
		}
	})

	t.Run("event-emit, event-list and event-get", func(t *testing.T) {
		name := fmt.Sprintf("gosdk.readiness.emit.%d", time.Now().UnixNano())
		res, err := client.Emit(ctx, name, map[string]any{"i": 7})
		if err != nil {
			t.Fatalf("Emit: %v", err)
		}
		// No function listens on a unique name, so the event is stored with no runs.
		if res.EventID == "" || len(res.RunIDs) != 0 {
			t.Fatalf("Emit result = %+v, want an event ID and no runs", res)
		}
		page, err := client.ListEvents(ctx, ListEventsOptions{Name: name, Limit: 5})
		if err != nil {
			t.Fatalf("ListEvents: %v", err)
		}
		if page.Count != 1 || len(page.Events) != 1 || page.Events[0].ID != res.EventID || page.Limit != 5 {
			t.Fatalf("ListEvents page = %+v, want the one emitted event %s with limit 5", page, res.EventID)
		}
		got, err := client.GetEvent(ctx, res.EventID)
		if err != nil {
			t.Fatalf("GetEvent: %v", err)
		}
		data, _ := got.Data.(map[string]any)
		if got.ID != res.EventID || got.Name != name || data["i"] != float64(7) {
			t.Fatalf("GetEvent = %+v, want %s %q data {i: 7}", got, res.EventID, name)
		}
	})

	t.Run("event-emit-sync", func(t *testing.T) {
		name := fmt.Sprintf("gosdk.readiness.sync.%d", time.Now().UnixNano())
		results, err := client.EmitSync(ctx, name, map[string]any{"i": 1}, 5*time.Second)
		if err != nil {
			t.Fatalf("EmitSync: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("EmitSync results = %+v, want none (no function listens on %q)", results, name)
		}
		page, err := client.ListEvents(ctx, ListEventsOptions{Name: name})
		if err != nil || page.Count != 1 {
			t.Fatalf("ListEvents after EmitSync = %+v, %v, want the one stored event", page, err)
		}
	})

	t.Run("function-at-version", func(t *testing.T) {
		entry, err := client.GetFunctionAtVersion(ctx, seed.FunctionID, seed.PausedVersion)
		if err != nil {
			t.Fatalf("GetFunctionAtVersion: %v", err)
		}
		if entry.FunctionID != seed.FunctionID || int64(entry.EntityVersion) != seed.PausedVersion || entry.ChangeType != FunctionChangeStatusChange {
			t.Fatalf("entry = %+v, want %s v%d status_change", entry, seed.FunctionID, seed.PausedVersion)
		}
		if entry.FunctionSnapshot == nil || entry.FunctionSnapshot.Status != FunctionStatusPaused {
			t.Fatalf("snapshot = %+v, want status paused", entry.FunctionSnapshot)
		}
		current, err := client.GetFunction(ctx, seed.FunctionID)
		if err != nil {
			t.Fatalf("GetFunction: %v", err)
		}
		if current.Status != FunctionStatusActive {
			t.Fatalf("current status = %s, want active (the history read must differ from now)", current.Status)
		}
	})

	t.Run("run-audit", func(t *testing.T) {
		trail, err := client.GetAuditTrail(ctx, seed.RunID)
		if err != nil {
			t.Fatalf("GetAuditTrail: %v", err)
		}
		var types []string
		for _, e := range trail.Events {
			types = append(types, e.EventType)
			if e.RunID != seed.RunID || e.FunctionID != seed.FunctionID || e.CreatedAt == "" {
				t.Errorf("event %+v does not belong to run %s / function %s", e, seed.RunID, seed.FunctionID)
			}
			if strings.HasPrefix(e.EventType, "step.") && e.StepID != seed.StepID {
				t.Errorf("%s step_id = %q, want %q", e.EventType, e.StepID, seed.StepID)
			}
		}
		want := []string{"run.created", "step.started", "step.completed", "run.status_changed"}
		if !slices.Equal(types, want) || trail.TotalCount != len(want) {
			t.Fatalf("audit = %v (total %d), want %v", types, trail.TotalCount, want)
		}

		var paged []string
		cursor := ""
		for page := 0; page < 2; page++ {
			p, err := client.GetAuditTrail(ctx, seed.RunID, GetAuditTrailOpts{Limit: 2, Cursor: cursor})
			if err != nil {
				t.Fatalf("GetAuditTrail page %d: %v", page, err)
			}
			if len(p.Events) != 2 || p.TotalCount != len(want) {
				t.Fatalf("page %d = %d events (total %d), want 2 of %d", page, len(p.Events), p.TotalCount, len(want))
			}
			for _, e := range p.Events {
				paged = append(paged, e.EventType)
			}
			cursor = p.NextCursor
			if page == 0 && cursor == "" {
				t.Fatal("first page has no next cursor")
			}
		}
		if !slices.Equal(paged, want) {
			t.Fatalf("paged audit = %v, want %v", paged, want)
		}
	})

	t.Run("run-streams", func(t *testing.T) {
		streams, err := client.GetRunStreams(ctx, seed.RunID)
		if err != nil {
			t.Fatalf("GetRunStreams: %v", err)
		}
		if !slices.Equal(streams.EntityIDs, []string{seed.EntityID}) {
			t.Fatalf("entity IDs = %v, want [%s]", streams.EntityIDs, seed.EntityID)
		}
	})

	t.Run("tt-step-output-at", func(t *testing.T) {
		before, _ := time.Parse(time.RFC3339Nano, seed.BeforeRun)
		after, _ := time.Parse(time.RFC3339Nano, seed.AfterStep)
		if _, err := client.GetStepOutputAt(ctx, seed.RunID, seed.StepID, before); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("before the run: err = %v, want not found", err)
		}
		snap, err := client.GetStepOutputAt(ctx, seed.RunID, seed.StepID, after)
		if err != nil {
			t.Fatalf("after the step: %v", err)
		}
		if snap.Status != "completed" || !sameJSON(snap.Output, seed.StepOutput) {
			t.Fatalf("snapshot = %+v (output %s), want completed with %v", snap, snap.Output, seed.StepOutput)
		}
		byName, err := client.GetStepOutputAt(ctx, seed.RunID, seed.StepName, after)
		if err != nil || byName.StepID != seed.StepID {
			t.Fatalf("lookup by step name %q = %+v, %v; want step %s", seed.StepName, byName, err, seed.StepID)
		}
	})

	t.Run("tt-run-state-at", func(t *testing.T) {
		after, _ := time.Parse(time.RFC3339Nano, seed.AfterStep)
		state, err := client.GetRunStateAt(ctx, seed.RunID, after)
		if err != nil {
			t.Fatalf("GetRunStateAt: %v", err)
		}
		if state.RunID != seed.RunID || state.Status != "completed" || len(state.Steps) != 1 || string(state.Input) != "{}" {
			t.Fatalf("state = %+v (input %s), want run %s completed with one step and input {}", state, state.Input, seed.RunID)
		}
		step := state.Steps[0]
		if step.StepID != seed.StepID || step.Name != seed.StepName || !sameJSON(step.Output, seed.StepOutput) {
			t.Fatalf("step = %+v (output %s), want %s named %s with %v", step, step.Output, seed.StepID, seed.StepName, seed.StepOutput)
		}
	})

	t.Run("tt-run-timeline", func(t *testing.T) {
		events, err := client.GetRunTimeline(ctx, seed.RunID)
		if err != nil {
			t.Fatalf("GetRunTimeline: %v", err)
		}
		var types []string
		for _, e := range events {
			types = append(types, e.EventType)
			if strings.HasPrefix(e.EventType, "step.") && (e.StepID != seed.StepID || e.StepName != seed.StepName) {
				t.Errorf("%s = step %q named %q, want %q named %q", e.EventType, e.StepID, e.StepName, seed.StepID, seed.StepName)
			}
		}
		if want := []string{"run.created", "step.started", "step.completed", "run.status_changed"}; !slices.Equal(types, want) {
			t.Fatalf("timeline = %v, want %v", types, want)
		}
	})

	t.Run("proj-partitions", func(t *testing.T) {
		res, err := client.ListProjectionPartitions(ctx, seed.PartitionProjection, ListProjectionPartitionsOptions{})
		if err != nil {
			t.Fatalf("ListProjectionPartitions: %v", err)
		}
		got := slices.Sorted(slices.Values(res.Partitions))
		if !slices.Equal(got, seed.Partitions) || res.Returned != len(seed.Partitions) {
			t.Fatalf("partitions = %v (returned %d), want %v", res.Partitions, res.Returned, seed.Partitions)
		}
	})

	t.Run("schema-test-upcast", func(t *testing.T) {
		data := map[string]any{"orderId": "o1"}
		res, err := client.Schemas().TestUpcast(ctx, TestUpcastInput{EventName: seed.SchemaEvent, FromVersion: 1, ToVersion: 3, Data: data})
		if err != nil {
			t.Fatalf("TestUpcast: %v", err)
		}
		want := []UpcastStep{
			{FromVersion: 1, ToVersion: 2, Description: "upcast to v2"},
			{FromVersion: 2, ToVersion: 3, Description: "no schema registered for version 3"},
		}
		if !res.Success || !reflect.DeepEqual(res.Data, data) || !reflect.DeepEqual(res.StepsApplied, want) {
			t.Fatalf("upcast = %+v, want data %v unchanged and steps %+v", res, data, want)
		}
	})

	t.Run("pubsub-cg-manage", func(t *testing.T) {
		name := fmt.Sprintf("gosdk-cg-%d", time.Now().UnixNano())
		pattern := "topic:gosdk.cg." + name
		if _, err := client.CreateConsumerGroup(ctx, ConsumerGroupConfig{Name: name, Pattern: pattern, MaxRedeliveries: 3}); err != nil {
			t.Fatalf("create: %v", err)
		}
		got, err := client.GetConsumerGroup(ctx, name)
		if err != nil || got.Name != name || got.Pattern != pattern || got.MaxRedeliveries != 3 {
			t.Fatalf("get = %+v, %v; want %s on %s with 3 redeliveries", got, err, name, pattern)
		}
		groups, err := client.ListConsumerGroups(ctx)
		if err != nil || !slices.ContainsFunc(groups, func(g *ConsumerGroup) bool { return g.Name == name }) {
			t.Fatalf("list: %v; %s missing", err, name)
		}
		seven := 7
		if updated, err := client.UpdateConsumerGroup(ctx, name, UpdateConsumerGroupInput{MaxRedeliveries: &seven}); err != nil || updated.MaxRedeliveries != 7 {
			t.Fatalf("update = %+v, %v; want 7 redeliveries", updated, err)
		}
		if got, err := client.GetConsumerGroup(ctx, name); err != nil || got.MaxRedeliveries != 7 {
			t.Fatalf("get after update = %+v, %v; want 7 redeliveries", got, err)
		}
		if err := client.DeleteConsumerGroup(ctx, name); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, err := client.GetConsumerGroup(ctx, name); err == nil || !strings.Contains(strings.ToLower(err.Error()), "not found") {
			t.Fatalf("get after delete: err = %v, want not found", err)
		}
	})

	t.Run("webhook-secret-ops and webhook-deliveries", func(t *testing.T) {
		liveWebhooks(t, client, os.Getenv("IRONFLOW_TEST_SERVER"))
	})

	t.Run("audit-export", func(t *testing.T) {
		want := []string{"run.status_changed", "step.completed", "step.started", "run.created"} // newest first
		if got := exportAudit(t, "run_id="+seed.RunID); !slices.Equal(got, want) {
			t.Fatalf("export records = %v, want %v", got, want)
		}
		if got := exportAudit(t, "run_id="+seed.RunID+"&event_type=step.completed"); !slices.Equal(got, []string{"step.completed"}) {
			t.Fatalf("filtered export records = %v, want [step.completed]", got)
		}
	})

	t.Run("metrics-endpoint", func(t *testing.T) {
		resp, err := http.Get(os.Getenv("IRONFLOW_TEST_SERVER") + "/metrics")
		if err != nil {
			t.Fatalf("GET /metrics: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		labels := `function_id="` + seed.FunctionID + `"`
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "ironflow_runs_total{") && strings.Contains(line, labels) && strings.Contains(line, `status="completed"`) {
				if v, err := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64); err != nil || v != 1 {
					t.Fatalf("%s: want value 1", line)
				}
				return
			}
		}
		t.Fatalf("no ironflow_runs_total sample for %s with status completed (HTTP %d)", labels, resp.StatusCode)
	})

	t.Run("proj-runner-lease", func(t *testing.T) {
		liveRunnerLease(t, client, os.Getenv("IRONFLOW_TEST_SERVER"), os.Getenv("IRONFLOW_TEST_API_KEY"))
	})
}

// liveRunnerLease starts two runners of one external projection. One holds the
// lease past the 30s TTL (renew), and stopping it hands over well before expiry
// (release). The standby retries every 5s with jitter.
func liveRunnerLease(t *testing.T, client *Client, serverURL, apiKey string) {
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	name, event := "gosdk-lease-"+suffix, "gosdk.lease."+suffix
	var mu sync.Mutex
	seen := map[string][]int{}
	count := func(id string) int { mu.Lock(); defer mu.Unlock(); return len(seen[id]) }
	start := func(id string) (stop func()) {
		proj := CreateProjection(ProjectionConfig{Name: name, Events: []string{event},
			Handler: func(_ map[string]any, e ProjectionEvent, _ ProjectionContext) (map[string]any, error) {
				i, _ := e.Data["i"].(float64)
				mu.Lock()
				seen[id] = append(seen[id], int(i))
				mu.Unlock()
				return nil, nil
			}})
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			// A worker with no functions never starts its runners (registration
			// needs one function), so each worker carries a no-op function.
			noop := CreateFunction(FunctionConfig{ID: name + "-noop", Triggers: []Trigger{{Event: name + ".noop"}}},
				func(Context) (any, error) { return nil, nil })
			_ = NewWorker(WorkerConfig{ServerURL: serverURL, APIKey: apiKey, Functions: []Function{noop}, Projections: []Projection{proj}, Logger: NewNoopLogger()}).Run(runCtx)
		}()
		return func() { cancel(); <-done }
	}
	i := 0
	emitUntil := func(cond func() bool, within, every time.Duration) bool {
		for end := time.Now().Add(within); time.Now().Before(end); time.Sleep(every) {
			i++
			if _, err := client.Emit(ctx, event, map[string]any{"i": i}); err != nil {
				t.Fatalf("emit: %v", err)
			}
			if cond() {
				return true
			}
		}
		return cond()
	}

	stopA := start("a")
	defer func() { stopA() }()
	if !emitUntil(func() bool { return count("a") > 0 }, 30*time.Second, 500*time.Millisecond) {
		t.Fatal("runner a never handled an event")
	}
	stopB := start("b")
	defer stopB()

	emitUntil(func() bool { return false }, 35*time.Second, 2*time.Second) // past one TTL
	want := i
	handledByA := func() bool { mu.Lock(); defer mu.Unlock(); return slices.Contains(seen["a"], want) }
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end) && !handledByA(); time.Sleep(200 * time.Millisecond) {
	}
	mu.Lock()
	gotA, gotB := slices.Contains(seen["a"], want), len(seen["b"])
	mu.Unlock()
	if !gotA || gotB != 0 {
		t.Fatalf("after 35s: a handled event %d = %v, b handled %d events; want a only (renew keeps the lease)", want, gotA, gotB)
	}

	stopA()
	stopA = func() {}
	afterStop := count("a")
	began := time.Now()
	if !emitUntil(func() bool { return count("b") > 0 }, 15*time.Second, time.Second) {
		t.Fatal("standby b did not take over within 15s of a stopping; release did not free the lease")
	}
	t.Logf("takeover after %s", time.Since(began).Round(100*time.Millisecond))
	if count("a") != afterStop {
		t.Fatal("stopped runner a handled events after stop")
	}
}

// exportLine is one line of the audit export: a manifest, a record or a footer.
type exportLine struct {
	Type string `json:"type"`
	Data struct {
		EventType string `json:"event_type"`
	} `json:"data"`
	Counts struct {
		AuditEvents int `json:"audit_events"`
	} `json:"counts"`
}

// exportAudit reads GET /api/v1/audit/export and returns the event types of its
// records, checking the manifest and the footer count around them.
func exportAudit(t *testing.T, query string) []string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, os.Getenv("IRONFLOW_TEST_SERVER")+"/api/v1/audit/export?"+query, nil)
	req.Header.Set("Authorization", "Bearer "+os.Getenv("IRONFLOW_TEST_API_KEY"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("export: HTTP %d, %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	var lines []exportLine
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		lines = append(lines, exportLine{})
		if err := json.Unmarshal(sc.Bytes(), &lines[len(lines)-1]); err != nil {
			t.Fatalf("export line %q: %v", sc.Text(), err)
		}
	}
	if len(lines) < 2 || lines[0].Type != "manifest" || lines[len(lines)-1].Type != "footer" {
		t.Fatalf("export lines = %+v, want manifest ... footer", lines)
	}
	var types []string
	for _, l := range lines[1 : len(lines)-1] {
		if l.Type != "record" {
			t.Fatalf("export line type %q, want record", l.Type)
		}
		types = append(types, l.Data.EventType)
	}
	if n := lines[len(lines)-1].Counts.AuditEvents; n != len(types) {
		t.Fatalf("footer counts %d audit events, body has %d", n, len(types))
	}
	return types
}

// liveWebhooks drives a signed source through every secret operation with real
// deliveries, then checks the delivery log records each one.
func liveWebhooks(t *testing.T, client *Client, serverURL string) {
	ctx := context.Background()
	wh := client.Webhooks()
	src, err := wh.CreateSource(ctx, CreateWebhookSourceInput{
		Name: fmt.Sprintf("gosdk-wh-%d", time.Now().UnixNano()), EventPrefix: "gosdk.wh",
		VerifyHeader: "x-sig", VerifyAlgorithm: "hmac-sha256", VerifySecret: "s1",
	})
	if err != nil || src.IngestToken == "" {
		t.Fatalf("CreateSource = %+v, %v; want an ingest token", src, err)
	}
	defer func() { _ = wh.DeleteSource(ctx, src.ID) }()
	deliver := func(secret, token, id string) int {
		body := []byte(`{"type":"gosdk.wh","id":"` + id + `","data":{"k":1}}`)
		req, _ := http.NewRequest(http.MethodPost, serverURL+"/api/v1/webhooks/"+src.ID+"?token="+url.QueryEscape(token), strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		if secret != "" {
			mac := hmac.New(sha256.New, []byte(secret))
			mac.Write(body)
			req.Header.Set("x-sig", hex.EncodeToString(mac.Sum(nil)))
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("deliver %s: %v", id, err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	expect := func(secret, token, id string, want int) {
		t.Helper()
		if got := deliver(secret, token, id); got != want {
			t.Fatalf("delivery %s = HTTP %d, want %d", id, got, want)
		}
	}
	t1 := src.IngestToken
	expect("s1", t1, "d1", 200)

	rotated, err := wh.RotateSecret(ctx, RotateWebhookSecretInput{ID: src.ID, VerifySecret: "s2"})
	if err != nil || !rotated.VerifySecretPrevSet {
		t.Fatalf("RotateSecret = %+v, %v; want the old secret kept as prev", rotated, err)
	}
	expect("s1", t1, "d2", 200) // grace window
	expect("s2", t1, "d3", 200)

	if expired, err := wh.ExpireSecretPrev(ctx, src.ID); err != nil || expired.VerifySecretPrevSet {
		t.Fatalf("ExpireSecretPrev = %+v, %v; want no prev secret", expired, err)
	}
	expect("s1", t1, "d4", 401)
	expect("s2", t1, "d5", 200)

	retokened, err := wh.RotateIngestToken(ctx, RotateWebhookIngestTokenInput{ID: src.ID})
	if err != nil || retokened.IngestToken == "" || retokened.IngestToken == t1 {
		t.Fatalf("RotateIngestToken = %+v, %v; want a new token", retokened, err)
	}
	t2 := retokened.IngestToken
	expect("s2", t1, "d6", 401)
	expect("s2", t2, "d7", 200)

	if disabled, err := wh.DisableSignatureVerification(ctx, src.ID, nil); err != nil || disabled.VerifySecretSet {
		t.Fatalf("DisableSignatureVerification = %+v, %v; want no current secret", disabled, err)
	}
	expect("", t2, "d8", 200)

	deliveries, total, err := wh.ListDeliveries(ctx, ListWebhookDeliveriesOpts{SourceID: src.ID})
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	type row struct{ ExternalID, Status, Error string }
	var got []row
	for _, d := range deliveries {
		got = append(got, row{d.ExternalID, d.Status, d.Error})
	}
	want := []row{ // newest first; a bad ingest token is rejected before the body is read
		{"d8", "accepted", ""}, {"d7", "accepted", ""}, {"", "rejected", "invalid ingest token"},
		{"d5", "accepted", ""}, {"d4", "rejected", "signature mismatch"}, {"d3", "accepted", ""},
		{"d2", "accepted", ""}, {"d1", "accepted", ""},
	}
	if total != len(want) || !slices.Equal(got, want) {
		t.Fatalf("deliveries (total %d) = %+v, want %+v", total, got, want)
	}
}
