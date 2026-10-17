package ironflow

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// helpersSeed mirrors the file tests/readiness-live/helpers-seed writes (#2948).
type helpersSeed struct {
	PlatformKey        string     `json:"platformKey"`
	CapacityFunctionID string     `json:"capacityFunctionId"`
	LeasedRunID        string     `json:"leasedRunId"`
	QueuedRunID        string     `json:"queuedRunId"`
	DebounceFunctionID string     `json:"debounceFunctionId"`
	Projection         string     `json:"projection"`
	ProjectionEvents   [][]string `json:"projectionEvents"`
	SchemaEvent        string     `json:"schemaEvent"`
	SchemaVersion      int32      `json:"schemaVersion"`
	SchemaDocs         []string   `json:"schemaDocs"`
}

// TestLiveHelpersReadiness drives the #2948 helpers against a seeded server.
// It skips unless IRONFLOW_READINESS_HELPERS_SEED is set; `make
// test-readiness-helpers-live` sets it.
func TestLiveHelpersReadiness(t *testing.T) {
	seedPath := os.Getenv("IRONFLOW_READINESS_HELPERS_SEED")
	if seedPath == "" {
		t.Skip("IRONFLOW_READINESS_HELPERS_SEED not set; run `make test-readiness-helpers-live`")
	}
	raw, err := os.ReadFile(seedPath) // #nosec G304 -- path comes from the make target
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	var seed helpersSeed
	if err := json.Unmarshal(raw, &seed); err != nil {
		t.Fatalf("decode seed: %v", err)
	}
	serverURL := os.Getenv("IRONFLOW_TEST_SERVER")
	client := NewClient(ClientConfig{ServerURL: serverURL, APIKey: os.Getenv("IRONFLOW_TEST_API_KEY")})
	platform := NewClient(ClientConfig{ServerURL: serverURL, APIKey: seed.PlatformKey})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	t.Run("deployment-register and deployment-read", func(t *testing.T) {
		input := RegisterDeploymentInput{
			FunctionID:     seed.DebounceFunctionID,
			DeploymentID:   "gosdk-v1",
			ArtifactDigest: "sha256:" + strings.Repeat("1", 64),
			Snapshot:       json.RawMessage(`{"triggers":[{"event":"orders.placed"}],"timeoutMs":30000,"metadata":{"who":"José"}}`),
			Mode:           PullMode,
		}
		first, err := client.Deployments().Register(ctx, input)
		if err != nil {
			t.Fatalf("Register: %v", err)
		}
		if !first.Created || first.Deployment.DeploymentID != "gosdk-v1" || first.Deployment.Mode != PullMode || first.Deployment.CreatedAt.IsZero() {
			t.Fatalf("Register = %+v, want a created pull deployment with a creation time", first)
		}
		var snap struct {
			TimeoutMs int               `json:"timeoutMs"`
			Metadata  map[string]string `json:"metadata"`
		}
		if err := json.Unmarshal(first.Deployment.Snapshot, &snap); err != nil || snap.TimeoutMs != 30000 || snap.Metadata["who"] != "José" {
			t.Fatalf("stored snapshot = %s (%v), want timeoutMs 30000 and who José", first.Deployment.Snapshot, err)
		}
		again, err := client.Deployments().Register(ctx, input)
		if err != nil || again.Created {
			t.Fatalf("re-Register = %+v, %v; want created=false", again, err)
		}

		got, err := client.Deployments().Get(ctx, seed.DebounceFunctionID, "gosdk-v1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.ArtifactDigest != input.ArtifactDigest || got.EnvironmentID == "" || got.CreatedBy == "" || !got.CreatedAt.Equal(first.Deployment.CreatedAt) {
			t.Fatalf("Get = %+v, want the registered deployment", got)
		}
		list, err := client.Deployments().List(ctx, seed.DebounceFunctionID, ListDeploymentsOptions{})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		found := false
		for _, d := range list {
			found = found || d.DeploymentID == "gosdk-v1"
		}
		if !found {
			t.Fatalf("List = %+v, want gosdk-v1", list)
		}
		routing, err := client.Deployments().GetExecutionRouting(ctx, seed.DebounceFunctionID)
		if err != nil {
			t.Fatalf("GetExecutionRouting: %v", err)
		}
		// Registering never activates: the function stays on legacy routing.
		if routing.FunctionID != seed.DebounceFunctionID || routing.Mode != FunctionRoutingLegacy || routing.ActiveDeploymentID != "" || routing.UpdatedAt.IsZero() {
			t.Fatalf("GetExecutionRouting = %+v, want legacy with no active deployment", routing)
		}
	})

	t.Run("capacity-views", func(t *testing.T) {
		fn := CapacityListOptions{Function: seed.CapacityFunctionID}
		lanes, err := platform.ListCapacityLanes(ctx, fn)
		if err != nil {
			t.Fatalf("ListCapacityLanes: %v", err)
		}
		if len(lanes) != 1 || lanes[0].Limit != 1 || lanes[0].ActiveReserved != 1 || lanes[0].LaneID == "" {
			t.Fatalf("lanes = %+v, want one lane with limit 1 and one reservation", lanes)
		}
		lane := lanes[0].LaneID

		queue, err := platform.ListCapacityQueue(ctx, fn)
		if err != nil {
			t.Fatalf("ListCapacityQueue: %v", err)
		}
		if len(queue) != 1 || queue[0].RunID != seed.QueuedRunID || queue[0].LaneID == nil || *queue[0].LaneID != lane || queue[0].ReservationOwner != nil {
			t.Fatalf("queue = %+v, want the queued run waiting on lane %s", queue, lane)
		}

		leases, err := platform.ListCapacityLeases(ctx, CapacityListOptions{})
		if err != nil {
			t.Fatalf("ListCapacityLeases: %v", err)
		}
		var lease *CapacityLease
		for i := range leases {
			if leases[i].RunID == seed.LeasedRunID {
				lease = &leases[i]
			}
		}
		if lease == nil || lease.LaneID == nil || *lease.LaneID != lane || !lease.ExpiresAt.After(lease.ActivatedAt) {
			t.Fatalf("leases = %+v, want the leased run on lane %s", leases, lane)
		}

		buckets, err := platform.ListCapacityBuckets(ctx, fn)
		if err != nil {
			t.Fatalf("ListCapacityBuckets: %v", err)
		}
		if len(buckets) != 1 || buckets[0].LaneID == nil || *buckets[0].LaneID != lane {
			t.Fatalf("buckets = %+v, want one bucket on lane %s", buckets, lane)
		}

		stats, err := platform.CapacityStats(ctx)
		if err != nil {
			t.Fatalf("CapacityStats: %v", err)
		}
		var stat *CapacityStat
		for i := range stats {
			if stats[i].FunctionID == seed.CapacityFunctionID {
				stat = &stats[i]
			}
		}
		if stat == nil || stat.ActiveLeases != 1 || stat.Queued != 1 || stat.LaneLimit != 1 {
			t.Fatalf("stats = %+v, want 1 lease, 1 queued, limit 1", stats)
		}

		sessions, err := platform.ListCapacitySessions(ctx, CapacityListOptions{})
		if err != nil {
			t.Fatalf("ListCapacitySessions: %v", err)
		}
		credits, err := platform.ListCapacityCredits(ctx, CapacityListOptions{})
		if err != nil {
			t.Fatalf("ListCapacityCredits: %v", err)
		}
		if len(sessions) == 0 {
			t.Fatal("no worker session; the seed's streaming worker should hold one")
		}
		s := sessions[0]
		var ordinals []int
		for _, c := range credits {
			if c.SessionID == s.SessionID {
				if c.WorkerID != s.WorkerID {
					t.Fatalf("credit %+v names worker %s, session has %s", c, c.WorkerID, s.WorkerID)
				}
				ordinals = append(ordinals, c.SlotOrdinal)
			}
		}
		if len(ordinals) == 0 || s.Draining || s.HeartbeatExpiry.IsZero() {
			t.Fatalf("session %+v with credit ordinals %v, want a live session holding credits", s, ordinals)
		}
	})

	t.Run("debounce-manage", func(t *testing.T) {
		key := "gosdk key/é"
		if _, err := client.Emit(ctx, seed.DebounceFunctionID, map[string]any{"k": key}); err != nil {
			t.Fatalf("Emit: %v", err)
		}
		entry := findDebounce(ctx, t, client, seed.DebounceFunctionID, key)
		if entry == nil {
			t.Fatalf("ListDebounceEntries has no entry for %q", key)
		}
		if entry.PeriodMs != 600000 || entry.EventID == "" || !entry.FiresAt.After(entry.ArmedAt) {
			t.Fatalf("entry = %+v, want a 10-minute timer", entry)
		}
		if err := client.CancelDebounce(ctx, entry.EnvironmentID, seed.DebounceFunctionID, key); err != nil {
			t.Fatalf("CancelDebounce: %v", err)
		}
		if again := findDebounce(ctx, t, client, seed.DebounceFunctionID, key); again != nil {
			t.Fatalf("entry still armed after cancel: %+v", again)
		}
	})

	t.Run("sql-execute", func(t *testing.T) {
		res, err := client.Projections().ExecuteSQL(ctx, "SELECT 1 AS n, 'José' AS who")
		if err != nil {
			t.Fatalf("ExecuteSQL: %v", err)
		}
		want := []map[string]any{{"n": "1", "who": "José"}}
		if !reflect.DeepEqual(res.Columns, []string{"n", "who"}) || !reflect.DeepEqual(res.Rows, want) || res.Count != 1 {
			t.Fatalf("ExecuteSQL = %+v, want %v", res, want)
		}
	})

	t.Run("proj-history and proj-revision", func(t *testing.T) {
		first, err := client.Projections().ListHistory(ctx, seed.Projection, DefinitionHistoryOptions{Limit: 1})
		if err != nil {
			t.Fatalf("ListHistory: %v", err)
		}
		if len(first.Entries) != 1 || !first.HasMore || first.NextBeforeRevision != first.Entries[0].Revision {
			t.Fatalf("page 1 = %+v, want one entry and a cursor", first)
		}
		newest := first.Entries[0]
		if !reflect.DeepEqual(newest.PreviousDefinition.Events, seed.ProjectionEvents[1]) || newest.ChangeType != "updated" || newest.ChangeReason != "change 2" || newest.RecordedAt.IsZero() {
			t.Fatalf("newest = %+v, want the second definition archived by change 2", newest)
		}
		second, err := client.Projections().ListHistory(ctx, seed.Projection, DefinitionHistoryOptions{Limit: 1, BeforeRevision: first.NextBeforeRevision})
		if err != nil {
			t.Fatalf("ListHistory page 2: %v", err)
		}
		if len(second.Entries) != 1 || second.HasMore || second.NextBeforeRevision != 0 || !reflect.DeepEqual(second.Entries[0].PreviousDefinition.Events, seed.ProjectionEvents[0]) {
			t.Fatalf("page 2 = %+v, want the first definition and no more", second)
		}
		got, err := client.Projections().GetRevision(ctx, seed.Projection, second.Entries[0].Revision)
		if err != nil {
			t.Fatalf("GetRevision: %v", err)
		}
		if !reflect.DeepEqual(*got, second.Entries[0]) {
			t.Fatalf("GetRevision = %+v, want %+v", got, second.Entries[0])
		}
	})

	t.Run("schema-history and schema-revision", func(t *testing.T) {
		page, err := client.Schemas().ListHistory(ctx, seed.SchemaEvent, seed.SchemaVersion, DefinitionHistoryOptions{})
		if err != nil {
			t.Fatalf("ListHistory: %v", err)
		}
		if len(page.Entries) != 2 || page.HasMore {
			t.Fatalf("history = %+v, want two archived documents", page)
		}
		if page.Entries[0].PreviousDefinition.SchemaJSON != seed.SchemaDocs[1] || page.Entries[1].PreviousDefinition.SchemaJSON != seed.SchemaDocs[0] {
			t.Fatalf("history documents = %q, %q; want %q, %q", page.Entries[0].PreviousDefinition.SchemaJSON, page.Entries[1].PreviousDefinition.SchemaJSON, seed.SchemaDocs[1], seed.SchemaDocs[0])
		}
		oldest := page.Entries[1]
		got, err := client.Schemas().GetRevision(ctx, seed.SchemaEvent, seed.SchemaVersion, oldest.Revision)
		if err != nil {
			t.Fatalf("GetRevision: %v", err)
		}
		if !reflect.DeepEqual(*got, oldest) || got.PreviousDefinition.Version != seed.SchemaVersion || got.ChangeType != "updated" {
			t.Fatalf("GetRevision = %+v, want %+v", got, oldest)
		}
	})
}

func findDebounce(ctx context.Context, t *testing.T, client *Client, functionID, key string) *DebounceEntry {
	t.Helper()
	entries, err := client.ListDebounceEntries(ctx)
	if err != nil {
		t.Fatalf("ListDebounceEntries: %v", err)
	}
	for i := range entries {
		if entries[i].FunctionID == functionID && entries[i].DebounceKey == key {
			return &entries[i]
		}
	}
	return nil
}
