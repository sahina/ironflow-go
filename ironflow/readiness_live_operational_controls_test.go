package ironflow

import (
	"slices"
	"testing"
)

// TestLiveReadinessOperationalControls covers the operational-controls group
// (#3003) for Go: only the environment-wide audit query. The circuit-breaker,
// flow-edge and outbox dead-letter calls exist in Python alone (ADR 0092).
func TestLiveReadinessOperationalControls(t *testing.T) {
	var seed struct {
		FunctionID string `json:"functionId"`
		RunID      string `json:"runId"`
		BeforeRun  string `json:"beforeRun"`
	}
	client, ctx := liveReadiness(t, &seed)

	t.Run("audit-env-wide", func(t *testing.T) {
		// Newest first, the reverse of the per-run trail.
		want := []string{"run.status_changed", "step.completed", "step.started", "run.created"}
		types := func(events []AuditEvent) []string {
			var out []string
			for _, e := range events {
				out = append(out, e.EventType)
			}
			return out
		}

		byRun, err := client.ListAuditEvents(ctx, ListAuditEventsOpts{RunID: seed.RunID})
		if err != nil {
			t.Fatalf("ListAuditEvents: %v", err)
		}
		if got := types(byRun.Events); !slices.Equal(got, want) || byRun.TotalCount != len(want) {
			t.Fatalf("by run = %v (total %d), want %v", got, byRun.TotalCount, want)
		}
		for _, e := range byRun.Events {
			if e.ID == "" || e.RunID != seed.RunID || e.FunctionID != seed.FunctionID || e.CreatedAt == "" {
				t.Fatalf("event %+v does not belong to run %s / function %s", e, seed.RunID, seed.FunctionID)
			}
		}

		only, err := client.ListAuditEvents(ctx, ListAuditEventsOpts{FunctionID: seed.FunctionID, EventType: "step.completed"})
		if err != nil || len(only.Events) != 1 || only.Events[0].EventType != "step.completed" || only.Events[0].RunID != seed.RunID {
			t.Fatalf("by function and type = %+v, %v, want the one step.completed of %s", only, err, seed.RunID)
		}

		var paged []string
		cursor := ""
		for page := 0; page < 2; page++ {
			p, err := client.ListAuditEvents(ctx, ListAuditEventsOpts{RunID: seed.RunID, Limit: 2, Cursor: cursor})
			if err != nil || len(p.Events) != 2 || p.TotalCount != len(want) {
				t.Fatalf("page %d = %+v, %v, want 2 events of %d", page, p, err, len(want))
			}
			paged = append(paged, types(p.Events)...)
			cursor = p.NextCursor
		}
		if !slices.Equal(paged, want) {
			t.Fatalf("paged = %v, want %v", paged, want)
		}

		// The seed took BeforeRun before it triggered the run, so the window decides.
		after, err := client.ListAuditEvents(ctx, ListAuditEventsOpts{RunID: seed.RunID, FromTimestamp: seed.BeforeRun})
		if err != nil || after.TotalCount != len(want) {
			t.Fatalf("from BeforeRun = %+v, %v, want %d events", after, err, len(want))
		}
		before, err := client.ListAuditEvents(ctx, ListAuditEventsOpts{RunID: seed.RunID, ToTimestamp: seed.BeforeRun})
		if err != nil || len(before.Events) != 0 {
			t.Fatalf("to BeforeRun = %+v, %v, want no events", before, err)
		}
	})
}
