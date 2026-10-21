package ironflow

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// TestLiveReadinessStreams covers the entity-streams group (#3003). The group
// needs no seed: this SDK creates and deletes its own stream, so no other
// SDK's stream and not the shared seed entity can be reached.
func TestLiveReadinessStreams(t *testing.T) {
	client, ctx := liveReadiness(t, &struct{}{})
	id := fmt.Sprintf("readiness-streams-go-%d", time.Now().UnixNano())
	const entityType = "readiness"
	event := func(n int) AppendEventInput {
		return AppendEventInput{Name: fmt.Sprintf("readiness.step.%d", n), EntityType: entityType, Data: map[string]any{"n": n, "who": "José"}}
	}
	data := func(n int) map[string]any { return map[string]any{"n": float64(n), "who": "José"} }

	t.Run("stream-append", func(t *testing.T) {
		first, err := client.AppendStreamEvent(ctx, id, event(1), WithExpectedVersion(0))
		if err != nil || first.EntityVersion != 1 || first.EventID == "" {
			t.Fatalf("append with expected version 0 = %+v, %v, want version 1", first, err)
		}
		second, err := client.AppendStreamEvent(ctx, id, event(2), WithExpectedVersion(1))
		if err != nil || second.EntityVersion != 2 || second.EventID == "" || second.EventID == first.EventID {
			t.Fatalf("append with expected version 1 = %+v, %v, want version 2 and a new event", second, err)
		}
		// Another writer got to version 2 first: the server answers aborted and writes nothing.
		if _, err = client.AppendStreamEvent(ctx, id, event(3), WithExpectedVersion(1)); !errors.Is(err, ErrContended) {
			t.Fatalf("stale append = %v, want ErrContended", err)
		}
		if _, err = client.AppendStreamEvent(ctx, id, event(3), WithExpectedVersion(0)); !errors.Is(err, ErrContended) {
			t.Fatalf("create-only append to an existing stream = %v, want ErrContended", err)
		}
		// "$" is reserved for platform events such as the delete tombstone.
		if _, err = client.AppendStreamEvent(ctx, id, AppendEventInput{Name: "$mine", EntityType: entityType, Data: data(3)}); !hasErrorCode(err, "invalid_argument") {
			t.Fatalf("append of a $-prefixed event = %v, want invalid_argument", err)
		}
		if info, err := client.GetStreamInfo(ctx, id); err != nil || info.Version != 2 {
			t.Fatalf("GetStreamInfo after the refused appends = %+v, %v, want version 2", info, err)
		}
	})

	t.Run("stream-read", func(t *testing.T) {
		events, err := client.ReadStream(ctx, id)
		if err != nil || len(events) != 2 {
			t.Fatalf("ReadStream = %+v, %v, want 2 events", events, err)
		}
		for i, e := range events {
			n := i + 1
			if e.Name != fmt.Sprintf("readiness.step.%d", n) || e.EntityVersion != ProtoInt64(n) || !reflect.DeepEqual(e.Data, data(n)) || e.ID == "" || e.Timestamp == "" {
				t.Fatalf("event %d = %+v, want readiness.step.%d at version %d with %v", i, e, n, n, data(n))
			}
		}
		last, err := client.ReadStream(ctx, id, ReadStreamOpts{Direction: "backward", Limit: 1})
		if err != nil || len(last) != 1 || last[0].EntityVersion != 2 {
			t.Fatalf("backward limit 1 = %+v, %v, want only version 2", last, err)
		}
		from, err := client.ReadStream(ctx, id, ReadStreamOpts{FromVersion: 2})
		if err != nil || len(from) != 1 || from[0].EntityVersion != 2 {
			t.Fatalf("from version 2 = %+v, %v, want only version 2", from, err)
		}
	})

	t.Run("stream-info", func(t *testing.T) {
		info, err := client.GetStreamInfo(ctx, id)
		if err != nil {
			t.Fatalf("GetStreamInfo: %v", err)
		}
		if info.EntityID != id || info.EntityType != entityType || info.Version != 2 || info.EventCount != 2 || info.CreatedAt == "" || info.UpdatedAt == "" {
			t.Fatalf("GetStreamInfo = %+v, want %s/%s at version 2 with 2 events", info, id, entityType)
		}
		if _, err := client.GetStreamInfo(ctx, id+"-unknown"); !hasErrorCode(err, "not_found") {
			t.Fatalf("GetStreamInfo of an unknown stream = %v, want not_found", err)
		}
	})

	t.Run("stream-list", func(t *testing.T) {
		list, err := client.ListStreams(ctx)
		if err != nil {
			t.Fatalf("ListStreams: %v", err)
		}
		for _, s := range list {
			if s.EntityID != id {
				continue
			}
			if s.EntityType != entityType || s.Version != 2 || s.EventCount != 2 || s.LastEventAt == "" {
				t.Fatalf("listed stream = %+v, want %s at version 2 with 2 events", s, entityType)
			}
			return
		}
		t.Fatalf("ListStreams did not list %s among %d streams", id, len(list))
	})

	t.Run("stream-history", func(t *testing.T) {
		history, err := client.GetEntityHistory(ctx, id)
		if err != nil || len(history) != 2 {
			t.Fatalf("GetEntityHistory = %+v, %v, want 2 entries", history, err)
		}
		for i, e := range history {
			n := i + 1
			if e.EventName != fmt.Sprintf("readiness.step.%d", n) || e.EntityVersion != int64(n) || !reflect.DeepEqual(e.Data, data(n)) || e.Timestamp == "" {
				t.Fatalf("entry %d = %+v, want readiness.step.%d at version %d with %v", i, e, n, n, data(n))
			}
		}
		one, err := client.GetEntityHistory(ctx, id, EntityHistoryOpts{Limit: 1})
		if err != nil || len(one) != 1 || one[0].EntityVersion != 1 {
			t.Fatalf("history limit 1 = %+v, %v, want only version 1", one, err)
		}
		// From and To are inclusive, so each event's own time selects it.
		at := func(i int) time.Time {
			ts, err := time.Parse(time.RFC3339Nano, history[i].Timestamp)
			if err != nil {
				t.Fatalf("timestamp %q: %v", history[i].Timestamp, err)
			}
			return ts
		}
		if upTo, err := client.GetEntityHistory(ctx, id, EntityHistoryOpts{To: at(0)}); err != nil || len(upTo) != 1 || upTo[0].EntityVersion != 1 {
			t.Fatalf("history up to the first event = %+v, %v, want only version 1", upTo, err)
		}
		if since, err := client.GetEntityHistory(ctx, id, EntityHistoryOpts{From: at(1)}); err != nil || len(since) != 1 || since[0].EntityVersion != 2 {
			t.Fatalf("history from the second event = %+v, %v, want only version 2", since, err)
		}
	})

	t.Run("snapshot-create and snapshot-get", func(t *testing.T) {
		state := func(n int) map[string]any { return map[string]any{"count": float64(n), "who": "José"} }
		for n := 1; n <= 2; n++ {
			snap, err := client.CreateSnapshot(ctx, id, CreateSnapshotInput{EntityType: entityType, EntityVersion: int64(n), State: state(n)})
			if err != nil || snap.SnapshotID == "" {
				t.Fatalf("CreateSnapshot at version %d = %+v, %v", n, snap, err)
			}
		}
		got, err := client.GetSnapshot(ctx, id)
		if err != nil {
			t.Fatalf("GetSnapshot: %v", err)
		}
		if got.SnapshotID == "" || got.EntityID != id || got.EntityType != entityType || got.EntityVersion != 2 || !reflect.DeepEqual(got.State, state(2)) || got.CreatedAt == "" {
			t.Fatalf("GetSnapshot = %+v, want the version 2 snapshot of %s with %v", got, id, state(2))
		}
		// The latest snapshot at or before the version asked for.
		earlier, err := client.GetSnapshot(ctx, id, GetSnapshotOpts{BeforeVersion: 1})
		if err != nil || earlier.EntityVersion != 1 || !reflect.DeepEqual(earlier.State, state(1)) {
			t.Fatalf("GetSnapshot before version 1 = %+v, %v, want the version 1 snapshot with %v", earlier, err, state(1))
		}
		_, err = client.CreateSnapshot(ctx, id, CreateSnapshotInput{EntityType: entityType, EntityVersion: 3, State: state(3)})
		if !hasErrorCode(err, "invalid_argument") {
			t.Fatalf("snapshot past the stream version = %v, want invalid_argument", err)
		}
		_, err = client.CreateSnapshot(ctx, id, CreateSnapshotInput{EntityType: entityType, EntityVersion: 2, State: state(2)})
		if !hasErrorCode(err, "already_exists") {
			t.Fatalf("second snapshot at version 2 = %v, want already_exists", err)
		}
	})

	t.Run("stream-delete", func(t *testing.T) {
		version, err := client.DeleteStream(ctx, id, false)
		if err != nil || version != 3 {
			t.Fatalf("DeleteStream = %d, %v, want the tombstone at version 3", version, err)
		}
		if _, err = client.AppendStreamEvent(ctx, id, event(3)); !hasErrorCode(err, "failed_precondition") {
			t.Fatalf("append to a deleted stream = %v, want failed_precondition", err)
		}
		if _, err = client.CreateSnapshot(ctx, id, CreateSnapshotInput{EntityType: entityType, EntityVersion: 2, State: map[string]any{}}); !hasErrorCode(err, "failed_precondition") {
			t.Fatalf("snapshot of a deleted stream = %v, want failed_precondition", err)
		}
		if _, err = client.GetSnapshot(ctx, id); !hasErrorCode(err, "not_found") {
			t.Fatalf("GetSnapshot after delete = %v, want not_found (the delete drops snapshots)", err)
		}
		// Reads keep answering, and the tombstone is the last event.
		events, err := client.ReadStream(ctx, id)
		if err != nil || len(events) != 3 || events[2].Name != "$stream.deleted" || events[2].EntityVersion != 3 {
			t.Fatalf("read after delete = %+v, %v, want 2 events and the tombstone", events, err)
		}
		if info, err := client.GetStreamInfo(ctx, id); err != nil || info.Version != 3 {
			t.Fatalf("GetStreamInfo after delete = %+v, %v, want version 3", info, err)
		}
		list, err := client.ListStreams(ctx)
		if err != nil {
			t.Fatalf("ListStreams: %v", err)
		}
		for _, s := range list {
			if s.EntityID == id {
				t.Fatalf("ListStreams still lists the deleted stream: %+v", s)
			}
		}
		if _, err = client.DeleteStream(ctx, id, false); !hasErrorCode(err, "failed_precondition") {
			t.Fatalf("second DeleteStream = %v, want failed_precondition", err)
		}
		// Purge removes the events below the tombstone; the tombstone stays.
		version, err = client.DeleteStream(ctx, id, true)
		if err != nil || version != 3 {
			t.Fatalf("DeleteStream with purge = %d, %v, want version 3", version, err)
		}
		events, err = client.ReadStream(ctx, id)
		if err != nil || len(events) != 1 || events[0].Name != "$stream.deleted" {
			t.Fatalf("read after purge = %+v, %v, want only the tombstone", events, err)
		}
	})
}
