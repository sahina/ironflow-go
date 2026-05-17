package ironflow

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// Golden JSON contract tests for peelProjection.
//
// Wire fixtures are copied verbatim from a real Ironflow server response
// (`internal/server/server.go:2531` ProjectionResponse). If these tests
// break in CI, either the server contract drifted or peel logic regressed.
// See issue #610 / CHANGELOG 0.20.0.

const happyWire = `{
  "name": "doc-processor-memory",
  "environment_id": "env_default",
  "version": 1,
  "events": ["doc.uploaded", "doc.published"],
  "partition_key": "",
  "mode": "managed",
  "status": "active",
  "type": "sdk",
  "description": "",
  "last_event_seq": 9,
  "created_at": "2026-04-26T12:00:00Z",
  "updated_at": "2026-04-26T12:05:00Z",
  "state": {
    "projection_name": "doc-processor-memory",
    "environment_id": "env_default",
    "partition_key": "__global__",
    "state": { "doc-1": { "docId": "doc-1", "status": "published" } },
    "last_event_id": "evt-9",
    "last_event_seq": 9,
    "last_event_time": "2026-04-26T12:05:00Z",
    "version": 1,
    "updated_at": "2026-04-26T12:05:00Z"
  }
}`

func TestPeelProjection_HappyPath(t *testing.T) {
	result, err := peelProjection(json.RawMessage(happyWire), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Name != "doc-processor-memory" {
		t.Errorf("Name = %q", result.Name)
	}
	if result.Partition != "__global__" {
		t.Errorf("Partition = %q", result.Partition)
	}
	state, ok := result.State.(map[string]any)
	if !ok {
		t.Fatalf("State type = %T, want map[string]any", result.State)
	}
	doc, ok := state["doc-1"].(map[string]any)
	if !ok {
		t.Fatalf("state[doc-1] type = %T", state["doc-1"])
	}
	if doc["status"] != "published" {
		t.Errorf("doc.status = %v", doc["status"])
	}
	if result.LastEventID != "evt-9" {
		t.Errorf("LastEventID = %q", result.LastEventID)
	}
	if result.LastEventSeq != 9 {
		t.Errorf("LastEventSeq = %d", result.LastEventSeq)
	}
	if result.LastEventTime == nil {
		t.Fatal("LastEventTime is nil")
	}
	if result.Version != 1 {
		t.Errorf("Version = %d", result.Version)
	}
	if result.Mode != "managed" {
		t.Errorf("Mode = %q", result.Mode)
	}
	if result.UpdatedAt.IsZero() {
		t.Error("UpdatedAt is zero")
	}
}

func TestPeelProjection_OuterStateAbsent(t *testing.T) {
	wire := `{
		"name": "fresh",
		"environment_id": "env_default",
		"version": 1,
		"mode": "managed",
		"status": "active",
		"type": "sdk",
		"last_event_seq": 0,
		"created_at": "2026-04-26T12:00:00Z",
		"updated_at": "2026-04-26T12:00:00Z"
	}`
	result, err := peelProjection(json.RawMessage(wire), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m, ok := result.State.(map[string]any); !ok || len(m) != 0 {
		t.Errorf("State = %v, want empty map", result.State)
	}
	if result.Partition != "__global__" {
		t.Errorf("Partition = %q", result.Partition)
	}
	if result.LastEventTime != nil {
		t.Errorf("LastEventTime = %v, want nil", result.LastEventTime)
	}
	if result.LastEventSeq != 0 {
		t.Errorf("LastEventSeq = %d", result.LastEventSeq)
	}
	if result.Version != 1 {
		t.Errorf("Version = %d", result.Version)
	}
	if result.Mode != "managed" {
		t.Errorf("Mode = %q", result.Mode)
	}
}

func TestPeelProjection_OuterStateNull(t *testing.T) {
	wire := `{
		"name": "null-state",
		"version": 1,
		"mode": "managed",
		"last_event_seq": 0,
		"updated_at": "2026-04-26T12:00:00Z",
		"state": null
	}`
	result, err := peelProjection(json.RawMessage(wire), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m, ok := result.State.(map[string]any); !ok || len(m) != 0 {
		t.Errorf("State = %v", result.State)
	}
	if result.LastEventTime != nil {
		t.Errorf("LastEventTime = %v", result.LastEventTime)
	}
}

func TestPeelProjection_InnerStateMissingThrows(t *testing.T) {
	wire := `{
		"name": "drifted",
		"version": 1,
		"mode": "managed",
		"last_event_seq": 0,
		"updated_at": "2026-04-26T12:00:00Z",
		"state": {
			"projection_name": "drifted",
			"partition_key": "__global__",
			"last_event_id": "evt-1"
		}
	}`
	_, err := peelProjection(json.RawMessage(wire), "")
	if err == nil {
		t.Fatal("expected drift error, got nil")
	}
	if !strings.Contains(err.Error(), "envelope drift") {
		t.Errorf("error = %q, want envelope drift", err.Error())
	}
}

func TestPeelProjection_InnerStateNull(t *testing.T) {
	wire := `{
		"name": "inner-null",
		"version": 1,
		"mode": "managed",
		"last_event_seq": 0,
		"updated_at": "2026-04-26T12:00:00Z",
		"state": {
			"projection_name": "inner-null",
			"partition_key": "__global__",
			"state": null
		}
	}`
	result, err := peelProjection(json.RawMessage(wire), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m, ok := result.State.(map[string]any); !ok || len(m) != 0 {
		t.Errorf("State = %v, want empty map", result.State)
	}
}

func TestPeelProjection_RequestedPartitionEchoedWhenNoStateRow(t *testing.T) {
	wire := `{
		"name": "by-customer",
		"version": 1,
		"mode": "managed",
		"last_event_seq": 0,
		"updated_at": "2026-04-26T12:00:00Z"
	}`
	result, err := peelProjection(json.RawMessage(wire), "customer-99")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Partition != "customer-99" {
		t.Errorf("Partition = %q, want customer-99", result.Partition)
	}
}

func TestPeelProjection_MalformedTimestampThrows(t *testing.T) {
	wire := `{
		"name": "bad-time",
		"version": 1,
		"mode": "managed",
		"last_event_seq": 0,
		"updated_at": "2026-04-26T12:00:00Z",
		"state": {
			"projection_name": "bad-time",
			"partition_key": "__global__",
			"state": {},
			"last_event_time": "not-a-timestamp"
		}
	}`
	_, err := peelProjection(json.RawMessage(wire), "")
	if err == nil {
		t.Fatal("expected drift error, got nil")
	}
	if !strings.Contains(err.Error(), "envelope drift") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestPeelProjection_EmptyResponseThrows(t *testing.T) {
	_, err := peelProjection(json.RawMessage(""), "")
	if err == nil {
		t.Fatal("expected error on empty response")
	}
}

func TestPeelProjection_MissingNameThrows(t *testing.T) {
	wire := `{"version": 1, "mode": "managed"}`
	_, err := peelProjection(json.RawMessage(wire), "")
	if err == nil {
		t.Fatal("expected missing-name error")
	}
	if !strings.Contains(err.Error(), "missing name") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestPeelProjection_GoZeroTimeSentinelTreatedAsNil(t *testing.T) {
	wire := `{
		"name": "zero-time",
		"version": 1,
		"mode": "managed",
		"last_event_seq": 0,
		"updated_at": "2026-04-26T12:00:00Z",
		"state": {
			"projection_name": "zero-time",
			"partition_key": "__global__",
			"state": {},
			"last_event_time": "0001-01-01T00:00:00Z"
		}
	}`
	result, err := peelProjection(json.RawMessage(wire), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.LastEventTime != nil {
		t.Errorf("LastEventTime = %v, want nil (year-0001 sentinel)", result.LastEventTime)
	}
}

func TestPeelProjection_EmptyPartitionKeyFallsBackToRequested(t *testing.T) {
	wire := `{
		"name": "empty-pk",
		"version": 1,
		"mode": "managed",
		"last_event_seq": 0,
		"updated_at": "2026-04-26T12:00:00Z",
		"state": {
			"projection_name": "empty-pk",
			"partition_key": "",
			"state": {}
		}
	}`
	result, err := peelProjection(json.RawMessage(wire), "customer-42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Partition != "customer-42" {
		t.Errorf("Partition = %q, want customer-42 (empty partition_key fallback)", result.Partition)
	}
}

func TestPeelProjection_StatusAndErrorMessageFromRegistry(t *testing.T) {
	wire := `{
		"name": "errored",
		"version": 1,
		"mode": "managed",
		"status": "error",
		"error_message": "handler panicked",
		"last_event_seq": 0,
		"updated_at": "2026-04-26T12:00:00Z"
	}`
	result, err := peelProjection(json.RawMessage(wire), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "error" {
		t.Errorf("Status = %q, want error", result.Status)
	}
	if result.ErrorMessage != "handler panicked" {
		t.Errorf("ErrorMessage = %q", result.ErrorMessage)
	}
}

func TestPeelProjection_RegistryLastEventSeqWinsOverInner(t *testing.T) {
	wire := `{
		"name": "rebuild-in-progress",
		"version": 7,
		"mode": "managed",
		"last_event_seq": 100,
		"updated_at": "2026-04-26T12:00:00Z",
		"state": {
			"projection_name": "rebuild-in-progress",
			"partition_key": "__global__",
			"state": {},
			"version": 1,
			"last_event_seq": 0
		}
	}`
	result, err := peelProjection(json.RawMessage(wire), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Version != 7 {
		t.Errorf("Version = %d, want 7 (registry wins)", result.Version)
	}
	if result.LastEventSeq != 100 {
		t.Errorf("LastEventSeq = %d, want 100 (registry wins)", result.LastEventSeq)
	}
}

func TestPeelProjection_DriftErrorIsIronflowError(t *testing.T) {
	wire := `{
		"name": "drifted",
		"version": 1,
		"mode": "managed",
		"last_event_seq": 0,
		"updated_at": "2026-04-26T12:00:00Z",
		"state": {
			"projection_name": "drifted",
			"partition_key": "__global__"
		}
	}`
	_, err := peelProjection(json.RawMessage(wire), "")
	if err == nil {
		t.Fatal("expected drift error, got nil")
	}
	var ife *IronflowError
	if !errors.As(err, &ife) {
		t.Fatalf("err is not *IronflowError: %T", err)
	}
	if ife.Code != "PROJECTION_ENVELOPE_DRIFT" {
		t.Errorf("Code = %q, want PROJECTION_ENVELOPE_DRIFT", ife.Code)
	}
}

func TestPeelProjection_UnknownModeDefaultsManaged(t *testing.T) {
	wire := `{
		"name": "weird-mode",
		"version": 1,
		"mode": "future-unknown-mode",
		"last_event_seq": 0,
		"updated_at": "2026-04-26T12:00:00Z"
	}`
	result, err := peelProjection(json.RawMessage(wire), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Mode != "managed" {
		t.Errorf("Mode = %q, want managed (default)", result.Mode)
	}
}
