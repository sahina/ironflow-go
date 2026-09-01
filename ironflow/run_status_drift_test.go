package ironflow

import (
	"strings"
	"testing"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

// runStatusFromWire and runStatusToWire are hand-written switches that mirror
// the RunStatus enum in api/proto/ironflow/v1/types.proto. Nothing about adding
// a value to that enum breaks the build here, and the strict decode means a new
// server-side status turns every GetRun/ListRuns/CancelRun/ResumeRun call into
// an error at runtime. This test is the drift gate: it walks the generated enum
// and fails the moment the proto gains a value the SDK cannot name (#1919).
//
// If this test fails, add the new status to BOTH switches in client.go and to
// the RunStatus constants in types.go — then add it here.
func TestRunStatusSwitchesCoverTheProtoEnum(t *testing.T) {
	// Statuses with no SDK mapping, and why. Everything else must map.
	exempt := map[string]string{
		// Never a real run's status: the zero value means "unset", and
		// EmitUnpopulated:false means it reaches the SDK as an absent key.
		"RUN_STATUS_UNSPECIFIED": "zero value, never emitted for a real run",
	}

	for value, name := range ironflowv1.RunStatus_name {
		if reason, ok := exempt[name]; ok {
			t.Logf("skipping %s (%d): %s", name, value, reason)
			continue
		}

		t.Run(name, func(t *testing.T) {
			status, err := runStatusFromWire(name)
			if err != nil {
				t.Fatalf("proto enum %s has no runStatusFromWire mapping: %v\n"+
					"Add it to runStatusFromWire, runStatusToWire, and the RunStatus constants.", name, err)
			}

			// Round-trip: the public value must encode back to the same name,
			// so the two switches cannot drift apart.
			wire, err := runStatusToWire(status)
			if err != nil {
				t.Fatalf("RunStatus %q decoded from %s has no runStatusToWire mapping: %v", status, name, err)
			}
			if wire != name {
				t.Errorf("round-trip mismatch: %s -> %q -> %s", name, status, wire)
			}
		})
	}
}

// The public RunStatus constants must not claim statuses the wire cannot carry.
// RunStatusPending is the deliberate exception: the proto reserves the name, so
// it is deprecated but still exported for source compatibility.
func TestRunStatusConstantsAreReachableOnTheWire(t *testing.T) {
	all := []RunStatus{
		RunStatusRunning, RunStatusCompleted, RunStatusFailed, RunStatusCancelled,
		RunStatusPaused, RunStatusWaitingForCapacity, RunStatusWaiting,
	}

	for _, status := range all {
		wire, err := runStatusToWire(status)
		if err != nil {
			t.Errorf("RunStatus %q has no wire encoding: %v", status, err)
			continue
		}
		if _, ok := ironflowv1.RunStatus_value[wire]; !ok {
			t.Errorf("RunStatus %q encodes to %s, which is not a value of the proto enum", status, wire)
		}
	}

	// Retired: reserved in the proto, so it must stay unmappable rather than
	// silently encode to a name the server discards.
	if _, err := runStatusToWire(RunStatusPending); err == nil {
		t.Error("RunStatusPending is reserved in the proto and must not encode")
	}
	if _, ok := ironflowv1.RunStatus_value["RUN_STATUS_PENDING"]; ok {
		t.Error("RUN_STATUS_PENDING is no longer reserved in the proto — revisit the deprecation")
	}
}

// Guards the prefix convention both switches rely on.
func TestRunStatusProtoNamesUseTheExpectedPrefix(t *testing.T) {
	for _, name := range ironflowv1.RunStatus_name {
		if !strings.HasPrefix(name, "RUN_STATUS_") {
			t.Errorf("enum value %q does not use the RUN_STATUS_ prefix", name)
		}
	}
}
