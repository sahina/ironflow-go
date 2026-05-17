package ironflow

import (
	"encoding/json"
	"strings"
	"testing"
)

// generateLargeString creates a string of approximately the specified size in bytes.
func generateLargeString(sizeBytes int) string {
	// Use a repeating pattern to make verification easier
	pattern := "abcdefghij"
	repeats := sizeBytes / len(pattern)
	return strings.Repeat(pattern, repeats)
}

// LargePayload is a test struct for large payload testing.
type LargePayload struct {
	ID       string            `json:"id"`
	Data     string            `json:"data"`
	Items    []string          `json:"items"`
	Metadata map[string]string `json:"metadata"`
}

func TestLargePayloadSerialization(t *testing.T) {
	t.Run("serializes 1KB payload without corruption", func(t *testing.T) {
		payload := LargePayload{
			ID:   "test-1kb",
			Data: generateLargeString(1024),
		}

		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("Failed to marshal 1KB payload: %v", err)
		}

		var result LargePayload
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("Failed to unmarshal 1KB payload: %v", err)
		}

		if result.ID != payload.ID {
			t.Errorf("ID mismatch: expected %s, got %s", payload.ID, result.ID)
		}
		if result.Data != payload.Data {
			t.Errorf("Data corrupted: expected length %d, got length %d", len(payload.Data), len(result.Data))
		}
	})

	t.Run("serializes 100KB payload without corruption", func(t *testing.T) {
		payload := LargePayload{
			ID:   "test-100kb",
			Data: generateLargeString(100 * 1024),
		}

		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("Failed to marshal 100KB payload: %v", err)
		}

		var result LargePayload
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("Failed to unmarshal 100KB payload: %v", err)
		}

		if result.Data != payload.Data {
			t.Errorf("Data corrupted: expected length %d, got length %d", len(payload.Data), len(result.Data))
		}
	})

	t.Run("serializes 1MB payload without corruption", func(t *testing.T) {
		payload := LargePayload{
			ID:   "test-1mb",
			Data: generateLargeString(1024 * 1024),
		}

		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("Failed to marshal 1MB payload: %v", err)
		}

		var result LargePayload
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("Failed to unmarshal 1MB payload: %v", err)
		}

		if result.Data != payload.Data {
			t.Errorf("Data corrupted: expected length %d, got length %d", len(payload.Data), len(result.Data))
		}
	})

	t.Run("serializes payload with large array", func(t *testing.T) {
		items := make([]string, 10000)
		for i := range items {
			items[i] = generateLargeString(100)
		}

		payload := LargePayload{
			ID:    "test-large-array",
			Items: items,
		}

		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("Failed to marshal payload with large array: %v", err)
		}

		var result LargePayload
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("Failed to unmarshal payload with large array: %v", err)
		}

		if len(result.Items) != len(payload.Items) {
			t.Errorf("Array length mismatch: expected %d, got %d", len(payload.Items), len(result.Items))
		}

		// Verify first and last items
		if result.Items[0] != payload.Items[0] {
			t.Error("First item corrupted")
		}
		if result.Items[len(items)-1] != payload.Items[len(items)-1] {
			t.Error("Last item corrupted")
		}
	})

	t.Run("serializes payload with large map", func(t *testing.T) {
		metadata := make(map[string]string, 1000)
		for i := 0; i < 1000; i++ {
			key := generateLargeString(50)
			metadata[key] = generateLargeString(100)
		}

		payload := LargePayload{
			ID:       "test-large-map",
			Metadata: metadata,
		}

		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("Failed to marshal payload with large map: %v", err)
		}

		var result LargePayload
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("Failed to unmarshal payload with large map: %v", err)
		}

		if len(result.Metadata) != len(payload.Metadata) {
			t.Errorf("Map size mismatch: expected %d, got %d", len(payload.Metadata), len(result.Metadata))
		}
	})
}

func TestStepWithLargePayload(t *testing.T) {
	t.Run("executes step with large input", func(t *testing.T) {
		largeData := generateLargeString(100 * 1024) // 100KB

		ctx := Context{
			exec: &executionContext{
				runID:          "run_large_input",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		result, err := Run(ctx, "process-large-input", func() (string, error) {
			// Process large input and return a derived result
			return largeData[:100], nil // Return first 100 chars
		})

		if err != nil {
			t.Fatalf("Step execution with large input failed: %v", err)
		}

		if len(result) != 100 {
			t.Errorf("Expected result length 100, got %d", len(result))
		}

		if len(ctx.exec.executedSteps) != 1 {
			t.Fatalf("Expected 1 executed step, got %d", len(ctx.exec.executedSteps))
		}
	})

	t.Run("executes step with large output", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_large_output",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		largeOutput := generateLargeString(100 * 1024) // 100KB

		result, err := Run(ctx, "produce-large-output", func() (string, error) {
			return largeOutput, nil
		})

		if err != nil {
			t.Fatalf("Step execution with large output failed: %v", err)
		}

		if result != largeOutput {
			t.Errorf("Output corrupted: expected length %d, got %d", len(largeOutput), len(result))
		}

		// Verify the step result stores the large output
		if len(ctx.exec.executedSteps) != 1 {
			t.Fatalf("Expected 1 executed step, got %d", len(ctx.exec.executedSteps))
		}

		stepOutput, ok := ctx.exec.executedSteps[0].Output.(string)
		if !ok {
			t.Fatalf("Expected string output, got %T", ctx.exec.executedSteps[0].Output)
		}
		if stepOutput != largeOutput {
			t.Errorf("Stored output corrupted: expected length %d, got %d", len(largeOutput), len(stepOutput))
		}
	})

	t.Run("executes step with large struct output", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_large_struct",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		largePayload := LargePayload{
			ID:   "large-struct-test",
			Data: generateLargeString(100 * 1024),
			Items: []string{
				generateLargeString(1024),
				generateLargeString(1024),
				generateLargeString(1024),
			},
		}

		result, err := Run(ctx, "produce-large-struct", func() (LargePayload, error) {
			return largePayload, nil
		})

		if err != nil {
			t.Fatalf("Step execution with large struct failed: %v", err)
		}

		if result.ID != largePayload.ID {
			t.Errorf("ID mismatch: expected %s, got %s", largePayload.ID, result.ID)
		}
		if result.Data != largePayload.Data {
			t.Errorf("Data corrupted: expected length %d, got %d", len(largePayload.Data), len(result.Data))
		}
		if len(result.Items) != len(largePayload.Items) {
			t.Errorf("Items length mismatch: expected %d, got %d", len(largePayload.Items), len(result.Items))
		}
	})
}

func TestMemoizationWithLargePayload(t *testing.T) {
	t.Run("returns memoized large string result", func(t *testing.T) {
		largeResult := generateLargeString(100 * 1024) // 100KB

		ctx := Context{
			exec: &executionContext{
				runID:        "run_memo_large",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_memo_large:large-step:0": {
						ID:     "run_memo_large:large-step:0",
						Name:   "large-step",
						Status: "completed",
						Output: largeResult,
					},
				},
				executedSteps: make([]*StepResult, 0),
			},
		}

		callCount := 0
		result, err := Run(ctx, "large-step", func() (string, error) {
			callCount++
			return "should-not-be-called", nil
		})

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if callCount != 0 {
			t.Errorf("Function should not have been called, but was called %d times", callCount)
		}

		if result != largeResult {
			t.Errorf("Memoized result corrupted: expected length %d, got %d", len(largeResult), len(result))
		}
	})

	t.Run("returns memoized large struct result", func(t *testing.T) {
		largePayload := LargePayload{
			ID:   "memoized-large",
			Data: generateLargeString(50 * 1024),
		}

		ctx := Context{
			exec: &executionContext{
				runID:        "run_memo_struct",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_memo_struct:struct-step:0": {
						ID:     "run_memo_struct:struct-step:0",
						Name:   "struct-step",
						Status: "completed",
						Output: largePayload,
					},
				},
				executedSteps: make([]*StepResult, 0),
			},
		}

		callCount := 0
		result, err := Run(ctx, "struct-step", func() (LargePayload, error) {
			callCount++
			return LargePayload{}, nil
		})

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if callCount != 0 {
			t.Errorf("Function should not have been called, but was called %d times", callCount)
		}

		if result.ID != largePayload.ID {
			t.Errorf("Memoized ID mismatch: expected %s, got %s", largePayload.ID, result.ID)
		}
		if result.Data != largePayload.Data {
			t.Errorf("Memoized data corrupted: expected length %d, got %d", len(largePayload.Data), len(result.Data))
		}
	})

	t.Run("handles memoized result with nested large data", func(t *testing.T) {
		type NestedPayload struct {
			Outer LargePayload `json:"outer"`
			Inner LargePayload `json:"inner"`
		}

		nested := NestedPayload{
			Outer: LargePayload{
				ID:   "outer",
				Data: generateLargeString(25 * 1024),
			},
			Inner: LargePayload{
				ID:   "inner",
				Data: generateLargeString(25 * 1024),
			},
		}

		ctx := Context{
			exec: &executionContext{
				runID:        "run_memo_nested",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_memo_nested:nested-step:0": {
						ID:     "run_memo_nested:nested-step:0",
						Name:   "nested-step",
						Status: "completed",
						Output: nested,
					},
				},
				executedSteps: make([]*StepResult, 0),
			},
		}

		result, err := Run(ctx, "nested-step", func() (NestedPayload, error) {
			return NestedPayload{}, nil
		})

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if result.Outer.ID != nested.Outer.ID {
			t.Errorf("Outer ID mismatch: expected %s, got %s", nested.Outer.ID, result.Outer.ID)
		}
		if result.Inner.Data != nested.Inner.Data {
			t.Errorf("Inner data corrupted: expected length %d, got %d", len(nested.Inner.Data), len(result.Inner.Data))
		}
	})
}

func TestEventDataWithLargePayload(t *testing.T) {
	t.Run("unmarshals large event data", func(t *testing.T) {
		largeData := LargePayload{
			ID:   "event-data",
			Data: generateLargeString(100 * 1024),
		}

		rawData, err := json.Marshal(largeData)
		if err != nil {
			t.Fatalf("Failed to marshal large event data: %v", err)
		}

		event := Event{
			ID:      "evt_large",
			Name:    "large.event",
			RawData: rawData,
		}

		var result LargePayload
		if err := event.Data(&result); err != nil {
			t.Fatalf("Failed to unmarshal large event data: %v", err)
		}

		if result.ID != largeData.ID {
			t.Errorf("ID mismatch: expected %s, got %s", largeData.ID, result.ID)
		}
		if result.Data != largeData.Data {
			t.Errorf("Data corrupted: expected length %d, got %d", len(largeData.Data), len(result.Data))
		}
	})
}

func TestPushRequestWithLargePayload(t *testing.T) {
	t.Run("creates execution context with large completed step outputs", func(t *testing.T) {
		largeOutput := generateLargeString(100 * 1024)

		req := &PushRequest{
			RunID:      "run_push_large",
			FunctionID: "test-function",
			Attempt:    1,
			Event: PushEvent{
				ID:        "evt_123",
				Name:      "test.event",
				Data:      json.RawMessage(`{}`),
				Timestamp: "2024-01-01T00:00:00Z",
			},
			Steps: []CompletedStep{
				{
					ID:     "run_push_large:step1:0",
					Name:   "step1",
					Status: "completed",
					Output: largeOutput,
				},
			},
		}

		ctx := newExecutionContext(req)

		step, ok := ctx.completedSteps["run_push_large:step1:0"]
		if !ok {
			t.Fatal("Expected to find completed step")
		}

		output, ok := step.Output.(string)
		if !ok {
			t.Fatalf("Expected string output, got %T", step.Output)
		}

		if output != largeOutput {
			t.Errorf("Large output corrupted: expected length %d, got %d", len(largeOutput), len(output))
		}
	})

	t.Run("creates execution context with large event data", func(t *testing.T) {
		largeEventData := LargePayload{
			ID:   "large-event",
			Data: generateLargeString(100 * 1024),
		}

		rawData, err := json.Marshal(largeEventData)
		if err != nil {
			t.Fatalf("Failed to marshal large event data: %v", err)
		}

		req := &PushRequest{
			RunID:      "run_push_event",
			FunctionID: "test-function",
			Attempt:    1,
			Event: PushEvent{
				ID:        "evt_large",
				Name:      "large.event",
				Data:      rawData,
				Timestamp: "2024-01-01T00:00:00Z",
			},
			Steps: []CompletedStep{},
		}

		ctx := newExecutionContext(req)

		if ctx.runID != req.RunID {
			t.Errorf("RunID mismatch: expected %s, got %s", req.RunID, ctx.runID)
		}

		// Verify event data can be accessed
		var result LargePayload
		if err := json.Unmarshal(req.Event.Data, &result); err != nil {
			t.Fatalf("Failed to unmarshal event data: %v", err)
		}

		if result.Data != largeEventData.Data {
			t.Errorf("Event data corrupted: expected length %d, got %d", len(largeEventData.Data), len(result.Data))
		}
	})
}

func BenchmarkLargePayloadSerialization(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
		{"1MB", 1024 * 1024},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			payload := LargePayload{
				ID:   "benchmark",
				Data: generateLargeString(s.size),
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := json.Marshal(payload)
				if err != nil {
					b.Fatal(err)
				}

				var result LargePayload
				if err := json.Unmarshal(data, &result); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
