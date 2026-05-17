package ironflow

import (
	"strings"
	"testing"
	"time"
)

func TestCreateFunction(t *testing.T) {
	t.Run("creates function with valid config", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "test-function",
			Triggers: []Trigger{{Event: "test.event"}},
		}, func(ctx Context) (any, error) {
			return map[string]bool{"success": true}, nil
		})

		if fn.Config.ID != "test-function" {
			t.Errorf("expected ID 'test-function', got '%s'", fn.Config.ID)
		}
		if len(fn.Config.Triggers) != 1 {
			t.Errorf("expected 1 trigger, got %d", len(fn.Config.Triggers))
		}
		if fn.Config.Triggers[0].Event != "test.event" {
			t.Errorf("expected trigger event 'test.event', got '%s'", fn.Config.Triggers[0].Event)
		}
	})

	t.Run("panics for empty id", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for empty id")
			}
		}()

		CreateFunction(FunctionConfig{
			ID:       "",
			Triggers: []Trigger{{Event: "test.event"}},
		}, func(ctx Context) (any, error) {
			return nil, nil
		})
	})

	t.Run("panics for invalid id format", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for invalid id format")
			}
		}()

		CreateFunction(FunctionConfig{
			ID:       "invalid id with spaces",
			Triggers: []Trigger{{Event: "test.event"}},
		}, func(ctx Context) (any, error) {
			return nil, nil
		})
	})

	t.Run("accepts valid id formats", func(t *testing.T) {
		validIDs := []string{"my-function", "myFunction", "my_function", "function123", "123function"}

		for _, id := range validIDs {
			fn := CreateFunction(FunctionConfig{
				ID:       id,
				Triggers: []Trigger{{Event: "test.event"}},
			}, func(ctx Context) (any, error) {
				return nil, nil
			})

			if fn.Config.ID != id {
				t.Errorf("expected ID '%s', got '%s'", id, fn.Config.ID)
			}
		}
	})

	t.Run("allows empty triggers for invocable functions", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "test-invocable",
			Triggers: []Trigger{},
		}, func(ctx Context) (any, error) {
			return nil, nil
		})
		if fn.Config.ID != "test-invocable" {
			t.Errorf("expected ID 'test-invocable', got '%s'", fn.Config.ID)
		}
	})

	t.Run("panics for trigger without event", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for trigger without event")
			}
		}()

		CreateFunction(FunctionConfig{
			ID:       "test-function",
			Triggers: []Trigger{{Event: ""}},
		}, func(ctx Context) (any, error) {
			return nil, nil
		})
	})
}

func TestNormalizeFunctionConfig(t *testing.T) {
	t.Run("applies default name from id", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "test-function",
			Triggers: []Trigger{{Event: "test.event"}},
		}, func(ctx Context) (any, error) {
			return nil, nil
		})

		if fn.Config.Name != "test-function" {
			t.Errorf("expected Name 'test-function', got '%s'", fn.Config.Name)
		}
	})

	t.Run("uses provided name", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "test-function",
			Name:     "Test Function",
			Triggers: []Trigger{{Event: "test.event"}},
		}, func(ctx Context) (any, error) {
			return nil, nil
		})

		if fn.Config.Name != "Test Function" {
			t.Errorf("expected Name 'Test Function', got '%s'", fn.Config.Name)
		}
	})

	t.Run("applies default timeout", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "test-function",
			Triggers: []Trigger{{Event: "test.event"}},
		}, func(ctx Context) (any, error) {
			return nil, nil
		})

		if fn.Config.Timeout != 10*time.Minute {
			t.Errorf("expected Timeout 10m, got %v", fn.Config.Timeout)
		}
	})

	t.Run("applies default retry config", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "test-function",
			Triggers: []Trigger{{Event: "test.event"}},
		}, func(ctx Context) (any, error) {
			return nil, nil
		})

		if fn.Config.Retry == nil {
			t.Fatal("expected Retry to be set")
		}
		if fn.Config.Retry.MaxAttempts != 3 {
			t.Errorf("expected MaxAttempts 3, got %d", fn.Config.Retry.MaxAttempts)
		}
		if fn.Config.Retry.InitialDelay != time.Second {
			t.Errorf("expected InitialDelay 1s, got %v", fn.Config.Retry.InitialDelay)
		}
		if fn.Config.Retry.BackoffFactor != 2.0 {
			t.Errorf("expected BackoffFactor 2.0, got %f", fn.Config.Retry.BackoffFactor)
		}
		if fn.Config.Retry.MaxDelay != 5*time.Minute {
			t.Errorf("expected MaxDelay 5m, got %v", fn.Config.Retry.MaxDelay)
		}
	})

	t.Run("applies default mode", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "test-function",
			Triggers: []Trigger{{Event: "test.event"}},
		}, func(ctx Context) (any, error) {
			return nil, nil
		})

		if fn.Config.Mode != PushMode {
			t.Errorf("expected Mode 'push', got '%s'", fn.Config.Mode)
		}
	})
}

func TestGetFunctionMetadata(t *testing.T) {
	t.Run("returns function metadata", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "test-function",
			Triggers: []Trigger{{Event: "test.event"}},
		}, func(ctx Context) (any, error) {
			return nil, nil
		})

		metadata := GetFunctionMetadata(fn)

		if metadata["id"] != "test-function" {
			t.Errorf("expected id 'test-function', got '%v'", metadata["id"])
		}
		if metadata["name"] != "test-function" {
			t.Errorf("expected name 'test-function', got '%v'", metadata["name"])
		}
		if metadata["mode"] != "push" {
			t.Errorf("expected mode 'push', got '%v'", metadata["mode"])
		}

		triggers, ok := metadata["triggers"].([]map[string]any)
		if !ok || len(triggers) != 1 {
			t.Error("expected 1 trigger in metadata")
		}
	})

	t.Run("includes concurrency config", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "test-function",
			Triggers: []Trigger{{Event: "test.event"}},
			Concurrency: &ConcurrencyConfig{
				Limit: 5,
				Key:   "event.data.userId",
			},
		}, func(ctx Context) (any, error) {
			return nil, nil
		})

		metadata := GetFunctionMetadata(fn)

		concurrency, ok := metadata["concurrency"].(map[string]any)
		if !ok {
			t.Fatal("expected concurrency in metadata")
		}
		if concurrency["limit"] != 5 {
			t.Errorf("expected limit 5, got %v", concurrency["limit"])
		}
		if concurrency["key"] != "event.data.userId" {
			t.Errorf("expected key 'event.data.userId', got '%v'", concurrency["key"])
		}
	})

	t.Run("includes actor_key", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "test-function",
			Triggers: []Trigger{{Event: "test.event"}},
			ActorKey: "event.data.sessionId",
		}, func(ctx Context) (any, error) {
			return nil, nil
		})

		metadata := GetFunctionMetadata(fn)

		if metadata["actor_key"] != "event.data.sessionId" {
			t.Errorf("expected actor_key 'event.data.sessionId', got '%v'", metadata["actor_key"])
		}
	})
}

func TestGetFunctionMetadata_IncludesMetadata(t *testing.T) {
	fn := CreateFunction(FunctionConfig{
		ID:       "metadata-function",
		Triggers: []Trigger{{Event: "test.event"}},
		Metadata: map[string]any{
			"service": "billing",
			"team":    "payments",
		},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	meta := GetFunctionMetadata(fn)

	md, ok := meta["metadata"].(map[string]any)
	if !ok {
		t.Fatal("expected metadata key in function metadata")
	}
	if md["service"] != "billing" {
		t.Errorf("expected service=billing, got %v", md["service"])
	}
	if md["team"] != "payments" {
		t.Errorf("expected team=payments, got %v", md["team"])
	}
}

func TestGetFunctionMetadata_OmitsNilMetadata(t *testing.T) {
	fn := CreateFunction(FunctionConfig{
		ID:       "no-metadata-function",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	meta := GetFunctionMetadata(fn)

	if _, ok := meta["metadata"]; ok {
		t.Error("expected no metadata key when Metadata is nil")
	}
}

// TestGetFunctionMetadata_CompensateOnCancel verifies the P2 flag flows
// into the registration metadata map that the SDK sends to the server.
func TestGetFunctionMetadata_CompensateOnCancel(t *testing.T) {
	t.Run("flag set", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:                 "fn-comp-on",
			Triggers:           []Trigger{{Event: "t"}},
			Mode:               PullMode,
			CompensateOnCancel: true,
		}, func(_ Context) (any, error) { return nil, nil })

		meta := GetFunctionMetadata(fn)
		if meta["compensate_on_cancel"] != true {
			t.Errorf("compensate_on_cancel = %v, want true", meta["compensate_on_cancel"])
		}
	})

	t.Run("flag omitted — key absent", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "fn-comp-off",
			Triggers: []Trigger{{Event: "t"}},
			Mode:     PullMode,
		}, func(_ Context) (any, error) { return nil, nil })

		meta := GetFunctionMetadata(fn)
		if _, ok := meta["compensate_on_cancel"]; ok {
			t.Error("compensate_on_cancel key should be omitted when false (default)")
		}
	})
}

// TestGetFunctionMetadata_CancelOn verifies cancelOn specs flow into the
// registration metadata. Issue #546 P3 / #572.
func TestGetFunctionMetadata_CancelOn(t *testing.T) {
	t.Run("specs serialized", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "fn-cancel-on",
			Triggers: []Trigger{{Event: "order.placed"}},
			CancelOn: []CancelOnConfig{
				{Event: "order.cancelled", Match: "data.orderId"},
				{Event: "order.refunded", Match: "data.orderId"},
			},
		}, func(_ Context) (any, error) { return nil, nil })

		meta := GetFunctionMetadata(fn)
		raw, ok := meta["cancel_on"]
		if !ok {
			t.Fatal("cancel_on key missing")
		}
		specs, ok := raw.([]map[string]any)
		if !ok {
			t.Fatalf("cancel_on type %T, want []map[string]any", raw)
		}
		if len(specs) != 2 {
			t.Fatalf("specs len = %d, want 2", len(specs))
		}
		if specs[0]["event"] != "order.cancelled" || specs[0]["match"] != "data.orderId" {
			t.Errorf("specs[0] = %+v", specs[0])
		}
	})

	t.Run("empty omitted", func(t *testing.T) {
		fn := CreateFunction(FunctionConfig{
			ID:       "fn-no-cancel-on",
			Triggers: []Trigger{{Event: "t"}},
		}, func(_ Context) (any, error) { return nil, nil })
		meta := GetFunctionMetadata(fn)
		if _, ok := meta["cancel_on"]; ok {
			t.Error("cancel_on key should be omitted when CancelOn is empty")
		}
	})
}

// TestCreateFunction_CancelOnValidation asserts shape validation runs at
// CreateFunction time. Issue #546 P3 / #572.
func TestCreateFunction_CancelOnValidation(t *testing.T) {
	cases := []struct {
		name    string
		specs   []CancelOnConfig
		wantErr string
	}{
		{
			name:    "empty event",
			specs:   []CancelOnConfig{{Event: "", Match: "data.id"}},
			wantErr: "cancelOn[0].event must be non-empty",
		},
		{
			name:    "empty match",
			specs:   []CancelOnConfig{{Event: "x", Match: ""}},
			wantErr: "cancelOn[0].match must be non-empty",
		},
		{
			name: "duplicate spec",
			specs: []CancelOnConfig{
				{Event: "x", Match: "data.id"},
				{Event: "x", Match: "data.id"},
			},
			wantErr: "cancelOn[1]: duplicate spec",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected panic, got nil")
				}
				msg, ok := r.(string)
				if !ok {
					t.Fatalf("panic type %T, want string", r)
				}
				if !strings.Contains(msg, c.wantErr) {
					t.Errorf("panic = %q, want substring %q", msg, c.wantErr)
				}
			}()
			CreateFunction(FunctionConfig{
				ID:       "fn-validate",
				Triggers: []Trigger{{Event: "t"}},
				CancelOn: c.specs,
			}, func(_ Context) (any, error) { return nil, nil })
		})
	}
}
