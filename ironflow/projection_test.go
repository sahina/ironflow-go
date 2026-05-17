package ironflow

import (
	"testing"
)

func TestCreateProjection(t *testing.T) {
	t.Run("creates projection with valid config", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:   "test",
			Events: []string{"order.created"},
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return state, nil
			},
			InitialState: func() map[string]any { return map[string]any{} },
		})

		if proj.Config.Name != "test" {
			t.Errorf("expected Name 'test', got '%s'", proj.Config.Name)
		}
		if proj.Config.Mode != ProjectionModeManaged {
			t.Errorf("expected Mode 'managed', got '%s'", proj.Config.Mode)
		}
		if len(proj.Config.Events) != 1 || proj.Config.Events[0] != "order.created" {
			t.Errorf("expected Events ['order.created'], got %v", proj.Config.Events)
		}
		if proj.Config.MaxRetries != 3 {
			t.Errorf("expected MaxRetries 3, got %d", proj.Config.MaxRetries)
		}
		if proj.Config.BatchSize != 100 {
			t.Errorf("expected BatchSize 100, got %d", proj.Config.BatchSize)
		}
	})

	t.Run("creates projection with multiple events", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:   "multi-event",
			Events: []string{"order.created", "order.updated", "order.cancelled"},
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return state, nil
			},
		})

		if len(proj.Config.Events) != 3 {
			t.Errorf("expected 3 events, got %d", len(proj.Config.Events))
		}
	})

	t.Run("creates projection with custom retries and batch size", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:       "custom",
			Events:     []string{"test"},
			MaxRetries: 5,
			BatchSize:  50,
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return state, nil
			},
		})

		if proj.Config.MaxRetries != 5 {
			t.Errorf("expected MaxRetries 5, got %d", proj.Config.MaxRetries)
		}
		if proj.Config.BatchSize != 50 {
			t.Errorf("expected BatchSize 50, got %d", proj.Config.BatchSize)
		}
	})

	t.Run("creates projection with partition key", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:         "partitioned",
			Events:       []string{"order.*"},
			PartitionKey: "$.data.customerId",
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return state, nil
			},
		})

		if proj.Config.PartitionKey != "$.data.customerId" {
			t.Errorf("expected PartitionKey '$.data.customerId', got '%s'", proj.Config.PartitionKey)
		}
	})
}

func TestCreateProjection_External(t *testing.T) {
	t.Run("creates external projection without initial state", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:   "external",
			Events: []string{"employee.*"},
			Mode:   ProjectionModeExternal,
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
		})

		if proj.Config.Mode != ProjectionModeExternal {
			t.Errorf("expected Mode 'external', got '%s'", proj.Config.Mode)
		}
		if proj.Config.InitialState != nil {
			t.Error("expected InitialState to be nil for external projection")
		}
	})

	t.Run("respects explicit external mode even with initial state", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:   "explicit-external",
			Events: []string{"test"},
			Mode:   ProjectionModeExternal,
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
			InitialState: func() map[string]any { return map[string]any{} },
		})

		if proj.Config.Mode != ProjectionModeExternal {
			t.Errorf("expected explicit Mode 'external', got '%s'", proj.Config.Mode)
		}
	})
}

func TestCreateProjection_AutoDetect(t *testing.T) {
	t.Run("auto-detects managed mode when InitialState is provided", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:   "auto-managed",
			Events: []string{"test"},
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return state, nil
			},
			InitialState: func() map[string]any { return map[string]any{} },
		})

		if proj.Config.Mode != ProjectionModeManaged {
			t.Errorf("expected auto-detected Mode 'managed', got '%s'", proj.Config.Mode)
		}
	})

	t.Run("auto-detects external mode when InitialState is nil", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:   "auto-external",
			Events: []string{"test"},
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
		})

		if proj.Config.Mode != ProjectionModeExternal {
			t.Errorf("expected auto-detected Mode 'external', got '%s'", proj.Config.Mode)
		}
	})

	t.Run("respects explicit managed mode", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:   "explicit-managed",
			Events: []string{"test"},
			Mode:   ProjectionModeManaged,
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return state, nil
			},
		})

		if proj.Config.Mode != ProjectionModeManaged {
			t.Errorf("expected explicit Mode 'managed', got '%s'", proj.Config.Mode)
		}
	})
}

func TestCreateProjection_Validation(t *testing.T) {
	t.Run("panics for empty name", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for empty name")
			}
		}()

		CreateProjection(ProjectionConfig{
			Events: []string{"test"},
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
		})
	})

	t.Run("panics for empty events", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for empty events")
			}
		}()

		CreateProjection(ProjectionConfig{
			Name: "test",
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
		})
	})

	t.Run("panics for nil events slice", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for nil events")
			}
		}()

		CreateProjection(ProjectionConfig{
			Name:   "test",
			Events: nil,
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
		})
	})
}

func TestCreateProjection_Defaults(t *testing.T) {
	t.Run("applies default MaxRetries of 3", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:   "defaults",
			Events: []string{"test"},
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
		})

		if proj.Config.MaxRetries != 3 {
			t.Errorf("expected default MaxRetries 3, got %d", proj.Config.MaxRetries)
		}
	})

	t.Run("applies default BatchSize of 100", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:   "defaults",
			Events: []string{"test"},
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
		})

		if proj.Config.BatchSize != 100 {
			t.Errorf("expected default BatchSize 100, got %d", proj.Config.BatchSize)
		}
	})

	t.Run("does not override explicit MaxRetries", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:       "explicit",
			Events:     []string{"test"},
			MaxRetries: 10,
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
		})

		if proj.Config.MaxRetries != 10 {
			t.Errorf("expected MaxRetries 10, got %d", proj.Config.MaxRetries)
		}
	})

	t.Run("does not override explicit BatchSize", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:      "explicit",
			Events:    []string{"test"},
			BatchSize: 25,
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
		})

		if proj.Config.BatchSize != 25 {
			t.Errorf("expected BatchSize 25, got %d", proj.Config.BatchSize)
		}
	})
}

func TestGetProjectionMetadata(t *testing.T) {
	t.Run("returns projection metadata", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:   "order-totals",
			Events: []string{"order.created", "order.updated"},
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return state, nil
			},
			InitialState: func() map[string]any { return map[string]any{} },
		})

		metadata := GetProjectionMetadata(proj)

		if metadata["name"] != "order-totals" {
			t.Errorf("expected name 'order-totals', got '%v'", metadata["name"])
		}
		if metadata["mode"] != "managed" {
			t.Errorf("expected mode 'managed', got '%v'", metadata["mode"])
		}
		if metadata["max_retries"] != 3 {
			t.Errorf("expected max_retries 3, got %v", metadata["max_retries"])
		}
		if metadata["batch_size"] != 100 {
			t.Errorf("expected batch_size 100, got %v", metadata["batch_size"])
		}

		events, ok := metadata["events"].([]string)
		if !ok || len(events) != 2 {
			t.Errorf("expected 2 events in metadata, got %v", metadata["events"])
		}
	})

	t.Run("includes partition key when set", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:         "partitioned",
			Events:       []string{"order.*"},
			PartitionKey: "$.data.customerId",
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
		})

		metadata := GetProjectionMetadata(proj)

		if metadata["partition_key"] != "$.data.customerId" {
			t.Errorf("expected partition_key '$.data.customerId', got '%v'", metadata["partition_key"])
		}
	})

	t.Run("omits partition key when not set", func(t *testing.T) {
		proj := CreateProjection(ProjectionConfig{
			Name:   "no-partition",
			Events: []string{"test"},
			Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
				return nil, nil
			},
		})

		metadata := GetProjectionMetadata(proj)

		if _, exists := metadata["partition_key"]; exists {
			t.Error("expected partition_key to be omitted when not set")
		}
	})
}
