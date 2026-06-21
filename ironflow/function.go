package ironflow

import (
	"fmt"
	"regexp"
	"time"
)

var functionIDPattern = regexp.MustCompile(`^[a-zA-Z0-9-_]+$`)

// CreateFunction creates a new Ironflow workflow function.
//
// Example:
//
//	var ProcessOrder = ironflow.CreateFunction(ironflow.FunctionConfig{
//	    ID:       "process-order",
//	    Name:     "Process Order",
//	    Triggers: []ironflow.Trigger{{Event: "order.placed"}},
//	    Retry:    &ironflow.RetryConfig{MaxAttempts: 3},
//	    Timeout:  5 * time.Minute,
//	}, func(ctx ironflow.Context) (any, error) {
//	    var event OrderPlacedEvent
//	    if err := ctx.Event.Data(&event); err != nil {
//	        return nil, err
//	    }
//
//	    result, err := ironflow.Run(ctx, "process", func() (any, error) {
//	        return processOrder(event)
//	    })
//	    return result, err
//	})
func CreateFunction(config FunctionConfig, handler FunctionHandler) Function {
	// Validate config
	if err := validateFunctionConfig(config); err != nil {
		panic(fmt.Sprintf("invalid function config: %v", err))
	}

	// Apply defaults
	normalizedConfig := normalizeFunctionConfig(config)

	return Function{
		Config:  normalizedConfig,
		Handler: handler,
	}
}

// validateFunctionConfig validates the function configuration.
func validateFunctionConfig(config FunctionConfig) error {
	if config.ID == "" {
		return fmt.Errorf("function ID is required")
	}

	if !functionIDPattern.MatchString(config.ID) {
		return fmt.Errorf("invalid function ID: %q (must contain only alphanumeric, hyphens, underscores)", config.ID)
	}

	// Triggers are optional — functions without triggers can be called via Invoke/InvokeAsync
	for i, trigger := range config.Triggers {
		if trigger.Event == "" && trigger.Cron == "" {
			return fmt.Errorf("trigger %d: event or cron is required", i)
		}
	}

	if config.Timeout < 0 {
		return fmt.Errorf("timeout must be non-negative")
	}

	if config.StepTimeout < 0 {
		return fmt.Errorf("step timeout must be non-negative")
	}

	if config.Concurrency != nil && config.Concurrency.Limit <= 0 {
		return fmt.Errorf("concurrency limit must be positive")
	}

	if config.Debounce != nil {
		if config.Debounce.Period < time.Second {
			return fmt.Errorf("debounce period must be >= 1s (got %s); scheduler tick floor is 1s", config.Debounce.Period)
		}
		if config.Debounce.MaxWait != 0 && config.Debounce.MaxWait < config.Debounce.Period {
			return fmt.Errorf("debounce maxWait must be >= period (got maxWait=%s, period=%s) or 0 for no cap", config.Debounce.MaxWait, config.Debounce.Period)
		}
	}

	if len(config.CancelOn) > 0 {
		seen := make(map[string]struct{}, len(config.CancelOn))
		for i, spec := range config.CancelOn {
			if spec.Event == "" {
				return fmt.Errorf("cancelOn[%d].event must be non-empty", i)
			}
			if spec.Match == "" {
				return fmt.Errorf("cancelOn[%d].match must be non-empty", i)
			}
			key := spec.Event + "|" + spec.Match
			if _, ok := seen[key]; ok {
				return fmt.Errorf("cancelOn[%d]: duplicate spec (event=%q, match=%q)", i, spec.Event, spec.Match)
			}
			seen[key] = struct{}{}
		}
	}

	if config.Retry != nil {
		if config.Retry.MaxAttempts <= 0 {
			return fmt.Errorf("retry maxAttempts must be positive")
		}
		if config.Retry.InitialDelay < 0 {
			return fmt.Errorf("retry initialDelay must be non-negative")
		}
		if config.Retry.BackoffFactor < 1 {
			return fmt.Errorf("retry backoffFactor must be at least 1")
		}
	}

	return nil
}

// normalizeFunctionConfig applies default values to the config.
func normalizeFunctionConfig(config FunctionConfig) FunctionConfig {
	normalized := config

	// Default name to ID
	if normalized.Name == "" {
		normalized.Name = normalized.ID
	}

	// Default timeout
	if normalized.Timeout == 0 {
		normalized.Timeout = 10 * time.Minute
	}

	// Default mode
	if normalized.Mode == "" {
		normalized.Mode = PushMode
	}

	// Default retry config
	if normalized.Retry == nil {
		normalized.Retry = &RetryConfig{}
	}
	if normalized.Retry.MaxAttempts == 0 {
		normalized.Retry.MaxAttempts = 3
	}
	if normalized.Retry.InitialDelay == 0 {
		normalized.Retry.InitialDelay = time.Second
	}
	if normalized.Retry.BackoffFactor == 0 {
		normalized.Retry.BackoffFactor = 2.0
	}
	if normalized.Retry.MaxDelay == 0 {
		normalized.Retry.MaxDelay = 5 * time.Minute
	}

	return normalized
}

// GetFunctionMetadata returns the function metadata for registration.
func GetFunctionMetadata(fn Function) map[string]any {
	config := fn.Config

	triggers := make([]map[string]any, len(config.Triggers))
	for i, t := range config.Triggers {
		triggers[i] = map[string]any{
			"event":      t.Event,
			"expression": t.Expression,
			"cron":       t.Cron,
		}
	}

	metadata := map[string]any{
		"id":       config.ID,
		"name":     config.Name,
		"triggers": triggers,
		"retry": map[string]any{
			"max_attempts":     config.Retry.MaxAttempts,
			"initial_delay_ms": config.Retry.InitialDelay.Milliseconds(),
			"backoff_factor":   config.Retry.BackoffFactor,
			"max_delay_ms":     config.Retry.MaxDelay.Milliseconds(),
		},
		"timeout_ms": config.Timeout.Milliseconds(),
		"mode":       string(config.Mode),
	}

	if config.Concurrency != nil {
		metadata["concurrency"] = map[string]any{
			"limit": config.Concurrency.Limit,
			"key":   config.Concurrency.Key,
		}
	}

	if config.Debounce != nil {
		dbn := map[string]any{
			"period_ms": config.Debounce.Period.Milliseconds(),
			"key":       config.Debounce.Key,
		}
		if config.Debounce.MaxWait > 0 {
			dbn["max_wait_ms"] = config.Debounce.MaxWait.Milliseconds()
		}
		metadata["debounce"] = dbn
	}

	if len(config.CancelOn) > 0 {
		specs := make([]map[string]any, len(config.CancelOn))
		for i, s := range config.CancelOn {
			specs[i] = map[string]any{
				"event": s.Event,
				"match": s.Match,
			}
		}
		metadata["cancel_on"] = specs
	}

	if config.ActorKey != "" {
		metadata["actor_key"] = config.ActorKey
	}

	if config.EndpointURL != "" {
		metadata["endpoint_url"] = config.EndpointURL
	}

	if len(config.Secrets) > 0 {
		metadata["secrets"] = config.Secrets
	}

	if config.Recording {
		metadata["recording"] = config.Recording
	}
	if config.RecordingRetention != "" {
		metadata["recording_retention"] = config.RecordingRetention
	}

	if config.Metadata != nil {
		metadata["metadata"] = config.Metadata
	}

	return metadata
}
