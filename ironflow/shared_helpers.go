package ironflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

// buildAuthHeaders returns HTTP headers with authentication if an API key is available.
func buildAuthHeaders(apiKey string) map[string]string {
	headers := make(map[string]string)
	if apiKey == "" {
		apiKey = GetAPIKey()
	}
	if apiKey != "" {
		headers["Authorization"] = "Bearer " + apiKey
	}
	return headers
}

// buildWorkerHeaders returns the auth headers plus the environment header.
// With no environment set it sends no header: the server then resolves the
// API key's environment or env_default, as it did before this option existed.
func buildWorkerHeaders(apiKey, environment string) map[string]string {
	headers := buildAuthHeaders(apiKey)
	if environment = resolveEnvironment(environment); environment != "" {
		headers[HeaderEnvironmentID] = environment
	}
	return headers
}

// resolveEnvironment returns the configured environment, or IRONFLOW_ENV when
// none is configured.
func resolveEnvironment(configured string) string {
	if configured != "" {
		return configured
	}
	return os.Getenv(EnvEnvironment)
}

// registerFunctions registers all worker functions with the Ironflow server
// via the ConnectRPC RegisterFunction endpoint.
func registerFunctions(ctx context.Context, serverURL string, headers map[string]string, functions map[string]Function, httpClient *http.Client, logger Logger) error {
	for id, fn := range functions {
		// Build triggers as maps with proto3 JSON field names (lowercase)
		triggers := make([]map[string]string, len(fn.Config.Triggers))
		for i, t := range fn.Config.Triggers {
			triggers[i] = map[string]string{
				"event":      t.Event,
				"expression": t.Expression,
				"cron":       t.Cron,
			}
		}

		// Always pull: a worker can only execute pull, and CreateFunction defaults
		// Mode to push, so honoring it here registered push functions with no
		// endpoint. A trigger before RegisterWorker flipped them to pull then
		// failed with "no endpoint URL configured" (#2426).
		body := map[string]any{
			"id":            fn.Config.ID,
			"name":          fn.Config.Name,
			"triggers":      triggers,
			"preferredMode": "EXECUTION_MODE_PULL",
		}
		if fn.Config.Description != "" {
			body["description"] = fn.Config.Description
		}

		if retry := retryRegisterBody(fn.Config.Retry); len(retry) > 0 {
			body["retry"] = retry
		}
		if fn.Config.Timeout > 0 {
			body["timeoutMs"] = fn.Config.Timeout.Milliseconds()
		}
		if fn.Config.Concurrency != nil {
			body["concurrency"] = map[string]any{
				"limit": fn.Config.Concurrency.Limit,
				"key":   fn.Config.Concurrency.Key,
			}
		}
		if fn.Config.Debounce != nil {
			dbn := map[string]any{
				"period_ms": fn.Config.Debounce.Period.Milliseconds(),
				"key":       fn.Config.Debounce.Key,
			}
			if fn.Config.Debounce.MaxWait > 0 {
				dbn["max_wait_ms"] = fn.Config.Debounce.MaxWait.Milliseconds()
			}
			body["debounce"] = dbn
		}
		if cb := fn.Config.CircuitBreaker; cb != nil {
			body["circuitBreaker"] = map[string]any{
				"failureThreshold":  cb.FailureThreshold,
				"openDurationMs":    cb.OpenDuration.Milliseconds(),
				"maxOpenDurationMs": cb.MaxOpenDuration.Milliseconds(),
			}
		}
		if th := fn.Config.Throttle; th != nil {
			body["throttle"] = map[string]any{
				"limit":    th.Limit,
				"periodMs": th.Period.Milliseconds(),
				"key":      th.Key,
			}
		}
		if fn.Config.ActorKey != "" {
			body["actorKey"] = fn.Config.ActorKey
		}
		if fn.Config.Recording {
			body["recording"] = fn.Config.Recording
		}
		if fn.Config.RecordingProfile != "" {
			body["recordingProfile"] = string(fn.Config.RecordingProfile)
		}
		if len(fn.Config.Secrets) > 0 {
			body["secrets"] = fn.Config.Secrets
		}
		if len(fn.Config.CancelOn) > 0 {
			specs := make([]map[string]string, len(fn.Config.CancelOn))
			for i, s := range fn.Config.CancelOn {
				specs[i] = map[string]string{"event": s.Event, "match": s.Match}
			}
			body["cancelOn"] = specs
		}

		// #1280: stamp the running binary's hash into metadata so a rebuild
		// (air / go build) changes the registered config → the engine bumps the
		// version → the reload barrier fires. Merges with any user metadata.
		if h := executableHash(); h != "" {
			meta := map[string]any{codeHashMetaKey: h}
			maps.Copy(meta, fn.Config.Metadata)
			body["metadata"] = meta
		}

		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal function %s: %w", id, err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			serverURL+"/ironflow.v1.IronflowService/RegisterFunction",
			bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("failed to create request for function %s: %w", id, err)
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to register function %s: %w", id, err)
		}
		// Closed after the status check: authError reads a 403 body.
		if resp.StatusCode >= 400 {
			defer func() { _ = resp.Body.Close() }()
			if authErr := authError(resp, fmt.Sprintf("failed to register function %s", id)); authErr != nil {
				return authErr
			}
			respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
			retryable, retryAfter := restRetrySignal(resp.StatusCode, resp.Header, respBody)
			detail := strings.TrimSpace(string(respBody))
			if len(detail) > 200 {
				detail = detail[:200]
			}
			regErr := NewError(fmt.Sprintf("failed to register function %s: status %d: %s", id, resp.StatusCode, detail), "FUNCTION_REGISTRATION_FAILED", retryable)
			regErr.RetryAfter = retryAfter
			return regErr
		}
		_ = resp.Body.Close()

		logger.Info("Registered function", "functionId", id)
	}
	return nil
}

// retryRegisterBody is the RegisterFunction retry object. It carries only the
// fields the user set: the server stores an unset field as 0 and resolves it
// when each run is created, so sending a default here would override the
// operator's spec.engine.retry.* (#3151).
func retryRegisterBody(r *RetryConfig) map[string]any {
	if r == nil {
		return nil
	}
	m := map[string]any{}
	if r.MaxAttempts > 0 {
		m["maxAttempts"] = r.MaxAttempts
	}
	if r.InitialDelay > 0 {
		m["initialDelayMs"] = r.InitialDelay.Milliseconds()
	}
	if r.BackoffFactor > 0 {
		m["backoffFactor"] = r.BackoffFactor
	}
	if r.MaxDelay > 0 {
		m["maxDelayMs"] = r.MaxDelay.Milliseconds()
	}
	if r.OnExhausted != "" {
		m["onExhausted"] = r.OnExhausted
	}
	return m
}

// generateWorkerID generates a unique worker ID.
func generateWorkerID() string {
	timestamp := time.Now().UnixNano()
	random := rand.Int63()
	return fmt.Sprintf("worker-%x-%x", timestamp, random)
}

// getHostname returns the hostname.
func getHostname() string {
	if hostname := os.Getenv("HOSTNAME"); hostname != "" {
		return hostname
	}
	if hostname, err := os.Hostname(); err == nil {
		return hostname
	}
	return "unknown"
}
