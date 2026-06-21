package ironflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
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

		modeStr := "EXECUTION_MODE_PULL"
		if fn.Config.Mode == PushMode {
			modeStr = "EXECUTION_MODE_PUSH"
		}

		body := map[string]any{
			"id":            fn.Config.ID,
			"name":          fn.Config.Name,
			"triggers":      triggers,
			"preferredMode": modeStr,
		}

		if fn.Config.Retry != nil {
			body["retry"] = map[string]any{
				"maxAttempts":    fn.Config.Retry.MaxAttempts,
				"initialDelayMs": fn.Config.Retry.InitialDelay.Milliseconds(),
				"backoffFactor":  fn.Config.Retry.BackoffFactor,
			}
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
		if fn.Config.ActorKey != "" {
			body["actorKey"] = fn.Config.ActorKey
		}
		if fn.Config.Recording {
			body["recording"] = fn.Config.Recording
		}
		if fn.Config.RecordingRetention != "" {
			body["recordingRetention"] = fn.Config.RecordingRetention
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
		_ = resp.Body.Close()

		if resp.StatusCode >= 400 {
			return fmt.Errorf("failed to register function %s: status %d", id, resp.StatusCode)
		}

		logger.Info("Registered function", "functionId", id)
	}
	return nil
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
