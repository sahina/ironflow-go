package ironflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// ServeConfig configures the HTTP handler.
type ServeConfig struct {
	// Functions are the functions to serve.
	Functions []Function

	// Projections are the projections to register with the server.
	Projections []Projection

	// Webhooks are the webhook sources to handle.
	Webhooks []Webhook

	// ServerURL is the Ironflow server URL for emitting webhook events.
	// When set, SDK-defined webhooks will emit events to the server after transform.
	ServerURL string

	// SigningKey is the secret for webhook signature verification.
	SigningKey string

	// SkipVerification skips signature verification (dev only).
	SkipVerification bool

	// Upcasters is an optional registry for automatic event schema upcasting.
	// When set, event data is upcasted to the latest version before being passed to handlers.
	Upcasters *UpcasterRegistry
}

// Serve creates an HTTP handler for Ironflow functions.
//
// Example:
//
//	handler := ironflow.Serve(ironflow.ServeConfig{
//	    Functions:  []ironflow.Function{ProcessOrder, SendNotification},
//	    SigningKey: os.Getenv("IRONFLOW_SIGNING_KEY"),
//	})
//
//	http.Handle("/api/ironflow", handler)
//	http.ListenAndServe(":3000", nil)
func Serve(config ServeConfig) http.Handler {
	// Build function map
	functionMap := make(map[string]Function)
	for _, fn := range config.Functions {
		if _, exists := functionMap[fn.Config.ID]; exists {
			log.Printf("[ironflow-serve] WARNING: duplicate function ID %q — the later definition will overwrite the earlier one", fn.Config.ID)
		}
		functionMap[fn.Config.ID] = fn
	}

	// Build webhook map
	webhookMap := make(map[string]Webhook)
	for _, wh := range config.Webhooks {
		webhookMap[wh.Config.ID] = wh
	}

	return &serveHandler{
		functions:        functionMap,
		webhooks:         webhookMap,
		serverURL:        config.ServerURL,
		signingKey:       config.SigningKey,
		skipVerification: config.SkipVerification,
		upcasters:        config.Upcasters,
	}
}

type serveHandler struct {
	functions        map[string]Function
	webhooks         map[string]Webhook
	serverURL        string
	signingKey       string
	skipVerification bool
	upcasters        *UpcasterRegistry
}

func (h *serveHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.sendError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "only POST is allowed")
		return
	}

	// Read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, "BODY_READ_ERROR", fmt.Sprintf("failed to read body: %v", err))
		return
	}
	defer func() { _ = r.Body.Close() }()

	// Check if this is a webhook request
	if provider, ok := strings.CutPrefix(r.URL.Path, "/webhooks/"); ok && provider != "" {
		h.handleWebhook(w, r, provider, body)
		return
	}

	// Verify signature
	if h.signingKey != "" && !h.skipVerification {
		signature := r.Header.Get("X-Ironflow-Signature")
		if err := VerifySignature(string(body), signature, h.signingKey, DefaultSignatureTolerance); err != nil {
			h.sendError(w, http.StatusUnauthorized, "SIGNATURE_INVALID", err.Error())
			return
		}
	}

	// Parse request
	var req PushRequest
	if err := json.Unmarshal(body, &req); err != nil {
		h.sendError(w, http.StatusBadRequest, "INVALID_JSON", fmt.Sprintf("failed to parse request: %v", err))
		return
	}

	// Find function
	fn, ok := h.functions[req.FunctionID]
	if !ok {
		h.sendError(w, http.StatusNotFound, "FUNCTION_NOT_FOUND", fmt.Sprintf("function not found: %s", req.FunctionID))
		return
	}

	// Execute function
	response := h.executeHandler(fn, &req)
	h.sendJSON(w, http.StatusOK, response)
}

func (h *serveHandler) executeHandler(fn Function, req *PushRequest) *PushResponse {
	// Create execution context
	exec := newExecutionContext(req)
	exec.stepTimeout = fn.Config.StepTimeout
	exec.serverURL = h.serverURL
	if apiKey := GetAPIKey(); apiKey != "" {
		exec.apiKey = apiKey
	}

	// Parse event timestamp
	timestamp, _ := time.Parse(time.RFC3339, req.Event.Timestamp)

	// Apply upcasting if registry is provided
	eventData := req.Event.Data
	if h.upcasters != nil {
		upcasted, err := h.upcasters.UpcastToLatest(req.Event.Name, eventData, req.Event.Version)
		if err == nil {
			eventData = upcasted
		}
	}

	// Build context
	ctx := Context{
		Event: Event{
			ID:             req.Event.ID,
			Name:           req.Event.Name,
			Version:        req.Event.Version,
			RawData:        eventData,
			Timestamp:      timestamp,
			IdempotencyKey: req.Event.IdempotencyKey,
			Source:         EventSourceType(req.Event.Source),
			Metadata:       req.Event.Metadata,
		},
		Run: RunInfo{
			ID:         req.RunID,
			FunctionID: req.FunctionID,
			Attempt:    req.Attempt,
			StartedAt:  time.Now(),
		},
		Secrets: NewSecretsReader(req.Secrets),
		exec:    exec,
	}

	// Execute with panic recovery for yield signals
	var result any
	var execErr error

	func() {
		defer func() {
			if r := recover(); r != nil {
				// Check if it's a yield signal
				if signal, ok := r.(*yieldSignal); ok {
					execErr = signal
				} else {
					// Re-panic for real panics
					panic(r)
				}
			}
		}()

		result, execErr = fn.Handler(ctx)
	}()

	// Handle yield signal
	if signal, ok := isYieldSignal(execErr); ok {
		return &PushResponse{
			Status: "yielded",
			Steps:  exec.executedSteps,
			Yield:  signal.info,
		}
	}

	// Handle error
	if execErr != nil {
		var code string
		var retryable bool

		if ironflowErr, ok := execErr.(*IronflowError); ok {
			code = ironflowErr.Code
			retryable = ironflowErr.Retryable
		} else if stepErr, ok := execErr.(*StepError); ok {
			code = stepErr.Code
			retryable = stepErr.Retryable
		} else {
			code = "ERROR"
			retryable = true
		}

		// Run compensations only if error is not retryable (terminal failure)
		if exec.hasCompensations() && !retryable {
			exec.executeCompensations()
		}

		return &PushResponse{
			Status: "failed",
			Steps:  exec.executedSteps,
			Error: &PushError{
				Message:   execErr.Error(),
				Code:      code,
				Retryable: retryable,
			},
		}
	}

	// Success
	return &PushResponse{
		Status: "completed",
		Steps:  exec.executedSteps,
		Result: result,
	}
}

func (h *serveHandler) sendError(w http.ResponseWriter, status int, code, message string) {
	h.sendJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

func (h *serveHandler) handleWebhook(w http.ResponseWriter, r *http.Request, provider string, body []byte) {
	wh, ok := h.webhooks[provider]
	if !ok {
		h.sendError(w, http.StatusNotFound, "WEBHOOK_NOT_FOUND", fmt.Sprintf("webhook source not found: %s", provider))
		return
	}

	// Verify
	if wh.Config.Verify != nil {
		req := &WebhookRequest{
			Body:   body,
			Header: r.Header,
			Method: r.Method,
			URL:    r.URL.String(),
		}
		if err := wh.Config.Verify(req); err != nil {
			h.sendError(w, http.StatusUnauthorized, "VERIFY_FAILED", err.Error())
			return
		}
	}

	// Transform
	event, err := wh.Config.Transform(body)
	if err != nil {
		h.sendError(w, http.StatusBadRequest, "TRANSFORM_FAILED", err.Error())
		return
	}

	// Emit event to Ironflow server if configured
	if h.serverURL != "" {
		emitReq := map[string]any{
			"name": event.Name,
			"data": event.Data,
		}
		if event.IdempotencyKey != "" {
			emitReq["idempotencyKey"] = event.IdempotencyKey
		}
		emitBody, err := json.Marshal(emitReq)
		if err != nil {
			h.sendError(w, http.StatusInternalServerError, "EMIT_FAILED", fmt.Sprintf("failed to marshal event: %v", err))
			return
		}
		// /api/v1/events requires auth (isPublicPath rejects every /api/ path),
		// so an unauthenticated emit fails the whole webhook outside dev mode.
		// Same env key executeHandler gives the step callbacks.
		emitHTTPReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
			h.serverURL+"/api/v1/events", bytes.NewReader(emitBody))
		if err != nil {
			h.sendError(w, http.StatusInternalServerError, "EMIT_FAILED", fmt.Sprintf("failed to build emit request: %v", err))
			return
		}
		emitHTTPReq.Header.Set("Content-Type", "application/json")
		for k, v := range buildAuthHeaders("") {
			emitHTTPReq.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(emitHTTPReq)
		if err != nil {
			h.sendError(w, http.StatusBadGateway, "EMIT_FAILED", fmt.Sprintf("failed to emit event: %v", err))
			return
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode >= 400 {
			respBody, _ := io.ReadAll(resp.Body)
			h.sendError(w, http.StatusBadGateway, "EMIT_FAILED", fmt.Sprintf("server rejected event: %s", string(respBody)))
			return
		}
	}

	h.sendJSON(w, http.StatusOK, map[string]any{
		"status": "accepted",
		"event":  event,
	})
}

func (h *serveHandler) sendJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
