package ironflow

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ============================================================================
// Helper: build a minimal valid PushRequest JSON body
// ============================================================================

// The PushRequest / PushResponse JSON shapes are user-facing: they are the spec a
// Tier-2 language (Java, C#, Rust) writes its push handler against, published as
// docs/reference/api/push-protocol.md. drift-check hashes markdown only and cannot
// see these structs, so if you add, rename, or retype a field here, update that
// page in the same PR.
func validPushBody(functionID string) string {
	req := PushRequest{
		RunID:      "run-001",
		FunctionID: functionID,
		Attempt:    1,
		Event: PushEvent{
			ID:        "evt-001",
			Name:      "test.event",
			Version:   1,
			Data:      json.RawMessage(`{"key":"value"}`),
			Timestamp: "2024-01-01T00:00:00Z",
		},
	}
	b, _ := json.Marshal(req)
	return string(b)
}

// ============================================================================
// Helper: a simple function for testing
// ============================================================================

var testFunction = CreateFunction(FunctionConfig{
	ID:       "test-fn",
	Triggers: []Trigger{{Event: "test.event"}},
}, func(ctx Context) (any, error) {
	return map[string]string{"status": "ok"}, nil
})

// ============================================================================
// HTTP Method Validation
// ============================================================================

func TestServeHTTP_MethodNotAllowed(t *testing.T) {
	handler := Serve(ServeConfig{
		Functions: []Function{testFunction},
	})

	methods := []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/api/ironflow", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, rec.Code)
			}

			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("failed to parse response body: %v", err)
			}

			errObj, ok := body["error"].(map[string]any)
			if !ok {
				t.Fatal("expected error object in response")
			}
			if errObj["code"] != "METHOD_NOT_ALLOWED" {
				t.Errorf("expected error code %q, got %q", "METHOD_NOT_ALLOWED", errObj["code"])
			}
		})
	}
}

// ============================================================================
// Invalid JSON Body
// ============================================================================

func TestServeHTTP_InvalidJSON(t *testing.T) {
	handler := Serve(ServeConfig{
		Functions:        []Function{testFunction},
		SkipVerification: true,
	})

	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader("not valid json{{{"))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}

	errObj, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatal("expected error object in response")
	}
	if errObj["code"] != "INVALID_JSON" {
		t.Errorf("expected error code %q, got %q", "INVALID_JSON", errObj["code"])
	}
}

// ============================================================================
// Unknown Function ID
// ============================================================================

func TestServeHTTP_UnknownFunction(t *testing.T) {
	handler := Serve(ServeConfig{
		Functions:        []Function{testFunction},
		SkipVerification: true,
	})

	body := validPushBody("nonexistent-fn")
	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, rec.Code)
	}

	var respBody map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &respBody); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}

	errObj, ok := respBody["error"].(map[string]any)
	if !ok {
		t.Fatal("expected error object in response")
	}
	if errObj["code"] != "FUNCTION_NOT_FOUND" {
		t.Errorf("expected error code %q, got %q", "FUNCTION_NOT_FOUND", errObj["code"])
	}
}

// ============================================================================
// Successful Function Execution
// ============================================================================

func TestServeHTTP_SuccessfulExecution(t *testing.T) {
	handler := Serve(ServeConfig{
		Functions:        []Function{testFunction},
		SkipVerification: true,
	})

	body := validPushBody("test-fn")
	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type %q, got %q", "application/json", contentType)
	}

	var resp PushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.Status != "completed" {
		t.Errorf("expected status %q, got %q", "completed", resp.Status)
	}

	if resp.Error != nil {
		t.Errorf("expected no error, got %+v", resp.Error)
	}

	resultMap, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected result to be a map, got %T", resp.Result)
	}
	if resultMap["status"] != "ok" {
		t.Errorf("expected result status %q, got %v", "ok", resultMap["status"])
	}
}

// ============================================================================
// Function Execution Error
// ============================================================================

func TestServeHTTP_FunctionError(t *testing.T) {
	errorFn := CreateFunction(FunctionConfig{
		ID:       "error-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, NewError("something broke", "CUSTOM_ERROR", false)
	})

	handler := Serve(ServeConfig{
		Functions:        []Function{errorFn},
		SkipVerification: true,
	})

	body := validPushBody("error-fn")
	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status %d for function error (still 200), got %d", http.StatusOK, rec.Code)
	}

	var resp PushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.Status != "failed" {
		t.Errorf("expected status %q, got %q", "failed", resp.Status)
	}

	if resp.Error == nil {
		t.Fatal("expected error in response")
	}
	if resp.Error.Message != "something broke" {
		t.Errorf("expected error message %q, got %q", "something broke", resp.Error.Message)
	}
	if resp.Error.Code != "CUSTOM_ERROR" {
		t.Errorf("expected error code %q, got %q", "CUSTOM_ERROR", resp.Error.Code)
	}
	if resp.Error.Retryable {
		t.Error("expected retryable to be false")
	}
}

// ============================================================================
// Signature Verification Tests
// ============================================================================

func TestServeSignature_MissingSignature(t *testing.T) {
	handler := Serve(ServeConfig{
		Functions:  []Function{testFunction},
		SigningKey: "whsec_test_key",
	})

	body := validPushBody("test-fn")
	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
	// No X-Ironflow-Signature header set
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected status %d, got %d", http.StatusUnauthorized, rec.Code)
	}

	var respBody map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &respBody); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}

	errObj, ok := respBody["error"].(map[string]any)
	if !ok {
		t.Fatal("expected error object in response")
	}
	if errObj["code"] != "SIGNATURE_INVALID" {
		t.Errorf("expected error code %q, got %q", "SIGNATURE_INVALID", errObj["code"])
	}
}

func TestServeSignature_InvalidSignature(t *testing.T) {
	handler := Serve(ServeConfig{
		Functions:  []Function{testFunction},
		SigningKey: "whsec_test_key",
	})

	body := validPushBody("test-fn")
	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
	req.Header.Set("X-Ironflow-Signature", "t=1704067200,v1="+strings.Repeat("ab", 32))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected status %d, got %d", http.StatusUnauthorized, rec.Code)
	}

	var respBody map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &respBody); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}

	errObj, ok := respBody["error"].(map[string]any)
	if !ok {
		t.Fatal("expected error object in response")
	}
	if errObj["code"] != "SIGNATURE_INVALID" {
		t.Errorf("expected error code %q, got %q", "SIGNATURE_INVALID", errObj["code"])
	}
}

func TestServeSignature_ValidSignature(t *testing.T) {
	signingKey := "whsec_test_key"
	handler := Serve(ServeConfig{
		Functions:  []Function{testFunction},
		SigningKey: signingKey,
	})

	body := validPushBody("test-fn")
	signature := SignPayload(body, signingKey)

	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
	req.Header.Set("X-Ironflow-Signature", signature)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var resp PushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.Status != "completed" {
		t.Errorf("expected status %q, got %q", "completed", resp.Status)
	}
}

func TestServeSignature_SkipVerification(t *testing.T) {
	handler := Serve(ServeConfig{
		Functions:        []Function{testFunction},
		SigningKey:       "whsec_test_key",
		SkipVerification: true,
	})

	body := validPushBody("test-fn")
	// No signature header — should still succeed because SkipVerification is true
	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var resp PushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.Status != "completed" {
		t.Errorf("expected status %q, got %q", "completed", resp.Status)
	}
}

func TestServeSignature_NoKeyNoVerification(t *testing.T) {
	// When no signingKey is configured, verification should be skipped entirely
	handler := Serve(ServeConfig{
		Functions: []Function{testFunction},
		// No SigningKey set
	})

	body := validPushBody("test-fn")
	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
	// No signature header
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status %d when no signing key configured, got %d", http.StatusOK, rec.Code)
	}

	var resp PushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.Status != "completed" {
		t.Errorf("expected status %q, got %q", "completed", resp.Status)
	}
}

// ============================================================================
// Multiple Functions
// ============================================================================

func TestServeHTTP_MultipleFunctions(t *testing.T) {
	fnA := CreateFunction(FunctionConfig{
		ID:       "fn-a",
		Triggers: []Trigger{{Event: "a.event"}},
	}, func(ctx Context) (any, error) {
		return map[string]string{"fn": "a"}, nil
	})

	fnB := CreateFunction(FunctionConfig{
		ID:       "fn-b",
		Triggers: []Trigger{{Event: "b.event"}},
	}, func(ctx Context) (any, error) {
		return map[string]string{"fn": "b"}, nil
	})

	handler := Serve(ServeConfig{
		Functions:        []Function{fnA, fnB},
		SkipVerification: true,
	})

	t.Run("routes to function A", func(t *testing.T) {
		body := validPushBody("fn-a")
		req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}

		var resp PushResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		resultMap, ok := resp.Result.(map[string]any)
		if !ok {
			t.Fatalf("expected result map, got %T", resp.Result)
		}
		if resultMap["fn"] != "a" {
			t.Errorf("expected fn %q, got %v", "a", resultMap["fn"])
		}
	})

	t.Run("routes to function B", func(t *testing.T) {
		body := validPushBody("fn-b")
		req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}

		var resp PushResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		resultMap, ok := resp.Result.(map[string]any)
		if !ok {
			t.Fatalf("expected result map, got %T", resp.Result)
		}
		if resultMap["fn"] != "b" {
			t.Errorf("expected fn %q, got %v", "b", resultMap["fn"])
		}
	})
}

// ============================================================================
// Empty Body
// ============================================================================

func TestServeHTTP_EmptyBody(t *testing.T) {
	handler := Serve(ServeConfig{
		Functions:        []Function{testFunction},
		SkipVerification: true,
	})

	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(""))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

// ============================================================================
// Response Content-Type
// ============================================================================

func TestServeHTTP_ResponseContentType(t *testing.T) {
	handler := Serve(ServeConfig{
		Functions:        []Function{testFunction},
		SkipVerification: true,
	})

	// Error response should also have Content-Type: application/json
	req := httptest.NewRequest(http.MethodGet, "/api/ironflow", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type %q for error responses, got %q", "application/json", contentType)
	}
}

func TestServe_DuplicateFunctionWarning(t *testing.T) {
	fn1 := CreateFunction(FunctionConfig{
		ID:       "dup-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return "first", nil
	})
	fn2 := CreateFunction(FunctionConfig{
		ID:       "dup-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return "second", nil
	})

	// Should not panic — just warn
	handler := Serve(ServeConfig{
		Functions: []Function{fn1, fn2},
	})

	// The second function should win — verify by calling it
	body := validPushBody("dup-fn")
	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp PushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Result != "second" {
		t.Errorf("expected second function to win, got result: %v", resp.Result)
	}
}
