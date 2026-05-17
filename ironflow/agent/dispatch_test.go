package agent

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const dispatchSecretHex = "abcdef0000000000000000000000000000000000000000000000000000000000"

func TestHandleAgentToolDispatch_DispatchesValidRequest(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	var received any
	registerLocal(RegisteredTool{
		AgentName:     "demo",
		QualifiedName: "demo.echo",
		HMACSecret:    dispatchSecretHex,
		Def: McpToolDef{
			Name:    "echo",
			Handler: func(input any) (any, error) { received = input; return map[string]any{"got": input}, nil },
		},
	})

	body := `{"qualified_name":"demo.echo","input":{"x":21}}`
	resp := postDispatch(t, body, signNow(body, dispatchSecretHex))
	if resp.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.status)
	}
	if resp.envelope.Error != nil {
		t.Fatalf("unexpected error envelope: %+v", resp.envelope.Error)
	}
	out, ok := resp.envelope.Output.(map[string]any)
	if !ok {
		t.Fatalf("output type = %T, want map", resp.envelope.Output)
	}
	if got, ok := out["got"].(map[string]any); !ok || got["x"] != 21.0 {
		t.Errorf("output = %v, want got.x=21", out)
	}
	if recvMap, ok := received.(map[string]any); !ok || recvMap["x"] != 21.0 {
		t.Errorf("handler received %v, want {x:21}", received)
	}
}

func TestHandleAgentToolDispatch_RejectsMissingHeaders(t *testing.T) {
	resp := postDispatchRaw(t, "{}", "", "")
	if resp.status != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.status)
	}
	if resp.envelope.Error == nil || resp.envelope.Error.Code != "SIGNATURE_MISMATCH" {
		t.Errorf("error = %+v, want SIGNATURE_MISMATCH", resp.envelope.Error)
	}
}

func TestHandleAgentToolDispatch_RejectsStaleTimestamp(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()
	registerLocal(staleEntry())

	body := `{"qualified_name":"demo.t","input":{}}`
	staleTs := time.Now().Add(-10 * time.Minute).Unix()
	sig := signAt(body, dispatchSecretHex, staleTs)

	resp := postDispatchRaw(t, body, strconv.FormatInt(staleTs, 10), sig)
	if resp.status != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.status)
	}
	if resp.envelope.Error == nil || resp.envelope.Error.Code != "TIMESTAMP_SKEW" {
		t.Errorf("error = %+v, want TIMESTAMP_SKEW", resp.envelope.Error)
	}
}

func TestHandleAgentToolDispatch_RejectsFutureSkew(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()
	registerLocal(staleEntry())

	body := `{"qualified_name":"demo.t","input":{}}`
	futureTs := time.Now().Add(10 * time.Minute).Unix()
	sig := signAt(body, dispatchSecretHex, futureTs)

	resp := postDispatchRaw(t, body, strconv.FormatInt(futureTs, 10), sig)
	if resp.status != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.status)
	}
	if resp.envelope.Error == nil || resp.envelope.Error.Code != "TIMESTAMP_SKEW" {
		t.Errorf("error = %+v, want TIMESTAMP_SKEW", resp.envelope.Error)
	}
}

func TestHandleAgentToolDispatch_RejectsBadSignature(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()
	registerLocal(staleEntry())

	body := `{"qualified_name":"demo.t","input":{}}`
	signed := signNow(body, dispatchSecretHex)
	sig := signed.sig
	last := sig[len(sig)-1]
	if last == '0' {
		sig = sig[:len(sig)-1] + "1"
	} else {
		sig = sig[:len(sig)-1] + "0"
	}

	resp := postDispatchRaw(t, body, signed.ts, sig)
	if resp.status != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.status)
	}
	if resp.envelope.Error == nil || resp.envelope.Error.Code != "SIGNATURE_MISMATCH" {
		t.Errorf("error = %+v, want SIGNATURE_MISMATCH", resp.envelope.Error)
	}
}

func TestHandleAgentToolDispatch_UnknownQualifiedNameCollapsesToSignatureMismatch(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	var logBuf bytes.Buffer
	prevOutput := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&logBuf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(prevOutput)
		log.SetFlags(prevFlags)
	}()

	body := `{"qualified_name":"ghost.tool","input":{}}`
	resp := postDispatch(t, body, signNow(body, dispatchSecretHex))
	if resp.status != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.status)
	}
	if resp.envelope.Error == nil || resp.envelope.Error.Code != "SIGNATURE_MISMATCH" {
		t.Errorf("error = %+v, want SIGNATURE_MISMATCH", resp.envelope.Error)
	}
	if resp.envelope.Error != nil && resp.envelope.Error.Message != "HMAC mismatch" {
		t.Errorf("message = %q, want %q", resp.envelope.Error.Message, "HMAC mismatch")
	}
	want := `ironflow.agent.dispatch unknown_tool qualified_name="ghost.tool"`
	if !strings.Contains(logBuf.String(), want) {
		t.Errorf("log output = %q, want to contain %q", logBuf.String(), want)
	}
}

func TestHandleAgentToolDispatch_UnknownQualifiedNameEscapesLogControlChars(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	var logBuf bytes.Buffer
	prevOutput := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&logBuf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(prevOutput)
		log.SetFlags(prevFlags)
	}()

	body := `{"qualified_name":"evil\nFAKE_LOG_LINE\rkey=injected","input":{}}`
	postDispatch(t, body, signNow(body, dispatchSecretHex))

	logged := logBuf.String()
	if strings.Contains(logged, "\nFAKE_LOG_LINE") {
		t.Errorf("control chars not escaped: %q", logged)
	}
	want := `ironflow.agent.dispatch unknown_tool qualified_name="evil\nFAKE_LOG_LINE\rkey=injected"`
	if !strings.Contains(logged, want) {
		t.Errorf("log output = %q, want to contain %q", logged, want)
	}
}

func TestHandleAgentToolDispatch_UnknownToolIndistinguishableFromBadSig(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	registerLocal(RegisteredTool{
		AgentName:     "demo",
		QualifiedName: "demo.t",
		HMACSecret:    dispatchSecretHex,
		Def: McpToolDef{
			Name:    "t",
			Handler: func(_ any) (any, error) { return nil, nil },
		},
	})

	prevOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(prevOutput)

	otherSecretHex := "ff" + strings.Repeat("0", 62)

	knownBody := `{"qualified_name":"demo.t","input":{}}`
	knownResp := postDispatch(t, knownBody, signNow(knownBody, otherSecretHex))

	unknownBody := `{"qualified_name":"ghost.tool","input":{}}`
	unknownResp := postDispatch(t, unknownBody, signNow(unknownBody, dispatchSecretHex))

	if knownResp.status != unknownResp.status {
		t.Errorf("status mismatch: known=%d unknown=%d", knownResp.status, unknownResp.status)
	}
	if knownResp.envelope.Error == nil || unknownResp.envelope.Error == nil {
		t.Fatalf("expected error envelopes on both")
	}
	if knownResp.envelope.Error.Code != unknownResp.envelope.Error.Code {
		t.Errorf("code mismatch: known=%s unknown=%s",
			knownResp.envelope.Error.Code, unknownResp.envelope.Error.Code)
	}
	if knownResp.envelope.Error.Message != unknownResp.envelope.Error.Message {
		t.Errorf("message mismatch: known=%q unknown=%q",
			knownResp.envelope.Error.Message, unknownResp.envelope.Error.Message)
	}
}

func TestHandleAgentToolDispatch_ValidateFailureMaps400(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	registerLocal(RegisteredTool{
		AgentName:     "demo",
		QualifiedName: "demo.s",
		HMACSecret:    dispatchSecretHex,
		Def: McpToolDef{
			Name:     "s",
			Validate: func(input any) error { return errors.New("q is required") },
			Handler:  func(_ any) (any, error) { return nil, nil },
		},
	})

	body := `{"qualified_name":"demo.s","input":{"q":""}}`
	resp := postDispatch(t, body, signNow(body, dispatchSecretHex))
	if resp.status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.status)
	}
	if resp.envelope.Error == nil || resp.envelope.Error.Code != "INPUT_SCHEMA_INVALID" {
		t.Errorf("error = %+v, want INPUT_SCHEMA_INVALID", resp.envelope.Error)
	}
}

func TestHandleAgentToolDispatch_HandlerErrorReturns200Envelope(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	registerLocal(RegisteredTool{
		AgentName:     "demo",
		QualifiedName: "demo.boom",
		HMACSecret:    dispatchSecretHex,
		Def: McpToolDef{
			Name:    "boom",
			Handler: func(_ any) (any, error) { return nil, errors.New("kaboom") },
		},
	})

	body := `{"qualified_name":"demo.boom","input":{}}`
	resp := postDispatch(t, body, signNow(body, dispatchSecretHex))
	if resp.status != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.status)
	}
	if resp.envelope.Error == nil {
		t.Fatalf("expected error envelope")
	}
	if resp.envelope.Error.Code != "HANDLER_ERROR" || resp.envelope.Error.Message != "kaboom" {
		t.Errorf("envelope = %+v, want HANDLER_ERROR kaboom", resp.envelope.Error)
	}
}

func TestDispatchHandler_RejectsWrongPath(t *testing.T) {
	server := httptest.NewServer(DispatchHandler())
	defer server.Close()

	resp, err := http.Post(server.URL+"/wrong/path", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestDispatchHandler_RejectsNonPost(t *testing.T) {
	server := httptest.NewServer(DispatchHandler())
	defer server.Close()

	resp, err := http.Get(server.URL + DispatchPath)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestDispatchPathConstant(t *testing.T) {
	if DispatchPath != "/ironflow/agent-tools/dispatch" {
		t.Errorf("DispatchPath = %q", DispatchPath)
	}
}

const altSecretHex = "1234567890abcdef0000000000000000000000000000000000000000000000ab"

// Cross-tenant HMAC isolation: agent-A's secret must not validate a
// dispatch addressed to agent-B. Defends against a compromised-key
// scenario crossing namespace boundaries.
func TestHandleAgentToolDispatch_RejectsCrossTenantSignature(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	registerLocal(RegisteredTool{
		AgentName:     "agentA",
		QualifiedName: "agentA.tool",
		HMACSecret:    dispatchSecretHex,
		Def:           McpToolDef{Name: "tool", Handler: func(_ any) (any, error) { return nil, nil }},
	})
	registerLocal(RegisteredTool{
		AgentName:     "agentB",
		QualifiedName: "agentB.tool",
		HMACSecret:    altSecretHex,
		Def:           McpToolDef{Name: "tool", Handler: func(_ any) (any, error) { return nil, nil }},
	})

	body := `{"qualified_name":"agentB.tool","input":{}}`
	// Sign with agent-A's secret while addressing agent-B.
	resp := postDispatch(t, body, signNow(body, dispatchSecretHex))
	if resp.status != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.status)
	}
	if resp.envelope.Error == nil || resp.envelope.Error.Code != "SIGNATURE_MISMATCH" {
		t.Errorf("error = %+v, want SIGNATURE_MISMATCH", resp.envelope.Error)
	}
}

// Re-register rotates HMAC secret: a dispatch signed with the prior
// secret must fail SIGNATURE_MISMATCH after the same agent re-registers.
// Lets operators recover from a leaked secret by re-issuing.
func TestExposeMcp_ReRegisterRotatesSecret(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	// Stub server returns S1 first, S2 on the second RegisterTool call.
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secret := dispatchSecretHex
		if callCount.Add(1) == 2 {
			secret = altSecretHex
		}
		writeRegisterResponse(w, secret, []string{"demo.tool"})
	}))
	defer server.Close()

	cfg := ExposeMcpConfig{
		Name:        "demo",
		Version:     "0.1.0",
		CallbackURL: testCallbackURL,
		ServerURL:   server.URL,
		APIKey:      testAPIKey,
		Tools:       []McpToolDef{{Name: "tool", InputSchemaJSON: testSchema, Handler: noopHandler}},
	}

	if _, err := ExposeMcp(cfg); err != nil {
		t.Fatalf("first ExposeMcp: %v", err)
	}
	entry, ok := lookupLocal("demo.tool")
	if !ok || entry.HMACSecret != dispatchSecretHex {
		t.Fatalf("after first register: secret=%q, want S1", entry.HMACSecret)
	}

	if _, err := ExposeMcp(cfg); err != nil {
		t.Fatalf("re-ExposeMcp: %v", err)
	}
	entry, ok = lookupLocal("demo.tool")
	if !ok || entry.HMACSecret != altSecretHex {
		t.Fatalf("after re-register: secret=%q, want S2 (rotated)", entry.HMACSecret)
	}

	// Dispatch signed with the OLD secret must now fail.
	body := `{"qualified_name":"demo.tool","input":{}}`
	resp := postDispatch(t, body, signNow(body, dispatchSecretHex))
	if resp.status != http.StatusUnauthorized {
		t.Errorf("dispatch with old secret status = %d, want 401", resp.status)
	}
	if resp.envelope.Error == nil || resp.envelope.Error.Code != "SIGNATURE_MISMATCH" {
		t.Errorf("error = %+v, want SIGNATURE_MISMATCH", resp.envelope.Error)
	}
}

// Concurrent registry access — exercised under `go test -race`. Catches
// missing locking on the package-level registry map. Not asserting
// specific outcomes, just that the operations are race-free.
func TestInternalRegistry_ConcurrentAccess(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	const goroutines = 20
	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	for i := 0; i < goroutines; i++ {
		agentName := fmt.Sprintf("agent%d", i)
		qualifiedName := agentName + ".t"
		entry := RegisteredTool{
			AgentName:     agentName,
			QualifiedName: qualifiedName,
			HMACSecret:    dispatchSecretHex,
			Def:           McpToolDef{Name: "t", Handler: func(_ any) (any, error) { return nil, nil }},
		}
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				registerLocal(entry)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = lookupLocal(qualifiedName)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				unregisterLocal(agentName)
			}
		}()
	}
	wg.Wait()
}

// helpers ---------------------------------------------------------------

type dispatchTestResponse struct {
	status   int
	envelope dispatchEnvelope
}

func postDispatch(t *testing.T, body string, tsAndSig signed) dispatchTestResponse {
	t.Helper()
	return postDispatchRaw(t, body, tsAndSig.ts, tsAndSig.sig)
}

func postDispatchRaw(t *testing.T, body, ts, sig string) dispatchTestResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, DispatchPath, strings.NewReader(body))
	if ts != "" {
		req.Header.Set(headerTimestamp, ts)
	}
	if sig != "" {
		req.Header.Set(headerSignature, sig)
	}
	HandleAgentToolDispatch(rec, req)
	respBytes, _ := io.ReadAll(rec.Result().Body)
	defer func() { _ = rec.Result().Body.Close() }()
	var env dispatchEnvelope
	_ = json.Unmarshal(respBytes, &env)
	return dispatchTestResponse{status: rec.Code, envelope: env}
}

type signed struct{ ts, sig string }

func signNow(body, secretHex string) signed {
	ts := time.Now().Unix()
	return signed{ts: strconv.FormatInt(ts, 10), sig: signAt(body, secretHex, ts)}
}

func signAt(body, secretHex string, ts int64) string {
	secret, _ := hex.DecodeString(secretHex)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write([]byte(body))
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

func staleEntry() RegisteredTool {
	return RegisteredTool{
		AgentName:     "demo",
		QualifiedName: "demo.t",
		HMACSecret:    dispatchSecretHex,
		Def: McpToolDef{
			Name:    "t",
			Handler: func(_ any) (any, error) { return nil, nil },
		},
	}
}
