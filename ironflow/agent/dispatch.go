// Package agent — inbound callback handler for Ironflow → SDK
// agent-tool dispatch (Go parity with @ironflow/node/agent dispatch.ts).
//
// The Ironflow server's agent_tools.Dispatcher POSTs an HMAC-signed
// request to {CallbackURL} (the user's Serve() mount). HandleAgentToolDispatch:
//
//  1. Verifies HMAC + replay window (5min past, 1min future).
//  2. Looks up the qualified tool in the local registry.
//  3. Runs McpToolDef.Validate (if set) on the decoded input.
//  4. Calls the handler, mapping success → {output} and any error →
//     200 + {error:{code:"HANDLER_ERROR", message}} so the Go server's
//     dispatcher decodes it as a tool error envelope (see
//     internal/agent_tools/dispatcher.go:182).
//
// Mounting: callers route POST DispatchPath at HandleAgentToolDispatch,
// or use DispatchHandler() for an http.Handler that only responds on
// the dispatch path.

package agent

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// DispatchPath is the HTTP path the SDK expects on its callback URL.
// CallbackURL passed to ExposeMcp must terminate at this suffix.
const DispatchPath = "/ironflow/agent-tools/dispatch"

const (
	headerSignature = "X-Ironflow-Signature"
	headerTimestamp = "X-Ironflow-Timestamp"
	signaturePrefix = "sha256="

	// Replay window. MUST stay in sync with the server-side constants
	// hmacReplayWindow + hmacFutureSkew in internal/agent_tools/hmac.go —
	// the server signs within those bounds and the SDK rejects outside
	// them. Drift here silently breaks dispatch.
	dispatchReplayWindow = 5 * time.Minute
	dispatchFutureSkew   = 1 * time.Minute

	// maxDispatchBodyBytes caps inbound dispatch payloads at 1 MiB.
	// The body is read BEFORE the HMAC check (the secret is selected
	// after parsing qualified_name) so an unauthenticated caller can
	// reach the io.ReadAll. The cap turns a memory-exhaustion vector
	// into a 400.
	maxDispatchBodyBytes = 1 << 20
)

// dispatchEnvelope is the response shape returned to the server.
// Exactly one of Output or Error is set per the agent_tools contract.
type dispatchEnvelope struct {
	Output any                  `json:"output,omitempty"`
	Error  *dispatchEnvelopeErr `json:"error,omitempty"`
}

type dispatchEnvelopeErr struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type dispatchPayload struct {
	QualifiedName string          `json:"qualified_name"`
	Input         json.RawMessage `json:"input"`
}

// DispatchHandler returns an http.Handler that responds to POST
// requests at exactly DispatchPath. r.URL.Path must equal DispatchPath
// — every other path 404s. Callers mounting under a prefix should
// strip the prefix so the handler sees the canonical path:
//
//	mux := http.NewServeMux()
//	mux.Handle("/api/ironflow/", ironflow.Serve(cfg))
//	mux.Handle("/api/ironflow"+agent.DispatchPath,
//	    http.StripPrefix("/api/ironflow", agent.DispatchHandler()))
//
// Exact-equality routing keeps the dispatch surface small enough for
// path-based WAF rules to whitelist DispatchPath verbatim.
func DispatchHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != DispatchPath {
			http.NotFound(w, r)
			return
		}
		HandleAgentToolDispatch(w, r)
	})
}

// HandleAgentToolDispatch processes an inbound dispatch request, writing
// status + envelope to w. Caller is responsible for routing — the
// handler does not check the URL path. The body is capped at
// maxDispatchBodyBytes via http.MaxBytesReader so unauthenticated
// callers cannot trigger unbounded reads.
func HandleAgentToolDispatch(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()
	rawBody, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxDispatchBodyBytes))
	if err != nil {
		writeEnvelope(w, http.StatusBadRequest, "INVALID_REQUEST", "failed to read body")
		return
	}

	sigHeader := r.Header.Get(headerSignature)
	tsHeader := r.Header.Get(headerTimestamp)
	if sigHeader == "" || tsHeader == "" {
		writeEnvelope(w, http.StatusUnauthorized, "SIGNATURE_MISMATCH", "missing HMAC headers")
		return
	}
	if !strings.HasPrefix(sigHeader, signaturePrefix) {
		writeEnvelope(w, http.StatusUnauthorized, "SIGNATURE_MISMATCH", "invalid signature format")
		return
	}

	tsUnix, err := strconv.ParseInt(tsHeader, 10, 64)
	if err != nil {
		writeEnvelope(w, http.StatusUnauthorized, "TIMESTAMP_SKEW", "invalid timestamp")
		return
	}
	now := time.Now()
	requestTime := time.Unix(tsUnix, 0)
	if now.Sub(requestTime) > dispatchReplayWindow {
		writeEnvelope(w, http.StatusUnauthorized, "TIMESTAMP_SKEW", "request timestamp too old")
		return
	}
	if requestTime.Sub(now) > dispatchFutureSkew {
		writeEnvelope(w, http.StatusUnauthorized, "TIMESTAMP_SKEW", "request timestamp too far in future")
		return
	}

	var payload dispatchPayload
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		writeEnvelope(w, http.StatusBadRequest, "INVALID_REQUEST", "callback body is not valid JSON")
		return
	}
	if payload.QualifiedName == "" {
		writeEnvelope(w, http.StatusBadRequest, "INVALID_REQUEST", "qualified_name missing")
		return
	}

	entry, ok := lookupLocal(payload.QualifiedName)
	if !ok {
		log.Printf("ironflow.agent.dispatch unknown_tool qualified_name=%q", payload.QualifiedName)
		writeEnvelope(w, http.StatusUnauthorized, "SIGNATURE_MISMATCH", "HMAC mismatch")
		return
	}

	receivedHex := sigHeader[len(signaturePrefix):]
	if !verifyHMAC(rawBody, tsUnix, receivedHex, entry.HMACSecret) {
		writeEnvelope(w, http.StatusUnauthorized, "SIGNATURE_MISMATCH", "HMAC mismatch")
		return
	}

	var input any
	if len(payload.Input) > 0 {
		if err := json.Unmarshal(payload.Input, &input); err != nil {
			writeEnvelope(w, http.StatusBadRequest, "INPUT_SCHEMA_INVALID", "input is not valid JSON: "+err.Error())
			return
		}
	}

	if entry.Def.Validate != nil {
		if err := entry.Def.Validate(input); err != nil {
			writeEnvelope(w, http.StatusBadRequest, "INPUT_SCHEMA_INVALID", err.Error())
			return
		}
	}

	output, handlerErr := entry.Def.Handler(input)
	if handlerErr != nil {
		// 200 + envelope so the Go dispatcher decodes it as a tool
		// error rather than a transport failure (mirrors JS S6).
		writeJSON(w, http.StatusOK, dispatchEnvelope{
			Error: &dispatchEnvelopeErr{Code: "HANDLER_ERROR", Message: handlerErr.Error()},
		})
		return
	}
	writeJSON(w, http.StatusOK, dispatchEnvelope{Output: output})
}

func writeEnvelope(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, dispatchEnvelope{
		Error: &dispatchEnvelopeErr{Code: code, Message: message},
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// verifyHMAC mirrors @ironflow/node/agent dispatch.ts verifyHmac. The
// canonical signed payload is `{ts}.{rawBody}`.
func verifyHMAC(rawBody []byte, ts int64, receivedHex, secretHex string) bool {
	secret, err := hex.DecodeString(secretHex)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(rawBody)
	expected := mac.Sum(nil)

	received, err := hex.DecodeString(receivedHex)
	if err != nil {
		return false
	}
	if len(received) != len(expected) || len(received) == 0 {
		return false
	}
	return hmac.Equal(received, expected)
}
