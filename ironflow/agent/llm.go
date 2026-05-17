package agent

import (
	"strings"

	"github.com/sahina/ironflow-go/ironflow"
)

// LLM runs a memoized completion with classified error surface.
//
// The wrapper does not own a provider router. Callers pass a Call
// closure that talks to their provider of choice and returns a
// normalized LLMCompleteResult. The wrapper:
//
//  1. Increments the per-run turn counter and gates against MaxTurns.
//  2. Wraps Call in ironflow.Run so the assistant response is memoized
//     for crash-resume.
//  3. Inspects FinishReason to raise classified errors:
//     - "refusal" / "safety" / "content_filter" → LLMRefusalError
//     - "max_tokens" / "length" → LLMMaxTokensError
//
// JSON-parse failures must be raised by the caller, by detecting
// invalid JSON in their closure and returning LLMInvalidJSONError.
func LLM(ctx Context, req LLMCompleteRequest) (LLMCompleteResult, error) {
	if ctx.runtime != nil {
		ctx.runtime.mu.Lock()
		ctx.runtime.turnCount++
		current := ctx.runtime.turnCount
		max := ctx.runtime.maxTurns
		ctx.runtime.mu.Unlock()
		if current > max {
			return LLMCompleteResult{}, NewMaxTurnsExceededError(max)
		}
	}

	result, err := ironflow.Run(ctx.Inner, "llm.turn", func() (LLMCompleteResult, error) {
		if req.Call == nil {
			return LLMCompleteResult{}, NewLLMRefusalError("llm.complete: request.Call is nil", nil)
		}
		return req.Call()
	})
	if err != nil {
		return result, err
	}

	if classified := classifyResult(result); classified != nil {
		return result, classified
	}
	return result, nil
}

// classifyResult returns a typed error when the provider's
// FinishReason matches a known classification. Returns nil otherwise
// (caller proceeds with the result).
func classifyResult(result LLMCompleteResult) error {
	if result.FinishReason == "" {
		return nil
	}
	normalized := strings.ToLower(result.FinishReason)
	switch normalized {
	case "refusal", "safety", "content_filter":
		return NewLLMRefusalError(
			"provider refused: "+result.FinishReason,
			map[string]any{
				"finishReason": result.FinishReason,
				"metadata":     result.Metadata,
			},
		)
	case "max_tokens", "length":
		return NewLLMMaxTokensError(
			"provider hit max_tokens ("+result.FinishReason+")",
			map[string]any{
				"finishReason": result.FinishReason,
				"metadata":     result.Metadata,
			},
		)
	default:
		return nil
	}
}
