package agent

import (
	"errors"
	"testing"

	"github.com/sahina/ironflow-go/ironflow"
)

func TestLLM_HappyPath_IncrementsTurn(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	interceptor.stepMocks["llm.turn"] = func() (any, error) {
		return LLMCompleteResult{Content: "hello"}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
	}, interceptor)

	if ctx.Turn() != 0 {
		t.Fatalf("Turn() before call = %d, want 0", ctx.Turn())
	}
	res, err := LLM(ctx, LLMCompleteRequest{
		Call: func() (LLMCompleteResult, error) { return LLMCompleteResult{Content: "hello"}, nil },
	})
	if err != nil {
		t.Fatalf("LLM: %v", err)
	}
	if res.Content != "hello" {
		t.Errorf("Content = %v, want 'hello'", res.Content)
	}
	if ctx.Turn() != 1 {
		t.Errorf("Turn() after call = %d, want 1", ctx.Turn())
	}
}

func TestLLM_MaxTurnsExceeded(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	interceptor.stepMocks["llm.turn"] = func() (any, error) {
		return LLMCompleteResult{}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
		MaxTurns: 2,
	}, interceptor)

	for i := 0; i < 2; i++ {
		if _, err := LLM(ctx, LLMCompleteRequest{
			Call: func() (LLMCompleteResult, error) { return LLMCompleteResult{}, nil },
		}); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	_, err := LLM(ctx, LLMCompleteRequest{
		Call: func() (LLMCompleteResult, error) { return LLMCompleteResult{}, nil },
	})
	if err == nil {
		t.Fatal("expected MaxTurnsExceededError")
	}
	var me *MaxTurnsExceededError
	if !errors.As(err, &me) {
		t.Fatalf("err is not MaxTurnsExceededError: %T %v", err, err)
	}
	if me.MaxTurns != 2 {
		t.Errorf("MaxTurns = %d, want 2", me.MaxTurns)
	}
}

func TestLLM_ClassifiesRefusal(t *testing.T) {
	cases := []string{"refusal", "safety", "content_filter"}
	for _, reason := range cases {
		t.Run(reason, func(t *testing.T) {
			interceptor := newFakeInterceptor(t)
			interceptor.stepMocks["llm.turn"] = func() (any, error) {
				return LLMCompleteResult{FinishReason: reason}, nil
			}
			ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)
			_, err := LLM(ctx, LLMCompleteRequest{
				Call: func() (LLMCompleteResult, error) { return LLMCompleteResult{FinishReason: reason}, nil },
			})
			if err == nil {
				t.Fatal("expected refusal error")
			}
			var rerr *LLMRefusalError
			if !errors.As(err, &rerr) {
				t.Fatalf("not LLMRefusalError: %T %v", err, err)
			}
			if rerr.Code != CodeLLMRefusal {
				t.Errorf("Code = %q, want %q", rerr.Code, CodeLLMRefusal)
			}
		})
	}
}

func TestLLM_ClassifiesMaxTokens(t *testing.T) {
	for _, reason := range []string{"max_tokens", "length"} {
		t.Run(reason, func(t *testing.T) {
			interceptor := newFakeInterceptor(t)
			interceptor.stepMocks["llm.turn"] = func() (any, error) {
				return LLMCompleteResult{FinishReason: reason}, nil
			}
			ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)
			_, err := LLM(ctx, LLMCompleteRequest{
				Call: func() (LLMCompleteResult, error) { return LLMCompleteResult{FinishReason: reason}, nil },
			})
			if err == nil {
				t.Fatal("expected max-tokens error")
			}
			var mt *LLMMaxTokensError
			if !errors.As(err, &mt) {
				t.Fatalf("not LLMMaxTokensError: %T %v", err, err)
			}
		})
	}
}

func TestLLM_PassesThroughUnknownFinishReason(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	interceptor.stepMocks["llm.turn"] = func() (any, error) {
		return LLMCompleteResult{FinishReason: "stop"}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)
	res, err := LLM(ctx, LLMCompleteRequest{
		Call: func() (LLMCompleteResult, error) { return LLMCompleteResult{FinishReason: "stop"}, nil },
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.FinishReason != "stop" {
		t.Errorf("FinishReason = %q, want 'stop'", res.FinishReason)
	}
}
