package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

func TestEngineExplicitPauseContinuesBeforeCompletionChecks(t *testing.T) {
	checks := 0
	model := &scriptedProvider{turns: [][]provider.Event{
		{{Kind: provider.EventTextDelta, Text: "Still working."}, {Kind: provider.EventDone, StopReason: provider.StopReasonPause, Usage: usagePerTurn(2)}},
		{{Kind: provider.EventDone, StopReason: provider.StopReasonPause, Usage: usagePerTurn(2)}},
		{{Kind: provider.EventTextDelta, Text: "Finished."}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: usagePerTurn(2)}},
	}}
	engine := Engine{Provider: model,
		OutputGuardrails: []OutputGuardrail{NewOutputGuardrail("verify-final", func(context.Context, OutputGuardrailInput) (OutputGuardrailResult, error) {
			checks++
			return AllowOutput(), nil
		})},
		Boundaries: BoundaryObserverFunc(func(_ context.Context, value Continuation) error { return ValidateContinuation(value) }),
	}
	result := engine.Run(context.Background(), Request{Prompt: "Do the task."}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatal(result.Failure)
	}
	if result.Text != "Finished." || result.Usage.TotalTokens != 6 || checks != 1 {
		t.Fatalf("result=%+v checks=%d", result, checks)
	}
	users := 0
	for _, current := range result.Messages {
		if current.Role == message.RoleUser {
			users++
		}
	}
	if users != 1 {
		t.Fatalf("pause injected synthetic user prompts: %d", users)
	}
}

func TestPauseAtIterationCeilingIsNotSuccessfulCompletion(t *testing.T) {
	model := &scriptedProvider{turns: [][]provider.Event{{{Kind: provider.EventTextDelta, Text: "Working"}, {Kind: provider.EventDone, StopReason: provider.StopReasonPause}}}}
	result := (Engine{Provider: model, LoopPolicy: LoopPolicy{MaxIterations: 1}}).Run(context.Background(), Request{Prompt: "Do the task."}, OutputPolicy{})
	if result.Failure == nil || !errors.Is(result.Failure, ErrPauseContinuationLimit) {
		t.Fatalf("pause reported as successful: %+v", result)
	}
}
