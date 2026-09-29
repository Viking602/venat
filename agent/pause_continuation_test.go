package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

func TestFinishPausedTurn_PreservesContextAndUsage(t *testing.T) {
	var observed Step
	contextUsage := provider.Usage{InputTokens: 2, OutputTokens: 3, TotalTokens: 5}
	engine := Engine{}
	input := LoopInput{
		StepObserver: StepObserverFunc(func(_ context.Context, step Step) error {
			observed = step
			return nil
		}),
		contextUsage: &contextUsage,
	}
	assistant := message.Message{
		Role:          message.RoleAssistant,
		ProviderState: []byte(`[{"type":"reasoning","id":"rs_1"}]`),
	}
	modelCall := &ModelCall{Model: "pause-model", InputTokens: 4, OutputTokens: 6, TotalTokens: 10, StopReason: provider.StopReasonPause}
	current, steps, out, retry, err := engine.finishPausedTurn(
		context.Background(), input, []message.Message{message.NewText(message.RoleUser, "work")},
		assistant, modelCall, provider.Usage{InputTokens: 4, OutputTokens: 6, TotalTokens: 10}, nil, 0, 0,
		provider.StopReasonPause,
	)
	if err != nil {
		t.Fatalf("finishPausedTurn() error = %v", err)
	}
	if !retry || out.StopReason != "" {
		t.Fatalf("pause result = retry %v, output %#v; want retry with no terminal output", retry, out)
	}
	if len(current) != 2 || current[1].Role != message.RoleAssistant || string(current[1].ProviderState) != string(assistant.ProviderState) {
		t.Fatalf("paused assistant context = %#v, want provider state preserved", current[1:])
	}
	if len(steps) != 1 || steps[0].Decision != StepDecisionContinue || len(steps[0].Observations) != 1 ||
		steps[0].Observations[0].Kind != pauseContinuationObservation {
		t.Fatalf("pause step = %#v, want continue observation", steps)
	}
	if steps[0].BudgetUsed.Tokens != 10 || steps[0].ContextUsage.TotalTokens != 5 {
		t.Fatalf("pause accounting = %#v, want model and context usage", steps[0])
	}
	if observed.Decision != StepDecisionContinue {
		t.Fatalf("observed decision = %q, want continue", observed.Decision)
	}
}

func TestFinishPausedTurn_CapsConsecutivePauses(t *testing.T) {
	steps := make([]Step, maxPauseContinuations)
	for index := range steps {
		steps[index] = Step{
			Index:        index,
			ModelCall:    &ModelCall{Model: "pause-model", OutputTokens: 1, TotalTokens: 1, StopReason: provider.StopReasonPause},
			Decision:     StepDecisionContinue,
			Observations: []Observation{{Kind: pauseContinuationObservation}},
			BudgetUsed:   BudgetUsage{Tokens: int64(index + 1)},
		}
	}
	assistant := message.Message{Role: message.RoleAssistant, Text: "still working"}
	_, gotSteps, out, retry, err := (Engine{}).finishPausedTurn(
		context.Background(), LoopInput{}, nil, assistant,
		&ModelCall{Model: "pause-model", OutputTokens: 1, TotalTokens: 1, StopReason: provider.StopReasonPause},
		provider.Usage{OutputTokens: 1, TotalTokens: maxPauseContinuations + 1}, steps, maxPauseContinuations, 0,
		provider.StopReasonPause,
	)
	if retry || out.StopReason != provider.StopReasonError {
		t.Fatalf("capped pause result = retry %v, stop %q; want factual failure", retry, out.StopReason)
	}
	var limitErr *PauseContinuationLimitError
	if !errors.As(err, &limitErr) || !errors.Is(err, ErrPauseContinuationLimit) {
		t.Fatalf("capped pause error = %v, want typed limit error", err)
	}
	if limitErr.Consecutive != maxPauseContinuations+1 || len(gotSteps) != maxPauseContinuations+1 ||
		gotSteps[len(gotSteps)-1].Decision != StepDecisionFail {
		t.Fatalf("capped pause trace = consecutive %d, steps %d, decision %q", limitErr.Consecutive, len(gotSteps), gotSteps[len(gotSteps)-1].Decision)
	}
}

func TestFinishPausedTurn_RespectsCancellationAndActualStopReason(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, retry, err := (Engine{}).finishPausedTurn(
		ctx, LoopInput{}, nil, message.Message{Role: message.RoleAssistant, Text: "paused"},
		&ModelCall{Model: "pause-model", OutputTokens: 1, TotalTokens: 1, StopReason: provider.StopReasonPause},
		provider.Usage{OutputTokens: 1, TotalTokens: 1}, nil, 0, 0, provider.StopReasonPause,
	)
	if retry || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled pause = retry %v, error %v; want cancellation", retry, err)
	}

	_, _, _, retry, err = (Engine{}).finishPausedTurn(
		context.Background(), LoopInput{}, nil, message.Message{Role: message.RoleAssistant, Text: "done"},
		&ModelCall{Model: "model", OutputTokens: 1, TotalTokens: 1, StopReason: provider.StopReasonComplete},
		provider.Usage{OutputTokens: 1, TotalTokens: 1}, nil, 0, 0, provider.StopReasonComplete,
	)
	if retry || err == nil {
		t.Fatalf("non-pause stop reason = retry %v, error %v; want no continuation and an explicit misuse error", retry, err)
	}
}

func TestConsecutivePauseContinuations_ResetAfterToolProgress(t *testing.T) {
	steps := []Step{
		{Observations: []Observation{{Kind: pauseContinuationObservation}}},
		{ToolCalls: []ToolCallTrace{{ID: "call-1", Name: "lookup"}}},
		{Observations: []Observation{{Kind: pauseContinuationObservation}}},
	}
	if got := consecutivePauseContinuations(steps); got != 1 {
		t.Fatalf("consecutive pauses after tool progress = %d, want 1", got)
	}
}
