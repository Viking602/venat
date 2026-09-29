package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

const maxPauseContinuations = 8

const pauseContinuationObservation = "pause_continuation"

// ErrPauseContinuationLimit identifies a factual failure after repeated
// explicit provider pauses.
var ErrPauseContinuationLimit = errors.New("pause continuation limit exceeded")

// PauseContinuationLimitError reports that a provider kept a turn paused
// beyond the bounded continuation policy. The model output and usage from the
// last pause remain in the returned trace; the run is not reported as success.
type PauseContinuationLimitError struct {
	Consecutive int
	Limit       int
}

func (e *PauseContinuationLimitError) Error() string {
	if e == nil {
		return "<nil pause continuation limit error>"
	}
	return fmt.Sprintf("agent: provider paused %d consecutive times (limit %d)", e.Consecutive, e.Limit)
}

func (e *PauseContinuationLimitError) Is(target error) bool {
	return target == ErrPauseContinuationLimit
}

// finishPausedTurn records an explicit provider pause and schedules another
// model turn without adding a synthetic user instruction. It intentionally
// runs before output guardrails and ordinary empty-output recovery: a pause is
// an incomplete model turn, not a terminal answer.
func (e Engine) finishPausedTurn(
	ctx context.Context,
	input LoopInput,
	current []message.Message,
	assistant message.Message,
	modelCall *ModelCall,
	totalUsage provider.Usage,
	steps []Step,
	iteration int,
	toolCallsUsed int,
	stopReason provider.StopReason,
) ([]message.Message, []Step, LoopOutput, bool, error) {
	if stopReason != provider.StopReasonPause {
		return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false,
			fmt.Errorf("agent: pause continuation called for stop reason %q", stopReason)
	}

	current = append(current, message.Clone(assistant))
	consecutive := consecutivePauseContinuations(steps) + 1
	step := Step{
		Index:     iteration,
		ModelCall: modelCall,
		Decision:  StepDecisionContinue,
		Observations: []Observation{{
			Kind:    pauseContinuationObservation,
			Message: fmt.Sprintf("provider pause; continuing the same task (%d/%d)", consecutive, maxPauseContinuations),
		}},
		BudgetUsed: BudgetUsage{Tokens: int64(totalUsage.TotalTokens), ToolCalls: toolCallsUsed},
	}
	if input.contextUsage != nil {
		step.ContextUsage = *input.contextUsage
	}
	steps = append(steps, step)

	var continuationErr error
	switch {
	case ctx.Err() != nil:
		steps[len(steps)-1].Decision = StepDecisionFail
		continuationErr = ctx.Err()
	case input.MaxTokens > 0 && modelCall.TotalTokens == 0:
		steps[len(steps)-1].Decision = StepDecisionFail
		continuationErr = fmt.Errorf("%w: paused response reported no usage", ErrBudgetExhausted)
	case consecutive > maxPauseContinuations && !input.Control.hasSteer():
		steps[len(steps)-1].Decision = StepDecisionFail
		continuationErr = &PauseContinuationLimitError{
			Consecutive: consecutive,
			Limit:       maxPauseContinuations,
		}
	}
	if continuationErr != nil {
		if observeErr := observeFinalizedStep(ctx, input.StepObserver, steps); observeErr != nil {
			continuationErr = errors.Join(continuationErr, observeErr)
		}
		return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false, continuationErr
	}
	if observeErr := observeFinalizedStep(ctx, input.StepObserver, steps); observeErr != nil {
		return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false, observeErr
	}
	if boundaryErr := e.observeBoundary(ctx, input, current, totalUsage, steps, toolCallsUsed, ContinuationReady); boundaryErr != nil {
		return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false, boundaryErr
	}
	return current, steps, LoopOutput{}, true, nil
}

func consecutivePauseContinuations(steps []Step) int {
	count := 0
	for index := len(steps) - 1; index >= 0; index-- {
		if !hasPauseContinuation(steps[index]) {
			break
		}
		count++
	}
	return count
}

func hasPauseContinuation(step Step) bool {
	paused := false
	for _, observation := range step.Observations {
		if observation.Kind == "pause_input_progress" {
			return false
		}
		if observation.Kind == pauseContinuationObservation {
			paused = true
		}
	}
	return paused
}

func pauseExhaustionError(steps []Step) error {
	consecutive := consecutivePauseContinuations(steps)
	if consecutive == 0 {
		return nil
	}
	return fmt.Errorf("%w: loop allowance exhausted while the provider remained paused (%d consecutive pauses)", ErrPauseContinuationLimit, consecutive)
}

func resetPauseForInput(steps []Step, inputCount int) {
	if inputCount == 0 || len(steps) == 0 || !hasPauseContinuation(steps[len(steps)-1]) {
		return
	}
	latest := &steps[len(steps)-1]
	latest.Observations = append(latest.Observations, Observation{Kind: "pause_input_progress", Message: "new input consumed at the next model boundary"})
}
