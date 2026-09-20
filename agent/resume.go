package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

// Resume continues a validated execution without rebuilding its initial
// context.
func (e Engine) Resume(ctx context.Context, continuation Continuation) Result {
	return e.resume(ctx, continuation, nil)
}

// ResumeStream is Resume with a transient live output Sink.
func (e Engine) ResumeStream(ctx context.Context, continuation Continuation, sink Sink) Result {
	return e.resume(ctx, continuation, sink)
}

func (e Engine) resume(ctx context.Context, continuation Continuation, sink Sink) Result {
	if err := ValidateContinuation(continuation); err != nil {
		return Result{Failure: (&AgentFailure{Kind: FailureKindEngineError, Reason: err.Error()}).WithCause(err)}
	}
	continuation = currentContinuation(continuation)
	if continuation.Request.SessionBudget == nil {
		continuation.Request.SessionBudget = cloneSessionBudget(e.LoopPolicy.SessionBudget)
	}
	if continuation.Request.ModelTimeouts == nil {
		resolved := e.modelTimeouts(continuation.Request)
		continuation.Request.ModelTimeouts = &resolved
	}
	if err := continuation.Request.Validate(); err != nil {
		result := continuationResult(continuation)
		result.Failure = (&AgentFailure{Kind: FailureKindEngineError, Reason: err.Error()}).WithCause(err)
		return result
	}
	format, policyErr := prepareOutputPolicy(continuation.OutputPolicy)
	if policyErr != nil {
		result := continuationResult(continuation)
		result.Failure = schemaInvalidFailure(policyErr)
		return result
	}
	controlledCtx, finish, controlErr := e.Control.start(ctx)
	if controlErr != nil {
		return resultWithFailure(ctx, continuationResult(continuation), controlErr, false)
	}
	defer finish()
	ctx = controlledCtx
	started := time.Now()
	runCtx, cancelRun, budgetDriven := e.runContextWithElapsed(ctx, continuation.Request, continuation.ActiveElapsed)
	defer cancelRun()

	runtime := newSkillRuntime(e.Skills, e.AvailableSkills)
	e.AvailableSkills = runtime.availableSkills()
	var err error
	e.Tools, err = runtime.attachTools(e.Tools)
	if err != nil {
		return resultWithFailure(runCtx, continuationResult(continuation), err, budgetDriven)
	}
	runtime.restoreActivations(continuation.Messages)
	compact, compactTo := e.compactors(runtime)
	maxTokens, maxToolCalls, maxSteps := e.budgetLimits(continuation.Request)
	input := LoopInput{
		Model:                e.Model,
		Temperature:          e.Temperature,
		TopP:                 e.TopP,
		ModelMaxTokens:       e.ModelMaxTokens,
		Messages:             message.CloneMessages(continuation.Messages),
		ToolMode:             e.ToolMode,
		MaxIterations:        e.LoopPolicy.MaxIterations,
		UnlimitedIterations:  e.LoopPolicy.UnlimitedIterations,
		MaxTokens:            maxTokens,
		MaxToolCalls:         maxToolCalls,
		MaxSteps:             maxSteps,
		ModelTimeouts:        e.modelTimeouts(continuation.Request),
		ContextTokenTarget:   e.LoopPolicy.ContextTokenTarget,
		OperationTurn:        continuation.NextOperationTurn,
		StopSequences:        e.StopSequences,
		ThinkingBudget:       e.ThinkingBudget,
		ResponseFormat:       format,
		ExtraBody:            e.ExtraBody,
		PromptCacheKey:       e.PromptCacheKey,
		ServiceTier:          e.ServiceTier,
		ParallelToolCalls:    cloneBoolPointer(e.ParallelToolCalls),
		ContextUsage:         e.ContextUsage,
		OutputGuardrails:     e.OutputGuardrails,
		OutputObserver:       e.OutputObserver,
		Sink:                 sink,
		Control:              e.Control,
		controlBound:         true,
		StepDecider:          e.StepDecider,
		StepObserver:         e.StepObserver,
		Compact:              compact,
		CompactTo:            compactTo,
		continuationRequest:  cloneRequest(continuation.Request),
		continuationPolicy:   continuation.OutputPolicy,
		repairCount:          continuation.RepairCount,
		activeElapsed:        continuation.ActiveElapsed,
		segmentStarted:       started,
		initialUsage:         continuation.Usage,
		initialSteps:         cloneSteps(continuation.Steps),
		initialToolCallsUsed: continuation.ToolCallsUsed,
	}

	if err := runCtx.Err(); err != nil {
		return resultWithFailure(runCtx, continuationResult(continuation), err, budgetDriven)
	}
	output, runErr := e.resumeLoop(runCtx, input, continuation)
	if runErr != nil {
		failed := resultFromLoopOutput(output, output.RepairCount)
		failed.Failure = loopErrorFailure(runCtx, runErr, budgetDriven)
		return failed
	}
	result := resultFromLoopOutput(output, output.RepairCount)
	policy := continuation.OutputPolicy
	if !policy.Validate || len(policy.Schema) == 0 {
		return result
	}
	return e.validateAndRepairStructuredOutput(runCtx, input, output, result, policy, budgetDriven)
}

// The backend verifies the original version/hash before invoking Resume. This
// pure upgrade affects only execution-owned memory; the next boundary atomically
// writes a current-version continuation and its new hash through the backend.
func currentContinuation(value Continuation) Continuation {
	value = cloneContinuation(value)
	value.SchemaVersion = ContinuationSchemaVersion
	return value
}

func (e Engine) resumeLoop(ctx context.Context, input LoopInput, continuation Continuation) (LoopOutput, error) {
	switch continuation.Phase {
	case ContinuationReady:
		return e.RunMessages(ctx, input)
	case ContinuationModelComplete:
		return e.resumeModelComplete(ctx, input, continuation)
	case ContinuationToolsComplete:
		return e.resumeToolsComplete(ctx, input, continuation)
	case ContinuationValidatingOutput:
		return e.resumeValidatingOutput(ctx, input, continuation)
	default:
		return decorateResumedOutput(continuationOutput(continuation), input), continuationError("unknown phase %q", continuation.Phase)
	}
}

func (e Engine) resumeModelComplete(ctx context.Context, input LoopInput, continuation Continuation) (LoopOutput, error) {
	messages := message.CloneMessages(continuation.Messages)
	steps := cloneSteps(continuation.Steps)
	assistant := messages[len(messages)-1]
	modelCall := steps[len(steps)-1].ModelCall
	iteration := steps[len(steps)-1].Index
	messages = messages[:len(messages)-1]
	steps = steps[:len(steps)-1]
	var operationTurn, operationSlot int
	_, _ = fmt.Sscanf(assistant.ToolCalls[0].OperationID, "turn:%d:call:%d", &operationTurn, &operationSlot)
	usage := continuation.Usage
	toolCallsUsed := continuation.ToolCallsUsed
	out, stop, err := e.runToolStep(
		ctx, &input, &messages, assistant, modelCall, &usage, &steps,
		iteration, &toolCallsUsed, operationTurn,
	)
	if stop || err != nil {
		return decorateResumedOutput(out, input), err
	}
	input.Messages = messages
	input.initialUsage = usage
	input.initialSteps = steps
	input.initialToolCallsUsed = toolCallsUsed
	return e.RunMessages(ctx, input)
}

func (e Engine) resumeToolsComplete(ctx context.Context, input LoopInput, continuation Continuation) (LoopOutput, error) {
	latest := continuation.Steps[len(continuation.Steps)-1]
	switch latest.Decision {
	case StepDecisionContinue, "":
		return e.RunMessages(ctx, input)
	case StepDecisionFinish:
		return decorateResumedOutput(continuationOutput(continuation), input), nil
	case StepDecisionFail:
		return decorateResumedOutput(continuationOutput(continuation), input), fmt.Errorf("%w: resumed failed tool step", ErrStepAborted)
	default:
		return decorateResumedOutput(continuationOutput(continuation), input), continuationError("tools_complete phase has decision %q", latest.Decision)
	}
}

func (e Engine) resumeValidatingOutput(ctx context.Context, input LoopInput, continuation Continuation) (LoopOutput, error) {
	messages := message.CloneMessages(continuation.Messages)
	steps := cloneSteps(continuation.Steps)
	assistant := messages[len(messages)-1]
	latest := steps[len(steps)-1]
	messages = messages[:len(messages)-1]
	steps = steps[:len(steps)-1]
	current, finalizedSteps, out, retry, err := e.finalizeNoToolStep(
		ctx, input, messages, assistant, latest.ModelCall, continuation.Usage,
		steps, latest.ModelCall.StopReason, latest.Index, continuation.ToolCallsUsed,
	)
	if err != nil || !retry {
		return decorateResumedOutput(out, input), err
	}
	input.Messages = current
	input.initialUsage = continuation.Usage
	input.initialSteps = finalizedSteps
	input.initialToolCallsUsed = continuation.ToolCallsUsed
	return e.RunMessages(ctx, input)
}

func continuationOutput(continuation Continuation) LoopOutput {
	return LoopOutput{
		Messages:          message.CloneMessages(continuation.Messages),
		Usage:             continuation.Usage,
		StopReason:        continuationStopReason(continuation),
		Iterations:        len(continuation.Steps),
		Thinking:          continuationThinking(continuation.Messages),
		Steps:             cloneSteps(continuation.Steps),
		ToolCallsUsed:     continuation.ToolCallsUsed,
		RepairCount:       continuation.RepairCount,
		ActiveElapsed:     continuation.ActiveElapsed,
		NextOperationTurn: continuation.NextOperationTurn,
	}
}

func decorateResumedOutput(output LoopOutput, input LoopInput) LoopOutput {
	output.NextOperationTurn = input.OperationTurn
	output.RepairCount = input.repairCount
	output.ActiveElapsed = input.activeElapsed + time.Since(input.segmentStarted)
	return output
}

func continuationResult(continuation Continuation) Result {
	output := continuationOutput(continuation)
	return resultFromLoopOutput(output, continuation.RepairCount)
}

func continuationStopReason(continuation Continuation) provider.StopReason {
	if len(continuation.Steps) == 0 {
		return ""
	}
	latest := continuation.Steps[len(continuation.Steps)-1]
	if continuation.Phase == ContinuationToolsComplete {
		switch latest.Decision {
		case StepDecisionFinish:
			return provider.StopReasonComplete
		case StepDecisionFail:
			return provider.StopReasonError
		}
	}
	if latest.ModelCall == nil {
		return ""
	}
	return latest.ModelCall.StopReason
}

func continuationThinking(messages []message.Message) string {
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].Role == message.RoleAssistant {
			return messages[index].Thinking
		}
	}
	return ""
}

func resultWithFailure(ctx context.Context, result Result, err error, budgetDriven bool) Result {
	if err == nil {
		return result
	}
	result.Failure = loopErrorFailure(ctx, err, budgetDriven)
	return result
}
