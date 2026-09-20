package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/skill"
	"github.com/Viking602/venat/tool"
)

var ErrToolBusMissing = errors.New("tool bus missing")

const (
	maxProviderTurnEvents = 65_536
	maxProviderTurnBytes  = 64 << 20
)

// ErrProviderTurnLimit reports a provider stream that exceeded the
// provider-neutral per-turn event or decoded-byte ceiling.
var ErrProviderTurnLimit = errors.New("provider turn exceeds safe stream limits")

// ErrBudgetExhausted is returned by RunMessages, wrapped with the exhausted
// dimension, when a per-loop budget (MaxTokens/MaxToolCalls/MaxSteps) is hit
// on a turn that would otherwise continue. The accompanying LoopOutput is the
// partial trace accumulated so far. Engine.run maps it to a
// FailureKindBudgetExhausted Result.
var ErrBudgetExhausted = errors.New("agent loop budget exhausted")

// ErrPanicRecovered wraps a panic recovered by RunMessages. Caller extension
// panics degrade to a typed engine failure instead of crashing the process.
// HookChain and parallel tool execution recover their own panics closer to the
// source; errors.Is(err, ErrPanicRecovered) still identifies loop recovery.
var ErrPanicRecovered = errors.New("agent loop recovered a panic")

// ErrStepAborted reports a StepDecider failure or explicit fail decision.
var ErrStepAborted = errors.New("agent loop aborted by step decider")

// LoopInput is the low-level message-oriented input to RunMessages.
type LoopInput struct {
	Model       string
	Messages    []message.Message
	Temperature float64
	TopP        float64
	// ModelMaxTokens caps output tokens for each provider request. MaxTokens
	// below remains the cumulative loop-spend budget.
	ModelMaxTokens int
	Metadata       map[string]string
	ToolMode       tool.Mode
	MaxIterations  int
	// UnlimitedIterations explicitly disables the model-turn ceiling. When
	// false, an unset MaxIterations retains the conservative default of 12.
	UnlimitedIterations bool
	// OperationTurn is the next durable tool-call turn ordinal. Checkpoint
	// recovery restores it so compaction cannot reuse a prior operation ID.
	OperationTurn int
	OnEvent       func(provider.Event) error

	// Sink receives transient provider and tool-result frames as the loop runs.
	// Its errors abort the current turn.
	Sink Sink
	// Control optionally accepts user input at safe model boundaries.
	Control *Control

	StopSequences  []string
	ThinkingBudget int
	ResponseFormat *provider.ResponseFormat
	// godoc-allow-any: provider-specific request extensions are intentionally open.
	ExtraBody         map[string]any
	PromptCacheKey    string
	ServiceTier       string
	ParallelToolCalls *bool
	ContextUsage      provider.ContextUsageObserver

	OutputGuardrails []OutputGuardrail
	OutputObserver   OutputGuardrailObserver

	// MaxTokens / MaxToolCalls / MaxSteps are the per-loop budget ceilings.
	// Zero means unbounded on that dimension. They are enforced fail-closed
	// but only on turns that would continue the loop: a run that is about to
	// finish is never failed for a budget it has not yet exceeded. MaxSteps
	// is a hard ceiling (exhausting it fails the run with ErrBudgetExhausted),
	// distinct from MaxIterations, whose soft ceiling yields StopReasonMaxTurns.
	// A successful turn that reports no usage fails closed under a positive
	// token ceiling before a final answer is accepted or tools are dispatched.
	MaxTokens    int64
	MaxToolCalls int
	MaxSteps     int
	// ModelTimeouts bounds provider connection, total-request, and stream-idle
	// phases for each model turn. Zero values use Codex-compatible defaults.
	ModelTimeouts ModelTimeoutPolicy

	// ContextTokenTarget is the usable token allowance for message history in
	// one provider request, after the caller reserves room for output, tools,
	// schemas, reasoning, and provider framing. It is independent of MaxTokens,
	// which remains the cumulative run-spend ceiling. When positive, the loop
	// prepares context before every model turn, including the first.
	// Without custom compactors it bounds the provider view using a conservative
	// text estimate while preserving the full execution transcript. Media needs
	// a caller-supplied model-aware CompactTo.
	ContextTokenTarget int

	// StepDecider may override the natural decision at continue boundaries.
	StepDecider StepDecider

	// StepObserver receives each finalized step before the loop advances.
	StepObserver StepObserver

	// Compact is the source-compatible history compaction hook. With no
	// ContextTokenTarget it retains the legacy trigger: the loop invokes it once
	// cumulative MaxTokens spend enters the final headroom band. With a positive
	// target and no CompactTo, the loop invokes Compact before every request as a
	// best-effort fallback, including when MaxTokens is zero; because Compact does
	// not receive the target it cannot guarantee a fit. Engine.Run wires this from
	// ContextManager.Compact.
	//
	// Consumed tokens only grow, so once the loop enters the headroom band the
	// trigger holds for every remaining turn and Compact runs before each one. A
	// compactor must therefore be idempotent — cheap and stable on a history it
	// already compacted — not a one-shot transform.
	//
	// Determinism: the loop triggers Compact deterministically (the same trigger
	// fires on replay), so a deterministic compactor keeps the run
	// replay-faithful (ADR-007) while an LLM-backed one does not. After Compact
	// returns successfully, the loop validates that its output contains only
	// complete tool turns and rejects malformed or split exchanges.
	Compact func(ctx context.Context, history []message.Message) ([]message.Message, error)

	// CompactTo is the token-aware context preparation hook. When
	// ContextTokenTarget is positive, the loop invokes CompactTo before every
	// provider request and prefers it over Compact. The implementation owns
	// model-specific token estimation and should return history unchanged when it
	// already fits. Engine.Run wires this from TargetContextManager.
	CompactTo func(ctx context.Context, history []message.Message, targetTokens int) ([]message.Message, error)

	continuationRequest  Request
	continuationPolicy   OutputPolicy
	controlBound         bool
	repairCount          int
	activeElapsed        time.Duration
	segmentStarted       time.Time
	initialUsage         provider.Usage
	initialSteps         []Step
	initialToolCallsUsed int
}

// LoopOutput is the message-level result from Engine.RunMessages. The
// execution-level Result type lives in result.go.
type LoopOutput struct {
	Messages          []message.Message
	Usage             provider.Usage
	StopReason        provider.StopReason
	Iterations        int
	Thinking          string
	Steps             []Step
	ToolCallsUsed     int
	RepairCount       int
	ActiveElapsed     time.Duration
	NextOperationTurn int
}

// Engine drives the bounded agent loop. Configure Provider, Tools, Hooks, and
// execution defaults at construction. Engine.RunMessages remains available as
// the low-level message-driven entry; it ignores the Engine defaults and reads
// everything it needs from LoopInput.
type Engine struct {
	Provider provider.Driver
	Tools    *tool.Bus
	Hooks    HookChain

	Model          string
	Temperature    float64
	TopP           float64
	ModelMaxTokens int
	ToolMode       tool.Mode
	LoopPolicy     LoopPolicy
	ContextBuilder ContextManager
	// OperationTurn seeds the next durable tool-call turn ordinal for resumed
	// executions. New executions leave it at zero.
	OperationTurn int

	// Skills are active reusable instructions Engine.Run injects into execution
	// context. RunMessages is the low-level message API and does not read this
	// Engine default.
	Skills []skill.Skill

	// AvailableSkills are disclosed as metadata and activated on demand. Build
	// resolves them from Spec.AvailableSkills; direct Engine construction may
	// also supply validated skills here.
	AvailableSkills []skill.Skill

	// The fields below are the engine-level defaults Engine.Run threads into
	// every LoopInput it builds. ResponseFormat and per-request Metadata are
	// deliberately not surfaced here: structured output on the Run path is
	// owned by OutputPolicy, and request Metadata has no Engine-level default.

	// ThinkingBudget caps provider reasoning tokens per turn; zero leaves the
	// provider default in place.
	ThinkingBudget int
	// StopSequences are forwarded to every provider turn the loop issues.
	StopSequences []string
	// godoc-allow-any: provider-specific request extensions are intentionally open.
	ExtraBody         map[string]any
	PromptCacheKey    string
	ServiceTier       string
	ParallelToolCalls *bool
	ContextUsage      provider.ContextUsageObserver
	ModelInterceptor  provider.StreamInterceptor
	ToolInterceptor   tool.Interceptor

	// OutputGuardrails run in order against the terminal assistant output and
	// may allow, replace, retry, or block it.
	OutputGuardrails []OutputGuardrail
	// OutputObserver receives every non-allow guardrail decision.
	OutputObserver OutputGuardrailObserver

	// StepDecider may override the natural decision at continue boundaries.
	StepDecider StepDecider

	// StepObserver receives each finalized step before the loop advances.
	StepObserver StepObserver
	// Boundaries observes safe continuation points before the next effect.
	Boundaries BoundaryObserver
	// Control is single-use and must not be shared between concurrent runs.
	Control *Control
}

// RunMessages is the low-level loop that drives one LoopInput to
// completion. Engine.Run is the execution-level wrapper most callers want.
func (e Engine) RunMessages(ctx context.Context, input LoopInput) (out LoopOutput, err error) {
	if !input.controlBound {
		var finish func()
		ctx, finish, err = input.Control.start(ctx)
		if err != nil {
			return LoopOutput{}, err
		}
		defer finish()
		input.controlBound = true
	}
	if input.segmentStarted.IsZero() {
		input.segmentStarted = time.Now()
	}
	defer func() {
		out.NextOperationTurn = input.OperationTurn
		out.ActiveElapsed = input.activeElapsed + time.Since(input.segmentStarted)
		out.RepairCount = input.repairCount
	}()
	input, stepCapacity := normalizeIterationPolicy(input)
	if input.ToolMode == "" {
		input.ToolMode = tool.ModeSequential
	}
	input.OperationTurn = max(input.OperationTurn, nextToolOperationTurn(input.Messages))
	current := message.CloneMessages(input.Messages)
	totalUsage := input.initialUsage
	steps := cloneSteps(input.initialSteps)
	if steps == nil {
		steps = make([]Step, 0, stepCapacity)
	}
	lastModelCall := (*ModelCall)(nil)
	toolCallsUsed := input.initialToolCallsUsed
	// turnsRun counts the model turns that have actually run (their usage folded
	// into totalUsage). It is set once a turn completes, before the per-turn Step
	// is recorded, so the panic path below reports Iterations consistently with
	// the non-panic returns even when a panic strikes after a turn ran but before
	// its Step exists (a guardrail/recorder panic or a sequential tool driver
	// panic). Deriving Iterations from len(steps) there would under-report a turn
	// whose usage and messages are already in the partial output.
	turnsRun := len(steps)
	// Recover any panic raised by caller-supplied extension code the loop drives
	// on this goroutine (guardrails, recorders, the sink when it emits tool-result
	// frames, and sequential tool drivers) so a misbehaving extension degrades to a
	// typed failure instead of crashing the worker. The accumulated trace is
	// preserved on the returned LoopOutput. Hook handlers and parallel tool
	// drivers are contained closer to their origin. Per-event callbacks and the
	// provider stream are recovered inside collect so partial output survives.
	defer recoverRunMessagesPanic(
		ctx,
		input,
		&out,
		&err,
		&current,
		&totalUsage,
		&steps,
		&lastModelCall,
		&turnsRun,
		&toolCallsUsed,
	)
	for iteration := len(steps); iterationAllowed(input, iteration); iteration++ {
		// A cancelled or expired context ends the loop promptly rather than
		// issuing another model turn; the cause (context.Canceled or
		// context.DeadlineExceeded) flows through loopErrorFailure, which maps a
		// budget-driven deadline to FailureKindBudgetExhausted.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return loopErrorOutput(current, totalUsage, steps, iteration, toolCallsUsed), ctxErr
		}
		pendingInput := input.Control.take()
		current = append(current, message.CloneMessages(pendingInput)...)
		// Enforce the per-loop budget before every turn after the first.
		// Reaching iteration N>0 means a prior turn chose to continue, so this
		// is exactly a "will continue" boundary; a run that finished earlier
		// returned before reaching here and is never charged a budget failure.
		if iteration > 0 {
			next, out, stop, preErr := loopTurnPreamble(ctx, input, current, totalUsage, steps, iteration, toolCallsUsed)
			if stop {
				return out, preErr
			}
			current = next
		} else if input.ContextTokenTarget > 0 {
			prepared, prepareErr := maybeCompactHistory(ctx, input, current, totalUsage)
			if prepareErr != nil {
				return loopErrorOutput(current, totalUsage, steps, iteration, toolCallsUsed), prepareErr
			}
			current = prepared
		}
		if inputErr := validatePendingInput(current, pendingInput); inputErr != nil {
			input.Control.acknowledge(inputErr)
			return loopErrorOutput(current, totalUsage, steps, iteration, toolCallsUsed), inputErr
		}
		boundaryErr := e.observeBoundary(ctx, input, current, totalUsage, steps, toolCallsUsed, ContinuationReady)
		input.Control.acknowledge(boundaryErr)
		if boundaryErr != nil {
			return loopErrorOutput(current, totalUsage, steps, iteration, toolCallsUsed), boundaryErr
		}
		assistant, usage, stopReason, identity, opened, turnErr := e.runTurn(ctx, current, input)
		if turnErr != nil {
			failure := handleTurnFailure(
				ctx, input, current, totalUsage, steps, turnsRun, iteration, toolCallsUsed,
				assistant, usage, stopReason, identity, opened, turnErr,
			)
			current, totalUsage, steps, turnsRun = failure.current, failure.usage, failure.steps, failure.turnsRun
			return failure.output, failure.err
		}
		operationTurn := input.OperationTurn
		input.OperationTurn++
		totalUsage = totalUsage.Add(usage)
		// The model turn ran and its usage is now counted, so a panic from here on
		// (guardrails, recorders, sink, sequential tool drivers) must report this
		// turn in Iterations rather than only the turns whose Step already exists.
		turnsRun = iteration + 1
		modelCall := &ModelCall{
			Provider:                      identity.Provider.Name,
			Model:                         identity.Model,
			InputTokens:                   usage.InputTokens,
			CachedInputTokens:             usage.CachedInputTokens,
			CachedInputTokensReported:     usage.CachedInputTokensReported,
			CacheWriteInputTokens:         usage.CacheWriteInputTokens,
			CacheWriteInputTokensReported: usage.CacheWriteInputTokensReported,
			OutputTokens:                  usage.OutputTokens,
			ReasoningTokens:               usage.ReasoningTokens,
			TotalTokens:                   usage.TotalTokens,
			StopReason:                    stopReason,
		}
		lastModelCall = modelCall
		if len(assistant.ToolCalls) == 0 {
			var retry bool
			current, steps, out, retry, err = e.finalizeNoToolStep(
				ctx, input, current, assistant, modelCall, totalUsage, steps,
				stopReason, iteration, toolCallsUsed,
			)
			if retry {
				continue
			}
			return out, err
		}
		var stop bool
		out, stop, err = e.runToolStep(
			ctx, &input, &current, assistant, modelCall, &totalUsage,
			&steps, iteration, &toolCallsUsed, operationTurn,
		)
		if stop {
			return out, err
		}
	}
	if len(steps) > 0 && responseRecoveryCount(steps[len(steps)-1:]) > 0 {
		return loopErrorOutput(current, totalUsage, steps, len(steps), toolCallsUsed), errIncompleteResponse
	}
	return LoopOutput{
		Messages:      current,
		Usage:         totalUsage,
		StopReason:    provider.StopReasonMaxTurns,
		Iterations:    input.MaxIterations,
		Steps:         steps,
		ToolCallsUsed: toolCallsUsed,
	}, nil
}

type turnFailureResult struct {
	current  []message.Message
	usage    provider.Usage
	steps    []Step
	turnsRun int
	output   LoopOutput
	err      error
}

func handleTurnFailure(
	ctx context.Context,
	input LoopInput,
	current []message.Message,
	totalUsage provider.Usage,
	steps []Step,
	turnsRun, iteration, toolCallsUsed int,
	assistant message.Message,
	usage provider.Usage,
	stopReason provider.StopReason,
	identity provider.StreamIdentity,
	opened bool,
	turnErr error,
) turnFailureResult {
	current, totalUsage, turnsRun = recordIncompleteTurn(current, assistant, totalUsage, usage, iteration, turnsRun)
	if opened {
		turnsRun = max(turnsRun, iteration+1)
		steps = append(steps, turnFailureStep(iteration, identity, usage, stopReason, StepDecisionFail, totalUsage, toolCallsUsed))
		if observeErr := observeFinalizedStep(ctx, input.StepObserver, steps); observeErr != nil {
			turnErr = errors.Join(turnErr, observeErr)
		}
	}
	return turnFailureResult{
		current: current, usage: totalUsage, steps: steps, turnsRun: turnsRun,
		output: loopErrorOutput(current, totalUsage, steps, turnsRun, toolCallsUsed),
		err:    turnErr,
	}
}

func turnFailureStep(
	iteration int,
	identity provider.StreamIdentity,
	usage provider.Usage,
	stopReason provider.StopReason,
	decision StepDecision,
	totalUsage provider.Usage,
	toolCallsUsed int,
) Step {
	return Step{
		Index: iteration,
		ModelCall: &ModelCall{
			Provider:                      identity.Provider.Name,
			Model:                         identity.Model,
			InputTokens:                   usage.InputTokens,
			CachedInputTokens:             usage.CachedInputTokens,
			CachedInputTokensReported:     usage.CachedInputTokensReported,
			CacheWriteInputTokens:         usage.CacheWriteInputTokens,
			CacheWriteInputTokensReported: usage.CacheWriteInputTokensReported,
			OutputTokens:                  usage.OutputTokens,
			ReasoningTokens:               usage.ReasoningTokens,
			TotalTokens:                   usage.TotalTokens,
			StopReason:                    stopReason,
		},
		Decision:   decision,
		BudgetUsed: BudgetUsage{Tokens: int64(totalUsage.TotalTokens), ToolCalls: toolCallsUsed},
	}
}

func recoverRunMessagesPanic(
	ctx context.Context,
	input LoopInput,
	out *LoopOutput,
	runErr *error,
	current *[]message.Message,
	totalUsage *provider.Usage,
	steps *[]Step,
	lastModelCall **ModelCall,
	turnsRun *int,
	toolCallsUsed *int,
) {
	panicValue := recover()
	if panicValue == nil {
		return
	}
	last := *lastModelCall
	if last != nil && (len(*steps) == 0 || (*steps)[len(*steps)-1].ModelCall != last) {
		*steps = append(*steps, Step{
			Index:      *turnsRun - 1,
			ModelCall:  last,
			Decision:   StepDecisionFail,
			BudgetUsed: BudgetUsage{Tokens: int64(totalUsage.TotalTokens), ToolCalls: *toolCallsUsed},
		})
		if observeErr := observeRecoveredStep(ctx, input.StepObserver, *steps); observeErr != nil {
			*runErr = errors.Join(*runErr, observeErr)
		}
	}
	*out = loopErrorOutput(*current, *totalUsage, *steps, *turnsRun, *toolCallsUsed)
	*runErr = errors.Join(*runErr, fmt.Errorf("%w: %v", ErrPanicRecovered, panicValue))
}

func (e Engine) runToolStep(
	ctx context.Context,
	input *LoopInput,
	current *[]message.Message,
	assistant message.Message,
	modelCall *ModelCall,
	totalUsage *provider.Usage,
	steps *[]Step,
	iteration int,
	toolCallsUsed *int,
	operationTurn int,
) (LoopOutput, bool, error) {
	for index := range assistant.ToolCalls {
		assistant.ToolCalls[index].OperationID = fmt.Sprintf("turn:%d:call:%d", operationTurn, index)
	}
	*current = append(*current, assistant)
	*steps = append(*steps, Step{
		Index:      iteration,
		ModelCall:  modelCall,
		Decision:   StepDecisionContinue,
		BudgetUsed: BudgetUsage{Tokens: int64(totalUsage.TotalTokens), ToolCalls: *toolCallsUsed},
	})
	if boundaryErr := e.observeBoundary(ctx, *input, *current, *totalUsage, *steps, *toolCallsUsed, ContinuationModelComplete); boundaryErr != nil {
		return loopErrorOutput(*current, *totalUsage, *steps, iteration+1, *toolCallsUsed), true, boundaryErr
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return loopErrorOutput(*current, *totalUsage, *steps, iteration+1, *toolCallsUsed), true, ctxErr
	}

	if input.MaxTokens > 0 && modelCall.TotalTokens == 0 {
		(*steps)[len(*steps)-1].Decision = StepDecisionFail
		if observeErr := observeFinalizedStep(ctx, input.StepObserver, *steps); observeErr != nil {
			return loopErrorOutput(*current, *totalUsage, *steps, iteration+1, *toolCallsUsed), true, observeErr
		}
		out, err := budgetAbort(*current, *totalUsage, *steps, iteration+1, *toolCallsUsed, "max tokens")
		return out, true, err
	}

	if dimension := preDispatchBudgetBlock(*input, *totalUsage, *toolCallsUsed, len(*steps), len(assistant.ToolCalls)); dimension != "" {
		(*steps)[len(*steps)-1].Decision = StepDecisionFail
		if observeErr := observeFinalizedStep(ctx, input.StepObserver, *steps); observeErr != nil {
			return loopErrorOutput(*current, *totalUsage, *steps, iteration+1, *toolCallsUsed), true, observeErr
		}
		out, err := budgetAbort(*current, *totalUsage, *steps, iteration+1, *toolCallsUsed, dimension)
		return out, true, err
	}

	prepared, terminal, prepErr := e.prepareToolCalls(ctx, assistant.ToolCalls)
	if prepErr != nil {
		return loopErrorOutput(*current, *totalUsage, *steps, iteration+1, *toolCallsUsed), true, prepErr
	}
	*toolCallsUsed += len(assistant.ToolCalls)
	dispatchCtx, childUsage := withAgentToolDispatchContext(ctx, input.MaxTokens, *totalUsage)
	results, dispatchErr := e.dispatchPreparedTools(dispatchCtx, prepared, input.ToolMode, input.Sink)
	*totalUsage = (*totalUsage).Add(childUsage.snapshot())
	(*steps)[len(*steps)-1].BudgetUsed = BudgetUsage{Tokens: int64(totalUsage.TotalTokens), ToolCalls: *toolCallsUsed}
	appendErr := appendToolResults(ctx, current, results, input.Sink)
	if executionErr := errors.Join(dispatchErr, appendErr); executionErr != nil {
		return loopErrorOutput(*current, *totalUsage, *steps, iteration+1, *toolCallsUsed), true, executionErr
	}
	if terminal {
		terminal = false
		for index, result := range results {
			if !result.IsError && e.Tools.IsTerminal(prepared[index].Name) {
				terminal = true
				break
			}
		}
	}
	nextSteps, out, stop, err := e.finalizeToolStep(
		ctx, *input, *current, *totalUsage, *steps, assistant,
		results, terminal, iteration, *toolCallsUsed,
	)
	*steps = nextSteps
	return out, stop, err
}

func normalizeIterationPolicy(input LoopInput) (LoopInput, int) {
	if input.UnlimitedIterations {
		return input, 16
	}
	if input.MaxIterations <= 0 {
		// The conservative default remains in place unless a caller explicitly
		// opts into unlimited interactive turns.
		input.MaxIterations = 12
	}
	return input, input.MaxIterations
}

func iterationAllowed(input LoopInput, iteration int) bool {
	return input.UnlimitedIterations || iteration < input.MaxIterations
}

func (e Engine) finalizeNoToolStep(
	ctx context.Context,
	input LoopInput,
	current []message.Message,
	assistant message.Message,
	modelCall *ModelCall,
	totalUsage provider.Usage,
	steps []Step,
	stopReason provider.StopReason,
	iteration int,
	toolCallsUsed int,
) ([]message.Message, []Step, LoopOutput, bool, error) {
	base := current
	current = appendFinalAssistant(current, assistant)
	steps = append(steps, Step{
		Index:      iteration,
		ModelCall:  modelCall,
		Decision:   StepDecisionFinish,
		BudgetUsed: BudgetUsage{Tokens: int64(totalUsage.TotalTokens), ToolCalls: toolCallsUsed},
	})
	if boundaryErr := e.observeBoundary(ctx, input, current, totalUsage, steps, toolCallsUsed, ContinuationValidatingOutput); boundaryErr != nil {
		return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false, boundaryErr
	}
	if input.MaxTokens > 0 && modelCall.TotalTokens == 0 {
		steps[len(steps)-1].Decision = StepDecisionFail
		if observeErr := observeFinalizedStep(ctx, input.StepObserver, steps); observeErr != nil {
			return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false, observeErr
		}
		out, budgetErr := budgetAbort(current, totalUsage, steps, iteration+1, toolCallsUsed, "max tokens")
		return current, steps, out, false, budgetErr
	}

	finalOutput, retryMessages, retryPolicy, guardErr := e.applyOutputGuardrails(
		ctx, input, base, assistant, iteration+1, totalUsage, stopReason,
	)
	if guardErr != nil {
		steps[len(steps)-1].Decision = StepDecisionFail
		return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false, guardErr
	}
	if len(retryMessages) > 0 {
		steps[len(steps)-1].Decision = StepDecisionContinue
		current = appendRetryContext(base, assistant, retryMessages, retryPolicy)
		if observeErr := observeFinalizedStep(ctx, input.StepObserver, steps); observeErr != nil {
			return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false, observeErr
		}
		return current, steps, LoopOutput{}, true, nil
	}
	correction, recoveryErr := responseRecovery(steps, finalOutput)
	if recoveryErr != nil {
		steps[len(steps)-1].Decision = StepDecisionFail
		recoveryErr = errors.Join(recoveryErr, observeFinalizedStep(ctx, input.StepObserver, steps))
		return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false, recoveryErr
	}
	if correction.Role != "" {
		steps[len(steps)-1].Decision = StepDecisionContinue
		steps[len(steps)-1].Observations = append(steps[len(steps)-1].Observations, Observation{Kind: "response_recovery", Message: finalOutput.Metadata[responseRecoveryKey]})
		current = append(appendFinalAssistant(base, finalOutput), correction)
		if observeErr := observeFinalizedStep(ctx, input.StepObserver, steps); observeErr != nil {
			return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false, observeErr
		}
		return current, steps, LoopOutput{}, true, nil
	}

	current = appendFinalAssistant(base, finalOutput)
	if input.Control.continueOrSeal() {
		steps[len(steps)-1].Decision = StepDecisionContinue
		if observeErr := observeFinalizedStep(ctx, input.StepObserver, steps); observeErr != nil {
			return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false, observeErr
		}
		return current, steps, LoopOutput{}, true, nil
	}
	if observeErr := observeFinalizedStep(ctx, input.StepObserver, steps); observeErr != nil {
		return current, steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), false, observeErr
	}
	return current, steps, LoopOutput{
		Messages:      current,
		Usage:         totalUsage,
		StopReason:    stopReason,
		Iterations:    iteration + 1,
		Thinking:      finalOutput.Thinking,
		Steps:         steps,
		ToolCallsUsed: toolCallsUsed,
	}, false, nil
}

func (e Engine) finalizeToolStep(
	ctx context.Context,
	input LoopInput,
	current []message.Message,
	totalUsage provider.Usage,
	steps []Step,
	assistant message.Message,
	results []tool.Result,
	terminal bool,
	iteration int,
	toolCallsUsed int,
) ([]Step, LoopOutput, bool, error) {
	latest := &steps[len(steps)-1]
	latest.ToolCalls = toolCallTraces(assistant.ToolCalls, results)
	latest.BudgetUsed = BudgetUsage{Tokens: int64(totalUsage.TotalTokens), ToolCalls: toolCallsUsed}
	latest.Decision = StepDecisionContinue
	if terminal {
		latest.Decision = StepDecisionFinish
	}

	var policyOut LoopOutput
	var policyErr error
	stop := terminal
	if !terminal {
		policyOut, stop, policyErr = stepDecisionOverride(
			input, current, totalUsage, steps, assistant.Thinking, iteration+1, toolCallsUsed,
		)
		if policyErr != nil {
			latest.Decision = StepDecisionFail
		}
	}
	observeErr := observeFinalizedStep(ctx, input.StepObserver, steps)
	boundaryErr := error(nil)
	if observeErr == nil {
		boundaryErr = e.observeBoundary(ctx, input, current, totalUsage, steps, toolCallsUsed, ContinuationToolsComplete)
	}
	if combined := errors.Join(policyErr, observeErr, boundaryErr); combined != nil {
		return steps, loopErrorOutput(current, totalUsage, steps, iteration+1, toolCallsUsed), true, combined
	}
	if terminal {
		return steps, LoopOutput{
			Messages:      current,
			Usage:         totalUsage,
			StopReason:    provider.StopReasonComplete,
			Iterations:    iteration + 1,
			Thinking:      assistant.Thinking,
			Steps:         steps,
			ToolCallsUsed: toolCallsUsed,
		}, true, nil
	}
	if stop {
		return steps, policyOut, true, nil
	}
	return steps, LoopOutput{}, false, nil
}

// observeFinalizedStep invokes the observer for the latest finalized step.
func observeFinalizedStep(ctx context.Context, observer StepObserver, steps []Step) error {
	if observer == nil {
		return nil
	}
	step := steps[len(steps)-1]
	if err := observer.ObserveStep(ctx, step); err != nil {
		return fmt.Errorf("agent: observe step %d: %w", step.Index, err)
	}
	return nil
}

func observeRecoveredStep(ctx context.Context, observer StepObserver, steps []Step) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrPanicRecovered, r)
		}
	}()
	return observeFinalizedStep(ctx, observer, steps)
}

func nextToolOperationTurn(messages []message.Message) int {
	next := 0
	for _, msg := range messages {
		for _, call := range msg.ToolCalls {
			var turn, callIndex int
			if count, err := fmt.Sscanf(call.OperationID, "turn:%d:call:%d", &turn, &callIndex); err == nil && count == 2 && turn >= next {
				next = turn + 1
			}
		}
	}
	return next
}

func (e Engine) observeBoundary(
	ctx context.Context,
	input LoopInput,
	messages []message.Message,
	usage provider.Usage,
	steps []Step,
	toolCallsUsed int,
	phase ContinuationPhase,
) error {
	if e.Boundaries == nil {
		return nil
	}
	continuation := Continuation{
		SchemaVersion:     ContinuationSchemaVersion,
		Request:           cloneRequest(input.continuationRequest),
		OutputPolicy:      input.continuationPolicy,
		Messages:          message.CloneMessages(messages),
		Usage:             usage,
		Steps:             cloneSteps(steps),
		ToolCallsUsed:     toolCallsUsed,
		RepairCount:       input.repairCount,
		ActiveElapsed:     input.activeElapsed + time.Since(input.segmentStarted),
		NextOperationTurn: input.OperationTurn,
		Phase:             phase,
	}
	continuation.OutputPolicy.Schema = append(json.RawMessage(nil), continuation.OutputPolicy.Schema...)
	if err := ValidateContinuation(continuation); err != nil {
		return fmt.Errorf("agent: build %s continuation: %w", phase, err)
	}
	if err := e.Boundaries.ObserveBoundary(ctx, cloneContinuation(continuation)); err != nil {
		return fmt.Errorf("agent: observe %s boundary: %w", phase, err)
	}
	return nil
}

// loopTurnPreamble runs the per-iteration preamble for turns after the first: it
// stops the loop when a budget dimension is exhausted (budgetAbort), then
// prepares the running history for the upcoming request. When stop is true the
// loop returns out/err as-is; otherwise next is the history — possibly
// compacted — to drive the upcoming turn with. iteration is the count of model
// turns already issued, matching the budget-abort contract.
func loopTurnPreamble(ctx context.Context, input LoopInput, current []message.Message, usage provider.Usage, steps []Step, iteration, toolCallsUsed int) (next []message.Message, out LoopOutput, stop bool, err error) {
	if _, dimension := budgetRemaining(input, usage, toolCallsUsed, len(steps)); dimension != "" {
		out, err = budgetAbort(current, usage, steps, iteration, toolCallsUsed, dimension)
		return current, out, true, err
	}
	// The budget still has room for this turn. Compacting after the budget check
	// means a turn that would abort never pays for a compaction it cannot use.
	compacted, compactErr := maybeCompactHistory(ctx, input, current, usage)
	if compactErr != nil {
		return current, loopErrorOutput(current, usage, steps, iteration, toolCallsUsed), true, compactErr
	}
	return compacted, LoopOutput{}, false, nil
}

// stepDecisionOverride consults the StepDecider at a continue boundary.
func stepDecisionOverride(input LoopInput, current []message.Message, usage provider.Usage, steps []Step, thinking string, iterations, toolCallsUsed int) (out LoopOutput, stop bool, err error) {
	if input.StepDecider == nil {
		return LoopOutput{}, false, nil
	}
	decision, decideErr := input.StepDecider.Decide(LoopSnapshot{Steps: cloneSteps(steps)})
	if decideErr != nil {
		return loopErrorOutput(current, usage, steps, iterations, toolCallsUsed), true,
			fmt.Errorf("%w: step decider: %w", ErrStepAborted, decideErr)
	}
	switch decision {
	case StepDecisionFinish:
		steps[len(steps)-1].Decision = decision
		return LoopOutput{
			Messages:      current,
			Usage:         usage,
			StopReason:    provider.StopReasonComplete,
			Iterations:    iterations,
			Thinking:      thinking,
			Steps:         steps,
			ToolCallsUsed: toolCallsUsed,
		}, true, nil
	case StepDecisionFail:
		steps[len(steps)-1].Decision = StepDecisionFail
		return loopErrorOutput(current, usage, steps, iterations, toolCallsUsed), true,
			fmt.Errorf("%w: step decider returned fail", ErrStepAborted)
	}
	return LoopOutput{}, false, nil
}

// budgetRemaining returns a copy of input whose budget ceilings are reduced by
// the usage already spent, and reports the first exhausted dimension (empty
// when the loop may still spend). It is the single source of truth for both
// the in-loop "will continue" check and the cross-call repair check. A
// positive token ceiling also rejects a successful zero-usage turn in
// RunMessages before it can continue.
func budgetRemaining(input LoopInput, usage provider.Usage, toolCallsUsed, steps int) (LoopInput, string) {
	next := input
	if input.MaxTokens > 0 {
		next.MaxTokens = input.MaxTokens - int64(usage.TotalTokens)
		if next.MaxTokens <= 0 {
			return input, "max tokens"
		}
	}
	if input.MaxToolCalls > 0 {
		next.MaxToolCalls = input.MaxToolCalls - toolCallsUsed
		if next.MaxToolCalls <= 0 {
			return input, "max tool calls"
		}
	}
	if input.MaxSteps > 0 {
		next.MaxSteps = input.MaxSteps - steps
		if next.MaxSteps <= 0 {
			return input, "max steps"
		}
	}
	return next, ""
}

// compactionBudgetHeadroomDivisor sets the token-budget headroom below which the
// loop compacts the running history: when MaxTokens is set and the remaining
// budget falls to MaxTokens/compactionBudgetHeadroomDivisor or less (one band of
// ~20% left), the next turn's history is compacted first. A divisor keeps the
// trigger in integer arithmetic, off the floating-point path.
const compactionBudgetHeadroomDivisor = 5

// maybeCompactHistory returns the history to drive the next turn. A positive
// ContextTokenTarget invokes token-targeted preparation on every request,
// preferring CompactTo and falling back to Compact for source-compatible
// managers. With no target it preserves the legacy behavior: Compact runs only
// when cumulative MaxTokens spend enters its final headroom band. A compaction
// error or incomplete tool turn aborts rather than sending malformed history.
func maybeCompactHistory(ctx context.Context, input LoopInput, current []message.Message, usage provider.Usage) ([]message.Message, error) {
	if input.ContextTokenTarget > 0 {
		if input.CompactTo == nil && input.Compact == nil {
			return current, nil
		}
	} else {
		if input.Compact == nil || input.MaxTokens <= 0 {
			return current, nil
		}
		remaining := input.MaxTokens - int64(usage.TotalTokens)
		if remaining > input.MaxTokens/compactionBudgetHeadroomDivisor {
			return current, nil
		}
	}

	compactionInput, err := cacheSafeCompactionInput(current)
	if err != nil {
		return current, fmt.Errorf("agent: compact history: %w", err)
	}
	var compacted []message.Message
	if input.ContextTokenTarget > 0 && input.CompactTo != nil {
		compacted, err = input.CompactTo(ctx, compactionInput, input.ContextTokenTarget)
	} else {
		compacted, err = input.Compact(ctx, compactionInput)
	}
	if err != nil {
		return current, fmt.Errorf("agent: compact history: %w", err)
	}
	if err := message.ValidateCompleteTurns(compacted); err != nil {
		return current, fmt.Errorf("agent: compact history: %w", err)
	}
	if err := validateCachePrefixPreserved(current, compacted); err != nil {
		return current, fmt.Errorf("agent: compact history: %w", err)
	}
	return compacted, nil
}

func validateCachePrefixPreserved(before, after []message.Message) error {
	boundary, err := message.CachePrefixBoundary(before)
	if err != nil {
		return err
	}
	if boundary == 0 {
		return nil
	}
	if len(after) < boundary {
		return errors.New("compaction removed an explicit cache prefix")
	}
	for index := range boundary {
		if !reflect.DeepEqual(before[index], after[index]) {
			return fmt.Errorf("compaction changed explicit cache prefix at message %d", index)
		}
	}
	return nil
}

// cacheSafeCompactionInput isolates histories carrying an explicit cache prefix
// before calling extension code. Without a deep copy, an in-place compactor can
// mutate both the source and returned aliases and defeat the post-call integrity
// comparison. Histories without a cache boundary retain the allocation-free
// legacy path.
func cacheSafeCompactionInput(messages []message.Message) ([]message.Message, error) {
	boundary, err := message.CachePrefixBoundary(messages)
	if err != nil {
		return nil, err
	}
	if boundary == 0 {
		return messages, nil
	}

	return message.CloneMessages(messages), nil
}

// dispatchExceedsToolBudget reports whether executing this turn's tool batch
// would push the running tool-call count past MaxToolCalls. A single model
// turn can emit more parallel calls than the budget allows, so the loop gates
// the batch here rather than discovering the overrun on the next pre-turn
// check — by which point the side-effecting tools have already executed. A
// batch that exactly fills the budget is allowed; the next pre-turn check
// stops the run afterwards.
func dispatchExceedsToolBudget(input LoopInput, toolCallsUsed, batch int) bool {
	return input.MaxToolCalls > 0 && toolCallsUsed+batch > input.MaxToolCalls
}

// preDispatchBudgetBlock reports the budget dimension that forbids dispatching
// this turn's tool batch, or "" when the batch may run. It guards what the
// top-of-loop pre-turn check cannot: the just-completed model turn's token
// spend (totalUsage now includes it, so a single turn that blows MaxTokens is
// caught before its side-effecting tools run rather than at the next pre-turn
// check), and the batch's own tool-call count, which one turn can inflate past
// MaxToolCalls. A non-terminal tool turn is a "will continue" boundary, so any
// exhausted dimension must abort here, before ExecuteBatch. The step ceiling is
// evaluated against the pre-increment count, matching the pre-turn check: this
// turn's step is not yet recorded, so step exhaustion it causes surfaces on the
// next pre-turn check, never prematurely here.
func preDispatchBudgetBlock(input LoopInput, usage provider.Usage, toolCallsUsed, steps, batch int) string {
	if _, dimension := budgetRemaining(input, usage, toolCallsUsed, steps); dimension != "" {
		return dimension
	}
	if dispatchExceedsToolBudget(input, toolCallsUsed, batch) {
		return "max tool calls"
	}
	return ""
}

// budgetAbort builds the partial LoopOutput returned when the loop stops for
// budget reasons, tagging the exhausted dimension on the ErrBudgetExhausted
// chain. iterations is the number of model turns that actually ran.
func budgetAbort(current []message.Message, usage provider.Usage, steps []Step, iterations, toolCallsUsed int, dimension string) (LoopOutput, error) {
	return LoopOutput{
		Messages:      current,
		Usage:         usage,
		StopReason:    provider.StopReasonAborted,
		Iterations:    iterations,
		Steps:         steps,
		ToolCallsUsed: toolCallsUsed,
	}, fmt.Errorf("%w: %s", ErrBudgetExhausted, dimension)
}

// loopErrorOutput builds the partial LoopOutput returned alongside a
// non-budget loop error. It mirrors budgetAbort for the error path: the
// messages, usage, and steps accumulated before the failure are preserved
// (StopReason is StopReasonError) so a scheduler and the durable record keep
// the trace that led up to the error instead of discarding it. iterations is
// the number of model turns that ran before the failure.
func loopErrorOutput(current []message.Message, usage provider.Usage, steps []Step, iterations, toolCallsUsed int) LoopOutput {
	return LoopOutput{
		Messages:      current,
		Usage:         usage,
		StopReason:    provider.StopReasonError,
		Iterations:    iterations,
		Steps:         steps,
		ToolCallsUsed: toolCallsUsed,
	}
}

// recordIncompleteTurn folds a failed turn's partial work into the running trace.
// A turn can fail after the model already streamed content — a per-event callback
// erroring or panicking, which collect surfaces as an error carrying the
// normalized partial assistant turn. When that turn carries content or usage,
// append it and fold its usage so the partial LoopOutput keeps the produced
// response and its token spend, and count the turn in Iterations; an empty turn
// leaves the trace, usage, and turn count untouched so a failure before any
// content or usage reports nothing extra.
func recordIncompleteTurn(current []message.Message, assistant message.Message, totalUsage, usage provider.Usage, iteration, turnsRun int) ([]message.Message, provider.Usage, int) {
	if assistant.Text == "" && assistant.Thinking == "" && assistant.RedactedThinking == "" && len(assistant.ToolCalls) == 0 && usage == (provider.Usage{}) {
		return current, totalUsage, turnsRun
	}
	return append(current, assistant), totalUsage.Add(usage), iteration + 1
}

// appendRetryContext assembles the messages a guardrail retry feeds back into
// the loop: optionally the rejected assistant output, any replacement
// context, and the retry instructions themselves.
func appendRetryContext(current []message.Message, assistant message.Message, retryMessages []message.Message, retryPolicy RetryPolicy) []message.Message {
	if retryPolicy.IncludeRejectedOutput && (assistant.Text != "" || assistant.Thinking != "") {
		current = append(current, assistant)
	}
	if len(retryPolicy.ReplacementContext) > 0 {
		current = append(current, cloneMessages(retryPolicy.ReplacementContext)...)
	}
	return append(current, retryMessages...)
}

// appendFinalAssistant appends the finalized assistant message to history,
// skipping it when the guardrails left nothing to record.
func appendFinalAssistant(current []message.Message, finalOutput message.Message) []message.Message {
	if finalOutput.Text != "" || finalOutput.Thinking != "" {
		return append(current, finalOutput)
	}
	return current
}

// toolCallTraces builds a replay-safe trace for each tool call in a turn.
// The bus returns results positionally aligned with calls (sequential
// appends in order; parallel writes results[i] for calls[i]), so we pair by
// index rather than ToolCallID — drivers are not required to echo the call
// ID. Per-call timing is intentionally left zero: the bus exposes no
// per-call duration, and a wall-clock reading here would be a
// nondeterministic field on a replayable Step (ADR-007).
func toolCallTraces(calls []message.ToolCall, results []message.ToolResult) []ToolCallTrace {
	if len(calls) == 0 {
		return nil
	}
	traces := make([]ToolCallTrace, 0, len(calls))
	for i, call := range calls {
		trace := ToolCallTrace{
			ID:        call.ID,
			Name:      call.Name,
			Arguments: call.Arguments,
		}
		if i < len(results) {
			result := results[i]
			trace.Output = result.Structured
			if result.IsError {
				trace.Error = result.Content
			}
		}
		traces = append(traces, trace)
	}
	return traces
}

func (e Engine) applyOutputGuardrails(ctx context.Context, input LoopInput, current []message.Message, assistant message.Message, iteration int, usage provider.Usage, stopReason provider.StopReason) (message.Message, []message.Message, RetryPolicy, error) {
	if len(input.OutputGuardrails) == 0 {
		return assistant, nil, RetryPolicy{}, nil
	}
	candidate := assistant
	for _, guardrail := range input.OutputGuardrails {
		if guardrail == nil {
			continue
		}
		result, err := guardrail.Check(ctx, OutputGuardrailInput{
			Model:               input.Model,
			Messages:            cloneMessages(current),
			Output:              candidate,
			Iteration:           iteration,
			MaxIterations:       input.MaxIterations,
			UnlimitedIterations: input.UnlimitedIterations,
			Usage:               usage,
			StopReason:          stopReason,
			Metadata:            cloneStringMap(input.Metadata),
		})
		if err != nil {
			return message.Message{}, nil, RetryPolicy{}, err
		}
		normalized, err := normalizeOutputGuardrailResult(result)
		if err != nil {
			return message.Message{}, nil, RetryPolicy{}, err
		}
		switch normalized.Action {
		case OutputGuardrailActionAllow:
			continue
		case OutputGuardrailActionReplace:
			e.recordOutputGuardrailDecision(ctx, input, guardrail.Name(), normalized.Action, normalized.Reason, iteration, normalized.Metadata)
			candidate = *normalized.Replacement
		case OutputGuardrailActionRetry:
			e.recordOutputGuardrailDecision(ctx, input, guardrail.Name(), normalized.Action, normalized.Reason, iteration, normalized.Metadata)
			if !input.UnlimitedIterations && iteration >= input.MaxIterations {
				return message.Message{}, nil, RetryPolicy{}, &OutputGuardrailRetryLimitExceededError{
					Guardrail: guardrail.Name(),
					Output:    candidate,
				}
			}
			return candidate, normalized.RetryMessages, normalized.RetryPolicy, nil
		case OutputGuardrailActionBlock:
			e.recordOutputGuardrailDecision(ctx, input, guardrail.Name(), normalized.Action, normalized.Reason, iteration, normalized.Metadata)
			return message.Message{}, nil, RetryPolicy{}, &OutputGuardrailTripwireTriggeredError{
				Guardrail: guardrail.Name(),
				Reason:    normalized.Reason,
				Output:    candidate,
			}
		}
	}
	return candidate, nil, RetryPolicy{}, nil
}

func (e Engine) recordOutputGuardrailDecision(ctx context.Context, input LoopInput, name string, action OutputGuardrailAction, reason string, iteration int, metadata map[string]string) {
	if input.OutputObserver == nil {
		return
	}
	merged := cloneStringMap(input.Metadata)
	if merged == nil {
		merged = map[string]string{}
	}
	for key, value := range metadata {
		merged[key] = value
	}
	input.OutputObserver.ObserveOutputGuardrailDecision(ctx, OutputGuardrailDecision{
		GuardrailName: name,
		Action:        action,
		Reason:        reason,
		Iteration:     iteration,
		Metadata:      merged,
	})
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	return cloneMutableValue(reflect.ValueOf(values)).Interface().(map[string]any)
}

func cloneMutableValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.New(value.Type()).Elem()
		cloned.Set(cloneMutableValue(value.Elem()))
		return cloned
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			cloned.SetMapIndex(iterator.Key(), cloneMutableValue(iterator.Value()))
		}
		return cloned
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := range value.Len() {
			cloned.Index(index).Set(cloneMutableValue(value.Index(index)))
		}
		return cloned
	case reflect.Array:
		cloned := reflect.New(value.Type()).Elem()
		for index := range value.Len() {
			cloned.Index(index).Set(cloneMutableValue(value.Index(index)))
		}
		return cloned
	default:
		return value
	}
}

func cloneBoolPointer(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneResponseFormat(value *provider.ResponseFormat) *provider.ResponseFormat {
	if value == nil {
		return nil
	}
	cloned := *value
	if value.Schema != nil {
		schema := cloneJSONSchema(*value.Schema)
		cloned.Schema = &schema
	}
	cloned.RawSchema = append(json.RawMessage(nil), value.RawSchema...)
	return &cloned
}

// runTurn executes a single model turn: context transform, request assembly,
// provider stream and event collection.
func (e Engine) runTurn(ctx context.Context, current []message.Message, input LoopInput) (message.Message, provider.Usage, provider.StopReason, provider.StreamIdentity, bool, error) {
	transformed, err := e.Hooks.TransformContext(ctx, current)
	if err != nil {
		return message.Message{}, provider.Usage{}, provider.StopReasonError, provider.StreamIdentity{}, false, err
	}
	request := provider.Request{
		Model:             input.Model,
		OperationID:       fmt.Sprintf("turn:%d:model", input.OperationTurn),
		Messages:          transformed,
		Temperature:       input.Temperature,
		TopP:              input.TopP,
		MaxTokens:         input.ModelMaxTokens,
		Metadata:          cloneStringMap(input.Metadata),
		StopSequences:     slices.Clone(input.StopSequences),
		ThinkingBudget:    input.ThinkingBudget,
		ResponseFormat:    cloneResponseFormat(input.ResponseFormat),
		PromptCacheKey:    input.PromptCacheKey,
		ServiceTier:       input.ServiceTier,
		ParallelToolCalls: cloneBoolPointer(input.ParallelToolCalls),
		ContextUsage:      input.ContextUsage,
		ExtraBody:         cloneAnyMap(input.ExtraBody),
	}
	if e.Tools != nil {
		request.Tools = e.Tools.Definitions()
	}
	if err := e.Hooks.BeforeModelCall(ctx, &request); err != nil {
		return message.Message{}, provider.Usage{}, provider.StopReasonError, provider.StreamIdentity{}, false, err
	}
	if input.ContextTokenTarget > 0 && input.Compact == nil && input.CompactTo == nil {
		request.Messages, err = fitContext(ctx, request.Messages, input.ContextTokenTarget)
		if err != nil {
			return message.Message{}, provider.Usage{}, provider.StopReasonError, provider.StreamIdentity{}, false, err
		}
	}
	request.OperationID = fmt.Sprintf("turn:%d:model", input.OperationTurn)
	if err := provider.ValidateExtraBody(request.ExtraBody); err != nil {
		return message.Message{}, provider.Usage{}, provider.StopReasonError, provider.StreamIdentity{}, false, err
	}
	modelTimeouts := input.ModelTimeouts.resolved()
	modelCtx, cancelModel := modelRequestContext(ctx, modelTimeouts)
	defer cancelModel()
	providerStream, err := openModelStream(modelCtx, modelTimeouts.ConnectTimeout, cancelModel, func(openCtx context.Context) (provider.Stream, error) {
		if interceptor := provider.ChainStreamInterceptors(e.ModelInterceptor); interceptor != nil {
			return interceptor.Stream(openCtx, e.Provider, request)
		}
		return e.Provider.Stream(openCtx, request)
	})
	if err != nil {
		if errors.Is(context.Cause(modelCtx), provider.ErrModelRequestTimeout) {
			err = errors.Join(provider.ErrModelRequestTimeout, context.DeadlineExceeded, err)
		}
		return message.Message{}, provider.Usage{}, provider.StopReasonError, provider.StreamIdentity{}, false, err
	}
	providerStream = provider.WithStreamIdleTimeout(modelCtx, providerStream, modelTimeouts.StreamIdleTimeout)
	assistant, usage, stop, collectErr := e.collect(modelCtx, providerStream, input.OnEvent, input.Sink)
	if collectErr != nil && errors.Is(modelCtx.Err(), context.DeadlineExceeded) && errors.Is(context.Cause(modelCtx), provider.ErrModelRequestTimeout) {
		collectErr = errors.Join(provider.ErrModelRequestTimeout, context.DeadlineExceeded, collectErr)
	}
	identity := provider.StreamIdentity{
		Provider: e.Provider.Metadata(),
		Model:    request.Model,
	}
	if identified, ok := providerStream.(provider.IdentifiedStream); ok {
		identity = identified.Identity()
		if identity.Provider.Name == "" {
			identity.Provider = e.Provider.Metadata()
		}
		if identity.Model == "" {
			identity.Model = request.Model
		}
	}
	return assistant, usage, stop, identity, true, collectErr
}

func modelRequestContext(ctx context.Context, policy ModelTimeoutPolicy) (context.Context, context.CancelFunc) {
	if policy.RequestTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeoutCause(ctx, policy.RequestTimeout, provider.ErrModelRequestTimeout)
}

func openModelStream(ctx context.Context, timeout time.Duration, cancelModel context.CancelFunc, open func(context.Context) (provider.Stream, error)) (provider.Stream, error) {
	if timeout <= 0 {
		return open(ctx)
	}
	timeoutCtx, cancelTimeout := context.WithTimeoutCause(ctx, timeout, provider.ErrModelConnectTimeout)
	defer cancelTimeout()
	type result struct {
		stream provider.Stream
		err    error
	}
	results := make(chan result, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				results <- result{err: fmt.Errorf("%w: provider stream panic: %v", ErrPanicRecovered, recovered)}
			}
		}()
		stream, err := open(ctx)
		results <- result{stream: stream, err: err}
	}()
	select {
	case opened := <-results:
		return opened.stream, opened.err
	case <-timeoutCtx.Done():
		if cause := context.Cause(timeoutCtx); !errors.Is(cause, provider.ErrModelConnectTimeout) {
			return nil, cause
		}
		cancelModel()
		go func() {
			opened := <-results
			if opened.stream != nil {
				_ = opened.stream.Close()
			}
		}()
		return nil, errors.Join(provider.ErrModelConnectTimeout, context.DeadlineExceeded)
	}
}

// prepareToolCalls runs each call's BeforeToolCall hook — which may rewrite the
// tool name — and records whether a prepared call targets a terminal tool.
// Unavailable names reach the Bus as rejected results the model can correct.
// Hook failures remain fatal; no driver runs before all hooks succeed.
func (e Engine) prepareToolCalls(ctx context.Context, calls []message.ToolCall) ([]tool.Call, bool, error) {
	prepared := make([]tool.Call, 0, len(calls))
	terminal := false
	for _, call := range calls {
		item := call
		item.Arguments = append(json.RawMessage(nil), item.Arguments...)
		operationID := item.OperationID
		if err := e.Hooks.BeforeToolCall(ctx, &item); err != nil {
			return nil, false, err
		}
		item.OperationID = operationID
		if e.Tools != nil && e.Tools.IsTerminal(item.Name) {
			terminal = true
		}
		prepared = append(prepared, item)
	}
	return prepared, terminal, nil
}

// dispatchPreparedTools executes the hook-prepared calls on the bus, bridges
// real-time tool updates to the loop sink, and runs AfterToolCall on each final
// result. Rejected names/arguments are completed error results and count toward
// the call budget, so repeated invalid calls cannot bypass execution limits.
//
// Every result ExecuteBatch returns is complete, including pre-execution
// rejection and successful effects. A later sequential call failing still yields
// the earlier successes, and a parallel call erroring or panicking still yields
// the completed slots. Those results are post-processed and returned alongside
// the batch error so the caller records them, sparing a resuming caller from
// replaying side-effecting calls or leaving the matching assistant tool calls
// dangling.
//
// If an AfterToolCall itself fails on one result (including a panic HookChain
// converts to ErrHookPanic), the results blessed before it are returned with
// that error and the failing result and any after it are dropped — so the
// returned results are always a prefix of what AfterToolCall post-processed. An
// AfterToolCall failure takes precedence over a batch error because it is the
// earlier hook in the pipeline; the loop surfaces whichever non-nil error this
// returns.
func (e Engine) dispatchPreparedTools(ctx context.Context, prepared []tool.Call, mode tool.Mode, sink Sink) ([]message.ToolResult, error) {
	var updates tool.UpdateSink
	if sink != nil {
		updates = func(update tool.Update) error {
			return sink.Emit(ctx, FrameFromToolUpdate(update))
		}
	}
	bus := e.Tools
	if bus == nil {
		bus = tool.NewBus()
	}
	results, batchErr := bus.ExecuteBatch(ctx, prepared, mode, tool.ExecuteOptions{
		Sink:        updates,
		Interceptor: e.ToolInterceptor,
	})
	items := make([]message.ToolResult, 0, len(results))
	for index, current := range results {
		item := current
		if index < len(prepared) {
			if item.ToolCallID == "" {
				item.ToolCallID = prepared[index].ID
			}
			if item.Name == "" {
				item.Name = prepared[index].Name
			}
		}
		if err := e.Hooks.AfterToolCall(ctx, &item); err != nil {
			return items, err
		}
		items = append(items, item)
	}
	return items, batchErr
}

// appendToolResults appends each tool result to the running history through the
// caller's slice pointer and, when a sink is set, emits a FrameToolResult for it.
// Tool results are a loop-level enrichment with no provider.Event equivalent. It
// appends each result to *current before emitting it, so the caller's history
// reflects every produced result the instant it exists, not only on return. That
// matters for both sink failure modes the loop's recover defer is meant to
// salvage: a sink Emit that returns an error surfaces it here with *current
// already holding the result, and a sink Emit that panics unwinds straight to the
// recover defer, which reads the same caller variable and still finds the result.
// A by-value return would lose the panic case, because the unwind skips the
// caller's assignment of the return value. The prompt, the assistant tool call,
// and every appended tool result thus survive on the partial LoopOutput rather
// than being discarded.
func appendToolResults(ctx context.Context, current *[]message.Message, results []message.ToolResult, sink Sink) error {
	for _, result := range results {
		*current = append(*current, message.NewToolResult(message.CloneToolResult(result)))
		if sink == nil {
			continue
		}
		toolResult := message.CloneToolResult(result)
		if err := sink.Emit(ctx, Frame{Kind: FrameToolResult, ToolResult: &toolResult}); err != nil {
			return err
		}
	}
	return nil
}

func (e Engine) collect(ctx context.Context, providerStream provider.Stream, onEvent func(provider.Event) error, sink Sink) (assistant message.Message, usage provider.Usage, stop provider.StopReason, err error) {
	defer func() { _ = providerStream.Close() }()
	assistant = message.Message{Role: message.RoleAssistant, Kind: message.KindStandard}
	events := make([]provider.Event, 0, 8)
	sawTerminal := false
	eventBytes := 0
	// A per-event callback (onEvent or the sink) can panic while handling an
	// event. Each event is recorded before it is delivered, so normalize the
	// events collected so far onto the assistant turn and surface the panic as an
	// ErrPanicRecovered error carrying that partial turn, rather than letting it
	// unwind to the loop's outer recover where the completed response would be
	// lost. errors.Is(err, ErrPanicRecovered) still holds end-to-end.
	defer func() {
		if r := recover(); r != nil {
			usage, _, _ = applyNormalized(&assistant, events, false)
			stop = provider.StopReasonError
			err = fmt.Errorf("%w: %v", ErrPanicRecovered, r)
		}
	}()
	for {
		// Stop draining the provider stream as soon as the context is done so a
		// cancelled or timed-out turn returns promptly instead of blocking on
		// the next event; the cause is surfaced for loopErrorFailure to classify.
		// Once the provider has delivered its terminal EventDone the response is
		// already complete (EventDone carries the StopReason), so a cancellation
		// landing in the window before io.EOF must not discard it — fall through
		// and normalize the events already collected rather than failing the turn.
		if !sawTerminal {
			if ctxErr := ctx.Err(); ctxErr != nil {
				partialUsage, partialStop, _ := applyNormalized(&assistant, events, false)
				if partialStop == "" {
					partialStop = provider.StopReasonError
				}
				return assistant, partialUsage, partialStop, ctxErr
			}
		}
		event, recvErr := providerStream.Recv()
		if recvErr != nil {
			if recvErr == io.EOF {
				break
			}
			// A context-aware stream (one that wraps a body read) can unblock Recv
			// with the context error rather than io.EOF. If that lands after the
			// terminal EventDone the response is already complete, so treat it like
			// io.EOF: stop draining and normalize the events already collected
			// instead of discarding the finished turn. Only context cancellation is
			// tolerated here — any other post-terminal error is a genuine fault and
			// still propagates.
			if sawTerminal && (errors.Is(recvErr, context.Canceled) || errors.Is(recvErr, context.DeadlineExceeded)) {
				break
			}
			partialUsage, partialStop, _ := applyNormalized(&assistant, events, false)
			if partialStop == "" {
				partialStop = provider.StopReasonError
			}
			return assistant, partialUsage, partialStop, recvErr
		}
		if event.Kind == provider.EventDone {
			sawTerminal = true
		}
		if len(events) >= maxProviderTurnEvents {
			partialUsage, partialStop, _ := applyNormalized(&assistant, events, false)
			return assistant, partialUsage, partialStop, fmt.Errorf(
				"%w: more than %d events",
				ErrProviderTurnLimit,
				maxProviderTurnEvents,
			)
		}
		size := providerEventSize(event)
		if size > maxProviderTurnBytes-eventBytes {
			partialUsage, partialStop, _ := applyNormalized(&assistant, events, false)
			return assistant, partialUsage, partialStop, fmt.Errorf(
				"%w: more than %d bytes",
				ErrProviderTurnLimit,
				maxProviderTurnBytes,
			)
		}
		eventBytes += size
		// Record the event before delivering it to the callbacks: a callback that
		// errors or panics must not discard the response already streamed, so both
		// the recover above and the error return below normalize the events held so
		// far rather than starting from an empty turn.
		events = append(events, cloneProviderEvent(event))
		if cbErr := e.fanOutEvent(ctx, event, onEvent, sink); cbErr != nil {
			usage, stop, _ = applyNormalized(&assistant, events, false)
			return assistant, usage, stop, cbErr
		}
	}
	usage, stop, err = applyNormalized(&assistant, events, true)
	if err != nil {
		return assistant, provider.Usage{}, provider.StopReasonError, err
	}
	return assistant, usage, stop, nil
}

func providerEventSize(event provider.Event) int {
	size := len(event.Text) + len(event.Thinking) + len(event.Signature) +
		len(event.RedactedThinking) + len(event.ProviderState)
	if event.ToolCall != nil {
		size += len(event.ToolCall.ID) + len(event.ToolCall.Name) + len(event.ToolCall.Arguments)
	}
	if event.ToolCallDelta != nil {
		size += len(event.ToolCallDelta.ID) + len(event.ToolCallDelta.Name) +
			len(event.ToolCallDelta.ArgumentsDelta)
	}
	size += len(event.Response.ID) + len(event.Response.Model)
	for key, value := range event.Response.Headers {
		size += len(key) + len(value)
	}
	if event.Err != nil {
		size += len(event.Err.Error())
	}
	return size
}

// fanOutEvent delivers one provider event to the caller callback, the hook chain,
// and the sink (frames only), returning the first error. collect records the
// event before calling this, so a callback failure here ends the turn with the
// response streamed so far already preserved for the partial trace.
func (e Engine) fanOutEvent(ctx context.Context, event provider.Event, onEvent func(provider.Event) error, sink Sink) error {
	if onEvent != nil {
		if err := onEvent(cloneProviderEvent(event)); err != nil {
			return err
		}
	}
	if err := e.Hooks.OnEvent(ctx, cloneProviderEvent(event)); err != nil {
		return err
	}
	if sink != nil {
		if frame, ok := FrameFromEvent(event); ok {
			if err := sink.Emit(ctx, frame); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyNormalized copies text, thinking, signatures, tool calls, and opaque
// provider state from the events collected so far onto the assistant turn,
// and reports the turn's usage and stop reason. It centralizes the
// success path and its failure paths (a callback error or a recovered panic),
// where the partial turn must still carry whatever the model already produced. A
// normalize error (a malformed stream prefix) leaves the assistant untouched and
// reports StopReasonError, so a failure path never masks its original cause with a
// normalize error.
func applyNormalized(assistant *message.Message, events []provider.Event, requireTerminal bool) (provider.Usage, provider.StopReason, error) {
	var (
		normalized provider.NormalizedResponse
		err        error
	)
	if requireTerminal {
		normalized, err = provider.NormalizeEvents(events)
	} else {
		normalized, err = provider.NormalizePartialEvents(events)
	}
	if err != nil && !(requireTerminal && errors.Is(err, provider.ErrInvalidToolCallArguments)) {
		return provider.Usage{}, provider.StopReasonError, err
	}
	assistant.Content = message.CloneContent(normalized.Content)
	assistant.ToolCalls = normalized.ToolCalls
	assistant.ProviderState = normalized.ProviderState
	assistant.Response = message.CloneResponseMetadata(normalized.Response)
	assistant.SyncLegacyContent()
	if requireTerminal {
		markIncompleteResponse(assistant, normalized.StopReason, err)
	}
	return normalized.Usage, normalized.StopReason, nil
}
