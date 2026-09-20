package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

// ErrInvalidContinuation reports persisted Agent state that cannot be resumed
// without guessing or repairing history.
var ErrInvalidContinuation = errors.New("invalid agent continuation")

// ContinuationSchemaVersion is the current wire version. Version 1 remains
// readable and re-encodable without changing its canonical hash.
const ContinuationSchemaVersion = 2

// ContinuationPhase identifies the next deterministic Engine action.
type ContinuationPhase string

const (
	ContinuationReady            ContinuationPhase = "ready"
	ContinuationModelComplete    ContinuationPhase = "model_complete"
	ContinuationToolsComplete    ContinuationPhase = "tools_complete"
	ContinuationValidatingOutput ContinuationPhase = "validating_output"
)

// Continuation is the complete provider-neutral state needed to resume one
// Engine execution. Pending tool calls are derived from Messages.
type Continuation struct {
	SchemaVersion     int               `json:"schemaVersion"`
	Request           Request           `json:"request"`
	OutputPolicy      OutputPolicy      `json:"outputPolicy"`
	Messages          []message.Message `json:"messages"`
	Usage             provider.Usage    `json:"usage,omitempty"`
	Steps             []Step            `json:"steps,omitempty"`
	ToolCallsUsed     int               `json:"toolCallsUsed,omitempty"`
	RepairCount       int               `json:"repairCount,omitempty"`
	ActiveElapsed     time.Duration     `json:"activeElapsed,omitempty"`
	NextOperationTurn int               `json:"nextOperationTurn,omitempty"`
	Phase             ContinuationPhase `json:"phase"`
}

// BoundaryObserver synchronously observes a safe continuation boundary.
type BoundaryObserver interface {
	ObserveBoundary(context.Context, Continuation) error
}

// BoundaryObserverFunc adapts a function to BoundaryObserver.
type BoundaryObserverFunc func(context.Context, Continuation) error

// ObserveBoundary delegates to f.
func (f BoundaryObserverFunc) ObserveBoundary(ctx context.Context, continuation Continuation) error {
	return f(ctx, continuation)
}

// JoinBoundaryObservers invokes non-nil observers in order and stops at the
// first error. Every observer receives its own deep clone.
func JoinBoundaryObservers(observers ...BoundaryObserver) BoundaryObserver {
	filtered := make([]BoundaryObserver, 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			filtered = append(filtered, observer)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return BoundaryObserverFunc(func(ctx context.Context, continuation Continuation) error {
		for _, observer := range filtered {
			if err := observer.ObserveBoundary(ctx, cloneContinuation(continuation)); err != nil {
				return err
			}
		}
		return nil
	})
}

// cloneContinuation returns an ownership-independent continuation.
func cloneContinuation(continuation Continuation) Continuation {
	continuation.Request = cloneRequest(continuation.Request)
	continuation.OutputPolicy.Schema = append(json.RawMessage(nil), continuation.OutputPolicy.Schema...)
	continuation.Messages = message.CloneMessages(continuation.Messages)
	continuation.Steps = cloneSteps(continuation.Steps)
	return continuation
}

func cloneRequest(request Request) Request {
	request.Content = message.CloneContent(request.Content)
	request.Budget = cloneBudget(request.Budget)
	request.SessionBudget = cloneSessionBudget(request.SessionBudget)
	request.ModelTimeouts = cloneModelTimeouts(request.ModelTimeouts)
	return request
}

func cloneSteps(steps []Step) []Step {
	if steps == nil {
		return nil
	}
	cloned := make([]Step, len(steps))
	for index, step := range steps {
		cloned[index] = step
		if step.ModelCall != nil {
			modelCall := *step.ModelCall
			cloned[index].ModelCall = &modelCall
		}
		cloned[index].ToolCalls = slices.Clone(step.ToolCalls)
		for toolIndex := range cloned[index].ToolCalls {
			cloned[index].ToolCalls[toolIndex].Arguments = append(json.RawMessage(nil), cloned[index].ToolCalls[toolIndex].Arguments...)
			cloned[index].ToolCalls[toolIndex].Output = append(json.RawMessage(nil), cloned[index].ToolCalls[toolIndex].Output...)
		}
		cloned[index].Observations = slices.Clone(step.Observations)
	}
	return cloned
}

// ValidateContinuation rejects corrupt or internally inconsistent resume state.
func ValidateContinuation(continuation Continuation) error {
	if continuation.SchemaVersion != 1 && continuation.SchemaVersion != ContinuationSchemaVersion {
		return continuationError("unsupported schema version %d", continuation.SchemaVersion)
	}
	if continuation.SchemaVersion == 1 && (len(continuation.Request.Content) > 0 || continuation.OutputPolicy.Native) {
		return continuationError("request content and native output require schema version 2")
	}
	if err := continuation.Request.Validate(); err != nil {
		return continuationError("invalid request content: %v", err)
	}
	if err := validateContinuationScalars(continuation); err != nil {
		return err
	}
	analysis, err := analyzeTranscript(continuation.Messages)
	if err != nil {
		return err
	}
	if err := validateContinuationPhaseTranscript(continuation, analysis); err != nil {
		return err
	}
	completedToolCalls, cumulativeUsage, err := validateContinuationSteps(continuation, analysis)
	if err != nil {
		return err
	}
	if err := validateContinuationTotals(continuation, analysis, completedToolCalls, cumulativeUsage); err != nil {
		return err
	}
	return validateContinuationPhaseSteps(continuation, analysis)
}

func validateContinuationPhaseTranscript(continuation Continuation, analysis transcriptAnalysis) error {
	switch continuation.Phase {
	case ContinuationReady:
		if analysis.pending {
			return continuationError("ready phase has pending tool calls")
		}
	case ContinuationModelComplete:
		if !analysis.pending {
			return continuationError("model_complete phase has no pending tool calls")
		}
	case ContinuationToolsComplete:
		if analysis.pending || !analysis.endsWithToolResults {
			return continuationError("tools_complete phase does not end with a complete tool turn")
		}
	case ContinuationValidatingOutput:
		if analysis.pending || len(continuation.Messages) == 0 {
			return continuationError("validating_output phase has incomplete output")
		}
		last := continuation.Messages[len(continuation.Messages)-1]
		if last.Role != message.RoleAssistant || len(last.ToolCalls) != 0 {
			return continuationError("validating_output phase must end with assistant output")
		}
	default:
		return continuationError("unknown phase %q", continuation.Phase)
	}
	return nil
}

func validateContinuationSteps(continuation Continuation, analysis transcriptAnalysis) (int, provider.Usage, error) {
	if continuation.NextOperationTurn < len(continuation.Steps) {
		return 0, provider.Usage{}, continuationError("next operation turn %d precedes %d completed model turns", continuation.NextOperationTurn, len(continuation.Steps))
	}
	if len(analysis.assistantIndices) < len(continuation.Steps) {
		return 0, provider.Usage{}, continuationError("%d steps have only %d assistant turns", len(continuation.Steps), len(analysis.assistantIndices))
	}
	firstOperationTurn := continuation.NextOperationTurn - len(continuation.Steps)
	firstGeneratedAssistant := len(analysis.assistantIndices) - len(continuation.Steps)
	completedToolCalls := 0
	var cumulativeUsage provider.Usage
	for index, step := range continuation.Steps {
		assistant := continuation.Messages[analysis.assistantIndices[firstGeneratedAssistant+index]]
		nextCompleted, nextUsage, err := validateContinuationStep(
			continuation,
			index,
			step,
			assistant,
			firstOperationTurn+index,
			completedToolCalls,
			cumulativeUsage,
		)
		if err != nil {
			return 0, provider.Usage{}, err
		}
		completedToolCalls = nextCompleted
		cumulativeUsage = nextUsage
	}
	return completedToolCalls, cumulativeUsage, nil
}

func validateContinuationStep(
	continuation Continuation,
	index int,
	step Step,
	assistant message.Message,
	operationTurn int,
	completedToolCalls int,
	cumulativeUsage provider.Usage,
) (int, provider.Usage, error) {
	if step.Index != index {
		return 0, provider.Usage{}, continuationError("step %d has index %d", index, step.Index)
	}
	if step.BudgetUsed.Tokens < 0 || step.BudgetUsed.ToolCalls < 0 || step.BudgetUsed.WallClock < 0 {
		return 0, provider.Usage{}, continuationError("step %d has negative budget usage", index)
	}
	if step.ModelCall == nil || !validModelCallUsage(*step.ModelCall) {
		return 0, provider.Usage{}, continuationError("step %d has missing, negative, or inconsistent model usage", index)
	}
	if index < len(continuation.Steps)-1 && step.Decision != StepDecisionContinue {
		return 0, provider.Usage{}, continuationError("step %d has terminal decision before a later step", index)
	}
	for callIndex, call := range assistant.ToolCalls {
		expected := fmt.Sprintf("turn:%d:call:%d", operationTurn, callIndex)
		if call.OperationID != expected {
			return 0, provider.Usage{}, continuationError("step %d tool call %d has operation ID %q, want %q", index, callIndex, call.OperationID, expected)
		}
	}
	pendingStep := continuation.Phase == ContinuationModelComplete && index == len(continuation.Steps)-1
	if pendingStep {
		if len(step.ToolCalls) != 0 || step.Decision != StepDecisionContinue {
			return 0, provider.Usage{}, continuationError("model_complete step %d is already finalized", index)
		}
	} else {
		if err := validateStepToolCalls(index, step.ToolCalls, assistant.ToolCalls); err != nil {
			return 0, provider.Usage{}, err
		}
		completedToolCalls += len(assistant.ToolCalls)
	}
	cumulativeUsage = cumulativeUsage.Add(modelCallUsage(*step.ModelCall))
	if err := validateContinuationStepBudget(
		continuation,
		index,
		step,
		assistant,
		pendingStep,
		completedToolCalls,
		cumulativeUsage,
	); err != nil {
		return 0, provider.Usage{}, err
	}
	return completedToolCalls, cumulativeUsage, nil
}

func validateContinuationStepBudget(
	continuation Continuation,
	index int,
	step Step,
	assistant message.Message,
	pendingStep bool,
	completedToolCalls int,
	cumulativeUsage provider.Usage,
) error {
	minimumTokens := int64(cumulativeUsage.TotalTokens)
	if index > 0 {
		previousTokens := continuation.Steps[index-1].BudgetUsed.Tokens
		currentModelTokens := int64(modelCallUsage(*step.ModelCall).TotalTokens)
		minimumTokens = max(minimumTokens, previousTokens+currentModelTokens)
	}
	if step.BudgetUsed.ToolCalls != completedToolCalls {
		return continuationError("step %d tool-call budget snapshot is inconsistent", index)
	}
	if pendingStep || len(assistant.ToolCalls) == 0 {
		if step.BudgetUsed.Tokens != minimumTokens {
			return continuationError("step %d token budget snapshot is inconsistent", index)
		}
	} else if step.BudgetUsed.Tokens < minimumTokens {
		return continuationError("step %d token budget snapshot is below model usage", index)
	}
	return nil
}

func validateContinuationTotals(continuation Continuation, analysis transcriptAnalysis, completedToolCalls int, cumulativeModelUsage provider.Usage) error {
	if continuation.ToolCallsUsed != completedToolCalls {
		return continuationError("toolCallsUsed %d does not match completed execution count %d", continuation.ToolCallsUsed, completedToolCalls)
	}
	expectedTokens := int64(0)
	if len(continuation.Steps) > 0 {
		expectedTokens = continuation.Steps[len(continuation.Steps)-1].BudgetUsed.Tokens
	}
	if int64(continuation.Usage.TotalTokens) != expectedTokens {
		return continuationError("cumulative token usage does not match step budget")
	}
	if !usageContains(continuation.Usage, cumulativeModelUsage) {
		return continuationError("cumulative usage is below model steps")
	}
	if analysis.maxOperationTurn >= continuation.NextOperationTurn {
		return continuationError("next operation turn %d does not follow transcript turn %d", continuation.NextOperationTurn, analysis.maxOperationTurn)
	}
	return nil
}

func validateContinuationPhaseSteps(continuation Continuation, analysis transcriptAnalysis) error {
	switch continuation.Phase {
	case ContinuationModelComplete:
		if len(continuation.Steps) == 0 || len(analysis.pendingCalls) == 0 {
			return continuationError("model_complete phase has no current model step")
		}
	case ContinuationToolsComplete:
		if len(continuation.Steps) == 0 || len(continuation.Steps[len(continuation.Steps)-1].ToolCalls) == 0 {
			return continuationError("tools_complete phase has no completed tool step")
		}
		if !validCompletedStepDecision(continuation.Steps[len(continuation.Steps)-1].Decision) {
			return continuationError("tools_complete phase has invalid decision %q", continuation.Steps[len(continuation.Steps)-1].Decision)
		}
	case ContinuationReady:
		if len(continuation.Steps) > 0 && continuation.Steps[len(continuation.Steps)-1].Decision != StepDecisionContinue {
			return continuationError("ready phase does not follow a continue decision")
		}
	case ContinuationValidatingOutput:
		if len(continuation.Steps) == 0 {
			return continuationError("validating_output phase has no current model step")
		}
		if continuation.Steps[len(continuation.Steps)-1].Decision != StepDecisionFinish {
			return continuationError("validating_output phase does not follow a finish decision")
		}
	}
	return nil
}

func modelCallUsage(call ModelCall) provider.Usage {
	return provider.Usage{
		InputTokens:                   call.InputTokens,
		CachedInputTokens:             call.CachedInputTokens,
		CachedInputTokensReported:     call.CachedInputTokensReported,
		CacheWriteInputTokens:         call.CacheWriteInputTokens,
		CacheWriteInputTokensReported: call.CacheWriteInputTokensReported,
		OutputTokens:                  call.OutputTokens,
		ReasoningTokens:               call.ReasoningTokens,
		TotalTokens:                   call.TotalTokens,
	}
}

func usageContains(total, minimum provider.Usage) bool {
	return total.InputTokens >= minimum.InputTokens &&
		total.CachedInputTokens >= minimum.CachedInputTokens &&
		(!minimum.CachedInputTokensReported || total.CachedInputTokensReported) &&
		total.CacheWriteInputTokens >= minimum.CacheWriteInputTokens &&
		(!minimum.CacheWriteInputTokensReported || total.CacheWriteInputTokensReported) &&
		total.OutputTokens >= minimum.OutputTokens &&
		total.ReasoningTokens >= minimum.ReasoningTokens &&
		total.TotalTokens >= minimum.TotalTokens
}

func validCompletedStepDecision(decision StepDecision) bool {
	return decision == StepDecisionContinue || decision == StepDecisionFinish || decision == StepDecisionFail
}

func validateStepToolCalls(stepIndex int, traces []ToolCallTrace, calls []message.ToolCall) error {
	if len(traces) != len(calls) {
		return continuationError("step %d has %d tool traces for %d calls", stepIndex, len(traces), len(calls))
	}
	for index := range calls {
		if traces[index].ID != calls[index].ID || traces[index].Name != calls[index].Name || !bytes.Equal(traces[index].Arguments, calls[index].Arguments) {
			return continuationError("step %d tool trace %d does not match transcript", stepIndex, index)
		}
	}
	return nil
}

func validateContinuationScalars(continuation Continuation) error {
	if continuation.ToolCallsUsed < 0 || continuation.RepairCount < 0 || continuation.ActiveElapsed < 0 || continuation.NextOperationTurn < 0 {
		return continuationError("negative counter or duration")
	}
	if err := validateContinuationBudget(continuation.Request.Budget); err != nil {
		return err
	}
	if err := validateSessionBudget(continuation.Request.SessionBudget); err != nil {
		return continuationError("%v", err)
	}
	if err := validateModelTimeouts(continuation.Request.ModelTimeouts); err != nil {
		return continuationError("%v", err)
	}
	if err := validateContinuationOutputPolicy(continuation.OutputPolicy); err != nil {
		return err
	}
	return validateContinuationUsage(continuation.Usage)
}

func validateContinuationBudget(budget *Budget) error {
	if budget == nil {
		return nil
	}
	if budget.MaxTokens < 0 || budget.MaxToolCalls < 0 || budget.MaxSteps < 0 || budget.MaxWallClock < 0 {
		return continuationError("negative request budget")
	}
	return nil
}

func validateContinuationOutputPolicy(policy OutputPolicy) error {
	if policy.MaxRepairAttempts < 0 {
		return continuationError("negative repair attempt limit")
	}
	if len(policy.Schema) > 0 && !json.Valid(policy.Schema) {
		return continuationError("output schema is invalid JSON")
	}
	return nil
}

func validateContinuationUsage(usage provider.Usage) error {
	if usage.InputTokens < 0 || usage.CachedInputTokens < 0 || usage.CacheWriteInputTokens < 0 || usage.OutputTokens < 0 || usage.ReasoningTokens < 0 || usage.TotalTokens < 0 {
		return continuationError("negative usage")
	}
	if usage.CachedInputTokens > usage.InputTokens || usage.CacheWriteInputTokens > usage.InputTokens || usage.ReasoningTokens > usage.OutputTokens || usage.TotalTokens < usage.InputTokens+usage.OutputTokens {
		return continuationError("inconsistent usage")
	}
	return nil
}

func validModelCallUsage(call ModelCall) bool {
	if call.InputTokens < 0 || call.CachedInputTokens < 0 || call.CacheWriteInputTokens < 0 || call.OutputTokens < 0 || call.ReasoningTokens < 0 || call.TotalTokens < 0 {
		return false
	}
	return call.CachedInputTokens <= call.InputTokens &&
		call.CacheWriteInputTokens <= call.InputTokens &&
		call.ReasoningTokens <= call.OutputTokens &&
		call.TotalTokens >= call.InputTokens+call.OutputTokens
}

type transcriptAnalysis struct {
	pending             bool
	endsWithToolResults bool
	pendingCalls        []message.ToolCall
	assistantIndices    []int
	maxOperationTurn    int
}

func analyzeTranscript(messages []message.Message) (transcriptAnalysis, error) {
	analysis := transcriptAnalysis{maxOperationTurn: -1}
	var pending []message.ToolCall
	pendingIndex := 0
	for messageIndex, current := range messages {
		if len(pending) > 0 {
			if current.Role != message.RoleTool || current.ToolResult == nil {
				return analysis, continuationError("message %d splits a tool result sequence", messageIndex)
			}
			expected := pending[pendingIndex]
			if current.ToolResult.ToolCallID != expected.ID {
				return analysis, continuationError("message %d answers tool call %q with %q", messageIndex, expected.ID, current.ToolResult.ToolCallID)
			}
			pendingIndex++
			analysis.endsWithToolResults = true
			if pendingIndex == len(pending) {
				pending = nil
				pendingIndex = 0
			}
			continue
		}
		analysis.endsWithToolResults = false
		if current.Role == message.RoleTool {
			return analysis, continuationError("message %d is an orphan tool result", messageIndex)
		}
		if current.Role != message.RoleAssistant {
			continue
		}
		analysis.assistantIndices = append(analysis.assistantIndices, messageIndex)
		if len(current.ToolCalls) == 0 {
			continue
		}
		seenIDs := make(map[string]struct{}, len(current.ToolCalls))
		for callIndex, call := range current.ToolCalls {
			if call.ID == "" || call.Name == "" {
				return analysis, continuationError("message %d has an unnamed tool call", messageIndex)
			}
			if _, duplicate := seenIDs[call.ID]; duplicate {
				return analysis, continuationError("message %d repeats tool call ID %q", messageIndex, call.ID)
			}
			seenIDs[call.ID] = struct{}{}
			if call.OperationID == "" {
				continue
			}
			var turn, slot int
			if _, scanErr := fmt.Sscanf(call.OperationID, "turn:%d:call:%d", &turn, &slot); scanErr != nil || call.OperationID != fmt.Sprintf("turn:%d:call:%d", turn, slot) || turn < 0 || slot != callIndex {
				return analysis, continuationError("message %d has invalid operation ID %q", messageIndex, call.OperationID)
			}
			analysis.maxOperationTurn = max(analysis.maxOperationTurn, turn)
		}
		pending = current.ToolCalls
	}
	analysis.pending = len(pending) > 0
	analysis.pendingCalls = slices.Clone(pending)
	return analysis, nil
}

func continuationError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidContinuation, fmt.Sprintf(format, args...))
}
