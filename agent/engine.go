package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/skill"
	"github.com/Viking602/venat/tool"
)

// Run executes one Request under the supplied OutputPolicy.
func (e Engine) Run(ctx context.Context, request Request, policy OutputPolicy) Result {
	return e.run(ctx, request, policy, nil)
}

// RunStream is Run with a transient live output Sink.
func (e Engine) RunStream(ctx context.Context, request Request, policy OutputPolicy, sink Sink) Result {
	return e.run(ctx, request, policy, sink)
}

func (e Engine) run(ctx context.Context, request Request, policy OutputPolicy, sink Sink) Result {
	policy = cloneAgentToolOutputPolicy(policy)
	format, policyErr := prepareOutputPolicy(policy)
	if policyErr != nil {
		return Result{Failure: schemaInvalidFailure(policyErr)}
	}
	request = cloneRequest(request)
	if request.SessionBudget == nil {
		request.SessionBudget = cloneSessionBudget(e.LoopPolicy.SessionBudget)
	}
	if request.ModelTimeouts == nil {
		resolved := e.modelTimeouts(request)
		request.ModelTimeouts = &resolved
	}
	if err := request.Validate(); err != nil {
		return Result{Failure: (&AgentFailure{Kind: FailureKindContextBuildFailed, Reason: err.Error()}).WithCause(err)}
	}
	controlledCtx, finish, controlErr := e.Control.start(ctx)
	if controlErr != nil {
		return Result{Failure: loopErrorFailure(ctx, controlErr, false)}
	}
	defer finish()
	ctx = controlledCtx
	started := time.Now()
	runCtx, cancelRun, budgetDriven := e.runContext(ctx, request)
	defer cancelRun()
	runtime := newSkillRuntime(e.Skills, e.AvailableSkills)
	e.AvailableSkills = runtime.availableSkills()
	var err error
	e.Tools, err = runtime.attachTools(e.Tools)
	if err != nil {
		return Result{Failure: (&AgentFailure{Kind: FailureKindEngineError, Reason: err.Error()}).WithCause(err)}
	}

	messages, err := e.buildContext(runCtx, request)
	if err != nil {
		kind := FailureKindContextBuildFailed
		if budgetDriven && errors.Is(err, context.DeadlineExceeded) && errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			kind = FailureKindBudgetExhausted
		}
		return Result{
			Failure: (&AgentFailure{
				Kind:   kind,
				Reason: err.Error(),
			}).WithCause(err),
		}
	}
	runtime.restoreActivations(messages)

	maxTokens, maxToolCalls, maxSteps := e.budgetLimits(request)
	compact, compactTo := e.compactors(runtime)
	input := LoopInput{
		Model:               e.Model,
		Temperature:         e.Temperature,
		TopP:                e.TopP,
		ModelMaxTokens:      e.ModelMaxTokens,
		Messages:            messages,
		ToolMode:            e.ToolMode,
		MaxIterations:       e.LoopPolicy.MaxIterations,
		UnlimitedIterations: e.LoopPolicy.UnlimitedIterations,
		MaxTokens:           maxTokens,
		MaxToolCalls:        maxToolCalls,
		MaxSteps:            maxSteps,
		ModelTimeouts:       e.modelTimeouts(request),
		ContextTokenTarget:  e.LoopPolicy.ContextTokenTarget,
		OperationTurn:       e.OperationTurn,
		StopSequences:       e.StopSequences,
		ThinkingBudget:      e.ThinkingBudget,
		ResponseFormat:      format,
		ExtraBody:           e.ExtraBody,
		PromptCacheKey:      e.PromptCacheKey,
		ServiceTier:         e.ServiceTier,
		ParallelToolCalls:   cloneBoolPointer(e.ParallelToolCalls),
		ContextUsage:        e.ContextUsage,
		OutputGuardrails:    e.OutputGuardrails,
		OutputObserver:      e.OutputObserver,
		Sink:                sink,
		Control:             e.Control,
		controlBound:        true,
		StepDecider:         e.StepDecider,
		StepObserver:        e.StepObserver,
		Compact:             compact,
		CompactTo:           compactTo,
		continuationRequest: cloneRequest(request),
		continuationPolicy:  policy,
		segmentStarted:      started,
	}

	output, runErr := e.RunMessages(runCtx, input)
	if runErr != nil {
		return Result{
			Messages:      output.Messages,
			Usage:         output.Usage,
			StopReason:    output.StopReason,
			Thinking:      output.Thinking,
			ToolCallsUsed: output.ToolCallsUsed,
			RepairCount:   output.RepairCount,
			Steps:         output.Steps,
			Failure:       loopErrorFailure(runCtx, runErr, budgetDriven),
		}
	}

	result := resultFromLoopOutput(output, 0)
	if !policy.Validate || len(policy.Schema) == 0 {
		return result
	}
	return e.validateAndRepairStructuredOutput(runCtx, input, output, result, policy, budgetDriven)
}

func (e Engine) validateAndRepairStructuredOutput(ctx context.Context, input LoopInput, output LoopOutput, result Result, policy OutputPolicy, budgetDriven bool) Result {
	schema, schemaErr := parseOutputPolicySchema(policy.Schema)
	if schemaErr != nil {
		result.Valid = false
		result.Structured = nil
		result.Failure = schemaInvalidFailure(schemaErr)
		return result
	}
	validationErr := validateResultStructuredOutput(&result, schema)
	if validationErr == nil {
		return result
	}
	if !policy.Repair || output.RepairCount >= policy.MaxRepairAttempts {
		result.Failure = schemaInvalidFailure(validationErr)
		if policy.Repair && policy.MaxRepairAttempts > 0 {
			result.Failure = &AgentFailure{
				Kind:   FailureKindRepairFailed,
				Reason: fmt.Sprintf("structured output repair failed after %d attempt(s): %s", output.RepairCount, validationErr),
			}
		}
		return result
	}

	for repairCount := output.RepairCount + 1; repairCount <= policy.MaxRepairAttempts; repairCount++ {
		if _, dimension := budgetRemaining(input, output.Usage, output.ToolCallsUsed, len(output.Steps)); dimension != "" {
			result.Usage = output.Usage
			result.Steps = cloneSteps(output.Steps)
			result.Messages = message.CloneMessages(output.Messages)
			result.ToolCallsUsed = output.ToolCallsUsed
			result.RepairCount = repairCount - 1
			result.Failure = &AgentFailure{
				Kind:   FailureKindBudgetExhausted,
				Reason: fmt.Sprintf("loop budget exhausted before repair attempt %d: %s", repairCount, dimension),
			}
			return result
		}

		repairInput := input
		repairInput.Messages = append(message.CloneMessages(output.Messages), repairInstructionMessage(policy.Schema, validationErr))
		repairInput.OperationTurn = output.NextOperationTurn
		repairInput.initialUsage = output.Usage
		repairInput.initialSteps = cloneSteps(output.Steps)
		if len(repairInput.initialSteps) > 0 {
			repairInput.initialSteps[len(repairInput.initialSteps)-1].Decision = StepDecisionContinue
		}
		repairInput.initialToolCallsUsed = output.ToolCallsUsed
		repairInput.activeElapsed = output.ActiveElapsed
		repairInput.segmentStarted = time.Now()
		repairInput.repairCount = repairCount
		repairOutput, repairErr := e.RunMessages(ctx, repairInput)
		if repairErr != nil {
			failed := resultFromLoopOutput(repairOutput, repairCount)
			failed.Failure = loopErrorFailure(ctx, repairErr, budgetDriven)
			return failed
		}
		output = repairOutput
		result = resultFromLoopOutput(output, repairCount)
		validationErr = validateResultStructuredOutput(&result, schema)
		if validationErr == nil {
			return result
		}
	}

	result.Failure = &AgentFailure{
		Kind:   FailureKindRepairFailed,
		Reason: fmt.Sprintf("structured output repair failed after %d attempt(s): %s", policy.MaxRepairAttempts, validationErr),
	}
	return result
}

func resultFromLoopOutput(output LoopOutput, repairCount int) Result {
	return Result{
		Text:          finalAssistantTextFromMessages(output.Messages),
		Thinking:      output.Thinking,
		Usage:         output.Usage,
		StopReason:    output.StopReason,
		Messages:      output.Messages,
		Steps:         output.Steps,
		ToolCallsUsed: output.ToolCallsUsed,
		Valid:         true,
		RepairCount:   repairCount,
	}
}

func validateResultStructuredOutput(result *Result, schema outputPolicySchema) error {
	structured, err := validateStructuredOutputAgainstSchema(schema, result.Text)
	if err != nil {
		result.Valid = false
		result.Structured = nil
		return err
	}
	result.Valid = true
	result.Structured = structured
	result.Failure = nil
	return nil
}

func schemaInvalidFailure(err error) *AgentFailure {
	return &AgentFailure{
		Kind:   FailureKindSchemaInvalid,
		Reason: err.Error(),
	}
}

func repairInstructionMessage(schema []byte, validationErr error) message.Message {
	return message.NewText(message.RoleUser, fmt.Sprintf(
		"Repair the previous JSON output so it satisfies the required JSON Schema. Return only the corrected JSON, with no prose.\n\nValidation error: %s\n\nJSON Schema:\n%s",
		validationErr,
		string(schema),
	))
}

func (e Engine) runContext(ctx context.Context, request Request) (context.Context, context.CancelFunc, bool) {
	return e.runContextWithElapsed(ctx, request, 0)
}

func (e Engine) runContextWithElapsed(ctx context.Context, request Request, elapsed time.Duration) (context.Context, context.CancelFunc, bool) {
	maxWallClock := e.maxWallClock(request)
	if maxWallClock <= 0 {
		return ctx, func() {}, false
	}
	remaining := maxWallClock - elapsed
	if remaining < 0 {
		remaining = 0
	}
	deadline := time.Now().Add(remaining)
	if parentDeadline, ok := ctx.Deadline(); ok && !deadline.Before(parentDeadline) {
		return ctx, func() {}, false
	}
	runCtx, cancel := context.WithDeadline(ctx, deadline)
	return runCtx, cancel, true
}

func (e Engine) maxWallClock(request Request) time.Duration {
	var maxWallClock time.Duration
	if request.Budget != nil {
		maxWallClock = request.Budget.MaxWallClock
	} else if e.LoopPolicy.Budget != nil {
		maxWallClock = e.LoopPolicy.Budget.MaxWallClock
	}
	session := e.sessionBudget(request)
	if session == nil || session.MaxWallClock <= 0 {
		return maxWallClock
	}
	if maxWallClock <= 0 || session.MaxWallClock < maxWallClock {
		return session.MaxWallClock
	}
	return maxWallClock
}

func (e Engine) budgetLimits(request Request) (maxTokens int64, maxToolCalls, maxSteps int) {
	if request.Budget != nil {
		maxTokens, maxToolCalls, maxSteps = request.Budget.MaxTokens, request.Budget.MaxToolCalls, request.Budget.MaxSteps
	} else if e.LoopPolicy.Budget != nil {
		maxTokens, maxToolCalls, maxSteps = e.LoopPolicy.Budget.MaxTokens, e.LoopPolicy.Budget.MaxToolCalls, e.LoopPolicy.Budget.MaxSteps
	}
	if session := e.sessionBudget(request); session != nil && session.MaxTokens > 0 && (maxTokens <= 0 || session.MaxTokens < maxTokens) {
		maxTokens = session.MaxTokens
	}
	return maxTokens, maxToolCalls, maxSteps
}

func (e Engine) sessionBudget(request Request) *SessionBudget {
	if request.SessionBudget != nil {
		return request.SessionBudget
	}
	return e.LoopPolicy.SessionBudget
}

func (e Engine) modelTimeouts(request Request) ModelTimeoutPolicy {
	if request.ModelTimeouts != nil {
		return request.ModelTimeouts.resolved()
	}
	if e.LoopPolicy.ModelTimeouts != nil {
		return e.LoopPolicy.ModelTimeouts.resolved()
	}
	return (ModelTimeoutPolicy{}).resolved()
}

// loopErrorFailure preserves factual failure classification and the original
// error chain. Retry and escalation policy remain application-owned.
func loopErrorFailure(ctx context.Context, err error, budgetDriven bool) *AgentFailure {
	failure := &AgentFailure{Reason: err.Error()}
	var tripwire *OutputGuardrailTripwireTriggeredError
	var retryLimit *OutputGuardrailRetryLimitExceededError
	switch {
	case errors.Is(err, errIncompleteResponse):
		failure.Kind = FailureKindRepairFailed
	case errors.Is(err, errContextLimit):
		failure.Kind = FailureKindContextBuildFailed
	case errors.Is(err, ErrBudgetExhausted):
		failure.Kind = FailureKindBudgetExhausted
	case budgetDriven && errors.Is(err, context.DeadlineExceeded) && errors.Is(ctx.Err(), context.DeadlineExceeded):
		failure.Kind = FailureKindBudgetExhausted
	case errors.Is(err, ErrToolBusMissing) || errors.Is(err, tool.ErrToolNotFound):
		failure.Kind = FailureKindToolUnavailable
	case errors.As(err, &tripwire), errors.As(err, &retryLimit):
		failure.Kind = FailureKindOutputBlocked
	case errors.Is(err, ErrStepAborted):
		failure.Kind = FailureKindStepAborted
	default:
		failure.Kind = FailureKindEngineError
	}
	return failure.WithCause(err)
}

func (e Engine) buildContext(ctx context.Context, request Request) ([]message.Message, error) {
	var (
		messages []message.Message
		err      error
	)
	if e.ContextBuilder != nil {
		messages, err = e.ContextBuilder.Build(ctx, request)
	} else {
		messages, err = defaultContextBuilder{}.Build(ctx, request)
	}
	if err != nil {
		return nil, err
	}
	return injectSkillMessages(messages, e.Skills, e.AvailableSkills), nil
}

func injectSkillMessages(messages []message.Message, active, available []skill.Skill) []message.Message {
	if len(active) == 0 && len(available) == 0 {
		return messages
	}
	messages = removeSkillContextMessages(messages)
	insertAt := 0
	for insertAt < len(messages) && messages[insertAt].Role == message.RoleSystem {
		insertAt++
	}
	skillMessages := make([]message.Message, 0, 2)
	if section := skill.RenderSystemSection(active); section != "" {
		current := message.NewText(message.RoleSystem, section)
		current.Metadata = map[string]string{skillContextMetadataKey: "active"}
		skillMessages = append(skillMessages, current)
	}
	if catalog := renderSkillCatalog(available); catalog != "" {
		current := message.NewText(message.RoleSystem, catalog)
		current.Metadata = map[string]string{skillContextMetadataKey: "catalog"}
		skillMessages = append(skillMessages, current)
	}
	out := make([]message.Message, 0, len(messages)+len(skillMessages))
	out = append(out, messages[:insertAt]...)
	out = append(out, skillMessages...)
	out = append(out, messages[insertAt:]...)
	return out
}

func removeSkillContextMessages(messages []message.Message) []message.Message {
	out := make([]message.Message, 0, len(messages))
	for _, current := range messages {
		if current.Metadata != nil && current.Metadata[skillContextMetadataKey] != "" {
			continue
		}
		out = append(out, current)
	}
	return out
}

// compactor binds an explicit history transform. Built-in pass-through builders
// leave this nil so a positive ContextTokenTarget uses the default provider-view
// fitter without erasing transcript and checkpoint evidence.
func (e Engine) compactor(runtime *skillRuntime) func(context.Context, []message.Message) ([]message.Message, error) {
	switch e.ContextBuilder.(type) {
	case nil, instructionsContext, defaultContextBuilder, ContextBuilderFunc:
		return nil
	}
	return func(ctx context.Context, history []message.Message) ([]message.Message, error) {
		compacted, err := e.ContextBuilder.Compact(ctx, history)
		if err != nil {
			return nil, err
		}
		return injectSkillMessages(compacted, runtime.skillsForCompaction(compacted), runtime.availableSkills()), nil
	}
}

// compactors binds the source-compatible ContextManager hook and, when the
// manager opts in, its token-targeted extension. The targeted path injects
// active skill context before fitting and verifies that compaction preserves it,
// so framework content cannot grow the request after the fit decision.
func (e Engine) compactors(runtime *skillRuntime) (
	func(context.Context, []message.Message) ([]message.Message, error),
	func(context.Context, []message.Message, int) ([]message.Message, error),
) {
	compact := e.compactor(runtime)
	targeted, ok := e.ContextBuilder.(TargetContextManager)
	if !ok {
		return compact, nil
	}
	compactTo := func(ctx context.Context, history []message.Message, targetTokens int) ([]message.Message, error) {
		prepared := injectSkillMessages(history, runtime.skillsForCompaction(history), runtime.availableSkills())
		compacted, err := targeted.CompactTo(ctx, prepared, targetTokens)
		if err != nil {
			return nil, err
		}
		if err := validateSkillContextPreserved(prepared, compacted); err != nil {
			return nil, err
		}
		return compacted, nil
	}
	return compact, compactTo
}

func validateSkillContextPreserved(before, after []message.Message) error {
	required := map[string]string{}
	for _, current := range before {
		if kind := current.Metadata[skillContextMetadataKey]; kind != "" {
			required[kind] = current.Text
		}
	}
	for _, current := range after {
		kind := current.Metadata[skillContextMetadataKey]
		if required[kind] == current.Text {
			delete(required, kind)
		}
	}
	if len(required) != 0 {
		return fmt.Errorf("agent: targeted context compaction removed framework skill context")
	}
	return nil
}

type defaultContextBuilder struct{}

func (defaultContextBuilder) Build(_ context.Context, request Request) ([]message.Message, error) {
	input, err := requestMessage(request)
	if err != nil {
		return nil, err
	}
	return []message.Message{
		message.NewText(message.RoleSystem, "You are a Venat agent."),
		input,
	}, nil
}

func (defaultContextBuilder) Compact(_ context.Context, history []message.Message) ([]message.Message, error) {
	return history, nil
}

func finalAssistantTextFromMessages(messages []message.Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		m := messages[i]
		if m.Role == message.RoleAssistant && strings.TrimSpace(m.Text) != "" {
			return m.Text
		}
	}
	return ""
}
