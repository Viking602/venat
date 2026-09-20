package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
)

const (
	defaultAgentToolMaxDepth        = 4
	defaultAgentToolTaskDescription = "Task for the child agent to complete."
)

// AgentToolConfig configures a synchronous, in-process child Engine exposed as
// one non-terminal tool.
type AgentToolConfig struct {
	Definition   tool.Definition
	Budget       *Budget
	OutputPolicy OutputPolicy
	MaxDepth     int
}

// NewAgentTool wraps an already configured child Engine as a synchronous tool.
// The child keeps its own provider, tools, hooks, skills, loop policy, and
// context manager.
func NewAgentTool(child Engine, config AgentToolConfig) (tool.Driver, error) {
	definition := cloneAgentToolDefinition(config.Definition)
	usesDefaultInput := agentToolSchemaIsZero(definition.InputSchema)
	if usesDefaultInput {
		definition.InputSchema = defaultAgentToolInputSchema()
	}
	if definition.Name == "" {
		return nil, fmt.Errorf("%w: agent tool name is empty", tool.ErrInvalidToolDefinition)
	}
	if strings.TrimSpace(definition.Description) == "" {
		return nil, fmt.Errorf("%w: agent tool %q description is blank", tool.ErrInvalidToolDefinition, definition.Name)
	}
	if definition.Terminal {
		return nil, fmt.Errorf("%w: agent tool %q must be non-terminal", tool.ErrInvalidToolDefinition, definition.Name)
	}
	if config.MaxDepth < 0 {
		return nil, fmt.Errorf("%w: agent tool %q has negative max depth", tool.ErrInvalidToolDefinition, definition.Name)
	}
	maxDepth := config.MaxDepth
	if maxDepth == 0 {
		maxDepth = defaultAgentToolMaxDepth
	}
	if err := validateAgentToolBudget(config.Budget); err != nil {
		return nil, fmt.Errorf("%w: agent tool %q budget: %w", tool.ErrInvalidToolDefinition, definition.Name, err)
	}
	if config.Budget == nil {
		if err := validateAgentToolBudget(child.LoopPolicy.Budget); err != nil {
			return nil, fmt.Errorf("%w: agent tool %q child budget: %w", tool.ErrInvalidToolDefinition, definition.Name, err)
		}
	}
	if config.OutputPolicy.MaxRepairAttempts < 0 {
		return nil, fmt.Errorf("%w: agent tool %q has negative output repair attempts", tool.ErrInvalidToolDefinition, definition.Name)
	}

	policy := cloneAgentToolOutputPolicy(config.OutputPolicy)
	if err := ValidateOutputPolicy(policy); err != nil {
		return nil, fmt.Errorf("%w: agent tool %q output schema: %w", tool.ErrInvalidToolDefinition, definition.Name, err)
	}

	validation := tool.NewBus(agentToolValidationDriver{definition: definition})
	if err := validation.Validate(); err != nil {
		return nil, fmt.Errorf("%w: agent tool %q: %w", tool.ErrInvalidToolDefinition, definition.Name, err)
	}

	child.LoopPolicy.Budget = cloneBudget(child.LoopPolicy.Budget)
	return &agentTool{
		child:            child,
		definition:       definition,
		budget:           cloneBudget(config.Budget),
		outputPolicy:     policy,
		maxDepth:         maxDepth,
		usesDefaultInput: usesDefaultInput,
		validation:       validation,
	}, nil
}

type agentTool struct {
	child            Engine
	definition       tool.Definition
	budget           *Budget
	outputPolicy     OutputPolicy
	maxDepth         int
	usesDefaultInput bool
	validation       *tool.Bus
}

func (driver *agentTool) Definition() tool.Definition {
	return cloneAgentToolDefinition(driver.definition)
}

func (driver *agentTool) Execute(ctx context.Context, call tool.Call, sink tool.UpdateSink) (tool.Result, error) {
	if rejected, ok, err := driver.validateInput(ctx, call); err != nil || !ok {
		return rejected, err
	}

	prompt, err := driver.prompt(call.Arguments)
	if err != nil {
		return driver.errorResult(call, fmt.Sprintf("subagent %q input rejected: %v", driver.definition.Name, err)), nil
	}

	childCtx, path, effectiveMaxDepth, allowed := nextAgentToolContext(ctx, driver.definition.Name, driver.maxDepth)
	if !allowed {
		return driver.errorResult(call, fmt.Sprintf(
			"subagent %q refused: max nesting depth %d reached",
			driver.definition.Name,
			effectiveMaxDepth,
		)), nil
	}

	request := Request{Prompt: prompt, Budget: cloneBudget(driver.budget)}
	var reservation *agentToolTokenReservation
	if tracker := agentToolReservationTrackerFromContext(childCtx); tracker != nil {
		baseBudget := driver.budget
		if baseBudget == nil {
			baseBudget = driver.child.LoopPolicy.Budget
		}
		reservation, request.Budget, err = tracker.reserve(childCtx, driver.definition.Name, baseBudget)
		if err != nil {
			return driver.emptyResult(call), err
		}
		defer func() {
			reservation.settle(provider.Usage{})
		}()
	} else if err := childCtx.Err(); err != nil {
		return driver.emptyResult(call), errors.Join(tool.ErrNotExecuted, err)
	}

	if err := childCtx.Err(); err != nil {
		if reservation != nil {
			reservation.settle(provider.Usage{})
		}
		return driver.emptyResult(call), errors.Join(tool.ErrNotExecuted, err)
	}

	var forwarder *agentToolUpdateForwarder
	var childSink Sink
	if sink != nil {
		pathJSON, marshalErr := json.Marshal(path)
		if marshalErr != nil {
			if reservation != nil {
				reservation.settle(provider.Usage{})
			}
			return driver.emptyResult(call), errors.Join(tool.ErrNotExecuted, marshalErr)
		}
		forwarder = &agentToolUpdateForwarder{
			name:     driver.definition.Name,
			pathJSON: string(pathJSON),
			sink:     sink,
		}
		childSink = SinkFunc(forwarder.emit)
	}

	childResult := driver.child.RunStream(childCtx, request, driver.outputPolicy, childSink)
	normalizedUsage := provider.Usage{}.Add(childResult.Usage)
	if reservation != nil {
		reservation.settle(normalizedUsage)
	}
	reportAgentToolUsage(childCtx, normalizedUsage)

	mapped := driver.mapResult(call, childResult)
	var sinkErr error
	if forwarder != nil {
		sinkErr = forwarder.firstError()
	}
	if infrastructureErr := errors.Join(ctx.Err(), sinkErr); infrastructureErr != nil {
		return mapped, infrastructureErr
	}
	return mapped, nil
}

func (driver *agentTool) validateInput(ctx context.Context, call tool.Call) (tool.Result, bool, error) {
	validationCall := call
	validationCall.Name = driver.definition.Name
	result, err := driver.validation.Execute(ctx, validationCall, tool.ExecuteOptions{})
	if err != nil {
		return driver.emptyResult(call), false, err
	}
	if !result.IsError {
		return tool.Result{}, true, nil
	}
	cause := strings.TrimPrefix(result.Content, driver.definition.Name+" rejected: ")
	return driver.errorResult(call, fmt.Sprintf("subagent %q input rejected: %s", driver.definition.Name, cause)), false, nil
}

func (driver *agentTool) prompt(arguments json.RawMessage) (string, error) {
	if !driver.usesDefaultInput {
		return string(arguments), nil
	}
	var input struct {
		Task string `json:"task"`
	}
	if err := json.Unmarshal(arguments, &input); err != nil {
		return "", err
	}
	if strings.TrimSpace(input.Task) == "" {
		return "", errors.New("task must not be blank")
	}
	return input.Task, nil
}

func (driver *agentTool) mapResult(call tool.Call, child Result) tool.Result {
	mapped := tool.Result{
		ToolCallID: call.ID,
		Name:       driver.definition.Name,
		Content:    child.Text,
		Structured: append(json.RawMessage(nil), child.Structured...),
	}
	terminalFallback, hasTerminalFallback := driver.terminalFallback(child.Messages)
	if hasTerminalFallback {
		if child.Text == "" {
			mapped.Content = terminalFallback.Content
			mapped.Parts = message.CloneContent(terminalFallback.Parts)
		}
		if len(child.Structured) == 0 {
			mapped.Structured = append(json.RawMessage(nil), terminalFallback.Structured...)
		}
		mapped.IsError = terminalFallback.IsError
	}

	if child.Failure != nil {
		structured, _ := json.Marshal(child.Failure)
		mapped.Content = child.Failure.Error()
		mapped.Parts = nil
		mapped.Structured = structured
		mapped.IsError = true
		return driver.finalizeResult(call, mapped)
	}
	if child.StopReason == provider.StopReasonMaxTurns {
		if child.Text != "" {
			mapped.Content = fmt.Sprintf(
				"subagent %q stopped before completion (max_turns): %s",
				driver.definition.Name,
				child.Text,
			)
		} else {
			mapped.Content = fmt.Sprintf(
				"subagent %q stopped before completion (max_turns): child ran out of iterations",
				driver.definition.Name,
			)
		}
		mapped.Parts = nil
		mapped.IsError = true
		return driver.finalizeResult(call, mapped)
	}
	if child.Text == "" && len(child.Structured) == 0 && !hasTerminalFallback {
		mapped.Content = fmt.Sprintf("subagent %q completed without a final answer", driver.definition.Name)
		mapped.IsError = true
	}
	return driver.finalizeResult(call, mapped)
}

func (driver *agentTool) terminalFallback(messages []message.Message) (tool.Result, bool) {
	if driver.child.Tools == nil {
		return tool.Result{}, false
	}
	for index := len(messages) - 1; index >= 0; index-- {
		current := messages[index]
		if current.ToolResult == nil {
			continue
		}
		name := current.ToolResult.Name
		if name == "" {
			name = current.Name
		}
		registered, ok := driver.child.Tools.Driver(name)
		if !ok || !registered.Definition().Terminal {
			continue
		}
		fallback := message.CloneToolResult(*current.ToolResult)
		fallback.SyncLegacyContent()
		return fallback, true
	}
	return tool.Result{}, false
}

func (driver *agentTool) emptyResult(call tool.Call) tool.Result {
	return tool.Result{ToolCallID: call.ID, Name: driver.definition.Name}
}

func (driver *agentTool) errorResult(call tool.Call, content string) tool.Result {
	return driver.finalizeResult(call, tool.Result{Content: content, IsError: true})
}

func (driver *agentTool) finalizeResult(call tool.Call, result tool.Result) tool.Result {
	result = message.CloneToolResult(result)
	result.ToolCallID = call.ID
	result.Name = driver.definition.Name
	result.SyncLegacyContent()
	return result
}

type agentToolValidationDriver struct {
	definition tool.Definition
}

func (driver agentToolValidationDriver) Definition() tool.Definition {
	return cloneAgentToolDefinition(driver.definition)
}

func (agentToolValidationDriver) Execute(_ context.Context, call tool.Call, _ tool.UpdateSink) (tool.Result, error) {
	return tool.Result{ToolCallID: call.ID, Name: call.Name}, nil
}

func defaultAgentToolInputSchema() tool.Schema {
	additionalProperties := false
	return tool.Schema{
		Type: "object",
		Properties: map[string]tool.Schema{
			"task": {
				Type:        "string",
				Description: defaultAgentToolTaskDescription,
			},
		},
		Required:             []string{"task"},
		AdditionalProperties: &additionalProperties,
	}
}

func agentToolSchemaIsZero(schema tool.Schema) bool {
	return schema.Type == "" &&
		schema.Description == "" &&
		len(schema.Properties) == 0 &&
		len(schema.Required) == 0 &&
		len(schema.Enum) == 0 &&
		schema.Items == nil &&
		schema.AdditionalProperties == nil
}

func cloneAgentToolDefinition(definition tool.Definition) tool.Definition {
	definition.InputSchema = cloneJSONSchema(definition.InputSchema)
	return definition
}

func cloneJSONSchema(schema tool.Schema) tool.Schema {
	schema.Required = slices.Clone(schema.Required)
	schema.Enum = slices.Clone(schema.Enum)
	if schema.Properties != nil {
		schema.Properties = maps.Clone(schema.Properties)
		for name, child := range schema.Properties {
			schema.Properties[name] = cloneJSONSchema(child)
		}
	}
	if schema.Items != nil {
		items := cloneJSONSchema(*schema.Items)
		schema.Items = &items
	}
	if schema.AdditionalProperties != nil {
		additionalProperties := *schema.AdditionalProperties
		schema.AdditionalProperties = &additionalProperties
	}
	return schema
}

func cloneAgentToolOutputPolicy(policy OutputPolicy) OutputPolicy {
	policy.Schema = append(json.RawMessage(nil), policy.Schema...)
	return policy
}

func validateAgentToolBudget(budget *Budget) error {
	if budget == nil {
		return nil
	}
	switch {
	case budget.MaxTokens < 0:
		return errors.New("max tokens is negative")
	case budget.MaxToolCalls < 0:
		return errors.New("max tool calls is negative")
	case budget.MaxSteps < 0:
		return errors.New("max steps is negative")
	case budget.MaxWallClock < 0:
		return errors.New("max wall clock is negative")
	default:
		return nil
	}
}

type agentToolNestingContextKey struct{}

type agentToolNestingState struct {
	path     []string
	maxDepth int
}

func nextAgentToolContext(ctx context.Context, name string, configuredMaxDepth int) (context.Context, []string, int, bool) {
	state, _ := ctx.Value(agentToolNestingContextKey{}).(agentToolNestingState)
	effectiveMaxDepth := configuredMaxDepth
	if state.maxDepth > 0 && state.maxDepth < effectiveMaxDepth {
		effectiveMaxDepth = state.maxDepth
	}
	path := slices.Clone(state.path)
	if len(path) >= effectiveMaxDepth {
		return ctx, path, effectiveMaxDepth, false
	}
	path = append(path, name)
	childState := agentToolNestingState{path: slices.Clone(path), maxDepth: effectiveMaxDepth}
	return context.WithValue(ctx, agentToolNestingContextKey{}, childState), path, effectiveMaxDepth, true
}

type agentToolUpdateForwarder struct {
	name     string
	pathJSON string
	sink     tool.UpdateSink

	mu  sync.Mutex
	err error
}

func (forwarder *agentToolUpdateForwarder) emit(_ context.Context, frame Frame) (err error) {
	update, ok := mapAgentToolFrame(frame, forwarder.pathJSON)
	if !ok {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			panicErr := fmt.Errorf("agent tool %q update sink panicked: %v", forwarder.name, recovered)
			forwarder.record(panicErr)
			panic(recovered)
		}
	}()
	if err := forwarder.sink(update); err != nil {
		forwarder.record(err)
		return err
	}
	return nil
}

func (forwarder *agentToolUpdateForwarder) record(err error) {
	if err == nil {
		return
	}
	forwarder.mu.Lock()
	defer forwarder.mu.Unlock()
	if forwarder.err == nil {
		forwarder.err = err
	}
}

func (forwarder *agentToolUpdateForwarder) firstError() error {
	forwarder.mu.Lock()
	defer forwarder.mu.Unlock()
	return forwarder.err
}

func mapAgentToolFrame(frame Frame, pathJSON string) (tool.Update, bool) {
	update := tool.Update{Kind: tool.UpdateProgress}
	var sourceData map[string]string
	switch frame.Kind {
	case FrameText:
		update.Message = frame.Text
		sourceData = map[string]string{"subagent.textPhase": string(frame.TextPhase)}
	case FrameToolCall:
		name := ""
		if frame.ToolCall != nil {
			name = frame.ToolCall.Name
		}
		update.Message = fmt.Sprintf("child tool %q requested", name)
		sourceData = map[string]string{"subagent.childToolName": name}
	case FrameToolResult:
		name := ""
		isError := false
		if frame.ToolResult != nil {
			name = frame.ToolResult.Name
			isError = frame.ToolResult.IsError
		}
		status := "completed"
		if isError {
			status = "failed"
		}
		update.Message = fmt.Sprintf("child tool %q %s", name, status)
		sourceData = map[string]string{
			"subagent.childToolName":    name,
			"subagent.childToolIsError": strconv.FormatBool(isError),
		}
	case FrameToolUpdate:
		if frame.ToolUpdate == nil {
			return tool.Update{}, false
		}
		childUpdate := tool.CloneUpdate(*frame.ToolUpdate)
		switch childUpdate.Kind {
		case tool.UpdateProgress:
			update.Message = childUpdate.Message
			sourceData = childUpdate.Data
		case tool.UpdateOutput:
			update.Message = "child tool emitted output"
			sourceData = map[string]string{
				"subagent.outputPartCount": strconv.Itoa(len(childUpdate.Parts)),
			}
		default:
			return tool.Update{}, false
		}
		if sourceData == nil {
			sourceData = make(map[string]string)
		} else {
			sourceData = maps.Clone(sourceData)
		}
		sourceData["subagent.childToolCallId"] = childUpdate.ToolCallID
		sourceData["subagent.childOperationId"] = childUpdate.OperationID
	case FrameDone:
		update.Message = "model turn completed"
		sourceData = map[string]string{"subagent.stopReason": string(frame.StopReason)}
	case FrameError:
		update.Message = "model turn failed"
	case FrameThinking, FrameToolCallDelta:
		return tool.Update{}, false
	default:
		return tool.Update{}, false
	}
	if sourceData == nil {
		sourceData = make(map[string]string, 2)
	} else {
		sourceData = maps.Clone(sourceData)
	}
	sourceData["subagent.path"] = pathJSON
	sourceData["subagent.frameKind"] = string(frame.Kind)
	update.Data = sourceData
	return update, true
}

type agentToolUsageContextKey struct{}

type agentToolUsageCollector struct {
	mu    sync.Mutex
	usage provider.Usage
}

func (collector *agentToolUsageCollector) add(usage provider.Usage) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.usage = collector.usage.Add(usage)
}

func (collector *agentToolUsageCollector) snapshot() provider.Usage {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	return collector.usage
}

func reportAgentToolUsage(ctx context.Context, usage provider.Usage) {
	collector, _ := ctx.Value(agentToolUsageContextKey{}).(*agentToolUsageCollector)
	if collector != nil {
		collector.add(usage)
	}
}

type agentToolReservationContextKey struct{}

type agentToolTokenTracker struct {
	gate chan struct{}

	mu        sync.Mutex
	remaining int64
}

func newAgentToolTokenTracker(remaining int64) *agentToolTokenTracker {
	return &agentToolTokenTracker{
		gate:      make(chan struct{}, 1),
		remaining: max(int64(0), remaining),
	}
}

func agentToolReservationTrackerFromContext(ctx context.Context) *agentToolTokenTracker {
	tracker, _ := ctx.Value(agentToolReservationContextKey{}).(*agentToolTokenTracker)
	return tracker
}

func (tracker *agentToolTokenTracker) reserve(ctx context.Context, name string, base *Budget) (*agentToolTokenReservation, *Budget, error) {
	select {
	case tracker.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, errors.Join(tool.ErrNotExecuted, ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		<-tracker.gate
		return nil, nil, errors.Join(tool.ErrNotExecuted, err)
	}

	tracker.mu.Lock()
	remaining := tracker.remaining
	if remaining <= 0 {
		tracker.mu.Unlock()
		<-tracker.gate
		return nil, nil, agentToolParentBudgetExhaustedError{name: name}
	}
	budget := cloneBudget(base)
	if budget == nil {
		budget = &Budget{}
	}
	claim := remaining
	if budget.MaxTokens > 0 && budget.MaxTokens < claim {
		claim = budget.MaxTokens
	}
	tracker.remaining -= claim
	tracker.mu.Unlock()

	budget.MaxTokens = claim
	reservation := &agentToolTokenReservation{tracker: tracker, claim: claim}
	return reservation, budget, nil
}

type agentToolTokenReservation struct {
	tracker *agentToolTokenTracker
	claim   int64
	once    sync.Once
}

func (reservation *agentToolTokenReservation) settle(usage provider.Usage) {
	if reservation == nil {
		return
	}
	reservation.once.Do(func() {
		normalized := provider.Usage{}.Add(usage)
		spent := min(reservation.claim, int64(normalized.TotalTokens))
		reservation.tracker.mu.Lock()
		reservation.tracker.remaining += reservation.claim - spent
		reservation.tracker.mu.Unlock()
		<-reservation.tracker.gate
	})
}

type agentToolParentBudgetExhaustedError struct {
	name string
}

func (err agentToolParentBudgetExhaustedError) Error() string {
	return fmt.Sprintf("agent tool %q refused: parent token budget exhausted", err.name)
}

func (agentToolParentBudgetExhaustedError) Unwrap() []error {
	return []error{tool.ErrNotExecuted, ErrBudgetExhausted}
}

func withAgentToolDispatchContext(ctx context.Context, maxTokens int64, usage provider.Usage) (context.Context, *agentToolUsageCollector) {
	collector := &agentToolUsageCollector{}
	ctx = context.WithValue(ctx, agentToolUsageContextKey{}, collector)
	if maxTokens <= 0 {
		return ctx, collector
	}
	normalized := provider.Usage{}.Add(usage)
	remaining := maxTokens - int64(normalized.TotalTokens)
	ctx = context.WithValue(ctx, agentToolReservationContextKey{}, newAgentToolTokenTracker(remaining))
	return ctx, collector
}
