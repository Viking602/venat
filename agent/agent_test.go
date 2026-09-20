package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

type fakeProvider struct{}

type failingProvider struct{}

func (failingProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "failing"}
}

func (failingProvider) Stream(context.Context, provider.Request) (provider.Stream, error) {
	return nil, errors.New("provider unavailable")
}

func (fakeProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "fake"}
}

func (f fakeProvider) Stream(_ context.Context, request provider.Request) (provider.Stream, error) {
	if len(request.Messages) >= 2 && request.Messages[len(request.Messages)-1].Role == message.RoleTool {
		return provider.NewSliceStream([]provider.Event{
			{Kind: provider.EventTextDelta, Text: "done"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		}), nil
	}
	return provider.NewSliceStream([]provider.Event{
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-1",
				Name:      "lookup",
				Arguments: json.RawMessage(`{"query":"venat"}`),
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}), nil
}

func TestEngineRunsToolLoop(t *testing.T) {
	driver, err := kit.Tool("lookup", func(_ context.Context, input struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "result:" + input.Query, nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}
	engine := Engine{
		Provider: fakeProvider{},
		Tools:    tool.NewBus(driver),
	}
	result, err := engine.RunMessages(context.Background(), LoopInput{
		Model: "test-model",
		Messages: []message.Message{
			message.NewText(message.RoleUser, "find venat"),
		},
		MaxIterations: 3,
		ToolMode:      tool.ModeSequential,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(result.Messages) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(result.Messages))
	}
	if result.Messages[len(result.Messages)-1].Text != "done" {
		t.Fatalf("expected final assistant text, got %#v", result.Messages[len(result.Messages)-1])
	}
}

func TestEngineModelCallUsesSelectedFallbackIdentity(t *testing.T) {
	engine := Engine{
		Provider: provider.Fallback(
			failingProvider{},
			&scriptedProvider{turns: [][]provider.Event{{
				{Kind: provider.EventTextDelta, Text: "fallback answer"},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
			}}},
		),
	}
	output, err := engine.RunMessages(context.Background(), LoopInput{
		Model:    "fallback-model",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err != nil {
		t.Fatalf("RunMessages() error = %v", err)
	}
	if len(output.Steps) != 1 || output.Steps[0].ModelCall == nil {
		t.Fatalf("steps = %#v, want one model call", output.Steps)
	}
	call := output.Steps[0].ModelCall
	if call.Provider != "scripted" || call.Model != "fallback-model" {
		t.Fatalf("model call identity = %#v, want scripted/fallback-model", call)
	}
}

type partialErrorProvider struct{}

func (partialErrorProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "selected-partial"}
}

func (partialErrorProvider) Stream(context.Context, provider.Request) (provider.Stream, error) {
	return &partialErrorStream{}, nil
}

type partialErrorStream struct {
	sent bool
}

func (s *partialErrorStream) Recv() (provider.Event, error) {
	if !s.sent {
		s.sent = true
		return provider.Event{
			Kind: provider.EventTextDelta, Text: "partial",
			Usage: provider.Usage{InputTokens: 2, OutputTokens: 1, TotalTokens: 3},
		}, nil
	}
	return provider.Event{}, errors.New("partial stream failure")
}

func (*partialErrorStream) Close() error { return nil }

func TestEnginePreservesFallbackIdentityOnPartialStreamError(t *testing.T) {
	engine := Engine{Provider: provider.Fallback(failingProvider{}, partialErrorProvider{})}
	output, err := engine.RunMessages(context.Background(), LoopInput{
		Model:    "selected-model",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err == nil || !strings.Contains(err.Error(), "partial stream failure") {
		t.Fatalf("RunMessages() error = %v, want partial stream failure", err)
	}
	if len(output.Steps) != 1 || output.Steps[0].ModelCall == nil {
		t.Fatalf("steps = %#v, want one failed model call", output.Steps)
	}
	call := output.Steps[0].ModelCall
	if call.Provider != "selected-partial" || call.Model != "selected-model" || call.TotalTokens != 3 {
		t.Fatalf("partial model call = %#v, want selected-partial/selected-model/3", call)
	}
}

func TestEnginePartialStreamErrorJoinsStepObserverFailure(t *testing.T) {
	observeErr := errors.New("step observation failed")
	engine := Engine{Provider: provider.Fallback(failingProvider{}, partialErrorProvider{})}
	_, err := engine.RunMessages(context.Background(), LoopInput{
		Model:    "selected-model",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
		StepObserver: StepObserverFunc(func(context.Context, Step) error {
			return observeErr
		}),
	})
	if !errors.Is(err, observeErr) || !strings.Contains(err.Error(), "partial stream failure") {
		t.Fatalf("RunMessages() error = %v, want stream and observer causes", err)
	}
}

func TestEngineGuardrailPanicPreservesFallbackUsageIdentity(t *testing.T) {
	var recorded []Step
	engine := Engine{
		Provider: provider.Fallback(
			failingProvider{},
			&scriptedProvider{turns: [][]provider.Event{{
				{Kind: provider.EventTextDelta, Text: "answer"},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: provider.Usage{TotalTokens: 3}},
			}}},
		),
	}
	output, err := engine.RunMessages(context.Background(), LoopInput{
		Model:    "selected-model",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
		OutputGuardrails: []OutputGuardrail{
			NewOutputGuardrail("panic", func(context.Context, OutputGuardrailInput) (OutputGuardrailResult, error) {
				panic("guardrail panic")
			}),
		},
		StepObserver: StepObserverFunc(func(_ context.Context, step Step) error {
			recorded = append(recorded, step)
			return nil
		}),
	})
	if err == nil || !errors.Is(err, ErrPanicRecovered) {
		t.Fatalf("RunMessages() error = %v, want ErrPanicRecovered", err)
	}
	if len(output.Steps) != 1 || output.Steps[0].ModelCall == nil || len(recorded) != 1 {
		t.Fatalf("output steps=%#v recorded=%#v, want one recovered step", output.Steps, recorded)
	}
	if output.Steps[0].ModelCall.Provider != "scripted" || recorded[0].ModelCall.Provider != "scripted" {
		t.Fatalf("recovered model identity output=%#v recorded=%#v, want scripted", output.Steps[0].ModelCall, recorded[0].ModelCall)
	}
}

// alwaysToolProvider emits a non-terminal tool call on every turn unless
// completeAfter is positive and that many tool turns have completed.
type alwaysToolProvider struct {
	calls         int
	completeAfter int
	usage         provider.Usage
}

func (*alwaysToolProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "always-tool"}
}

func (p *alwaysToolProvider) Stream(_ context.Context, _ provider.Request) (provider.Stream, error) {
	p.calls++
	if p.completeAfter > 0 && p.calls > p.completeAfter {
		return provider.NewSliceStream([]provider.Event{
			{Kind: provider.EventTextDelta, Text: "done"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: p.usage},
		}), nil
	}
	return provider.NewSliceStream([]provider.Event{
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-1",
				Name:      "lookup",
				Arguments: json.RawMessage(`{"query":"x"}`),
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse, Usage: p.usage},
	}), nil
}

func TestRunMessagesDefaultMaxIterationsIsTwelve(t *testing.T) {
	driver, err := kit.Tool("lookup", func(_ context.Context, _ struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "result", nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}
	prov := &alwaysToolProvider{}
	engine := Engine{Provider: prov, Tools: tool.NewBus(driver)}
	result, err := engine.RunMessages(context.Background(), LoopInput{
		Model:    "test-model",
		Messages: []message.Message{message.NewText(message.RoleUser, "loop forever")},
		// MaxIterations intentionally unset -> exercises the default ceiling.
	})
	if err != nil {
		t.Fatalf("RunMessages() error = %v", err)
	}
	if result.StopReason != provider.StopReasonMaxTurns {
		t.Fatalf("expected StopReasonMaxTurns at the ceiling, got %q", result.StopReason)
	}
	if result.Iterations != 12 {
		t.Fatalf("expected default ceiling of 12 iterations, got %d", result.Iterations)
	}
	if prov.calls != 12 {
		t.Fatalf("expected 12 provider calls at the default ceiling, got %d", prov.calls)
	}
}

func TestRunMessagesUnlimitedIterationsRunsUntilCompletion(t *testing.T) {
	driver, err := kit.Tool("lookup", func(_ context.Context, _ struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "result", nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}
	prov := &alwaysToolProvider{completeAfter: 20}
	engine := Engine{Provider: prov, Tools: tool.NewBus(driver)}
	result, err := engine.RunMessages(context.Background(), LoopInput{
		Model:               "test-model",
		Messages:            []message.Message{message.NewText(message.RoleUser, "continue until done")},
		UnlimitedIterations: true,
	})
	if err != nil {
		t.Fatalf("RunMessages() error = %v", err)
	}
	if result.StopReason != provider.StopReasonComplete || result.Iterations != 21 {
		t.Fatalf("result stop=%q iterations=%d, want complete after 21", result.StopReason, result.Iterations)
	}
	if prov.calls != 21 {
		t.Fatalf("provider calls = %d, want 21", prov.calls)
	}
}

func TestEngineRunForwardsUnlimitedIterations(t *testing.T) {
	prov := &alwaysToolProvider{completeAfter: 20}
	engine := newLoopToolEngine(t, prov)
	engine.LoopPolicy = LoopPolicy{UnlimitedIterations: true}

	result := engine.Run(context.Background(), Request{Prompt: "continue until done"}, OutputPolicy{})

	if result.Failure != nil {
		t.Fatalf("Engine.Run() failure = %v", result.Failure)
	}
	if result.StopReason != provider.StopReasonComplete || len(result.Steps) != 21 {
		t.Fatalf("result stop=%q steps=%d, want complete after 21", result.StopReason, len(result.Steps))
	}
}

func TestEngineReturnsToolFeedbackWhenToolBusMissing(t *testing.T) {
	engine := Engine{Provider: fakeProvider{}}
	out, err := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "find venat")},
		MaxIterations: 1,
	})
	if err != nil || out.ToolCallsUsed != 1 || out.Messages[len(out.Messages)-1].ToolResult == nil || !out.Messages[len(out.Messages)-1].ToolResult.IsError {
		t.Fatalf("expected completed rejection, got %+v, %v", out, err)
	}
}

// scriptedProvider returns a pre-scripted event list per invocation, in
// order, so tests can drive Engine through multiple turns deterministically.
type scriptedProvider struct {
	turns     [][]provider.Event
	requests  []provider.Request
	callIndex int
}

func (*scriptedProvider) Metadata() provider.Metadata { return provider.Metadata{Name: "scripted"} }

func (s *scriptedProvider) Stream(_ context.Context, request provider.Request) (provider.Stream, error) {
	s.requests = append(s.requests, request)
	events := s.turns[s.callIndex]
	s.callIndex++
	return provider.NewSliceStream(events), nil
}

func TestEngineCollectsThinkingDeltas(t *testing.T) {
	driver := &scriptedProvider{
		turns: [][]provider.Event{{
			{Kind: provider.EventThinkingDelta, Thinking: "thought-1"},
			{Kind: provider.EventThinkingDelta, Thinking: ";thought-2"},
			{Kind: provider.EventTextDelta, Text: "final answer"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		}},
	}
	engine := Engine{Provider: driver}
	result, err := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "hi")},
		MaxIterations: 1,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Thinking != "thought-1;thought-2" {
		t.Fatalf("expected accumulated thinking on Result, got %q", result.Thinking)
	}
	last := result.Messages[len(result.Messages)-1]
	if last.Thinking != "thought-1;thought-2" {
		t.Fatalf("expected thinking on assistant message, got %q", last.Thinking)
	}
	if last.Text != "final answer" {
		t.Fatalf("expected text answer, got %q", last.Text)
	}
}

func TestEngineForwardsStopAndThinkingBudget(t *testing.T) {
	driver := &scriptedProvider{
		turns: [][]provider.Event{{
			{Kind: provider.EventTextDelta, Text: "ok"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		}},
	}
	engine := Engine{Provider: driver}
	_, err := engine.RunMessages(context.Background(), LoopInput{
		Model:          "test-model",
		Messages:       []message.Message{message.NewText(message.RoleUser, "hi")},
		MaxIterations:  1,
		StopSequences:  []string{"Wait,"},
		ThinkingBudget: 3000,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(driver.requests) != 1 {
		t.Fatalf("expected 1 call, got %d", len(driver.requests))
	}
	req := driver.requests[0]
	if len(req.StopSequences) != 1 || req.StopSequences[0] != "Wait," {
		t.Fatalf("stop not forwarded, got %#v", req.StopSequences)
	}
	if req.ThinkingBudget != 3000 {
		t.Fatalf("thinking budget not forwarded, got %d", req.ThinkingBudget)
	}
}

func TestEngineForwardsExtraBody(t *testing.T) {
	driver := &scriptedProvider{
		turns: [][]provider.Event{{
			{Kind: provider.EventTextDelta, Text: "ok"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		}},
	}
	engine := Engine{Provider: driver}
	_, err := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "hi")},
		MaxIterations: 1,
		ExtraBody: map[string]any{
			"chat_template_kwargs": map[string]any{"thinking": true},
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(driver.requests) != 1 {
		t.Fatalf("expected 1 call, got %d", len(driver.requests))
	}
	requireExtraBodyThinkingEnabled(t, driver.requests[0].ExtraBody)
}

func requireExtraBodyThinkingEnabled(t *testing.T, body map[string]any) {
	t.Helper()
	extra, ok := body["chat_template_kwargs"].(map[string]any)
	if !ok || extra["thinking"] != true {
		t.Fatalf("expected extra body to be forwarded, got %#v", body)
	}
}

type opaqueHostStub struct{}

func TestEngineForwardsTypedProviderOptions(t *testing.T) {
	driver := &scriptedProvider{turns: [][]provider.Event{{
		{Kind: provider.EventTextDelta, Text: "ok"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}}}
	parallel := true
	observed := provider.ContextUsage{}
	engine := Engine{
		Provider:          driver,
		Model:             "test-model",
		PromptCacheKey:    "session-cache",
		ServiceTier:       "priority",
		ParallelToolCalls: &parallel,
		ContextUsage:      func(usage provider.ContextUsage) { observed = usage },
	}
	result := engine.Run(context.Background(), Request{Prompt: "test typed channels"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatalf("Run() failure = %#v", result.Failure)
	}
	if len(driver.requests) != 1 {
		t.Fatalf("provider requests = %d, want 1", len(driver.requests))
	}
	request := driver.requests[0]
	if request.PromptCacheKey != "session-cache" || request.ServiceTier != "priority" ||
		request.ParallelToolCalls == nil || !*request.ParallelToolCalls || request.ParallelToolCalls == &parallel {
		t.Fatalf("typed request channels = %#v", request)
	}
	if request.ContextUsage == nil {
		t.Fatalf("context usage observer was not forwarded: %#v", request)
	}
	request.ContextUsage(provider.ContextUsage{UsedTokens: 321, MaxTokens: 1000})
	if observed.UsedTokens != 321 || observed.MaxTokens != 1000 {
		t.Fatalf("context usage observer received %#v", observed)
	}
}

func TestEngineRejectsHostObjectsInExtraBodyBeforeProviderCall(t *testing.T) {
	driver := &scriptedProvider{turns: [][]provider.Event{{
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}}}
	engine := Engine{Provider: driver}
	_, err := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "hi")},
		MaxIterations: 1,
		ExtraBody:     map[string]any{"native_host": opaqueHostStub{}},
	})
	if err == nil || !strings.Contains(err.Error(), "typed Request field") {
		t.Fatalf("host-object ExtraBody error = %v", err)
	}
	if len(driver.requests) != 0 {
		t.Fatalf("provider received %d requests after validation failure", len(driver.requests))
	}
}

func TestEngineAccumulatesUsageAcrossTurns(t *testing.T) {
	driver := &scriptedProvider{
		turns: [][]provider.Event{
			{
				{
					Kind: provider.EventToolCall,
					ToolCall: &message.ToolCall{
						ID:        "call-1",
						Name:      "lookup",
						Arguments: json.RawMessage(`{"query":"venat"}`),
					},
				},
				{
					Kind:       provider.EventDone,
					StopReason: provider.StopReasonToolUse,
					Usage: provider.Usage{
						InputTokens:  11,
						OutputTokens: 7,
						TotalTokens:  18,
					},
				},
			},
			{
				{Kind: provider.EventTextDelta, Text: "done"},
				{
					Kind:       provider.EventDone,
					StopReason: provider.StopReasonComplete,
					Usage: provider.Usage{
						InputTokens:  5,
						OutputTokens: 3,
						TotalTokens:  8,
					},
				},
			},
		},
	}
	driverTool, err := kit.Tool("lookup", func(_ context.Context, input struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "result:" + input.Query, nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}
	engine := Engine{
		Provider: driver,
		Tools:    tool.NewBus(driverTool),
	}
	result, err := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "find venat")},
		MaxIterations: 3,
		ToolMode:      tool.ModeSequential,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Usage.InputTokens != 16 || result.Usage.OutputTokens != 10 || result.Usage.TotalTokens != 26 {
		t.Fatalf("expected accumulated usage, got %#v", result.Usage)
	}
}

func TestCollectBuildsToolCallsFromDeltasInStableOrder(t *testing.T) {
	engine := Engine{}
	assistant, _, _, err := engine.collect(context.Background(), provider.NewSliceStream([]provider.Event{
		{
			Kind: provider.EventToolCallDelta,
			ToolCallDelta: &provider.ToolCallDelta{
				ID:             "call-b",
				Name:           "beta",
				ArgumentsDelta: `{"value":"b"}`,
			},
		},
		{
			Kind: provider.EventToolCallDelta,
			ToolCallDelta: &provider.ToolCallDelta{
				ID:             "call-a",
				Name:           "alpha",
				ArgumentsDelta: `{"value":"a"}`,
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}), nil, nil)
	if err != nil {
		t.Fatalf("collect() error = %v", err)
	}
	if len(assistant.ToolCalls) != 2 {
		t.Fatalf("expected 2 tool calls, got %#v", assistant.ToolCalls)
	}
	if assistant.ToolCalls[0].ID != "call-b" || assistant.ToolCalls[1].ID != "call-a" {
		t.Fatalf("expected stable tool call ordering, got %#v", assistant.ToolCalls)
	}
}

func TestCollectMergesFullAndDeltaToolCalls(t *testing.T) {
	engine := Engine{}
	assistant, _, _, err := engine.collect(context.Background(), provider.NewSliceStream([]provider.Event{
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:   "call-1",
				Name: "lookup",
			},
		},
		{
			Kind: provider.EventToolCallDelta,
			ToolCallDelta: &provider.ToolCallDelta{
				ID:             "call-1",
				ArgumentsDelta: `{"query":"venat"}`,
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}), nil, nil)
	if err != nil {
		t.Fatalf("collect() error = %v", err)
	}
	if len(assistant.ToolCalls) != 1 {
		t.Fatalf("expected one merged tool call, got %#v", assistant.ToolCalls)
	}
	if got := string(assistant.ToolCalls[0].Arguments); got != `{"query":"venat"}` {
		t.Fatalf("expected merged arguments, got %q", got)
	}
}

func TestCollectMarksInvalidToolCallJSONForCorrection(t *testing.T) {
	engine := Engine{}
	assistant, _, _, err := engine.collect(context.Background(), provider.NewSliceStream([]provider.Event{
		{
			Kind: provider.EventToolCallDelta,
			ToolCallDelta: &provider.ToolCallDelta{
				ID:             "call-1",
				Name:           "lookup",
				ArgumentsDelta: `{"query":`,
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}), nil, nil)
	if err != nil || len(assistant.ToolCalls) != 0 || assistant.Metadata[responseRecoveryKey] != "invalid_tool_arguments" {
		t.Fatalf("expected correction without raw malformed calls: %+v, %v", assistant, err)
	}
}

func TestCollectRejectsDuplicateToolCallID(t *testing.T) {
	engine := Engine{}
	_, _, _, err := engine.collect(context.Background(), provider.NewSliceStream([]provider.Event{
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-1",
				Name:      "lookup",
				Arguments: json.RawMessage(`{"query":"one"}`),
			},
		},
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-1",
				Name:      "lookup",
				Arguments: json.RawMessage(`{"query":"two"}`),
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}), nil, nil)
	if !errors.Is(err, provider.ErrDuplicateToolCallID) {
		t.Fatalf("expected duplicate tool call id error, got %v", err)
	}
}

type terminalTool struct{}

func (terminalTool) Definition() tool.Definition {
	return tool.Definition{
		Name:        "submit_report",
		Description: "submit report",
		Terminal:    true,
		InputSchema: tool.Schema{
			Type: "object",
			Properties: map[string]message.JSONSchema{
				"answer": {Type: "string"},
			},
			Required: []string{"answer"},
		},
	}
}

func (terminalTool) Execute(_ context.Context, call tool.Call, _ tool.UpdateSink) (tool.Result, error) {
	return tool.Result{
		ToolCallID: call.ID,
		Name:       call.Name,
		Structured: call.Arguments,
	}, nil
}

func TestEngineStopsAfterTerminalTool(t *testing.T) {
	driver := &scriptedProvider{
		turns: [][]provider.Event{{
			{
				Kind: provider.EventToolCall,
				ToolCall: &message.ToolCall{
					ID:        "call-1",
					Name:      "submit_report",
					Arguments: json.RawMessage(`{"answer":"done"}`),
				},
			},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		}},
	}
	engine := Engine{
		Provider: driver,
		Tools:    tool.NewBus(terminalTool{}),
	}
	result, err := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "finish")},
		MaxIterations: 3,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(driver.requests) != 1 {
		t.Fatalf("expected terminal tool to stop model loop after first turn, got %d requests", len(driver.requests))
	}
	if len(result.Messages) != 3 {
		t.Fatalf("expected assistant and terminal tool result only, got %#v", result.Messages)
	}
	if result.Messages[len(result.Messages)-1].ToolResult == nil {
		t.Fatalf("expected final message to be tool result, got %#v", result.Messages[len(result.Messages)-1])
	}
}

func TestEnginePersistsProviderState(t *testing.T) {
	state := json.RawMessage(`[{"type":"reasoning","id":"rs_1"}]`)
	driver := &scriptedProvider{
		turns: [][]provider.Event{{
			{Kind: provider.EventTextDelta, Text: "done"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, ProviderState: state},
		}},
	}
	engine := Engine{Provider: driver}
	result, err := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "hi")},
		MaxIterations: 1,
	})
	if err != nil {
		t.Fatalf("RunMessages() error = %v", err)
	}
	last := result.Messages[len(result.Messages)-1]
	if string(last.ProviderState) != string(state) {
		t.Fatalf("assistant ProviderState = %s, want %s", last.ProviderState, state)
	}
}

func TestCollectRejectsProviderTurnOverAggregateByteLimit(t *testing.T) {
	payload := strings.Repeat("x", 1<<20)
	events := make([]provider.Event, 65)
	for index := range events {
		events[index] = provider.Event{Kind: provider.EventTextDelta, Text: payload}
	}
	_, _, _, err := (Engine{}).collect(
		context.Background(),
		provider.NewSliceStream(events),
		nil,
		nil,
	)
	if !errors.Is(err, ErrProviderTurnLimit) {
		t.Fatalf("provider turn limit error = %v", err)
	}
}
