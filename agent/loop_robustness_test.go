package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

// TestRunRecoversGuardrailPanicAsEngineError pins the loop's panic backstop:
// caller-supplied extension code (here an output guardrail) that panics must
// degrade to a typed FailureKindEngineError carrying ErrPanicRecovered, not
// crash the worker. If the backstop were missing this test would abort the
// package binary instead of failing an assertion.
func TestRunRecoversGuardrailPanicAsEngineError(t *testing.T) {
	engine := Engine{
		Provider: singleTurnProvider("answer"),
		Model:    "test-model",
		OutputGuardrails: []OutputGuardrail{
			NewOutputGuardrail("boom", func(_ context.Context, _ OutputGuardrailInput) (OutputGuardrailResult, error) {
				panic("guardrail exploded")
			}),
		},
	}

	result := engine.Run(context.Background(), Request{Prompt: "do the thing"}, OutputPolicy{})

	if result.Failure == nil {
		t.Fatal("expected a failure when a guardrail panics")
	}
	if result.Failure.Kind != FailureKindEngineError {
		t.Fatalf("Failure.Kind = %s, want engine_error", result.Failure.Kind)
	}
	if !errors.Is(result.Failure, ErrPanicRecovered) {
		t.Fatalf("errors.Is(Failure, ErrPanicRecovered) = false, Failure = %v", result.Failure)
	}
}

// panicBeforeModelHook panics from BeforeModelCall, proving HookChain contains
// caller panics before they reach the Engine loop.
type panicBeforeModelHook struct{}

func (panicBeforeModelHook) TransformContext(_ context.Context, m []message.Message) ([]message.Message, error) {
	return m, nil
}

func (panicBeforeModelHook) BeforeModelCall(_ context.Context, _ *provider.Request) error {
	panic("hook exploded")
}
func (panicBeforeModelHook) BeforeToolCall(_ context.Context, _ *tool.Call) error  { return nil }
func (panicBeforeModelHook) AfterToolCall(_ context.Context, _ *tool.Result) error { return nil }
func (panicBeforeModelHook) OnEvent(_ context.Context, _ provider.Event) error     { return nil }

// TestRunRecoversHookPanicAsEngineError pins HookChain panic recovery and the
// typed Engine failure path.
func TestRunRecoversHookPanicAsEngineError(t *testing.T) {
	engine := Engine{
		Provider: singleTurnProvider("answer"),
		Model:    "test-model",
		Hooks:    NewHookChain(panicBeforeModelHook{}),
	}

	result := engine.Run(context.Background(), Request{Prompt: "do the thing"}, OutputPolicy{})

	if result.Failure == nil {
		t.Fatal("expected a failure when a hook panics")
	}
	if result.Failure.Kind != FailureKindEngineError {
		t.Fatalf("Failure.Kind = %s, want engine_error", result.Failure.Kind)
	}
	if !errors.Is(result.Failure, ErrHookPanic) {
		t.Fatalf("errors.Is(Failure, ErrHookPanic) = false, Failure = %v", result.Failure)
	}
}

// erroringSecondTurnProvider serves a tool call on the first turn and fails on
// the second, so the loop accumulates a real trace (assistant tool call + tool
// result + one Step) before the error lands.
type erroringSecondTurnProvider struct{ calls int }

func (*erroringSecondTurnProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "err-second-turn"}
}

func (p *erroringSecondTurnProvider) Stream(_ context.Context, _ provider.Request) (provider.Stream, error) {
	p.calls++
	if p.calls == 1 {
		return provider.NewSliceStream([]provider.Event{
			{
				Kind: provider.EventToolCall,
				ToolCall: &message.ToolCall{
					ID:        "call-1",
					Name:      "lookup",
					Arguments: json.RawMessage(`{"query":"x"}`),
				},
			},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		}), nil
	}
	return nil, errors.New("provider down on turn 2")
}

// TestRunMessagesPreservesTraceOnNonBudgetError pins that a non-budget loop
// error returns the partial trace accumulated so far rather than an empty
// LoopOutput: the messages and steps from completed turns survive so a
// scheduler and the durable record keep the context that led up to the failure.
func TestRunMessagesPreservesTraceOnNonBudgetError(t *testing.T) {
	driver, err := kit.Tool("lookup", func(_ context.Context, _ struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "result", nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}
	engine := Engine{
		Provider: &erroringSecondTurnProvider{},
		Tools:    tool.NewBus(driver),
	}

	out, runErr := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 3,
		ToolMode:      tool.ModeSequential,
	})

	if runErr == nil {
		t.Fatal("expected the second-turn provider error to surface")
	}
	// user prompt + assistant tool call + tool result must all survive.
	if len(out.Messages) < 3 {
		t.Fatalf("preserved messages = %d, want >= 3 (the completed first turn)", len(out.Messages))
	}
	if len(out.Steps) < 1 {
		t.Fatalf("preserved steps = %d, want >= 1 (the completed first turn)", len(out.Steps))
	}
	if out.StopReason != provider.StopReasonError {
		t.Fatalf("StopReason = %s, want %s", out.StopReason, provider.StopReasonError)
	}
}

// TestRunMessagesPreservesTraceWhenSinkEmitFails pins the trace-preservation
// contract for the one failure mode that previously broke it: when a Sink is
// configured and Emit fails on the tool-result frame, appendToolResults must
// return the history accumulated so far rather than nil, so the prompt, the
// assistant tool call, and the tool result all survive on the returned
// LoopOutput. Before the fix appendToolResults returned nil on an Emit failure,
// clobbering current so the partial-trace error path preserved an empty history.
func TestRunMessagesPreservesTraceWhenSinkEmitFails(t *testing.T) {
	driver, err := kit.Tool("lookup", func(_ context.Context, _ struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "result", nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}

	sinkErr := errors.New("sink delivery failed")
	failingSink := SinkFunc(func(_ context.Context, frame Frame) error {
		// Let the model turn stream cleanly; fail only on the tool result so the
		// failure lands inside appendToolResults, the path under test.
		if frame.Kind == FrameToolResult {
			return sinkErr
		}
		return nil
	})

	engine := Engine{
		Provider: &erroringSecondTurnProvider{},
		Tools:    tool.NewBus(driver),
	}

	out, runErr := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 3,
		ToolMode:      tool.ModeSequential,
		Sink:          failingSink,
	})

	if runErr == nil {
		t.Fatal("expected the sink Emit failure to surface as the loop error")
	}
	if !errors.Is(runErr, sinkErr) {
		t.Fatalf("errors.Is(runErr, sinkErr) = false, runErr = %v", runErr)
	}
	// prompt + assistant tool call + tool result must all survive the failure.
	if len(out.Messages) < 3 {
		t.Fatalf("preserved messages = %d, want >= 3 (prompt, assistant tool call, tool result)", len(out.Messages))
	}
	if out.StopReason != provider.StopReasonError {
		t.Fatalf("StopReason = %s, want %s", out.StopReason, provider.StopReasonError)
	}
	// The tool call was dispatched by dispatchPreparedTools before the sink failed
	// on its result frame, so the partial output must charge it: a caller resuming from
	// this LoopOutput would otherwise under-count its MaxToolCalls budget.
	if out.ToolCallsUsed < 1 {
		t.Fatalf("ToolCallsUsed = %d, want >= 1 (the dispatched tool call must be charged on the partial output)", out.ToolCallsUsed)
	}
}

// TestRunMessagesPreservesTraceWhenSinkEmitPanics pins the panic twin of the
// trace-preservation contract. A Sink whose Emit panics on the tool-result frame
// is a recovered panic source the loop's recover defer is meant to salvage. The
// defer reports the caller's current slice, and appendToolResults appends each
// result to that slice before emitting it, so the produced tool result must
// survive on the recovered LoopOutput. A by-value return would lose it: the panic
// unwinds before the caller assigns appendToolResults' return value, leaving the
// outer history one message short of the result it already produced.
func TestRunMessagesPreservesTraceWhenSinkEmitPanics(t *testing.T) {
	driver, err := kit.Tool("lookup", func(_ context.Context, _ struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "result", nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}

	panickingSink := SinkFunc(func(_ context.Context, frame Frame) error {
		// Let the model turn stream cleanly; panic only on the tool result so the
		// panic unwinds out of appendToolResults, the path under test.
		if frame.Kind == FrameToolResult {
			panic("sink delivery panicked")
		}
		return nil
	})

	engine := Engine{
		Provider: &erroringSecondTurnProvider{},
		Tools:    tool.NewBus(driver),
	}

	out, runErr := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 3,
		ToolMode:      tool.ModeSequential,
		Sink:          panickingSink,
	})

	if runErr == nil || !errors.Is(runErr, ErrPanicRecovered) {
		t.Fatalf("expected the sink Emit panic to be recovered as ErrPanicRecovered, got %v", runErr)
	}
	if out.StopReason != provider.StopReasonError {
		t.Fatalf("StopReason = %s, want %s", out.StopReason, provider.StopReasonError)
	}
	// The tool result was appended before the panicking Emit, so it must survive on
	// the recovered partial output rather than being lost with the unwound call.
	toolResults := 0
	for _, m := range out.Messages {
		if m.Role == message.RoleTool {
			toolResults++
		}
	}
	if toolResults < 1 {
		t.Fatalf("recovered messages have %d tool results, want >= 1 (the produced result must survive the sink panic); messages = %d", toolResults, len(out.Messages))
	}
	// The dispatched call is charged on the partial output, same as the error path.
	if out.ToolCallsUsed < 1 {
		t.Fatalf("ToolCallsUsed = %d, want >= 1 (the dispatched tool call must be charged on the partial output)", out.ToolCallsUsed)
	}
}

// TestRunMessagesPreservesStreamedTurnWhenSinkPanicsOnDoneFrame pins the
// trace-preservation contract for a panic that strikes inside collect while the
// model turn is still streaming, rather than later in appendToolResults. A Sink
// whose Emit panics on the terminal done frame (EventDone maps to FrameDone) blows
// up after the assistant text has streamed but before collect used to append its
// events, so the produced response lived only in collect's local slice and the
// loop's outer recover salvaged a turn with only the prompt. collect now records
// each event before delivering it and recovers the panic itself, normalizing the
// events held so far into the partial assistant turn; RunMessages then records that
// turn. The recovered LoopOutput must therefore carry the assistant message with
// its streamed text. Before this fix the panic unwound to the outer recover with
// the events discarded, leaving only the prompt in Messages.
func TestRunMessagesPreservesStreamedTurnWhenSinkPanicsOnDoneFrame(t *testing.T) {
	driver := &scriptedProvider{
		turns: [][]provider.Event{{
			{Kind: provider.EventTextDelta, Text: "streamed answer"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		}},
	}

	panickingSink := SinkFunc(func(_ context.Context, frame Frame) error {
		// Let the text frames stream cleanly; panic on the terminal done frame so
		// the panic unwinds from inside collect, after the response has streamed.
		if frame.Kind == FrameDone {
			panic("sink panicked on done frame")
		}
		return nil
	})

	engine := Engine{Provider: driver, Model: "test-model"}

	out, runErr := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 1,
		Sink:          panickingSink,
	})

	if runErr == nil || !errors.Is(runErr, ErrPanicRecovered) {
		t.Fatalf("expected the sink done-frame panic to be recovered as ErrPanicRecovered, got %v", runErr)
	}
	if out.StopReason != provider.StopReasonError {
		t.Fatalf("StopReason = %s, want %s", out.StopReason, provider.StopReasonError)
	}
	// The assistant turn streamed its text before the sink panicked on the done
	// frame, so the recovered output must carry that turn, not only the prompt.
	var assistant *message.Message
	for i := range out.Messages {
		if out.Messages[i].Role == message.RoleAssistant {
			assistant = &out.Messages[i]
		}
	}
	if assistant == nil {
		t.Fatalf("recovered messages carry no assistant turn, want the streamed response preserved; messages = %d", len(out.Messages))
	}
	if assistant.Text != "streamed answer" {
		t.Fatalf("preserved assistant text = %q, want %q (the streamed response must survive the sink panic)", assistant.Text, "streamed answer")
	}
	// The turn ran and produced content, so the partial output must count it.
	if out.Iterations < 1 {
		t.Fatalf("Iterations = %d, want >= 1 (the streamed turn must be counted on the partial output)", out.Iterations)
	}
}

// afterToolCallFailOnSecond rejects the second result it post-processes, modeling
// an AfterToolCall hook that fails partway through a multi-tool batch after the
// tools have already side-effected. All Handler methods have pointer receivers so
// the invocation counter persists across calls within one batch.
type afterToolCallFailOnSecond struct{ seen int }

func (*afterToolCallFailOnSecond) TransformContext(_ context.Context, m []message.Message) ([]message.Message, error) {
	return m, nil
}

func (*afterToolCallFailOnSecond) BeforeModelCall(_ context.Context, _ *provider.Request) error {
	return nil
}
func (*afterToolCallFailOnSecond) BeforeToolCall(_ context.Context, _ *tool.Call) error { return nil }
func (h *afterToolCallFailOnSecond) AfterToolCall(_ context.Context, _ *tool.Result) error {
	h.seen++
	if h.seen >= 2 {
		return errAfterToolCall
	}
	return nil
}
func (*afterToolCallFailOnSecond) OnEvent(_ context.Context, _ provider.Event) error { return nil }

var errAfterToolCall = errors.New("after-tool-call rejected result")

// TestRunMessagesPreservesPrefixWhenAfterToolCallFails pins the trace-preservation
// contract for an AfterToolCall hook that fails partway through a batch. The whole
// batch runs in ExecuteBatch before any after-hook, so when the hook rejects the
// second result both tools have already side-effected. dispatchPreparedTools must
// return the already-post-processed first result rather than discarding the whole
// batch, and the loop must append it before surfacing the hook error, so the
// partial trace records the produced tool result instead of leaving the assistant
// tool calls dangling for a resuming caller to replay. Before the fix
// dispatchPreparedTools returned nil on the after-hook error, so the partial output
// carried the assistant tool calls and the charged count but no tool results.
func TestRunMessagesPreservesPrefixWhenAfterToolCallFails(t *testing.T) {
	driver, err := kit.Tool("lookup", func(_ context.Context, _ struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "result", nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}

	prov := &scriptedProvider{
		turns: [][]provider.Event{{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"query":"a"}`)}},
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "call-2", Name: "lookup", Arguments: json.RawMessage(`{"query":"b"}`)}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		}},
	}

	engine := Engine{
		Provider: prov,
		Tools:    tool.NewBus(driver),
		Hooks:    NewHookChain(&afterToolCallFailOnSecond{}),
	}

	out, runErr := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 2,
		ToolMode:      tool.ModeSequential,
	})

	if runErr == nil || !errors.Is(runErr, errAfterToolCall) {
		t.Fatalf("expected the after-tool-call rejection to surface, got %v", runErr)
	}
	if out.StopReason != provider.StopReasonError {
		t.Fatalf("StopReason = %s, want %s", out.StopReason, provider.StopReasonError)
	}
	// The first result was post-processed before the second's hook failed, so it
	// must survive on the partial output rather than being dropped with the batch.
	toolResults := 0
	for _, m := range out.Messages {
		if m.Role == message.RoleTool {
			toolResults++
		}
	}
	if toolResults < 1 {
		t.Fatalf("preserved tool results = %d, want >= 1 (the post-processed prefix must survive the after-hook failure); messages = %d", toolResults, len(out.Messages))
	}
	// Both calls were charged before dispatch, so the partial output reports them.
	if out.ToolCallsUsed != 2 {
		t.Fatalf("ToolCallsUsed = %d, want 2 (both calls were charged before dispatch)", out.ToolCallsUsed)
	}
}

// ctxAwareStream models a real provider stream — anthropic and openai wrap an
// HTTP body — whose Recv unblocks via the request context rather than io.EOF.
// It replays its queued events in order; once they are drained it surfaces a
// cancelled context as ctx.Err() (the way a body read does) instead of io.EOF.
// This is the one termination style SliceStream cannot model, because
// SliceStream ignores the context and always ends a drained stream with io.EOF.
type ctxAwareStream struct {
	ctx    context.Context
	events []provider.Event
	pos    int
}

func (s *ctxAwareStream) Recv() (provider.Event, error) {
	if s.pos < len(s.events) {
		event := s.events[s.pos]
		s.pos++
		return event, nil
	}
	if err := s.ctx.Err(); err != nil {
		return provider.Event{}, err
	}
	return provider.Event{}, io.EOF
}

func (*ctxAwareStream) Close() error { return nil }

// ctxAwareProvider hands collect a ctxAwareStream wired to the request context,
// so a cancellation that lands after the terminal EventDone reaches Recv as
// ctx.Err() rather than io.EOF.
type ctxAwareProvider struct{ events []provider.Event }

func (ctxAwareProvider) Metadata() provider.Metadata { return provider.Metadata{Name: "ctx-aware"} }

func (p ctxAwareProvider) Stream(ctx context.Context, _ provider.Request) (provider.Stream, error) {
	return &ctxAwareStream{ctx: ctx, events: p.events}, nil
}

// TestRunMessagesPreservesCompletedTurnWhenStreamUnblocksViaContext pins the
// companion case to the io.EOF one: when a context-aware provider stream has
// already delivered its terminal EventDone and the context is then cancelled,
// the next Recv returns context.Canceled rather than io.EOF. collect must treat
// that as the end of an already-complete turn and normalize the events it holds,
// not discard the turn as a failure. The bundled anthropic/openai streams
// short-circuit to io.EOF after EventDone so they never hit this path, but the
// public provider.Stream contract permits a stream that unblocks Recv through
// the context, and the trace-preservation guarantee must hold for it too.
func TestRunMessagesPreservesCompletedTurnWhenStreamUnblocksViaContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	driver := ctxAwareProvider{events: []provider.Event{
		{Kind: provider.EventTextDelta, Text: "done"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}}
	engine := Engine{Provider: driver, Model: "test-model"}

	out, runErr := engine.RunMessages(ctx, LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 1,
		OnEvent: func(ev provider.Event) error {
			// Cancel the moment the terminal event is observed so the NEXT Recv,
			// on this context-aware stream, returns context.Canceled not io.EOF.
			if ev.Kind == provider.EventDone {
				cancel()
			}
			return nil
		},
	})

	if runErr != nil {
		t.Fatalf("a turn completed via EventDone must survive a context-cancelled Recv, not only an io.EOF one: %v", runErr)
	}
	if out.StopReason != provider.StopReasonComplete {
		t.Fatalf("StopReason = %s, want %s (the completed turn's terminal reason)", out.StopReason, provider.StopReasonComplete)
	}
	last := out.Messages[len(out.Messages)-1]
	if last.Text != "done" {
		t.Fatalf("assistant text = %q, want %q (the completed response must be preserved)", last.Text, "done")
	}
}

// panicToolDriver panics from Execute. Run sequentially it executes inline on
// the loop's goroutine, so its panic propagates to RunMessages' own recover
// (parallel drivers are contained by the bus instead). It models a misbehaving
// tool driver that crashes after a model turn has already run.
type panicToolDriver struct{}

func (panicToolDriver) Definition() tool.Definition {
	return tool.Definition{Name: "boom"}
}

func (panicToolDriver) Execute(_ context.Context, _ tool.Call, _ tool.UpdateSink) (tool.Result, error) {
	panic("tool driver exploded")
}

// TestRunMessagesReportsIterationWhenToolDriverPanics pins that the panic
// backstop reports Iterations from the turns that actually ran, not from the
// completed-Step count. A sequential tool driver panics after the model turn has
// run and its assistant tool-call message is already in the partial trace, but
// before this turn's Step is recorded. Deriving Iterations from len(steps) would
// report 0 while the partial output carries the turn's messages — an
// inconsistency for a direct RunMessages consumer. Iterations must be >= 1.
func TestRunMessagesReportsIterationWhenToolDriverPanics(t *testing.T) {
	driver := &scriptedProvider{turns: [][]provider.Event{{
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-1",
				Name:      "boom",
				Arguments: json.RawMessage(`{}`),
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}}}
	engine := Engine{
		Provider: driver,
		Model:    "test-model",
		Tools:    tool.NewBus(panicToolDriver{}),
	}

	out, runErr := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 2,
		ToolMode:      tool.ModeSequential,
	})

	if runErr == nil || !errors.Is(runErr, ErrPanicRecovered) {
		t.Fatalf("expected ErrPanicRecovered from the panicking tool driver, got %v", runErr)
	}
	// The assistant tool-call message is preserved, so reporting Iterations=0
	// would contradict the partial trace.
	if len(out.Messages) < 2 {
		t.Fatalf("preserved messages = %d, want >= 2 (prompt + assistant tool call)", len(out.Messages))
	}
	if out.Iterations < 1 {
		t.Fatalf("Iterations = %d, want >= 1 (the model turn ran before the tool-driver panic)", out.Iterations)
	}
}

// Rejected model calls consume the dispatch budget so repeated invalid names
// cannot bypass the bound even though no driver was invoked.
func TestRunMessagesChargesRejectedCallsAndContinues(t *testing.T) {
	realDriver, err := kit.Tool("real", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}
	driver := &scriptedProvider{turns: [][]provider.Event{{
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-1",
				Name:      "ghost", // not registered on the bus
				Arguments: json.RawMessage(`{}`),
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}, {
		{Kind: provider.EventTextDelta, Text: "corrected"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}}}
	engine := Engine{
		Provider: driver,
		Model:    "test-model",
		Tools:    tool.NewBus(realDriver),
	}

	out, runErr := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 2,
		ToolMode:      tool.ModeSequential,
	})

	if runErr != nil {
		t.Fatal(runErr)
	}
	if out.ToolCallsUsed != 1 {
		t.Fatalf("ToolCallsUsed = %d, want 1 rejected attempt", out.ToolCallsUsed)
	}
	if out.StopReason != provider.StopReasonComplete || len(driver.requests) != 2 {
		t.Fatalf("StopReason = %s, calls=%d", out.StopReason, len(driver.requests))
	}
}

// aliasRewriteHook rewrites a model-emitted tool name to a registered one from
// BeforeToolCall. tool.Call is passed by pointer precisely so a hook can remap an
// alias or a hallucinated name onto a real tool before the bus looks it up, so
// availability must be judged after this hook runs, not on the raw call.
type aliasRewriteHook struct{ from, to string }

func (aliasRewriteHook) TransformContext(_ context.Context, m []message.Message) ([]message.Message, error) {
	return m, nil
}
func (aliasRewriteHook) BeforeModelCall(_ context.Context, _ *provider.Request) error { return nil }
func (h aliasRewriteHook) BeforeToolCall(_ context.Context, call *tool.Call) error {
	if call.Name == h.from {
		call.Name = h.to
	}
	return nil
}
func (aliasRewriteHook) AfterToolCall(_ context.Context, _ *tool.Result) error { return nil }
func (aliasRewriteHook) OnEvent(_ context.Context, _ provider.Event) error     { return nil }

type argumentMutatingHook struct{ aliasRewriteHook }

func (argumentMutatingHook) BeforeToolCall(_ context.Context, call *tool.Call) error {
	call.Arguments[0] = '['
	return nil
}

func TestPrepareToolCallsIsolatesModelArgumentsFromHooks(t *testing.T) {
	calls := []message.ToolCall{{
		ID:        "call-1",
		Name:      "side_effect",
		Arguments: json.RawMessage(`{}`),
	}}
	want := string(calls[0].Arguments)
	engine := Engine{
		Tools: tool.NewBus(&recordingDriver{}),
		Hooks: NewHookChain(argumentMutatingHook{}),
	}

	prepared, _, err := engine.prepareToolCalls(context.Background(), calls)
	if err != nil {
		t.Fatalf("prepareToolCalls() error = %v", err)
	}
	if string(calls[0].Arguments) != want {
		t.Fatalf("model arguments = %q, want unchanged %q", calls[0].Arguments, want)
	}
	if len(prepared) != 1 || string(prepared[0].Arguments) == want {
		t.Fatalf("prepared calls = %#v, want isolated hook mutation", prepared)
	}
}

// TestRunMessagesValidatesAvailabilityAfterHookRewritesToolName pins the order of
// the tool-call hooks and the availability check. A BeforeToolCall hook is the
// documented way to rewrite a model-emitted alias or hallucinated tool name onto
// a real tool, and the bus dispatches the hook-mutated call — so availability
// must be judged after the hook runs. The model emits a call to "alias" (not
// registered); the hook rewrites it to the registered "side_effect" tool. The
// turn must dispatch that tool, not fail ErrToolNotFound on the raw name, and the
// dispatched call must be charged. Checking the raw name before the hook would
// reject a name the hook was about to fix.
func TestRunMessagesValidatesAvailabilityAfterHookRewritesToolName(t *testing.T) {
	sideEffect := &recordingDriver{} // registered name "side_effect"
	driver := &scriptedProvider{turns: [][]provider.Event{
		{
			{
				Kind: provider.EventToolCall,
				ToolCall: &message.ToolCall{
					ID:        "call-1",
					Name:      "alias", // not registered; a hook rewrites it to "side_effect"
					Arguments: json.RawMessage(`{}`),
				},
			},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		},
		{
			{Kind: provider.EventTextDelta, Text: "done"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		},
	}}
	engine := Engine{
		Provider: driver,
		Model:    "test-model",
		Tools:    tool.NewBus(sideEffect),
		Hooks:    NewHookChain(aliasRewriteHook{from: "alias", to: "side_effect"}),
	}

	out, runErr := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 3,
		ToolMode:      tool.ModeSequential,
	})

	if runErr != nil {
		t.Fatalf("a BeforeToolCall hook rewriting alias->side_effect must let the tool dispatch, got error %v", runErr)
	}
	if !sideEffect.invoked {
		t.Fatal("the hook-rewritten tool must have been dispatched, but the driver never ran")
	}
	if out.ToolCallsUsed != 1 {
		t.Fatalf("ToolCallsUsed = %d, want 1 (the hook-rewritten tool dispatched, so it must be charged)", out.ToolCallsUsed)
	}
	if out.StopReason != provider.StopReasonComplete {
		t.Fatalf("StopReason = %s, want %s", out.StopReason, provider.StopReasonComplete)
	}
}

// TestRunMessagesChargesToolBatchWhenSequentialDriverPanics pins the panic-path
// half of the tool-charge invariant. A sequential tool driver runs inline on the
// loop's goroutine, so its panic unwinds straight to RunMessages' recover defer,
// bypassing any charge placed after the dispatch call. The dispatched batch must
// still be counted against ToolCallsUsed on the recovered partial output, or a
// caller resuming from it under-counts MaxToolCalls — the same invariant the
// tool-error and recovered parallel-panic paths already preserve. Charging the
// batch before dispatch is what makes the recovered output report it.
func TestRunMessagesChargesToolBatchWhenSequentialDriverPanics(t *testing.T) {
	driver := &scriptedProvider{turns: [][]provider.Event{{
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-1",
				Name:      "boom",
				Arguments: json.RawMessage(`{}`),
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}}}
	engine := Engine{
		Provider: driver,
		Model:    "test-model",
		Tools:    tool.NewBus(panicToolDriver{}),
	}

	out, runErr := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 2,
		ToolMode:      tool.ModeSequential,
	})

	if runErr == nil || !errors.Is(runErr, ErrPanicRecovered) {
		t.Fatalf("expected ErrPanicRecovered from the panicking tool driver, got %v", runErr)
	}
	// The bus dispatched the call inline before the driver panicked, so the
	// recovered partial output must charge it; reporting 0 would let a resuming
	// caller exceed its MaxToolCalls budget.
	if out.ToolCallsUsed < 1 {
		t.Fatalf("ToolCallsUsed = %d, want >= 1 (the dispatched tool call must be charged on the panic-recovered output)", out.ToolCallsUsed)
	}
}

// recordingDriver records whether Execute was invoked. It deliberately does
// not consult the context, modeling a side-effecting tool whose execution
// under a cancelled context must be prevented by the loop rather than left to
// the driver — the tool bus dispatches without a pre-flight context check.
type recordingDriver struct{ invoked bool }

func (*recordingDriver) Definition() tool.Definition {
	return tool.Definition{Name: "side_effect"}
}

func (d *recordingDriver) Execute(_ context.Context, _ tool.Call, _ tool.UpdateSink) (tool.Result, error) {
	d.invoked = true
	return tool.Result{ToolCallID: "call-1", Name: "side_effect", Content: "ran"}, nil
}

// TestRunMessagesDoesNotDispatchToolsWhenCancelledAfterDone pins the boundary of
// the completed-turn preservation: a turn whose terminal EventDone asks for
// tools must NOT have those tools dispatched when the run context is already
// cancelled. Preserving a completed final-answer turn under cancellation is
// correct — the response is finished — but a tool-use turn is a request to
// perform side-effecting work, and the bus dispatches to drivers without a
// pre-flight context check, so the loop must refuse dispatch once the context is
// done. Here OnEvent cancels the moment EventDone (StopReasonToolUse) is seen;
// the recording driver must never run, and the assistant tool-call request must
// still be preserved in the trace.
func TestRunMessagesDoesNotDispatchToolsWhenCancelledAfterDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	driver := &scriptedProvider{turns: [][]provider.Event{{
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-1",
				Name:      "side_effect",
				Arguments: json.RawMessage(`{}`),
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}}}
	sideEffect := &recordingDriver{}
	engine := Engine{
		Provider: driver,
		Model:    "test-model",
		Tools:    tool.NewBus(sideEffect),
	}

	out, runErr := engine.RunMessages(ctx, LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 2,
		ToolMode:      tool.ModeSequential,
		OnEvent: func(ev provider.Event) error {
			// Cancel the instant the tool-use turn completes, landing the
			// cancellation in the window before the loop dispatches its tools.
			if ev.Kind == provider.EventDone {
				cancel()
			}
			return nil
		},
	})

	if sideEffect.invoked {
		t.Fatal("a side-effecting tool must not be dispatched when the run context is cancelled before dispatch")
	}
	if runErr == nil {
		t.Fatal("expected the cancellation to surface as the loop error")
	}
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("errors.Is(runErr, context.Canceled) = false, runErr = %v", runErr)
	}
	if out.StopReason != provider.StopReasonError {
		t.Fatalf("StopReason = %s, want %s", out.StopReason, provider.StopReasonError)
	}
	// The assistant tool-call request must survive: prompt + assistant tool call.
	if len(out.Messages) < 2 {
		t.Fatalf("preserved messages = %d, want >= 2 (prompt + the assistant tool-call request)", len(out.Messages))
	}
	if last := out.Messages[len(out.Messages)-1]; len(last.ToolCalls) == 0 {
		t.Fatal("the assistant tool-call request must be preserved in the trace")
	}
}

// TestRunFastExitsOnCancelledContext pins the loop-top context check: a context
// that is already cancelled ends the run before any model turn is issued, and
// the cancellation cause is preserved on the typed failure so errors.Is walks
// to context.Canceled across the boundary.
func TestRunFastExitsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	driver := singleTurnProvider("answer")
	engine := Engine{Provider: driver, Model: "test-model"}

	result := engine.Run(ctx, Request{Prompt: "do the thing"}, OutputPolicy{})

	if result.Failure == nil {
		t.Fatal("expected a failure when the context is already cancelled")
	}
	if !errors.Is(result.Failure, context.Canceled) {
		t.Fatalf("errors.Is(Failure, context.Canceled) = false, Failure = %v", result.Failure)
	}
	if len(driver.requests) != 0 {
		t.Fatalf("provider was called %d time(s); the loop must fast-exit before any model turn", len(driver.requests))
	}
}

// erroringToolDriver returns an error from Execute rather than a result. Run
// sequentially through the bus it makes ExecuteBatch return that error, so
// dispatchPreparedTools surfaces it and the loop takes the dispErr partial-error
// return — the path that previously left the dispatched call uncharged. It models a tool
// that runs and fails (a remote 500, a validation error) after a model turn has
// already requested it.
type erroringToolDriver struct{}

func (erroringToolDriver) Definition() tool.Definition {
	return tool.Definition{Name: "explode"}
}

func (erroringToolDriver) Execute(_ context.Context, _ tool.Call, _ tool.UpdateSink) (tool.Result, error) {
	return tool.Result{}, errors.New("tool driver failed")
}

// TestRunMessagesChargesToolBatchWhenDriverErrors pins that a tool-execution
// error charges this turn's dispatched batch against ToolCallsUsed before the
// partial-error return. The bus dispatched the call before the driver failed, so
// a caller that persists or resumes from the returned partial LoopOutput must
// see it counted — otherwise the failed call escapes MaxToolCalls and later work
// can overrun the budget. The increment previously sat only on the success path
// below the toolErr check, so this output reported ToolCallsUsed = 0; the loop
// now charges the batch as soon as dispatch is attempted, mirroring the
// batch-level reservation the pre-dispatch budget gate already makes.
func TestRunMessagesChargesToolBatchWhenDriverErrors(t *testing.T) {
	driver := &scriptedProvider{turns: [][]provider.Event{{
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-1",
				Name:      "explode",
				Arguments: json.RawMessage(`{}`),
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}}}
	engine := Engine{
		Provider: driver,
		Model:    "test-model",
		Tools:    tool.NewBus(erroringToolDriver{}),
	}

	out, runErr := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 2,
		ToolMode:      tool.ModeSequential,
	})

	if runErr == nil {
		t.Fatal("expected the tool driver error to surface as the loop error")
	}
	if out.StopReason != provider.StopReasonError {
		t.Fatalf("StopReason = %s, want %s", out.StopReason, provider.StopReasonError)
	}
	// The bus dispatched the call before the driver returned its error, so the
	// partial output must charge it against MaxToolCalls; reporting 0 would let a
	// resuming caller exceed its tool-call budget.
	if out.ToolCallsUsed < 1 {
		t.Fatalf("ToolCallsUsed = %d, want >= 1 (the dispatched tool call must be charged on the toolErr partial output)", out.ToolCallsUsed)
	}
}

// TestRunMessagesPreservesSucceededResultsWhenBatchFails pins that a batch with
// mixed success and failure keeps the results that already ran on the partial
// LoopOutput. The model requests two tools in one turn — one that side-effects
// and succeeds, one that fails — and the loop surfaces the failure. Because the
// bus dispatched and ran the successful tool before its sibling failed, dropping
// that result would leave the assistant's tool-call message dangling (a tool_use
// with no matching tool_result, which the provider rejects on replay) and a
// resuming caller would re-run the side-effecting call. Both dispatch modes must
// preserve the survivor: sequential ExecuteBatch returns the earlier success, and
// parallel executeParallel returns the slot that completed before the join.
func TestRunMessagesPreservesSucceededResultsWhenBatchFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode tool.Mode
	}{
		{"sequential", tool.ModeSequential},
		{"parallel", tool.ModeParallel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// side_effect is requested first so it runs and succeeds before explode
			// fails; in parallel mode both run to completion before the error joins.
			prov := &scriptedProvider{turns: [][]provider.Event{{
				{
					Kind: provider.EventToolCall,
					ToolCall: &message.ToolCall{
						ID:        "call-1",
						Name:      "side_effect",
						Arguments: json.RawMessage(`{}`),
					},
				},
				{
					Kind: provider.EventToolCall,
					ToolCall: &message.ToolCall{
						ID:        "call-2",
						Name:      "explode",
						Arguments: json.RawMessage(`{}`),
					},
				},
				{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
			}}}
			sideEffect := &recordingDriver{}
			engine := Engine{
				Provider: prov,
				Model:    "test-model",
				Tools:    tool.NewBus(sideEffect, erroringToolDriver{}),
			}

			out, runErr := engine.RunMessages(context.Background(), LoopInput{
				Model:         "test-model",
				Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
				MaxIterations: 2,
				ToolMode:      tc.mode,
			})

			if runErr == nil {
				t.Fatal("expected the failing tool driver error to surface as the loop error")
			}
			if !sideEffect.invoked {
				t.Fatal("the succeeding tool must have run (and side-effected) before the batch error")
			}
			if out.StopReason != provider.StopReasonError {
				t.Fatalf("StopReason = %s, want %s", out.StopReason, provider.StopReasonError)
			}
			// The succeeded tool's result must survive on the partial trace as a
			// RoleTool message; discarding it would dangle the assistant tool call.
			toolResults := 0
			for _, m := range out.Messages {
				if m.Role == message.RoleTool {
					toolResults++
				}
			}
			if toolResults < 1 {
				t.Fatalf("preserved tool results = %d, want >= 1 (the succeeded tool's result must survive the sibling failure); messages = %d", toolResults, len(out.Messages))
			}
		})
	}
}

// TestRunMessagesPreservesCompletedTurnWhenCancelledAfterDone pins that a
// context cancellation landing AFTER the provider delivered its terminal
// EventDone (which already carries the StopReason) but before the stream
// returns io.EOF does not discard the already-complete turn. collect's pre-Recv
// context check exists to avoid blocking on a slow provider, but a response that
// has finished must not be turned into a failure solely because EOF has not been
// read yet. Here an OnEvent callback cancels the context the moment it observes
// EventDone, deterministically landing the cancellation in that window.
func TestRunMessagesPreservesCompletedTurnWhenCancelledAfterDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	driver := singleTurnProvider("done")
	engine := Engine{Provider: driver, Model: "test-model"}

	out, runErr := engine.RunMessages(ctx, LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "go")},
		MaxIterations: 1,
		OnEvent: func(ev provider.Event) error {
			if ev.Kind == provider.EventDone {
				cancel()
			}
			return nil
		},
	})

	if runErr != nil {
		t.Fatalf("a turn completed via EventDone must not fail when the context is cancelled before EOF: %v", runErr)
	}
	if out.StopReason != provider.StopReasonComplete {
		t.Fatalf("StopReason = %s, want %s (the completed turn's terminal reason)", out.StopReason, provider.StopReasonComplete)
	}
	last := out.Messages[len(out.Messages)-1]
	if last.Text != "done" {
		t.Fatalf("assistant text = %q, want %q (the completed response must be preserved)", last.Text, "done")
	}
}
