package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
)

type streamOverlapContextKey struct{}

type streamOverlapResult struct {
	result   message.ToolResult
	prepared tool.Call
	usage    provider.Usage
	err      error
}

// Only the collecting goroutine changes the entry map and admission state.
// Workers publish their result by closing done; take/finish join before reading.
type streamOverlapState struct {
	engine        Engine
	sink          Sink
	ctx           context.Context
	cancel        context.CancelFunc
	safe          map[string]struct{}
	operationTurn int
	mode          tool.Mode
	limit         int
	barrier       bool
	sinkMu        sync.Mutex
	startedFlag   atomic.Bool
	entries       map[string]*streamOverlapEntry
}

type streamOverlapEntry struct {
	done          chan struct{}
	result        streamOverlapResult
	deferDispatch bool
}

func newStreamOverlapState(ctx context.Context, engine Engine, input LoopInput) *streamOverlapState {
	if len(input.SafeStreamingTools) == 0 || engine.Tools == nil {
		return nil
	}
	safe := make(map[string]struct{}, len(input.SafeStreamingTools))
	for _, name := range input.SafeStreamingTools {
		if name != "" {
			safe[name] = struct{}{}
		}
	}
	if len(safe) == 0 {
		return nil
	}
	limit := tool.DefaultBatchConcurrency
	if input.MaxToolCalls > 0 {
		limit = min(limit, max(0, input.MaxToolCalls-input.initialToolCallsUsed))
	}
	if engine.Hooks.Len() > 0 {
		engine.Hooks = NewHookChain(&overlapHooks{chain: engine.Hooks})
	}
	overlapCtx, cancel := context.WithCancel(ctx)
	if input.MaxTokens > 0 {
		remaining := input.MaxTokens - int64(input.initialUsage.TotalTokens)
		overlapCtx = context.WithValue(overlapCtx, agentToolReservationContextKey{}, newAgentToolTokenTracker(remaining))
	}
	overlapCtx = withControlDispatch(overlapCtx, input.Control)
	return &streamOverlapState{
		engine: engine, sink: input.Sink, ctx: overlapCtx, cancel: cancel,
		safe: safe, operationTurn: input.OperationTurn, mode: input.ToolMode, limit: limit,
		entries: make(map[string]*streamOverlapEntry),
	}
}

func streamOverlapFromContext(ctx context.Context) *streamOverlapState {
	state, _ := ctx.Value(streamOverlapContextKey{}).(*streamOverlapState)
	return state
}

func (state *streamOverlapState) eligible(call tool.Call) bool {
	if _, ok := state.safe[call.Name]; !ok {
		return false
	}
	driver, ok := state.engine.Tools.Driver(call.Name)
	if !ok {
		return false
	}
	definition := driver.Definition()
	return !definition.Terminal && definition.Concurrency != tool.ConcurrencySequential
}

func (state *streamOverlapState) observe(event provider.Event) {
	if state == nil || state.barrier || event.Kind != provider.EventToolCall || event.ToolCall == nil {
		return
	}
	call := *event.ToolCall
	if _, exists := state.entries[call.ID]; exists {
		return
	}
	if len(state.entries) >= state.limit || call.ID == "" || !json.Valid(call.Arguments) || !state.eligible(call) {
		state.barrier = true
		return
	}
	call.OperationID = fmt.Sprintf("turn:%d:call:%d", state.operationTurn, len(state.entries))
	entry := &streamOverlapEntry{done: make(chan struct{})}
	state.entries[call.ID] = entry
	// Prepare on the collecting goroutine so a hook rewrite to an unsafe tool
	// establishes a barrier before subsequent calls can start. Reuse this exact
	// prepared call later; approval/audit hooks must not run twice.
	prepared, _, err := state.engine.prepareToolCalls(state.ctx, []message.ToolCall{call})
	if err != nil {
		entry.result.err = err
		state.barrier = true
		close(entry.done)
		return
	}
	entry.result.prepared = prepared[0]
	if !state.eligible(prepared[0]) {
		entry.deferDispatch = true
		state.barrier = true
		close(entry.done)
		return
	}
	if state.mode != tool.ModeParallel {
		state.barrier = true
	}
	state.startedFlag.Store(true)
	go state.execute(entry)
}

func (state *streamOverlapState) execute(entry *streamOverlapEntry) {
	defer func() {
		if recovered := recover(); recovered != nil {
			entry.result.err = fmt.Errorf("%w: early tool: %v", ErrPanicRecovered, recovered)
		}
		close(entry.done)
	}()
	childUsage := &agentToolUsageCollector{}
	dispatchCtx := context.WithValue(state.ctx, agentToolUsageContextKey{}, childUsage)
	results, err := state.engine.dispatchPreparedTools(dispatchCtx, []tool.Call{entry.result.prepared}, tool.ModeSequential, state.serializedSink())
	entry.result.usage = childUsage.snapshot()
	if len(results) > 0 {
		entry.result.result = results[0]
	}
	entry.result.err = err
}

func (state *streamOverlapState) finish(success bool) {
	if state == nil {
		return
	}
	if !success {
		state.cancel()
	}
	for _, entry := range state.entries {
		<-entry.done
	}
	state.cancel()
}

func (state *streamOverlapState) take(calls []message.ToolCall) (map[string]streamOverlapResult, []message.ToolCall, error) {
	if state == nil {
		return nil, calls, nil
	}
	out := make(map[string]streamOverlapResult, len(state.entries))
	pending := make([]message.ToolCall, 0, len(calls))
	var joined error
	for _, call := range calls {
		entry := state.entries[call.ID]
		if entry == nil {
			pending = append(pending, call)
			continue
		}
		<-entry.done
		if entry.deferDispatch {
			pending = append(pending, call)
			continue
		}
		out[call.ID] = entry.result
		joined = errors.Join(joined, entry.result.err)
	}
	return out, pending, joined
}

func (state *streamOverlapState) preparePending(ctx context.Context, engine Engine, calls []message.ToolCall) ([]tool.Call, error) {
	if state == nil {
		prepared, _, err := engine.prepareToolCalls(ctx, calls)
		return prepared, err
	}
	prepared := make([]tool.Call, 0, len(calls))
	for _, call := range calls {
		if entry := state.entries[call.ID]; entry != nil && entry.deferDispatch {
			prepared = append(prepared, entry.result.prepared)
			continue
		}
		current, _, err := engine.prepareToolCalls(ctx, []message.ToolCall{call})
		if err != nil {
			return nil, err
		}
		prepared = append(prepared, current...)
	}
	return prepared, nil
}

func withStreamOverlap(ctx context.Context, state *streamOverlapState) context.Context {
	if state == nil {
		return ctx
	}
	return context.WithValue(ctx, streamOverlapContextKey{}, state)
}

func (state *streamOverlapState) serializedSink() Sink {
	if state == nil || state.sink == nil {
		return nil
	}
	return SinkFunc(func(ctx context.Context, frame Frame) error {
		state.sinkMu.Lock()
		defer state.sinkMu.Unlock()
		return state.sink.Emit(ctx, frame)
	})
}

// Hook callbacks used to run on the collecting goroutine. Keep their serial
// invocation contract while tool execution overlaps the provider stream.
type overlapHooks struct {
	mu    sync.Mutex
	chain HookChain
}

func (hooks *overlapHooks) TransformContext(ctx context.Context, messages []message.Message) ([]message.Message, error) {
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	return hooks.chain.TransformContext(ctx, messages)
}
func (hooks *overlapHooks) BeforeModelCall(ctx context.Context, request *provider.Request) error {
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	return hooks.chain.BeforeModelCall(ctx, request)
}
func (hooks *overlapHooks) BeforeToolCall(ctx context.Context, call *tool.Call) error {
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	return hooks.chain.BeforeToolCall(ctx, call)
}
func (hooks *overlapHooks) AfterToolCall(ctx context.Context, result *tool.Result) error {
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	return hooks.chain.AfterToolCall(ctx, result)
}
func (hooks *overlapHooks) OnEvent(ctx context.Context, event provider.Event) error {
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	return hooks.chain.OnEvent(ctx, event)
}

// Once a call has entered hook preparation, retrying the model turn could
// replay an approval or tool effect even if the stream ended with a typed error.
func (state *streamOverlapState) started() bool {
	return state != nil && state.startedFlag.Load()
}

func (e Engine) collectTurnStream(ctx context.Context, stream provider.Stream, input LoopInput) (message.Message, provider.Usage, provider.StopReason, error) {
	sink := input.Sink
	if input.overlap != nil {
		e = input.overlap.engine
		sink = input.overlap.serializedSink()
	}
	assistant, usage, stop, err := e.collect(withStreamOverlap(ctx, input.overlap), stream, input.OnEvent, sink)
	input.overlap.finish(err == nil)
	return assistant, usage, stop, err
}
