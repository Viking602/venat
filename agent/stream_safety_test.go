package agent

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
)

type safetyTool struct {
	definition tool.Definition
	run        func(context.Context) (tool.Result, error)
}

func (driver safetyTool) Definition() tool.Definition { return driver.definition }
func (driver safetyTool) Execute(ctx context.Context, _ tool.Call, _ tool.UpdateSink) (tool.Result, error) {
	return driver.run(ctx)
}

type safetyHook struct {
	before func(*tool.Call)
	event  func()
	after  func()
}

func (*safetyHook) TransformContext(_ context.Context, messages []message.Message) ([]message.Message, error) {
	return messages, nil
}
func (*safetyHook) BeforeModelCall(context.Context, *provider.Request) error { return nil }
func (hook *safetyHook) BeforeToolCall(_ context.Context, call *tool.Call) error {
	if hook.before != nil {
		hook.before(call)
	}
	return nil
}
func (hook *safetyHook) AfterToolCall(context.Context, *tool.Result) error {
	if hook.after != nil {
		hook.after()
	}
	return nil
}
func (hook *safetyHook) OnEvent(context.Context, provider.Event) error {
	if hook.event != nil {
		hook.event()
	}
	return nil
}

type observedDoneStream struct {
	provider.Stream
	done *atomic.Bool
}

func (stream observedDoneStream) Recv() (provider.Event, error) {
	event, err := stream.Stream.Recv()
	if event.Kind == provider.EventDone {
		stream.done.Store(true)
	}
	return event, err
}

func TestStreamingHookRewriteRunsOnceAndBlocksLaterEarlyReads(t *testing.T) {
	var done atomic.Bool
	calls, before := map[string]int{}, map[string]int{}
	makeTool := func(name string) tool.Driver {
		return safetyTool{definition: tool.Definition{Name: name, InputSchema: tool.Schema{Type: "object"}}, run: func(context.Context) (tool.Result, error) {
			if !done.Load() {
				return tool.Result{}, errors.New("rewritten unsafe call or later read crossed stream barrier")
			}
			calls[name]++
			return tool.Result{Content: name}, nil
		}}
	}
	hook := &safetyHook{before: func(call *tool.Call) {
		before[call.ID]++
		if call.Name == "read" {
			call.Name = "write"
		}
	}}
	turn := 0
	model := agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
		turn++
		if turn == 1 {
			return observedDoneStream{Stream: provider.NewSliceStream([]provider.Event{
				{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "a", Name: "read", Arguments: []byte(`{}`)}},
				{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "b", Name: "later_read", Arguments: []byte(`{}`)}},
				{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
			}), done: &done}, nil
		}
		return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: "done"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
	})
	engine := Engine{Provider: model, Tools: tool.NewBus(makeTool("read"), makeTool("write"), makeTool("later_read")), Hooks: NewHookChain(hook), SafeStreamingTools: []string{"read", "later_read"}}
	result := engine.Run(context.Background(), Request{Prompt: "read"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatal(result.Failure)
	}
	if calls["write"] != 1 || calls["later_read"] != 1 || calls["read"] != 0 || before["a"] != 1 || before["b"] != 1 {
		t.Fatalf("calls=%v hooks=%v", calls, before)
	}
}

func TestStreamingHooksKeepSerialCallbackContract(t *testing.T) {
	counter := 0 // deliberately unsynchronized: the runtime owns hook serialization.
	increment := func() {
		for range 10 {
			counter++
			runtime.Gosched()
		}
	}
	hook := &safetyHook{before: func(*tool.Call) { increment() }, event: increment, after: increment}
	read := safetyTool{definition: tool.Definition{Name: "read", InputSchema: tool.Schema{Type: "object"}}, run: func(context.Context) (tool.Result, error) { return tool.Result{Content: "read"}, nil }}
	turn := 0
	model := agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
		turn++
		if turn > 1 {
			return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: "done"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
		}
		events := []provider.Event{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "a", Name: "read", Arguments: []byte(`{}`)}},
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "b", Name: "read", Arguments: []byte(`{}`)}},
		}
		for range 8 {
			events = append(events, provider.Event{Kind: provider.EventTextDelta, Text: "working "})
		}
		events = append(events, provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse})
		return provider.NewSliceStream(events), nil
	})
	result := (Engine{Provider: model, Tools: tool.NewBus(read), Hooks: NewHookChain(hook), ToolMode: tool.ModeParallel, SafeStreamingTools: []string{"read"}}).Run(context.Background(), Request{Prompt: "read twice"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatal(result.Failure)
	}
	if counter != 170 {
		t.Fatalf("hook callback count=%d, want 170", counter)
	}
}

type gatedSafetyStream struct {
	ctx       context.Context
	started   <-chan struct{}
	completed chan struct{}
	step      int
	fail      bool
}

func (stream *gatedSafetyStream) Recv() (provider.Event, error) {
	stream.step++
	if stream.step == 1 {
		return provider.Event{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "read", Name: "read", Arguments: []byte(`{}`)}}, nil
	}
	if stream.step == 2 {
		select {
		case <-stream.started:
		case <-stream.ctx.Done():
			return provider.Event{}, stream.ctx.Err()
		}
		if stream.fail {
			return provider.Event{}, errors.New("stream failed after read started")
		}
		close(stream.completed)
		return provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse}, nil
	}
	return provider.Event{}, io.EOF
}
func (*gatedSafetyStream) Close() error { return nil }

func TestStreamingReadLifetimeAndFailureCancellation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "read survives model done", true: "stream failure cancels read"}[fail], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started, completed := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			read := safetyTool{definition: tool.Definition{Name: "read", InputSchema: tool.Schema{Type: "object"}}, run: func(toolCtx context.Context) (tool.Result, error) {
				calls.Add(1)
				close(started)
				select {
				case <-completed:
					return tool.Result{Content: "read after model done"}, toolCtx.Err()
				case <-toolCtx.Done():
					return tool.Result{}, toolCtx.Err()
				}
			}}
			turn := 0
			model := agentToolProviderFunc(func(ctx context.Context, _ provider.Request) (provider.Stream, error) {
				turn++
				if turn == 1 {
					return &gatedSafetyStream{ctx: ctx, started: started, completed: completed, fail: fail}, nil
				}
				return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: "done"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
			})
			result := (Engine{Provider: model, Tools: tool.NewBus(read), SafeStreamingTools: []string{"read"}}).Run(ctx, Request{Prompt: "read"}, OutputPolicy{})
			if (result.Failure != nil) != fail || calls.Load() != 1 || ctx.Err() != nil {
				t.Fatalf("failure=%v calls=%d ctx=%v", result.Failure, calls.Load(), ctx.Err())
			}
		})
	}
}

func TestStreamingOptInDoesNotOverrideSequentialDefinition(t *testing.T) {
	var done atomic.Bool
	read := safetyTool{
		definition: tool.Definition{Name: "read", InputSchema: tool.Schema{Type: "object"}, Concurrency: tool.ConcurrencySequential},
		run: func(context.Context) (tool.Result, error) {
			if !done.Load() {
				return tool.Result{}, errors.New("sequential call started before stream finished")
			}
			return tool.Result{Content: "read"}, nil
		},
	}
	turn := 0
	model := agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
		turn++
		if turn > 1 {
			return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: "done"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
		}
		return observedDoneStream{Stream: provider.NewSliceStream([]provider.Event{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "read", Name: "read", Arguments: []byte(`{}`)}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		}), done: &done}, nil
	})
	result := (Engine{Provider: model, Tools: tool.NewBus(read), ToolMode: tool.ModeParallel, SafeStreamingTools: []string{"read"}}).Run(context.Background(), Request{Prompt: "read"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatal(result.Failure)
	}
}

func TestStreamingToolPanicBecomesExecutionFailure(t *testing.T) {
	read := safetyTool{definition: tool.Definition{Name: "read", InputSchema: tool.Schema{Type: "object"}}, run: func(context.Context) (tool.Result, error) { panic("read failed") }}
	model := &scriptedProvider{turns: [][]provider.Event{{
		{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "read", Name: "read", Arguments: []byte(`{}`)}},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}}}
	result := (Engine{Provider: model, Tools: tool.NewBus(read), SafeStreamingTools: []string{"read"}}).Run(context.Background(), Request{Prompt: "read"}, OutputPolicy{})
	if result.Failure == nil || !errors.Is(result.Failure, ErrPanicRecovered) {
		t.Fatalf("failure=%v", result.Failure)
	}
}
