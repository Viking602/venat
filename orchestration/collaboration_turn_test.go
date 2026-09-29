package orchestration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/provider"
)

type immediateTurnProvider struct{}

func (immediateTurnProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "turn-capture"}
}
func (immediateTurnProvider) Stream(_ context.Context, request provider.Request) (provider.Stream, error) {
	text := request.Messages[len(request.Messages)-1].Text
	return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: text}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
}

// Pause Await after it captures its turn but before its select can return.
// This makes the later follow-up completion win the scheduling race without sleeps.
type pausedAwaitContext struct {
	context.Context
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func (ctx *pausedAwaitContext) Done() <-chan struct{} {
	ctx.enterOnce.Do(func() { close(ctx.entered) })
	<-ctx.release
	return ctx.Context.Done()
}
func (ctx *pausedAwaitContext) unblock() { ctx.releaseOnce.Do(func() { close(ctx.release) }) }

func TestAwaitReturnsCapturedTurnAfterAnotherTurnCompletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	runtime, err := NewRuntime(ctx, RuntimeOptions{Factory: func(TaskRequest) (agent.Engine, error) { return agent.Engine{Provider: immediateTurnProvider{}}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handle, err := runtime.Spawn(TaskRequest{ID: "worker", Request: agent.Request{Prompt: "first"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Await(ctx); err != nil {
		t.Fatal(err)
	}
	paused := &pausedAwaitContext{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
	defer paused.unblock()
	result := make(chan TaskResult, 1)
	go func() { completed, _ := handle.Await(paused); result <- completed }()
	select {
	case <-paused.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := handle.Send(ctx, agent.Request{Prompt: "second"}); err != nil {
		t.Fatal(err)
	}
	second, err := handle.Await(ctx)
	if err != nil || second.Turn != 2 || second.Result.Text != "second" {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	paused.unblock()
	select {
	case first := <-result:
		if first.Turn != 1 || first.Result.Text != "first" {
			t.Fatalf("original waiter received a later turn: %+v", first)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
