package orchestration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

type collaborationTestProvider struct {
	mu      sync.Mutex
	prompts [][]string
	delays  map[string]time.Duration
}

func (p *collaborationTestProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "collaboration-test"}
}

func (p *collaborationTestProvider) Stream(ctx context.Context, request provider.Request) (provider.Stream, error) {
	texts := make([]string, len(request.Messages))
	for index, current := range request.Messages {
		texts[index] = current.Text
	}
	p.mu.Lock()
	p.prompts = append(p.prompts, texts)
	p.mu.Unlock()
	last := texts[len(texts)-1]
	delay := p.delays[last]
	timer := time.NewTimer(delay)
	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
		return nil, ctx.Err()
	}
	answer := last
	if strings.HasPrefix(last, "follow-up") {
		answer = "follow-up preserved " + strings.Join(texts, "|")
	}
	return provider.NewSliceStream([]provider.Event{
		{Kind: provider.EventTextDelta, Text: answer, TextPhase: provider.TextPhaseFinalAnswer},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}), nil
}

func TestRuntime_ParallelCompletionFollowupAndIndependentCancel(t *testing.T) {
	workerProvider := &collaborationTestProvider{delays: map[string]time.Duration{"fast": 5 * time.Millisecond, "slow": 200 * time.Millisecond, "follow-up": 5 * time.Millisecond}}
	runtime, err := NewRuntime(context.Background(), RuntimeOptions{
		MaxConcurrency: 2,
		QueueSize:      4,
		Factory: func(TaskRequest) (agent.Engine, error) {
			return agent.Engine{Provider: workerProvider, Model: "test"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	fast, err := runtime.Spawn(TaskRequest{ID: "fast", Request: agent.Request{Prompt: "fast"}})
	if err != nil {
		t.Fatal(err)
	}
	slow, err := runtime.Spawn(TaskRequest{ID: "slow", Request: agent.Request{Prompt: "slow"}})
	if err != nil {
		t.Fatal(err)
	}
	fastResult, err := fast.Await(context.Background())
	if err != nil || fastResult.Result.Text != "fast" {
		t.Fatalf("fast result = %#v, err=%v", fastResult, err)
	}
	if snapshot, inspectErr := slow.Snapshot(); inspectErr != nil || (snapshot.Status != TaskQueued && snapshot.Status != TaskRunning) {
		t.Fatalf("slow snapshot = %#v, err=%v; fast result should arrive first", snapshot, inspectErr)
	}
	if err := fast.Send(context.Background(), agent.Request{Prompt: "follow-up"}); err != nil {
		t.Fatal(err)
	}
	followup, err := fast.Await(context.Background())
	if err != nil || !strings.Contains(followup.Result.Text, "fast|follow-up") {
		t.Fatalf("follow-up = %#v, err=%v", followup, err)
	}
	if err := slow.Cancel(); err != nil {
		t.Fatal(err)
	}
	if _, err := slow.Await(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("slow cancellation error = %v, want context.Canceled", err)
	}
}

func TestRuntime_DrainCompletionsCursorAndHookLabel(t *testing.T) {
	workerProvider := &collaborationTestProvider{delays: map[string]time.Duration{"one": time.Millisecond}}
	runtime, err := NewRuntime(context.Background(), RuntimeOptions{MaxConcurrency: 1, QueueSize: 1, Factory: func(TaskRequest) (agent.Engine, error) {
		return agent.Engine{Provider: workerProvider, Model: "test"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handle, err := runtime.Spawn(TaskRequest{ID: "one", Request: agent.Request{Prompt: "one"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, cursor, err := runtime.DrainCompletions(0)
	if err != nil || len(first) != 1 || cursor == 0 {
		t.Fatalf("drain = %#v cursor=%d err=%v", first, cursor, err)
	}
	second, next, err := runtime.DrainCompletions(cursor)
	if err != nil || len(second) != 0 || next != cursor {
		t.Fatalf("duplicate drain = %#v cursor=%d err=%v", second, next, err)
	}
	hook := runtime.CompletionHook()
	messages, err := hook.TransformContext(context.Background(), []message.Message{message.NewText(message.RoleUser, "parent")})
	if err != nil || len(messages) != 2 || !strings.Contains(messages[1].Text, "[UNTRUSTED WORKER RESULT") {
		t.Fatalf("hook completion = %#v err=%v", messages, err)
	}
	repeated, err := hook.TransformContext(context.Background(), []message.Message{message.NewText(message.RoleUser, "parent")})
	if err != nil || len(repeated) != 1 {
		t.Fatalf("hook duplicate completion = %#v err=%v", repeated, err)
	}
}

func TestRuntime_FollowupsResetPerTurnIterationAllowance(t *testing.T) {
	workerProvider := &collaborationTestProvider{}
	runtime, err := NewRuntime(context.Background(), RuntimeOptions{MaxConcurrency: 1, QueueSize: 4, Factory: func(TaskRequest) (agent.Engine, error) {
		return agent.Engine{Provider: workerProvider, Model: "test", LoopPolicy: agent.LoopPolicy{MaxIterations: 1}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handle, err := runtime.Spawn(TaskRequest{ID: "repeat", Request: agent.Request{Prompt: "first"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index := range 3 {
		if err := handle.Send(context.Background(), agent.Request{Prompt: "follow-up"}); err != nil {
			t.Fatalf("follow-up %d: %v", index, err)
		}
		result, err := handle.Await(context.Background())
		if err != nil || !strings.Contains(result.Result.Text, "follow-up preserved") {
			t.Fatalf("follow-up %d result=%#v err=%v", index, result, err)
		}
	}
}

func TestRuntime_RetainedCompletionsAllowUnboundedSequentialTurns(t *testing.T) {
	runtime, err := NewRuntime(context.Background(), RuntimeOptions{
		MaxConcurrency: 1,
		QueueSize:      1,
		Factory: func(TaskRequest) (agent.Engine, error) {
			return agent.Engine{Provider: &collaborationTestProvider{}, Model: "test"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	var cursor uint64
	for index := range 200 {
		id := fmt.Sprintf("sequential-%d", index)
		handle, spawnErr := runtime.Spawn(TaskRequest{ID: id, Request: agent.Request{Prompt: id}})
		if spawnErr != nil {
			t.Fatalf("spawn %d: %v", index, spawnErr)
		}
		result, awaitErr := handle.Await(context.Background())
		if awaitErr != nil {
			t.Fatalf("await %d: %v", index, awaitErr)
		}
		if result.Sequence == 0 {
			t.Fatalf("completion %d has no sequence: %#v", index, result)
		}
		_, cursor, err = runtime.DrainCompletions(cursor)
		if err != nil {
			t.Fatalf("drain %d: %v", index, err)
		}
	}
	if _, _, err := runtime.DrainCompletions(0); !errors.Is(err, ErrCompletionCursorStale) {
		t.Fatalf("stale cursor error = %v, want ErrCompletionCursorStale", err)
	}
	var cursorErr *CompletionCursorError
	_, _, err = runtime.DrainCompletions(0)
	if !errors.As(err, &cursorErr) || cursorErr.Earliest <= 1 {
		t.Fatalf("stale cursor details = %#v, err=%v", cursorErr, err)
	}
}

func TestRuntime_CancelQueuedFollowupCompletesCurrentTurnOnce(t *testing.T) {
	workerProvider := &collaborationTestProvider{delays: map[string]time.Duration{"block": 200 * time.Millisecond}}
	runtime, err := NewRuntime(context.Background(), RuntimeOptions{
		MaxConcurrency: 1,
		QueueSize:      2,
		Factory: func(TaskRequest) (agent.Engine, error) {
			return agent.Engine{Provider: workerProvider, Model: "test"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	target, err := runtime.Spawn(TaskRequest{ID: "target", Request: agent.Request{Prompt: "target"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	block, err := runtime.Spawn(TaskRequest{ID: "block", Request: agent.Request{Prompt: "block"}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		snapshot, snapshotErr := block.Snapshot()
		if snapshotErr != nil {
			t.Fatal(snapshotErr)
		}
		if snapshot.Status == TaskRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("block task did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if err := target.Send(context.Background(), agent.Request{Prompt: "follow-up"}); err != nil {
		t.Fatal(err)
	}
	if err := target.Cancel(); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Await(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued follow-up await error = %v, want context.Canceled", err)
	}
	if _, err := block.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	completions, _, err := runtime.DrainCompletions(0)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, completion := range completions {
		if completion.ID == "target" && completion.Turn == 2 {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("target follow-up completions = %d, want 1", count)
	}
}

func TestRuntime_WaitForTasksBroadcastsAcrossAdvisoryConsumer(t *testing.T) {
	workerProvider := &collaborationTestProvider{delays: map[string]time.Duration{"slow": 30 * time.Millisecond}}
	runtime, err := NewRuntime(context.Background(), RuntimeOptions{
		MaxConcurrency: 1,
		QueueSize:      1,
		Factory: func(TaskRequest) (agent.Engine, error) {
			return agent.Engine{Provider: workerProvider, Model: "test"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handle, err := runtime.Spawn(TaskRequest{ID: "slow", Request: agent.Request{Prompt: "slow"}})
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	var waiters sync.WaitGroup
	waiters.Add(3)
	errs := make(chan error, 3)
	for range 3 {
		go func() {
			defer waiters.Done()
			<-ready
			errs <- runtime.WaitForTasks(context.Background(), "slow")
		}()
	}
	close(ready)
	advisory := make(chan TaskCompletion, 1)
	go func() {
		advisory <- <-runtime.Completions()
	}()
	if _, err := handle.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	waiters.Wait()
	close(errs)
	for waitErr := range errs {
		if waitErr != nil {
			t.Fatalf("waiter error = %v", waitErr)
		}
	}
	select {
	case <-advisory:
	case <-time.After(time.Second):
		t.Fatal("advisory completion consumer did not wake")
	}
}

func TestRuntime_FollowupQueueRollbackClosesFreshAwaitChannel(t *testing.T) {
	workerProvider := &collaborationTestProvider{delays: map[string]time.Duration{"block": 200 * time.Millisecond}}
	runtime, err := NewRuntime(context.Background(), RuntimeOptions{
		MaxConcurrency: 1,
		QueueSize:      2,
		Factory: func(TaskRequest) (agent.Engine, error) {
			return agent.Engine{Provider: workerProvider, Model: "test"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	target, err := runtime.Spawn(TaskRequest{ID: "target", Request: agent.Request{Prompt: "target"}})
	if err != nil {
		t.Fatal(err)
	}
	want, err := target.Await(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	block, err := runtime.Spawn(TaskRequest{ID: "block", Request: agent.Request{Prompt: "block"}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		snapshot, snapshotErr := block.Snapshot()
		if snapshotErr != nil {
			t.Fatal(snapshotErr)
		}
		if snapshot.Status == TaskRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("block task did not start")
		}
		time.Sleep(time.Millisecond)
	}
	for _, id := range []string{"queued-one", "queued-two"} {
		if _, err := runtime.Spawn(TaskRequest{ID: id, Request: agent.Request{Prompt: id}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := target.Send(context.Background(), agent.Request{Prompt: "rejected"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("queue rollback error = %v, want ErrQueueFull", err)
	}
	after, err := target.Await(context.Background())
	if err != nil || after.Sequence != want.Sequence || after.Turn != want.Turn {
		t.Fatalf("await after rollback = %#v, err=%v; want %#v", after, err, want)
	}
}

func TestRuntime_ResultSequenceMatchesInspectAndAwait(t *testing.T) {
	runtime, err := NewRuntime(context.Background(), RuntimeOptions{MaxConcurrency: 1, QueueSize: 1, Factory: func(TaskRequest) (agent.Engine, error) {
		return agent.Engine{Provider: &collaborationTestProvider{}, Model: "test"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handle, err := runtime.Spawn(TaskRequest{ID: "sequence", Request: agent.Request{Prompt: "sequence"}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := handle.Await(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := handle.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if result.Sequence == 0 || snapshot.Result.Sequence != result.Sequence {
		t.Fatalf("sequence mismatch: await=%d inspect=%d", result.Sequence, snapshot.Result.Sequence)
	}
}

func TestRuntime_InterruptedParentLeavesCompletionForFreshHook(t *testing.T) {
	workerProvider := &collaborationTestProvider{}
	runtime, err := NewRuntime(context.Background(), RuntimeOptions{MaxConcurrency: 1, QueueSize: 1, Factory: func(TaskRequest) (agent.Engine, error) {
		return agent.Engine{Provider: workerProvider, Model: "test"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	child, err := runtime.Spawn(TaskRequest{ID: "child", Request: agent.Request{Prompt: "child"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := child.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	parentContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	parentControl := &agent.Control{}
	boundaryStarted := make(chan struct{})
	var boundaryOnce sync.Once
	parent := agent.Engine{
		Provider: &collaborationTestProvider{},
		Model:    "test",
		Control:  parentControl,
		Hooks:    agent.NewHookChain(runtime.CompletionHook(parentControl)),
		Boundaries: agent.BoundaryObserverFunc(func(ctx context.Context, _ agent.Continuation) error {
			boundaryOnce.Do(func() { close(boundaryStarted) })
			<-ctx.Done()
			return ctx.Err()
		}),
	}
	parentDone := make(chan agent.Result, 1)
	go func() {
		parentDone <- parent.Run(parentContext, agent.Request{Prompt: "parent"}, agent.OutputPolicy{})
	}()
	select {
	case <-boundaryStarted:
	case <-time.After(time.Second):
		t.Fatal("parent did not reach boundary")
	}
	cancel()
	select {
	case <-parentDone:
	case <-time.After(time.Second):
		t.Fatal("parent did not abort")
	}
	freshMessages, err := runtime.CompletionHook().TransformContext(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(freshMessages) != 1 || !strings.Contains(freshMessages[0].Text, `"child"`) {
		t.Fatalf("fresh hook messages = %#v", freshMessages)
	}
}
