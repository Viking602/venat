package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type concurrencyTracker struct {
	active    atomic.Int32
	maxActive atomic.Int32
	mu        sync.Mutex
	order     []string
}

func (tracker *concurrencyTracker) enter(id string) func() {
	active := tracker.active.Add(1)
	for {
		current := tracker.maxActive.Load()
		if active <= current || tracker.maxActive.CompareAndSwap(current, active) {
			break
		}
	}
	tracker.mu.Lock()
	tracker.order = append(tracker.order, id)
	tracker.mu.Unlock()
	return func() { tracker.active.Add(-1) }
}

type concurrencyDriver struct {
	definition Definition
	tracker    *concurrencyTracker
}

func (driver concurrencyDriver) Definition() Definition { return driver.definition }

func (driver concurrencyDriver) Execute(_ context.Context, call Call, _ UpdateSink) (Result, error) {
	leave := driver.tracker.enter(call.ID)
	defer leave()
	time.Sleep(15 * time.Millisecond)
	return Result{ToolCallID: call.ID, Name: call.Name, Content: "ok"}, nil
}

func TestSequentialToolMakesParallelBatchOrdered(t *testing.T) {
	tracker := &concurrencyTracker{}
	driver := concurrencyDriver{definition: Definition{
		Name: "skill", InputSchema: Schema{Type: "object"},
		Concurrency: ConcurrencySequential, ConcurrencyGroup: "skills",
	}, tracker: tracker}
	calls := []Call{
		{ID: "first", Name: "skill", Arguments: json.RawMessage(`{}`)},
		{ID: "second", Name: "skill", Arguments: json.RawMessage(`{}`)},
		{ID: "third", Name: "skill", Arguments: json.RawMessage(`{}`)},
	}
	if _, err := NewBus(driver).ExecuteBatch(context.Background(), calls, ModeParallel, ExecuteOptions{}); err != nil {
		t.Fatal(err)
	}
	if tracker.maxActive.Load() != 1 || !slices.Equal(tracker.order, []string{"first", "second", "third"}) {
		t.Fatalf("sequential execution max=%d order=%v", tracker.maxActive.Load(), tracker.order)
	}
}

type barrierDriver struct {
	definition Definition
	started    chan<- string
	release    <-chan struct{}
	fail       bool
}

func (driver barrierDriver) Definition() Definition { return driver.definition }

func (driver barrierDriver) Execute(_ context.Context, call Call, _ UpdateSink) (Result, error) {
	driver.started <- call.ID
	<-driver.release
	if driver.fail {
		return Result{}, errors.New("barrier failed")
	}
	return Result{ToolCallID: call.ID, Name: call.Name, Content: call.ID}, nil
}

func TestParallelBatchRunsContiguousGroupsAroundSequentialBarrier(t *testing.T) {
	started := make(chan string, 5)
	releases := map[string]chan struct{}{
		"read-a": make(chan struct{}),
		"read-b": make(chan struct{}),
		"edit":   make(chan struct{}),
		"done-d": make(chan struct{}),
		"done-e": make(chan struct{}),
	}
	bus := NewBus(
		barrierDriver{definition: Definition{Name: "read-a", InputSchema: Schema{Type: "object"}}, started: started, release: releases["read-a"]},
		barrierDriver{definition: Definition{Name: "read-b", InputSchema: Schema{Type: "object"}}, started: started, release: releases["read-b"]},
		barrierDriver{definition: Definition{Name: "edit", InputSchema: Schema{Type: "object"}, Concurrency: ConcurrencySequential}, started: started, release: releases["edit"]},
		barrierDriver{definition: Definition{Name: "done-d", InputSchema: Schema{Type: "object"}}, started: started, release: releases["done-d"]},
		barrierDriver{definition: Definition{Name: "done-e", InputSchema: Schema{Type: "object"}}, started: started, release: releases["done-e"]},
	)
	calls := []Call{
		{ID: "read-a", Name: "read-a", Arguments: json.RawMessage(`{}`)},
		{ID: "read-b", Name: "read-b", Arguments: json.RawMessage(`{}`)},
		{ID: "edit", Name: "edit", Arguments: json.RawMessage(`{}`)},
		{ID: "done-d", Name: "done-d", Arguments: json.RawMessage(`{}`)},
		{ID: "done-e", Name: "done-e", Arguments: json.RawMessage(`{}`)},
	}
	result := make(chan struct {
		results []Result
		err     error
	}, 1)
	go func() {
		results, err := bus.ExecuteBatch(context.Background(), calls, ModeParallel, ExecuteOptions{})
		result <- struct {
			results []Result
			err     error
		}{results, err}
	}()
	if first := <-started; first != "read-a" && first != "read-b" {
		t.Fatalf("first call = %q, want read-a/read-b", first)
	}
	if second := <-started; second != "read-a" && second != "read-b" {
		t.Fatalf("second call = %q, want read-a/read-b", second)
	}
	select {
	case got := <-started:
		t.Fatalf("barrier crossed before reads completed: %s", got)
	default:
	}
	close(releases["read-a"])
	close(releases["read-b"])
	if got := <-started; got != "edit" {
		t.Fatalf("barrier call = %q, want edit", got)
	}
	select {
	case got := <-started:
		t.Fatalf("post-barrier call started early: %s", got)
	default:
	}
	close(releases["edit"])
	seen := map[string]bool{<-started: true, <-started: true}
	if !seen["done-d"] || !seen["done-e"] {
		t.Fatalf("post-barrier calls = %#v, want done-d and done-e", seen)
	}
	close(releases["done-d"])
	close(releases["done-e"])
	out := <-result
	if out.err != nil || len(out.results) != len(calls) {
		t.Fatalf("batch result = %#v, err=%v", out.results, out.err)
	}
}

func TestParallelBatchFailureDoesNotCrossSequentialBarrier(t *testing.T) {
	started := make(chan string, 5)
	releases := map[string]chan struct{}{
		"read-a": make(chan struct{}),
		"read-b": make(chan struct{}),
		"edit":   make(chan struct{}),
	}
	bus := NewBus(
		barrierDriver{definition: Definition{Name: "read-a", InputSchema: Schema{Type: "object"}}, started: started, release: releases["read-a"]},
		barrierDriver{definition: Definition{Name: "read-b", InputSchema: Schema{Type: "object"}}, started: started, release: releases["read-b"]},
		barrierDriver{definition: Definition{Name: "edit", InputSchema: Schema{Type: "object"}, Concurrency: ConcurrencySequential}, started: started, release: releases["edit"], fail: true},
		barrierDriver{definition: Definition{Name: "done-d", InputSchema: Schema{Type: "object"}}, started: started, release: make(chan struct{})},
		barrierDriver{definition: Definition{Name: "done-e", InputSchema: Schema{Type: "object"}}, started: started, release: make(chan struct{})},
	)
	calls := []Call{
		{ID: "read-a", Name: "read-a", Arguments: json.RawMessage(`{}`)},
		{ID: "read-b", Name: "read-b", Arguments: json.RawMessage(`{}`)},
		{ID: "edit", Name: "edit", Arguments: json.RawMessage(`{}`)},
		{ID: "done-d", Name: "done-d", Arguments: json.RawMessage(`{}`)},
		{ID: "done-e", Name: "done-e", Arguments: json.RawMessage(`{}`)},
	}
	result := make(chan struct {
		results []Result
		err     error
	}, 1)
	go func() {
		results, err := bus.ExecuteBatch(context.Background(), calls, ModeParallel, ExecuteOptions{})
		result <- struct {
			results []Result
			err     error
		}{results, err}
	}()
	<-started
	<-started
	close(releases["read-a"])
	close(releases["read-b"])
	if got := <-started; got != "edit" {
		t.Fatalf("barrier call = %q, want edit", got)
	}
	close(releases["edit"])
	out := <-result
	var batchErr *BatchExecutionError
	if !errors.As(out.err, &batchErr) {
		t.Fatalf("batch error = %v, want BatchExecutionError", out.err)
	}
	if len(out.results) != 2 || len(batchErr.Failures) != 3 {
		t.Fatalf("results=%d failures=%d, want 2 and 3", len(out.results), len(batchErr.Failures))
	}
	for _, failure := range batchErr.Failures[1:] {
		if !errors.Is(failure.Err, ErrNotExecuted) {
			t.Fatalf("failure %q = %v, want ErrNotExecuted", failure.CallID, failure.Err)
		}
	}
}

func TestExclusiveConcurrencyGroupSerializesDifferentTools(t *testing.T) {
	tracker := &concurrencyTracker{}
	bus := NewBus(
		concurrencyDriver{definition: Definition{Name: "one", InputSchema: Schema{Type: "object"}, Concurrency: ConcurrencyExclusive, ConcurrencyGroup: "workspace"}, tracker: tracker},
		concurrencyDriver{definition: Definition{Name: "two", InputSchema: Schema{Type: "object"}, Concurrency: ConcurrencyExclusive, ConcurrencyGroup: "workspace"}, tracker: tracker},
	)
	calls := []Call{{ID: "one", Name: "one", Arguments: json.RawMessage(`{}`)}, {ID: "two", Name: "two", Arguments: json.RawMessage(`{}`)}}
	if _, err := bus.ExecuteBatch(context.Background(), calls, ModeParallel, ExecuteOptions{}); err != nil {
		t.Fatal(err)
	}
	if tracker.maxActive.Load() != 1 {
		t.Fatalf("exclusive group max active = %d", tracker.maxActive.Load())
	}
}

func TestClonedAndSubsetBusesShareConcurrencyLimiters(t *testing.T) {
	tracker := &concurrencyTracker{}
	root := NewBus(concurrencyDriver{definition: Definition{
		Name: "write", InputSchema: Schema{Type: "object"},
		Concurrency: ConcurrencyExclusive, ConcurrencyGroup: "workspace",
	}, tracker: tracker})
	buses := []*Bus{root.Clone(), root.Subset([]string{"write"})}
	var group sync.WaitGroup
	errs := make(chan error, len(buses))
	for index, bus := range buses {
		group.Add(1)
		go func(index int, bus *Bus) {
			defer group.Done()
			_, err := bus.Execute(context.Background(), Call{
				ID: string(rune('a' + index)), Name: "write", Arguments: json.RawMessage(`{}`),
			}, ExecuteOptions{})
			errs <- err
		}(index, bus)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if tracker.maxActive.Load() != 1 {
		t.Fatalf("cloned buses bypassed exclusive limiter: max active = %d", tracker.maxActive.Load())
	}
}

func TestPerToolMaxConcurrencyCapsParallelBatch(t *testing.T) {
	tracker := &concurrencyTracker{}
	driver := concurrencyDriver{definition: Definition{
		Name: "bounded", InputSchema: Schema{Type: "object"}, MaxConcurrency: 2,
	}, tracker: tracker}
	calls := make([]Call, 6)
	for index := range calls {
		calls[index] = Call{ID: string(rune('a' + index)), Name: "bounded", Arguments: json.RawMessage(`{}`)}
	}
	if _, err := NewBus(driver).ExecuteBatch(context.Background(), calls, ModeParallel, ExecuteOptions{}); err != nil {
		t.Fatal(err)
	}
	if tracker.maxActive.Load() != 2 {
		t.Fatalf("bounded tool max active = %d, want 2", tracker.maxActive.Load())
	}
}

func TestToolBatchRejectsUnboundedProviderFanout(t *testing.T) {
	tracker := &concurrencyTracker{}
	driver := concurrencyDriver{
		definition: Definition{Name: "lookup", InputSchema: Schema{Type: "object"}},
		tracker:    tracker,
	}
	calls := make([]Call, MaxBatchCalls+1)
	for index := range calls {
		calls[index] = Call{ID: fmt.Sprintf("call-%d", index), Name: "lookup", Arguments: json.RawMessage(`{}`)}
	}
	if _, err := NewBus(driver).ExecuteBatch(context.Background(), calls, ModeParallel, ExecuteOptions{}); !errors.Is(err, ErrTooManyToolCalls) {
		t.Fatalf("oversized batch error = %v", err)
	}
	if tracker.maxActive.Load() != 0 {
		t.Fatalf("oversized batch executed %d concurrent calls", tracker.maxActive.Load())
	}
}

func TestConcurrencyGroupRejectsConflictingLimits(t *testing.T) {
	tracker := &concurrencyTracker{}
	bus := NewBus(concurrencyDriver{definition: Definition{
		Name: "one", InputSchema: Schema{Type: "object"}, ConcurrencyGroup: "shared", MaxConcurrency: 1,
	}, tracker: tracker})
	err := bus.Register(concurrencyDriver{definition: Definition{
		Name: "two", InputSchema: Schema{Type: "object"}, ConcurrencyGroup: "shared", MaxConcurrency: 2,
	}, tracker: tracker})
	if !errors.Is(err, ErrInvalidToolDefinition) {
		t.Fatalf("conflicting group error = %v", err)
	}
}
