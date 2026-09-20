package orchestration

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Viking602/venat/agent"
)

func TestDriveCompletesEmptyScheduleWithoutExecutor(t *testing.T) {
	executorCalls := 0
	state, err := Drive(context.Background(), SchedulerFunc(func(context.Context, State) ([]Dispatch, error) {
		return nil, nil
	}), ExecutorFunc(func(context.Context, Dispatch, agent.Sink) (agent.Result, error) {
		executorCalls++
		return agent.Result{}, nil
	}), DriveOptions{})
	if err != nil {
		t.Fatalf("Drive() error = %v", err)
	}
	if state.Tick != 0 || len(state.Outcomes) != 0 || executorCalls != 0 {
		t.Fatalf("Drive() = %#v with %d executor calls, want empty completion", state, executorCalls)
	}
}

func TestDriveMaxTicksBoundsNewTicks(t *testing.T) {
	initial := State{Tick: 5}
	executorCalls := 0
	scheduler := SchedulerFunc(func(_ context.Context, state State) ([]Dispatch, error) {
		return []Dispatch{validDispatch(string(rune('a' + state.Tick)))}, nil
	})
	state, err := Drive(context.Background(), scheduler, ExecutorFunc(func(_ context.Context, dispatch Dispatch, _ agent.Sink) (agent.Result, error) {
		executorCalls++
		return agent.Result{Text: dispatch.ID, Valid: true}, nil
	}), DriveOptions{InitialState: &initial, MaxTicks: 2})
	if !errors.Is(err, ErrMaxTicks) {
		t.Fatalf("Drive() error = %v, want ErrMaxTicks", err)
	}
	if state.Tick != 7 || executorCalls != 2 || len(state.Outcomes) != 2 {
		t.Fatalf("Drive() = %#v with %d executor calls, want two new ticks", state, executorCalls)
	}
}

func TestDriveValidatesWholeBatchBeforeExecution(t *testing.T) {
	tests := []struct {
		name  string
		state State
		batch []Dispatch
	}{
		{name: "duplicate in batch", batch: []Dispatch{validDispatch("same"), validDispatch("same")}},
		{name: "duplicate from prior outcome", state: State{Tick: 1, Outcomes: []Outcome{{Tick: 1, Dispatch: validDispatch("same")}}}, batch: []Dispatch{validDispatch("same")}},
		{name: "one invalid dispatch", batch: []Dispatch{validDispatch("valid"), {ID: "invalid"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executorCalls := 0
			state, err := Drive(context.Background(), SchedulerFunc(func(context.Context, State) ([]Dispatch, error) {
				return test.batch, nil
			}), ExecutorFunc(func(context.Context, Dispatch, agent.Sink) (agent.Result, error) {
				executorCalls++
				return agent.Result{}, nil
			}), DriveOptions{InitialState: &test.state})
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Drive() error = %v, want ErrInvalidArgument", err)
			}
			if executorCalls != 0 {
				t.Fatalf("executor calls = %d, want 0", executorCalls)
			}
			if !reflect.DeepEqual(state, test.state) {
				t.Fatalf("state = %#v, want unchanged %#v", state, test.state)
			}
		})
	}
}

func TestDriveTreatsAgentFailureAsOutcomeData(t *testing.T) {
	agentFailure := &agent.AgentFailure{Kind: agent.FailureKindOutputBlocked, Reason: "blocked"}
	scheduler := SchedulerFunc(func(_ context.Context, state State) ([]Dispatch, error) {
		if state.Tick == 0 {
			return []Dispatch{validDispatch("blocked")}, nil
		}
		return nil, nil
	})
	state, err := Drive(context.Background(), scheduler, ExecutorFunc(func(context.Context, Dispatch, agent.Sink) (agent.Result, error) {
		return agent.Result{Failure: agentFailure}, nil
	}), DriveOptions{})
	if err != nil {
		t.Fatalf("Drive() error = %v", err)
	}
	if state.Tick != 1 || len(state.Outcomes) != 1 || state.Outcomes[0].Result.Failure == nil || state.Outcomes[0].Result.Failure.Kind != agent.FailureKindOutputBlocked {
		t.Fatalf("Drive() state = %#v, want Agent failure folded as data", state)
	}
}

func TestDriveReturnsDeterministicDispatchErrorsAndPartialState(t *testing.T) {
	failA := errors.New("failure-a")
	failC := errors.New("failure-c")
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	schedulerCalls := 0
	scheduler := SchedulerFunc(func(context.Context, State) ([]Dispatch, error) {
		schedulerCalls++
		return []Dispatch{validDispatch("c"), validDispatch("b"), validDispatch("a")}, nil
	})
	executor := ExecutorFunc(func(_ context.Context, dispatch Dispatch, _ agent.Sink) (agent.Result, error) {
		started <- struct{}{}
		<-release
		switch dispatch.ID {
		case "a":
			return agent.Result{Text: "partial-a"}, failA
		case "c":
			return agent.Result{Text: "partial-c"}, failC
		default:
			return agent.Result{Text: "success-b", Valid: true}, nil
		}
	})
	done := make(chan struct{})
	var state State
	var driveErr error
	go func() {
		state, driveErr = Drive(context.Background(), scheduler, executor, DriveOptions{UnlimitedConcurrency: true})
		close(done)
	}()
	for range 3 {
		<-started
	}
	close(release)
	<-done

	if !errors.Is(driveErr, failA) || !errors.Is(driveErr, failC) {
		t.Fatalf("Drive() error = %v, want both executor failures", driveErr)
	}
	if strings.Index(driveErr.Error(), `dispatch "a"`) > strings.Index(driveErr.Error(), `dispatch "c"`) {
		t.Fatalf("Drive() errors are not sorted by dispatch ID: %v", driveErr)
	}
	var dispatchErr *DispatchError
	if !errors.As(driveErr, &dispatchErr) || dispatchErr.Dispatch.ID != "a" || dispatchErr.Result.Text != "partial-a" {
		t.Fatalf("first DispatchError = %#v, want a with partial result", dispatchErr)
	}
	if state.Tick != 0 || len(state.Outcomes) != 1 || state.Outcomes[0].Tick != 1 || state.Outcomes[0].Dispatch.ID != "b" {
		t.Fatalf("partial state = %#v, want successful b at pending tick 1", state)
	}
	if schedulerCalls != 1 {
		t.Fatalf("scheduler calls = %d, want 1", schedulerCalls)
	}
	_, err := Drive(context.Background(), SchedulerFunc(func(context.Context, State) ([]Dispatch, error) { return nil, nil }), executor, DriveOptions{InitialState: &state})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("partial state accepted as clean InitialState: %v", err)
	}
}

func TestDriveContainsExecutorPanicWithZeroPartialResult(t *testing.T) {
	scheduler := SchedulerFunc(func(context.Context, State) ([]Dispatch, error) {
		return []Dispatch{validDispatch("panic")}, nil
	})
	state, err := Drive(context.Background(), scheduler, ExecutorFunc(func(context.Context, Dispatch, agent.Sink) (agent.Result, error) {
		panic("executor exploded")
	}), DriveOptions{})
	if !errors.Is(err, ErrExecutorPanic) {
		t.Fatalf("Drive() error = %v, want ErrExecutorPanic", err)
	}
	var dispatchErr *DispatchError
	if !errors.As(err, &dispatchErr) || !reflect.DeepEqual(dispatchErr.Result, agent.Result{}) {
		t.Fatalf("DispatchError = %#v, want zero partial Result", dispatchErr)
	}
	if state.Tick != 0 || len(state.Outcomes) != 0 {
		t.Fatalf("state = %#v, want no folded panic outcome", state)
	}
}

func TestDriveMaxWallClockReturnsTypedLimit(t *testing.T) {
	started := make(chan struct{})
	state, err := Drive(context.Background(), oneTickScheduler("slow"), ExecutorFunc(func(ctx context.Context, _ Dispatch, _ agent.Sink) (agent.Result, error) {
		close(started)
		<-ctx.Done()
		return agent.Result{}, ctx.Err()
	}), DriveOptions{MaxWallClock: 10 * time.Millisecond})
	if !errors.Is(err, ErrMaxWallClock) {
		t.Fatalf("Drive() error = %v, want ErrMaxWallClock", err)
	}
	if state.Tick != 0 {
		t.Fatalf("state tick = %d, want 0", state.Tick)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("executor did not start")
	}
}

func TestDriveDispatchTimeoutReturnsTypedLimit(t *testing.T) {
	_, err := Drive(context.Background(), oneTickScheduler("slow"), ExecutorFunc(func(ctx context.Context, _ Dispatch, _ agent.Sink) (agent.Result, error) {
		<-ctx.Done()
		return agent.Result{}, ctx.Err()
	}), DriveOptions{DispatchTimeout: 10 * time.Millisecond})
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("Drive() error = %v, want ErrDispatchTimeout", err)
	}
}

func TestDriveMaxWallClockBoundsScheduler(t *testing.T) {
	_, err := Drive(context.Background(), SchedulerFunc(func(context.Context, State) ([]Dispatch, error) {
		time.Sleep(20 * time.Millisecond)
		return nil, nil
	}), ExecutorFunc(func(context.Context, Dispatch, agent.Sink) (agent.Result, error) {
		return agent.Result{}, nil
	}), DriveOptions{MaxWallClock: 10 * time.Millisecond})
	if !errors.Is(err, ErrMaxWallClock) {
		t.Fatalf("Drive() error = %v, want ErrMaxWallClock", err)
	}
}

func TestDriveFailsFastAndDoesNotLaunchQueuedDispatch(t *testing.T) {
	root := errors.New("root infrastructure failure")
	bothStarted := make(chan struct{})
	var startMu sync.Mutex
	started := make(map[string]bool)
	startCount := 0
	executor := ExecutorFunc(func(ctx context.Context, dispatch Dispatch, _ agent.Sink) (agent.Result, error) {
		startMu.Lock()
		started[dispatch.ID] = true
		startCount++
		if startCount == 2 {
			close(bothStarted)
		}
		startMu.Unlock()
		<-bothStarted
		if dispatch.ID == "a" {
			return agent.Result{Text: "partial"}, root
		}
		<-ctx.Done()
		return agent.Result{}, ctx.Err()
	})
	_, err := Drive(context.Background(), SchedulerFunc(func(context.Context, State) ([]Dispatch, error) {
		return []Dispatch{validDispatch("a"), validDispatch("b"), validDispatch("c")}, nil
	}), executor, DriveOptions{MaxConcurrency: 2})
	if !errors.Is(err, root) {
		t.Fatalf("Drive() error = %v, want root failure", err)
	}
	startMu.Lock()
	defer startMu.Unlock()
	if started["c"] || len(started) != 2 {
		t.Fatalf("started dispatches = %v, want only a and b", started)
	}
}
