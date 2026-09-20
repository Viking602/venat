package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/message"
)

const (
	defaultMaxTicks       = 64
	defaultMaxConcurrency = 4
)

// DriveOptions bounds work added by one Drive invocation.
type DriveOptions struct {
	MaxTicks             int
	MaxConcurrency       int
	UnlimitedConcurrency bool
	// MaxWallClock bounds the complete Drive invocation, including scheduler
	// waits and all ticks. Zero leaves the caller context as the outer bound.
	MaxWallClock time.Duration
	// DispatchTimeout bounds one Executor call. Zero inherits the Drive context.
	DispatchTimeout time.Duration
	Sink            agent.Sink
	InitialState    *State
}

// Drive repeatedly schedules and executes deterministic ticks until Scheduler
// returns an empty batch or a mechanical boundary fails.
func Drive(ctx context.Context, scheduler Scheduler, executor Executor, options DriveOptions) (State, error) {
	if nilInterface(scheduler) {
		return State{}, fmt.Errorf("%w: nil scheduler", ErrInvalidArgument)
	}
	if nilInterface(executor) {
		return State{}, fmt.Errorf("%w: nil executor", ErrInvalidArgument)
	}
	if options.MaxTicks < 0 || options.MaxConcurrency < 0 || options.MaxWallClock < 0 || options.DispatchTimeout < 0 {
		return State{}, fmt.Errorf("%w: negative drive limit", ErrInvalidArgument)
	}
	driveCtx, cancel := driveContext(ctx, options.MaxWallClock)
	defer cancel()
	maxTicks := options.MaxTicks
	if maxTicks == 0 {
		maxTicks = defaultMaxTicks
	}
	maxConcurrency := options.MaxConcurrency
	if maxConcurrency == 0 && !options.UnlimitedConcurrency {
		maxConcurrency = defaultMaxConcurrency
	}

	state := State{}
	if options.InitialState != nil {
		if err := validateInitialState(*options.InitialState); err != nil {
			return State{}, err
		}
		state = cloneState(*options.InitialState)
	}
	completedTicks := 0
	for {
		if err := driveCtx.Err(); err != nil {
			return state, contextLimitError(driveCtx, err)
		}
		dispatches, err := nextDispatches(driveCtx, scheduler, state)
		if err != nil {
			return state, err
		}
		if len(dispatches) == 0 {
			return state, nil
		}
		if completedTicks >= maxTicks {
			return state, ErrMaxTicks
		}

		next, runErr := executeBatch(driveCtx, state, dispatches, executor, options.Sink, maxConcurrency, options.UnlimitedConcurrency, options.DispatchTimeout)
		state = next
		if runErr != nil {
			return state, runErr
		}
		state.Tick++
		completedTicks++
	}
}

func nextDispatches(ctx context.Context, scheduler Scheduler, state State) ([]Dispatch, error) {
	dispatches, err := callScheduler(ctx, scheduler, cloneState(state))
	if err != nil {
		schedulerErr := &SchedulerError{Tick: state.Tick, Err: err}
		if ctx.Err() != nil {
			return nil, errors.Join(schedulerErr, contextLimitError(ctx, ctx.Err()))
		}
		return nil, schedulerErr
	}
	if err := ctx.Err(); err != nil {
		return nil, contextLimitError(ctx, err)
	}
	dispatches, err = validateBatch(state, dispatches)
	if err != nil {
		return nil, &SchedulerError{Tick: state.Tick, Err: err}
	}
	return dispatches, nil
}

func callScheduler(ctx context.Context, scheduler Scheduler, state State) (dispatches []Dispatch, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			dispatches = nil
			err = fmt.Errorf("%w: %v", ErrSchedulerPanic, recovered)
		}
	}()
	return scheduler.Next(ctx, state)
}

func validateInitialState(state State) error {
	if state.Tick < 0 {
		return fmt.Errorf("%w: initial state has negative tick", ErrInvalidArgument)
	}
	seen := make(map[string]struct{}, len(state.Outcomes))
	for index, outcome := range state.Outcomes {
		if outcome.Tick < 1 || outcome.Tick > state.Tick {
			return fmt.Errorf("%w: initial outcome %q has tick %d outside [1,%d]", ErrInvalidArgument, outcome.Dispatch.ID, outcome.Tick, state.Tick)
		}
		if err := ValidateDispatch(outcome.Dispatch); err != nil {
			return fmt.Errorf("initial outcome %d: %w", index, err)
		}
		if _, duplicate := seen[outcome.Dispatch.ID]; duplicate {
			return fmt.Errorf("%w: duplicate initial dispatch ID %q", ErrInvalidArgument, outcome.Dispatch.ID)
		}
		seen[outcome.Dispatch.ID] = struct{}{}
		if index > 0 && !outcomeLess(state.Outcomes[index-1], outcome) {
			return fmt.Errorf("%w: initial outcomes are not sorted by tick and dispatch ID", ErrInvalidArgument)
		}
	}
	return nil
}

func validateBatch(state State, dispatches []Dispatch) ([]Dispatch, error) {
	seen := make(map[string]struct{}, len(state.Outcomes)+len(dispatches))
	for _, outcome := range state.Outcomes {
		seen[outcome.Dispatch.ID] = struct{}{}
	}
	validated := make([]Dispatch, len(dispatches))
	for index, dispatch := range dispatches {
		if err := ValidateDispatch(dispatch); err != nil {
			return nil, fmt.Errorf("scheduler dispatch %d: %w", index, err)
		}
		if _, duplicate := seen[dispatch.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate dispatch ID %q", ErrInvalidArgument, dispatch.ID)
		}
		seen[dispatch.ID] = struct{}{}
		validated[index] = cloneDispatch(dispatch)
	}
	return validated, nil
}

type dispatchExecution struct {
	dispatch Dispatch
	result   agent.Result
	err      error
}

func executeBatch(
	ctx context.Context,
	state State,
	dispatches []Dispatch,
	executor Executor,
	sink agent.Sink,
	maxConcurrency int,
	unlimited bool,
	dispatchTimeout time.Duration,
) (State, error) {
	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	records := make(chan dispatchExecution, len(dispatches))
	var wait sync.WaitGroup
	var permits chan struct{}
	if !unlimited {
		permits = make(chan struct{}, maxConcurrency)
	}
	sharedSink := &serialSink{sink: sink}

launchLoop:
	for _, dispatch := range dispatches {
		if batchCtx.Err() != nil {
			break
		}
		if permits != nil {
			select {
			case permits <- struct{}{}:
			case <-batchCtx.Done():
				break launchLoop
			}
			if batchCtx.Err() != nil {
				<-permits
				break
			}
		}
		dispatch := cloneDispatch(dispatch)
		wait.Add(1)
		go func() {
			defer wait.Done()
			if permits != nil {
				defer func() { <-permits }()
			}
			var dispatchSink agent.Sink
			if sink != nil {
				dispatchSink = sourceSink{source: dispatch.ID, next: sharedSink}
			}
			callCtx, cancelCall := dispatchContext(batchCtx, dispatchTimeout)
			result, err := callExecutor(callCtx, executor, cloneDispatch(dispatch), dispatchSink)
			if err != nil {
				err = contextLimitError(callCtx, err)
			} else if errors.Is(context.Cause(callCtx), ErrDispatchTimeout) || errors.Is(context.Cause(callCtx), ErrMaxWallClock) {
				err = contextLimitError(callCtx, nil)
			}
			cancelCall()
			records <- dispatchExecution{dispatch: dispatch, result: cloneResult(result), err: err}
			if err != nil {
				cancel()
			}
		}()
	}
	wait.Wait()
	close(records)

	next := cloneState(state)
	nextTick := state.Tick + 1
	successes := make([]Outcome, 0, len(dispatches))
	failures := make([]*DispatchError, 0)
	for record := range records {
		if record.err != nil {
			failures = append(failures, &DispatchError{
				Dispatch: cloneDispatch(record.dispatch),
				Result:   cloneResult(record.result),
				Err:      record.err,
			})
			continue
		}
		successes = append(successes, Outcome{
			Tick:     nextTick,
			Dispatch: cloneDispatch(record.dispatch),
			Result:   cloneResult(record.result),
		})
	}
	sort.Slice(successes, func(left, right int) bool {
		return successes[left].Dispatch.ID < successes[right].Dispatch.ID
	})
	next.Outcomes = append(next.Outcomes, successes...)
	sort.Slice(next.Outcomes, func(left, right int) bool {
		return outcomeLess(next.Outcomes[left], next.Outcomes[right])
	})

	sort.Slice(failures, func(left, right int) bool {
		return failures[left].Dispatch.ID < failures[right].Dispatch.ID
	})
	joined := make([]error, 0, len(failures)+1)
	for _, failure := range failures {
		joined = append(joined, failure)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		joined = append(joined, contextLimitError(ctx, ctxErr))
	}
	return next, errors.Join(joined...)
}

func driveContext(ctx context.Context, maxWallClock time.Duration) (context.Context, context.CancelFunc) {
	if maxWallClock <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeoutCause(ctx, maxWallClock, ErrMaxWallClock)
}

func dispatchContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeoutCause(ctx, timeout, ErrDispatchTimeout)
}

func contextLimitError(ctx context.Context, err error) error {
	if err == nil {
		if ctx.Err() == nil {
			return nil
		}
		err = ctx.Err()
	}
	cause := context.Cause(ctx)
	if cause == nil || errors.Is(err, cause) {
		return err
	}
	return errors.Join(err, cause)
}

func callExecutor(ctx context.Context, executor Executor, dispatch Dispatch, sink agent.Sink) (result agent.Result, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = agent.Result{}
			err = fmt.Errorf("%w: %v", ErrExecutorPanic, recovered)
		}
	}()
	return executor.Execute(ctx, dispatch, sink)
}

func outcomeLess(left, right Outcome) bool {
	if left.Tick != right.Tick {
		return left.Tick < right.Tick
	}
	return left.Dispatch.ID < right.Dispatch.ID
}

func cloneState(state State) State {
	state.Outcomes = slices.Clone(state.Outcomes)
	for index := range state.Outcomes {
		state.Outcomes[index].Dispatch = cloneDispatch(state.Outcomes[index].Dispatch)
		state.Outcomes[index].Result = cloneResult(state.Outcomes[index].Result)
	}
	return state
}

func cloneDispatch(dispatch Dispatch) Dispatch {
	dispatch.Request.Content = message.CloneContent(dispatch.Request.Content)
	if dispatch.Request.Budget != nil {
		budget := *dispatch.Request.Budget
		dispatch.Request.Budget = &budget
	}
	if dispatch.Request.SessionBudget != nil {
		budget := *dispatch.Request.SessionBudget
		dispatch.Request.SessionBudget = &budget
	}
	if dispatch.Request.ModelTimeouts != nil {
		policy := *dispatch.Request.ModelTimeouts
		dispatch.Request.ModelTimeouts = &policy
	}
	dispatch.OutputPolicy.Schema = append(json.RawMessage(nil), dispatch.OutputPolicy.Schema...)
	if dispatch.Handoff != nil {
		handoff := *dispatch.Handoff
		handoff.Payload = append(json.RawMessage(nil), handoff.Payload...)
		dispatch.Handoff = &handoff
	}
	dispatch.Metadata = maps.Clone(dispatch.Metadata)
	return dispatch
}

func cloneResult(result agent.Result) agent.Result {
	result.Structured = append(json.RawMessage(nil), result.Structured...)
	result.Messages = message.CloneMessages(result.Messages)
	result.Steps = slices.Clone(result.Steps)
	for index := range result.Steps {
		if result.Steps[index].ModelCall != nil {
			modelCall := *result.Steps[index].ModelCall
			result.Steps[index].ModelCall = &modelCall
		}
		result.Steps[index].ToolCalls = slices.Clone(result.Steps[index].ToolCalls)
		for callIndex := range result.Steps[index].ToolCalls {
			trace := &result.Steps[index].ToolCalls[callIndex]
			trace.Arguments = append(json.RawMessage(nil), trace.Arguments...)
			trace.Output = append(json.RawMessage(nil), trace.Output...)
		}
		result.Steps[index].Observations = slices.Clone(result.Steps[index].Observations)
	}
	if result.Failure != nil {
		failure := *result.Failure
		result.Failure = &failure
	}
	return result
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

type serialSink struct {
	mu   sync.Mutex
	sink agent.Sink
}

func (sink *serialSink) Emit(ctx context.Context, frame agent.Frame) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.sink.Emit(ctx, frame)
}

type sourceSink struct {
	source string
	next   agent.Sink
}

func (sink sourceSink) Emit(ctx context.Context, frame agent.Frame) error {
	frame.Source = sink.source
	return sink.next.Emit(ctx, frame)
}
