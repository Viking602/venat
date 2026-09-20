package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/message"
)

func validDispatch(id string) Dispatch {
	return Dispatch{ID: id, Route: "route", Request: agent.Request{Prompt: id}}
}

func TestValidateDispatch(t *testing.T) {
	negativeBudget := validDispatch("negative-budget")
	negativeBudget.Request.Budget = &agent.Budget{MaxSteps: -1}
	negativeRepair := validDispatch("negative-repair")
	negativeRepair.OutputPolicy.MaxRepairAttempts = -1
	tests := []struct {
		name     string
		dispatch Dispatch
		wantErr  bool
	}{
		{name: "valid", dispatch: validDispatch("valid")},
		{name: "matching handoff", dispatch: Dispatch{ID: "handoff", Route: "worker", Handoff: &Handoff{To: "worker", Payload: json.RawMessage(`{"key":"value"}`)}}},
		{name: "empty id", dispatch: Dispatch{Route: "route"}, wantErr: true},
		{name: "blank route", dispatch: Dispatch{ID: "blank", Route: "  "}, wantErr: true},
		{name: "invalid schema", dispatch: Dispatch{ID: "schema", Route: "route", OutputPolicy: agent.OutputPolicy{Schema: json.RawMessage(`{"type":`)}}, wantErr: true},
		{name: "native schema", dispatch: Dispatch{ID: "native", Route: "route", OutputPolicy: agent.OutputPolicy{Native: true, Schema: json.RawMessage(`{"type":"invalid"}`)}}, wantErr: true},
		{name: "provider content", dispatch: Dispatch{ID: "content", Route: "route", Request: agent.Request{Content: []message.ContentPart{{Kind: message.ContentReasoning, Text: "injected"}}}}, wantErr: true},
		{name: "invalid payload", dispatch: Dispatch{ID: "payload", Route: "route", Handoff: &Handoff{Payload: json.RawMessage(`{`)}}, wantErr: true},
		{name: "conflicting handoff", dispatch: Dispatch{ID: "conflict", Route: "route", Handoff: &Handoff{To: "other"}}, wantErr: true},
		{name: "negative budget", dispatch: negativeBudget, wantErr: true},
		{name: "negative repair", dispatch: negativeRepair, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateDispatch(test.dispatch)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateDispatch() error = %v, wantErr %v", err, test.wantErr)
			}
			if test.wantErr && !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("ValidateDispatch() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestDriveGivesSchedulerDeepClone(t *testing.T) {
	initial := State{
		Tick: 1,
		Outcomes: []Outcome{{
			Tick: 1,
			Dispatch: Dispatch{
				ID:    "prior",
				Route: "route",
				Request: agent.Request{
					Prompt:        "prior",
					Budget:        &agent.Budget{MaxSteps: 2},
					SessionBudget: &agent.SessionBudget{MaxWallClock: time.Minute},
					ModelTimeouts: &agent.ModelTimeoutPolicy{ConnectTimeout: time.Second},
				},
				OutputPolicy: agent.OutputPolicy{Schema: json.RawMessage(`{"type":"string"}`)},
				Handoff:      &Handoff{To: "route", Payload: json.RawMessage(`{"key":"value"}`)},
				Metadata:     map[string]string{"owner": "original"},
			},
			Result: agent.Result{
				Structured: json.RawMessage(`"original"`),
				Messages: []message.Message{{
					Role:     message.RoleAssistant,
					Metadata: map[string]string{"owner": "original"},
				}},
			},
		}},
	}
	want := cloneState(initial)
	scheduler := SchedulerFunc(func(_ context.Context, state State) ([]Dispatch, error) {
		state.Outcomes[0].Dispatch.Request.Budget.MaxSteps = 99
		state.Outcomes[0].Dispatch.Request.SessionBudget.MaxWallClock = time.Hour
		state.Outcomes[0].Dispatch.Request.ModelTimeouts.ConnectTimeout = time.Hour
		state.Outcomes[0].Dispatch.OutputPolicy.Schema[0] = '['
		state.Outcomes[0].Dispatch.Handoff.Payload[0] = '['
		state.Outcomes[0].Dispatch.Metadata["owner"] = "changed"
		state.Outcomes[0].Result.Structured[0] = '['
		state.Outcomes[0].Result.Messages[0].Metadata["owner"] = "changed"
		return nil, nil
	})

	got, err := Drive(context.Background(), scheduler, ExecutorFunc(func(context.Context, Dispatch, agent.Sink) (agent.Result, error) {
		t.Fatal("executor must not run for empty schedule")
		return agent.Result{}, nil
	}), DriveOptions{InitialState: &initial})
	if err != nil {
		t.Fatalf("Drive() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(initial, want) {
		t.Fatalf("scheduler mutation escaped clone\ngot:  %#v\nwant: %#v\nsrc:  %#v", got, want, initial)
	}
}

func TestDriveWrapsSchedulerFailureAndPanicAtCurrentTick(t *testing.T) {
	failure := errors.New("scheduler unavailable")
	tests := []struct {
		name      string
		scheduler Scheduler
		want      error
	}{
		{
			name: "error",
			scheduler: SchedulerFunc(func(context.Context, State) ([]Dispatch, error) {
				return nil, failure
			}),
			want: failure,
		},
		{
			name: "panic",
			scheduler: SchedulerFunc(func(context.Context, State) ([]Dispatch, error) {
				panic("scheduler exploded")
			}),
			want: ErrSchedulerPanic,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			initial := State{Tick: 3}
			_, err := Drive(context.Background(), test.scheduler, ExecutorFunc(func(context.Context, Dispatch, agent.Sink) (agent.Result, error) {
				return agent.Result{}, nil
			}), DriveOptions{InitialState: &initial})
			var schedulerErr *SchedulerError
			if !errors.As(err, &schedulerErr) || schedulerErr.Tick != 3 || !errors.Is(err, test.want) {
				t.Fatalf("Drive() error = %v, want SchedulerError tick 3 containing %v", err, test.want)
			}
		})
	}
}

func TestDrivePreservesSchedulerContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := SchedulerFunc(func(ctx context.Context, _ State) ([]Dispatch, error) {
		cancel()
		return nil, ctx.Err()
	})
	_, err := Drive(ctx, scheduler, ExecutorFunc(func(context.Context, Dispatch, agent.Sink) (agent.Result, error) {
		return agent.Result{}, nil
	}), DriveOptions{})
	var schedulerErr *SchedulerError
	if !errors.As(err, &schedulerErr) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Drive() error = %v, want SchedulerError containing context.Canceled", err)
	}
}

func TestDriveRejectsInvalidOptionsAndInitialState(t *testing.T) {
	executor := ExecutorFunc(func(context.Context, Dispatch, agent.Sink) (agent.Result, error) { return agent.Result{}, nil })
	scheduler := SchedulerFunc(func(context.Context, State) ([]Dispatch, error) { return nil, nil })
	unsorted := State{Tick: 1, Outcomes: []Outcome{{Tick: 1, Dispatch: validDispatch("b")}, {Tick: 1, Dispatch: validDispatch("a")}}}
	partial := State{Tick: 0, Outcomes: []Outcome{{Tick: 1, Dispatch: validDispatch("partial")}}}
	tests := []struct {
		name      string
		scheduler Scheduler
		executor  Executor
		options   DriveOptions
	}{
		{name: "nil scheduler", executor: executor},
		{name: "nil executor", scheduler: scheduler},
		{name: "negative ticks", scheduler: scheduler, executor: executor, options: DriveOptions{MaxTicks: -1}},
		{name: "negative concurrency", scheduler: scheduler, executor: executor, options: DriveOptions{MaxConcurrency: -1}},
		{name: "unsorted initial state", scheduler: scheduler, executor: executor, options: DriveOptions{InitialState: &unsorted}},
		{name: "partial initial state", scheduler: scheduler, executor: executor, options: DriveOptions{InitialState: &partial}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Drive(context.Background(), test.scheduler, test.executor, test.options)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Drive() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}
