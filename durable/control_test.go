package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Viking602/venat/agent"
	. "github.com/Viking602/venat/durable"
	"github.com/Viking602/venat/durable/internal/testbackend"
	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

func TestRuntime_ControlCancellationDoesNotReplayStartedTool(t *testing.T) {
	store := testbackend.New()
	runtime := newTestRuntime(t, store, Options{OwnerID: "first"})
	control := &agent.Control{}
	started := make(chan struct{})
	var calls atomic.Int32
	action, err := kit.Tool("action", func(ctx context.Context, _ struct{}) (string, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){toolTurn("action")}}
	engine := testEngine(driver, action)
	engine.Control = control
	done := make(chan error, 1)
	go func() {
		_, err := runtime.Start(context.Background(), "control-cancel", engine, testRequest("act"), agent.OutputPolicy{})
		done <- err
	}()
	t.Cleanup(control.Cancel)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("tool did not start")
	}
	control.Cancel()
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not finish")
	}
	var required *ReconcileRequiredError
	if !errors.As(err, &required) || !errors.Is(err, context.Canceled) || errors.Is(err, tool.ErrNotExecuted) {
		t.Fatalf("cancel error=%v", err)
	}
	if len(required.Attempts) != 1 || required.Attempts[0].Kind != AttemptKindTool || required.Attempts[0].Status != AttemptStatusUnknown {
		t.Fatalf("attempts=%+v", required.Attempts)
	}
	second := newTestRuntime(t, store.Reopen(), Options{OwnerID: "second"})
	_, err = second.Resume(context.Background(), "control-cancel", testEngine(driver, action))
	if !errors.Is(err, ErrReconcileRequired) || calls.Load() != 1 || driver.callCount() != 1 {
		t.Fatalf("resume error=%v tools=%d model_calls=%d", err, calls.Load(), driver.callCount())
	}
}

func TestRuntime_InvalidUserContentOrNativeSchemaIsNotAdmitted(t *testing.T) {
	for _, test := range []struct {
		name    string
		request agent.Request
		policy  agent.OutputPolicy
	}{
		{name: "provider content", request: agent.Request{Content: []message.ContentPart{{Kind: message.ContentReasoning, Text: "injected"}}}},
		{name: "native schema", policy: agent.OutputPolicy{Native: true, Schema: json.RawMessage(`{"type":"invalid"}`)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := testbackend.New()
			runtime := newTestRuntime(t, store, Options{OwnerID: "preflight"})
			driver := &runtimeProvider{}
			_, err := runtime.Start(context.Background(), "invalid", testEngine(driver), test.request, test.policy)
			if !errors.Is(err, ErrInvalidArgument) || driver.callCount() != 0 {
				t.Fatalf("error=%v calls=%d", err, driver.callCount())
			}
			if _, err := store.LoadExecution(context.Background(), "invalid"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("invalid execution was stored: %v", err)
			}
		})
	}
}

type failInputCheckpointBackend struct{ Backend }

func (backend failInputCheckpointBackend) SaveCheckpoint(ctx context.Context, request SaveCheckpointRequest) (Execution, error) {
	value, err := backend.Backend.SaveCheckpoint(ctx, request)
	if err == nil && request.Checkpoint.Continuation.Phase == agent.ContinuationReady && len(request.Checkpoint.Continuation.Steps) > 0 {
		return value, errInjectedBackend
	}
	return value, err
}

func TestRuntime_InputCheckpointResponseLossCanBeInspectedAndResumed(t *testing.T) {
	store := testbackend.New()
	runtime := newTestRuntime(t, failInputCheckpointBackend{Backend: store}, Options{OwnerID: "first"})
	control := &agent.Control{}
	var ack <-chan error
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		func(ctx context.Context, request provider.Request) (provider.Stream, error) {
			var err error
			ack, err = control.Send(agent.Request{Prompt: "correction"})
			if err != nil {
				return nil, err
			}
			return finalEvents("initial")(ctx, request)
		},
	}}
	engine := testEngine(driver)
	engine.Control = control
	_, err := runtime.Start(context.Background(), "input-loss", engine, testRequest("original"), agent.OutputPolicy{})
	if !errors.Is(err, errInjectedBackend) {
		t.Fatalf("start error=%v", err)
	}
	if inputErr := <-ack; !errors.Is(inputErr, errInjectedBackend) {
		t.Fatalf("ack=%v", inputErr)
	}
	execution, err := store.LoadExecution(context.Background(), "input-loss")
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := execution.Checkpoint
	if checkpoint == nil || checkpoint.Continuation.Messages[len(checkpoint.Continuation.Messages)-1].Text != "correction" {
		t.Fatalf("checkpoint=%+v", checkpoint)
	}
	resumedDriver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		func(ctx context.Context, request provider.Request) (provider.Stream, error) {
			count := 0
			for _, current := range request.Messages {
				if current.Role == message.RoleUser && current.Text == "correction" {
					count++
				}
			}
			if count != 1 {
				return nil, errors.New("recorded input missing or duplicated")
			}
			return finalEvents("corrected")(ctx, request)
		},
	}}
	second := newTestRuntime(t, store.Reopen(), Options{OwnerID: "second"})
	result, err := second.Resume(context.Background(), "input-loss", testEngine(resumedDriver))
	if err != nil || result.Failure != nil || result.Text != "corrected" || driver.callCount() != 1 || resumedDriver.callCount() != 1 {
		t.Fatalf("resume=%+v error=%v", result, err)
	}
}

func TestRuntime_AcknowledgedInputAndSettledModelSurviveReopen(t *testing.T) {
	store := testbackend.New()
	fault := &failSaveBackend{Backend: store, phase: agent.ContinuationModelComplete}
	runtime := newTestRuntime(t, fault, Options{OwnerID: "first"})
	control := &agent.Control{}
	var ack <-chan error
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		func(ctx context.Context, request provider.Request) (provider.Stream, error) {
			var err error
			ack, err = control.Send(agent.Request{Prompt: "run the check"})
			if err != nil {
				return nil, err
			}
			return finalEvents("initial")(ctx, request)
		},
		toolTurn("check"),
	}}
	toolCalls := 0
	check, err := kit.Tool("check", func(context.Context, struct{}) (string, error) { toolCalls++; return "checked", nil })
	if err != nil {
		t.Fatal(err)
	}
	engine := testEngine(driver, check)
	engine.Control = control
	engine.LoopPolicy.ContextTokenTarget = 1500
	_, err = runtime.Start(context.Background(), "input-reopen", engine, testRequest("original"), agent.OutputPolicy{})
	if !errors.Is(err, errInjectedBackend) {
		t.Fatalf("start=%v", err)
	}
	if inputErr := <-ack; inputErr != nil {
		t.Fatalf("ack=%v", inputErr)
	}
	if toolCalls != 0 {
		t.Fatal("tool ran before the failed boundary")
	}
	resumedDriver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){finalEvents("complete")}}
	second := newTestRuntime(t, store.Reopen(), Options{OwnerID: "second"})
	resumedEngine := testEngine(resumedDriver, check)
	resumedEngine.LoopPolicy.ContextTokenTarget = 1500
	result, err := second.Resume(context.Background(), "input-reopen", resumedEngine)
	if err != nil || result.Failure != nil || toolCalls != 1 || resumedDriver.callCount() != 1 || result.Text != "complete" {
		t.Fatalf("result=%+v err=%v tools=%d calls=%d", result, err, toolCalls, resumedDriver.callCount())
	}
}
