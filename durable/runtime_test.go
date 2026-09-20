package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
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

var errInjectedBackend = errors.New("injected backend failure")

type runtimeProvider struct {
	mu        sync.Mutex
	responses []func(context.Context, provider.Request) (provider.Stream, error)
	calls     int
}

func (driver *runtimeProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "runtime-test"}
}

func (driver *runtimeProvider) Stream(ctx context.Context, request provider.Request) (provider.Stream, error) {
	driver.mu.Lock()
	index := driver.calls
	driver.calls++
	if index >= len(driver.responses) {
		driver.mu.Unlock()
		return nil, errors.New("runtime test provider exhausted")
	}
	response := driver.responses[index]
	driver.mu.Unlock()
	return response(ctx, request)
}

func (driver *runtimeProvider) callCount() int {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	return driver.calls
}

func providerEvents(events ...provider.Event) func(context.Context, provider.Request) (provider.Stream, error) {
	return func(context.Context, provider.Request) (provider.Stream, error) {
		return provider.NewSliceStream(events), nil
	}
}

func finalEvents(text string) func(context.Context, provider.Request) (provider.Stream, error) {
	return providerEvents(
		provider.Event{Kind: provider.EventTextDelta, Text: text},
		provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	)
}

func testEngine(driver provider.Driver, drivers ...tool.Driver) agent.Engine {
	engine := agent.Engine{Provider: driver, Model: "runtime-model", ToolMode: tool.ModeSequential}
	if len(drivers) > 0 {
		engine.Tools = tool.NewBus(drivers...)
	}
	return engine
}

func testRequest(prompt string) agent.Request {
	return agent.Request{Prompt: prompt, Budget: &agent.Budget{MaxSteps: 4, MaxToolCalls: 4}}
}

func newTestRuntime(t *testing.T, backend Backend, options Options) *Runtime {
	t.Helper()
	runtime, err := New(backend, options)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return runtime
}

func TestNew_ValidatesOptionsAndAcceptsDefaults(t *testing.T) {
	backend := testbackend.New()
	_ = newTestRuntime(t, backend, Options{})

	var typedNil *testbackend.Backend
	for name, test := range map[string]struct {
		backend Backend
		options Options
	}{
		"nil backend":         {backend: nil},
		"typed nil backend":   {backend: typedNil},
		"blank owner":         {backend: backend, options: Options{OwnerID: "  "}},
		"negative lease":      {backend: backend, options: Options{LeaseTTL: -time.Second}},
		"negative settlement": {backend: backend, options: Options{SettlementTimeout: -time.Second}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(test.backend, test.options); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("New() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

type failSaveBackend struct {
	Backend
	phase       agent.ContinuationPhase
	afterCommit bool
	failed      atomic.Bool
}

func (backend *failSaveBackend) SaveCheckpoint(ctx context.Context, request SaveCheckpointRequest) (Execution, error) {
	if request.Checkpoint.Continuation.Phase != backend.phase || backend.failed.Swap(true) {
		return backend.Backend.SaveCheckpoint(ctx, request)
	}
	if !backend.afterCommit {
		return Execution{}, errInjectedBackend
	}
	execution, err := backend.Backend.SaveCheckpoint(ctx, request)
	if err != nil {
		return Execution{}, err
	}
	return execution, errInjectedBackend
}

func TestRuntime_ResumesPersistedCheckpointAfterProcessReopen(t *testing.T) {
	store := testbackend.New()
	fault := &failSaveBackend{Backend: store, phase: agent.ContinuationReady, afterCommit: true}
	first := newTestRuntime(t, fault, Options{OwnerID: "worker-one"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){finalEvents("restored")}}

	result, err := first.Start(context.Background(), "checkpoint-reopen", testEngine(driver), testRequest("persist me"), agent.OutputPolicy{})
	if !errors.Is(err, errInjectedBackend) {
		t.Fatalf("Start() error = %v, want injected checkpoint failure; result = %#v", err, result)
	}
	if driver.callCount() != 0 {
		t.Fatalf("provider calls before restart = %d, want 0", driver.callCount())
	}
	execution, err := store.LoadExecution(context.Background(), "checkpoint-reopen")
	if err != nil {
		t.Fatalf("LoadExecution() error = %v", err)
	}
	if execution.Checkpoint == nil || execution.Checkpoint.Continuation.Phase != agent.ContinuationReady || execution.Lease != nil {
		t.Fatalf("persisted execution = %#v, want released ready checkpoint", execution)
	}

	second := newTestRuntime(t, store.Reopen(), Options{OwnerID: "worker-two"})
	result, err = second.Resume(context.Background(), "checkpoint-reopen", testEngine(driver))
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if result.Text != "restored" || driver.callCount() != 1 {
		t.Fatalf("resumed result = %#v, provider calls = %d", result, driver.callCount())
	}
}

func TestRuntime_ReopensModelCompleteCheckpointWithoutRepeatingModel(t *testing.T) {
	store := testbackend.New()
	fault := &failSaveBackend{Backend: store, phase: agent.ContinuationModelComplete, afterCommit: true}
	first := newTestRuntime(t, fault, Options{OwnerID: "worker-one"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		providerEvents(
			provider.Event{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"query":"venat"}`)}},
			provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		),
		finalEvents("tool complete"),
	}}
	var toolCalls atomic.Int32
	lookup, err := kit.Tool("lookup", func(_ context.Context, input struct {
		Query string `json:"query"`
	},
	) (string, error) {
		toolCalls.Add(1)
		return "found:" + input.Query, nil
	})
	if err != nil {
		t.Fatalf("kit.Tool() error = %v", err)
	}

	if _, err := first.Start(context.Background(), "model-complete-reopen", testEngine(driver, lookup), testRequest("look up"), agent.OutputPolicy{}); !errors.Is(err, errInjectedBackend) {
		t.Fatalf("Start() error = %v, want injected model-complete checkpoint failure", err)
	}
	if driver.callCount() != 1 || toolCalls.Load() != 0 {
		t.Fatalf("before reopen: provider calls = %d, tool calls = %d", driver.callCount(), toolCalls.Load())
	}

	second := newTestRuntime(t, store.Reopen(), Options{OwnerID: "worker-two"})
	result, err := second.Resume(context.Background(), "model-complete-reopen", testEngine(driver, lookup))
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if result.Text != "tool complete" || driver.callCount() != 2 || toolCalls.Load() != 1 {
		t.Fatalf("result = %#v, provider calls = %d, tool calls = %d", result, driver.callCount(), toolCalls.Load())
	}
}

func TestRuntime_ReplaysSettledModelAndTerminalResult(t *testing.T) {
	store := testbackend.New()
	fault := &failSaveBackend{Backend: store, phase: agent.ContinuationValidatingOutput, afterCommit: true}
	first := newTestRuntime(t, fault, Options{OwnerID: "worker-one"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){finalEvents("once")}}

	_, err := first.Start(context.Background(), "model-replay", testEngine(driver), testRequest("answer"), agent.OutputPolicy{})
	if !errors.Is(err, errInjectedBackend) {
		t.Fatalf("Start() error = %v, want injected checkpoint failure", err)
	}
	if driver.callCount() != 1 {
		t.Fatalf("provider calls = %d, want 1", driver.callCount())
	}

	second := newTestRuntime(t, store.Reopen(), Options{OwnerID: "worker-two"})
	result, err := second.Resume(context.Background(), "model-replay", testEngine(driver))
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if result.Text != "once" || driver.callCount() != 1 {
		t.Fatalf("replayed result = %#v, provider calls = %d; want settled effect replay", result, driver.callCount())
	}

	var frames []agent.Frame
	third := newTestRuntime(t, store.Reopen(), Options{OwnerID: "worker-three"})
	replayed, err := third.ResumeStream(context.Background(), "model-replay", testEngine(driver), agent.SinkFunc(func(_ context.Context, frame agent.Frame) error {
		frames = append(frames, frame)
		return nil
	}))
	if err != nil {
		t.Fatalf("terminal ResumeStream() error = %v", err)
	}
	if replayed.Text != "once" || len(frames) != 0 || driver.callCount() != 1 {
		t.Fatalf("terminal replay = %#v, frames = %#v, provider calls = %d", replayed, frames, driver.callCount())
	}
}

func TestRuntime_PersistsToolOutcomeAcrossProcessReopen(t *testing.T) {
	store := testbackend.New()
	fault := &failSaveBackend{Backend: store, phase: agent.ContinuationToolsComplete, afterCommit: true}
	first := newTestRuntime(t, fault, Options{OwnerID: "worker-one"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		providerEvents(
			provider.Event{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"query":"venat"}`)}},
			provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		),
		finalEvents("tool complete"),
	}}
	var toolCalls atomic.Int32
	lookup, err := kit.Tool("lookup", func(_ context.Context, input struct {
		Query string `json:"query"`
	},
	) (string, error) {
		toolCalls.Add(1)
		return "found:" + input.Query, nil
	})
	if err != nil {
		t.Fatalf("kit.Tool() error = %v", err)
	}

	_, err = first.Start(context.Background(), "tool-reopen", testEngine(driver, lookup), testRequest("look up"), agent.OutputPolicy{})
	if !errors.Is(err, errInjectedBackend) {
		t.Fatalf("Start() error = %v, want injected tools checkpoint failure", err)
	}
	if toolCalls.Load() != 1 || driver.callCount() != 1 {
		t.Fatalf("before reopen: tool calls = %d, provider calls = %d", toolCalls.Load(), driver.callCount())
	}

	second := newTestRuntime(t, store.Reopen(), Options{OwnerID: "worker-two"})
	result, err := second.Resume(context.Background(), "tool-reopen", testEngine(driver, lookup))
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if result.Text != "tool complete" || toolCalls.Load() != 1 || driver.callCount() != 2 {
		t.Fatalf("result = %#v, tool calls = %d, provider calls = %d", result, toolCalls.Load(), driver.callCount())
	}
}

type partialFailureStream struct {
	events []provider.Event
	index  int
	err    error
}

func (stream *partialFailureStream) Recv() (provider.Event, error) {
	if stream.index < len(stream.events) {
		event := stream.events[stream.index]
		stream.index++
		return event, nil
	}
	return provider.Event{}, stream.err
}

func (*partialFailureStream) Close() error { return nil }

type runtimeRetryableError struct{}

func (runtimeRetryableError) Error() string   { return "retryable transport failure" }
func (runtimeRetryableError) Retryable() bool { return true }

func TestRuntime_ProviderOpenPanicBecomesUnknownAndReleasesLease(t *testing.T) {
	store := testbackend.New()
	runtime := newTestRuntime(t, store, Options{
		OwnerID:           "provider-panic-worker",
		SettlementTimeout: 20 * time.Millisecond,
	})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		func(context.Context, provider.Request) (provider.Stream, error) {
			panic("provider stream open exploded")
		},
	}}

	result, err := runtime.Start(
		context.Background(),
		"provider-open-panic",
		testEngine(driver),
		testRequest("work"),
		agent.OutputPolicy{},
	)
	var required *ReconcileRequiredError
	if !errors.As(err, &required) || len(required.Attempts) != 1 {
		t.Fatalf("Start() error = %v, want one ReconcileRequiredError", err)
	}
	if !errors.Is(err, agent.ErrPanicRecovered) {
		t.Fatalf("Start() error = %v, want ErrPanicRecovered", err)
	}
	if result.Failure == nil || !errors.Is(result.Failure, agent.ErrPanicRecovered) {
		t.Fatalf("Start() result failure = %v, want ErrPanicRecovered", result.Failure)
	}
	attempt := required.Attempts[0]
	if attempt.Kind != AttemptKindModel || attempt.Status != AttemptStatusUnknown {
		t.Fatalf("reconciliation attempt = %#v, want unknown model attempt", attempt)
	}
	if driver.callCount() != 1 {
		t.Fatalf("provider calls = %d, want 1", driver.callCount())
	}
	execution, loadErr := store.LoadExecution(context.Background(), "provider-open-panic")
	if loadErr != nil {
		t.Fatalf("LoadExecution() error = %v", loadErr)
	}
	if execution.Lease != nil {
		t.Fatalf("execution lease = %#v, want released", execution.Lease)
	}
}

type panicReceiveStream struct{}

func (panicReceiveStream) Recv() (provider.Event, error) {
	panic("provider stream receive exploded")
}

func (panicReceiveStream) Close() error { return nil }

type panicDurableTool struct{}

func (panicDurableTool) Definition() tool.Definition {
	return tool.Definition{Name: "lookup"}
}

func (panicDurableTool) Execute(context.Context, tool.Call, tool.UpdateSink) (tool.Result, error) {
	panic("tool execute exploded")
}

func TestRuntime_EffectPanicBecomesUnknownAndReleasesLease(t *testing.T) {
	for _, test := range []struct {
		name        string
		executionID ExecutionID
		engine      func() agent.Engine
		wantKind    AttemptKind
	}{
		{
			name:        "provider receive",
			executionID: "provider-receive-panic",
			engine: func() agent.Engine {
				driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
					func(context.Context, provider.Request) (provider.Stream, error) {
						return panicReceiveStream{}, nil
					},
				}}
				return testEngine(driver)
			},
			wantKind: AttemptKindModel,
		},
		{
			name:        "tool execute",
			executionID: "tool-execute-panic",
			engine: func() agent.Engine {
				driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
					providerEvents(
						provider.Event{
							Kind: provider.EventToolCall,
							ToolCall: &message.ToolCall{
								ID:        "call-1",
								Name:      "lookup",
								Arguments: json.RawMessage(`{"query":"venat"}`),
							},
						},
						provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
					),
				}}
				return testEngine(driver, panicDurableTool{})
			},
			wantKind: AttemptKindTool,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := testbackend.New()
			runtime := newTestRuntime(t, store, Options{
				OwnerID:           string(test.executionID) + "-worker",
				SettlementTimeout: 20 * time.Millisecond,
			})
			result, err := runtime.Start(
				context.Background(),
				test.executionID,
				test.engine(),
				testRequest("work"),
				agent.OutputPolicy{},
			)
			var required *ReconcileRequiredError
			if !errors.As(err, &required) || len(required.Attempts) != 1 {
				t.Fatalf("Start() error = %v, want one ReconcileRequiredError", err)
			}
			if !errors.Is(err, agent.ErrPanicRecovered) {
				t.Fatalf("Start() error = %v, want ErrPanicRecovered", err)
			}
			if result.Failure == nil || !errors.Is(result.Failure, agent.ErrPanicRecovered) {
				t.Fatalf("Start() result failure = %v, want ErrPanicRecovered", result.Failure)
			}
			attempt := required.Attempts[0]
			if attempt.Kind != test.wantKind || attempt.Status != AttemptStatusUnknown {
				t.Fatalf("reconciliation attempt = %#v, want unknown %s attempt", attempt, test.wantKind)
			}
			execution, loadErr := store.LoadExecution(context.Background(), test.executionID)
			if loadErr != nil {
				t.Fatalf("LoadExecution() error = %v", loadErr)
			}
			if execution.Lease != nil {
				t.Fatalf("execution lease = %#v, want released", execution.Lease)
			}
		})
	}
}

func TestRuntime_PartialRetryStreamBecomesUnknownWithoutReopen(t *testing.T) {
	store := testbackend.New()
	runtime := newTestRuntime(t, store, Options{OwnerID: "partial-retry-worker"})
	opens := 0
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		func(ctx context.Context, _ provider.Request) (provider.Stream, error) {
			return provider.OpenRetryingStream(ctx, func() (provider.Stream, error) {
				opens++
				return &partialFailureStream{
					events: []provider.Event{{Kind: provider.EventTextDelta, Text: "partial"}},
					err:    runtimeRetryableError{},
				}, nil
			}, provider.StreamRetryOptions{Delay: func(int) time.Duration { return 0 }})
		},
	}}

	_, err := runtime.Start(
		context.Background(),
		"partial-retry",
		testEngine(driver),
		testRequest("work"),
		agent.OutputPolicy{},
	)
	var required *ReconcileRequiredError
	var partial *provider.PartialStreamError
	if !errors.As(err, &required) || len(required.Attempts) != 1 {
		t.Fatalf("Start() error = %v, want one ReconcileRequiredError", err)
	}
	if !errors.As(err, &partial) || partial.Retryable() {
		t.Fatalf("Start() error = %v, want non-retryable PartialStreamError", err)
	}
	if required.Attempts[0].Status != AttemptStatusUnknown {
		t.Fatalf("attempt status = %q, want %q", required.Attempts[0].Status, AttemptStatusUnknown)
	}
	if opens != 1 || driver.callCount() != 1 {
		t.Fatalf("retry stream opens = %d, provider calls = %d, want 1 each", opens, driver.callCount())
	}
}

func TestRuntime_ReconcileModelAttemptResolutions(t *testing.T) {
	for _, test := range []struct {
		name              string
		reconciliation    func(Attempt) Reconciliation
		wantText          string
		wantFailure       bool
		wantProviderCalls int
	}{
		{
			name: "succeed",
			reconciliation: func(attempt Attempt) Reconciliation {
				return Reconciliation{
					AttemptNumber: attempt.Number, AttemptVersion: attempt.Version, Resolution: ReconcileResolutionSucceed,
					ModelEvents: []provider.Event{{Kind: provider.EventTextDelta, Text: "reconciled"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}},
				}
			},
			wantText: "reconciled", wantProviderCalls: 1,
		},
		{
			name: "fail",
			reconciliation: func(attempt Attempt) Reconciliation {
				return Reconciliation{
					AttemptNumber: attempt.Number, AttemptVersion: attempt.Version, Resolution: ReconcileResolutionFail,
					ModelEvents: []provider.Event{{Kind: provider.EventTextDelta, Text: "partial"}},
					Failure:     &FailureRecord{Code: "operator_failed", Message: "confirmed failure"},
				}
			},
			wantText: "partial", wantFailure: true, wantProviderCalls: 1,
		},
		{
			name: "retry",
			reconciliation: func(attempt Attempt) Reconciliation {
				return Reconciliation{AttemptNumber: attempt.Number, AttemptVersion: attempt.Version, Resolution: ReconcileResolutionRetry}
			},
			wantText: "retried", wantProviderCalls: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := testbackend.New()
			runtime := newTestRuntime(t, store, Options{OwnerID: "reconciler"})
			driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
				func(context.Context, provider.Request) (provider.Stream, error) {
					return &partialFailureStream{events: []provider.Event{{Kind: provider.EventTextDelta, Text: "partial"}}, err: errors.New("connection outcome unknown")}, nil
				},
				finalEvents("retried"),
			}}

			_, err := runtime.Start(context.Background(), "reconcile-"+ExecutionID(test.name), testEngine(driver), testRequest("work"), agent.OutputPolicy{})
			var required *ReconcileRequiredError
			if !errors.As(err, &required) || len(required.Attempts) != 1 {
				t.Fatalf("Start() error = %v, want one ReconcileRequiredError", err)
			}
			attempt := required.Attempts[0]
			if attempt.Kind != AttemptKindModel || attempt.Status != AttemptStatusUnknown {
				t.Fatalf("unknown attempt = %#v", attempt)
			}
			if err := runtime.Reconcile(context.Background(), required.Execution.ID, attempt.OperationID, test.reconciliation(attempt)); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			result, err := runtime.Resume(context.Background(), required.Execution.ID, testEngine(driver))
			if err != nil {
				t.Fatalf("Resume() error = %v", err)
			}
			if result.Text != test.wantText || (result.Failure != nil) != test.wantFailure || driver.callCount() != test.wantProviderCalls {
				t.Fatalf("result = %#v, provider calls = %d", result, driver.callCount())
			}
		})
	}
}

func TestRuntime_ReconcileToolResultIdentityAndReplay(t *testing.T) {
	store := testbackend.New()
	runtime := newTestRuntime(t, store, Options{OwnerID: "tool-reconciler"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		providerEvents(
			provider.Event{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "action", Arguments: json.RawMessage(`{}`)}},
			provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		),
		finalEvents("reconciled tool"),
	}}
	action := &failToolDriver{
		definition: message.ToolDefinition{Name: "action", InputSchema: message.JSONSchema{Type: "object"}},
		err:        errors.New("effect outcome unknown"),
	}
	_, err := runtime.Start(context.Background(), "tool-reconcile", testEngine(driver, action), testRequest("act"), agent.OutputPolicy{})
	var required *ReconcileRequiredError
	if !errors.As(err, &required) || len(required.Attempts) != 1 {
		t.Fatalf("Start() error = %v, want one ReconcileRequiredError", err)
	}
	attempt := required.Attempts[0]
	wrong := tool.Result{Name: "different", Content: "done"}
	if err := runtime.Reconcile(context.Background(), "tool-reconcile", attempt.OperationID, Reconciliation{
		AttemptNumber: attempt.Number, AttemptVersion: attempt.Version, Resolution: ReconcileResolutionSucceed, ToolResult: &wrong,
	}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Reconcile() conflicting identity error = %v, want ErrInvalidArgument", err)
	}
	resultValue := tool.Result{Content: "done"}
	if err := runtime.Reconcile(context.Background(), "tool-reconcile", attempt.OperationID, Reconciliation{
		AttemptNumber: attempt.Number, AttemptVersion: attempt.Version, Resolution: ReconcileResolutionSucceed, ToolResult: &resultValue,
	}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	result, err := runtime.Resume(context.Background(), "tool-reconcile", testEngine(driver, action))
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if result.Text != "reconciled tool" || action.calls.Load() != 1 || driver.callCount() != 2 {
		t.Fatalf("result = %#v, tool calls = %d, provider calls = %d", result, action.calls.Load(), driver.callCount())
	}
}

type failFinishAttemptBackend struct{ Backend }

func (backend failFinishAttemptBackend) FinishAttempt(context.Context, FinishAttemptRequest) (Attempt, error) {
	return Attempt{}, errInjectedBackend
}

func TestRuntime_SettlesProviderTerminalBeforeDelivery(t *testing.T) {
	store := testbackend.New()
	runtime := newTestRuntime(t, failFinishAttemptBackend{Backend: store}, Options{OwnerID: "settlement"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){finalEvents("partial")}}
	var frames []agent.Frame
	_, err := runtime.StartStream(context.Background(), "settlement-order", testEngine(driver), testRequest("answer"), agent.OutputPolicy{}, agent.SinkFunc(func(_ context.Context, frame agent.Frame) error {
		frames = append(frames, frame)
		return nil
	}))
	if !errors.Is(err, errInjectedBackend) {
		t.Fatalf("StartStream() error = %v, want settlement failure", err)
	}
	for _, frame := range frames {
		if frame.Kind == agent.FrameDone {
			t.Fatalf("terminal frame delivered before durable settlement: %#v", frames)
		}
	}
}

type orderRecorder struct {
	mu     sync.Mutex
	values []string
}

func (recorder *orderRecorder) add(value string) {
	recorder.mu.Lock()
	recorder.values = append(recorder.values, value)
	recorder.mu.Unlock()
}

func (recorder *orderRecorder) snapshot() []string {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]string(nil), recorder.values...)
}

type recordingBackend struct {
	Backend
	recorder *orderRecorder
}

func (backend recordingBackend) StartAttempt(ctx context.Context, request StartAttemptRequest) (AttemptStart, error) {
	backend.recorder.add("durable-effect")
	return backend.Backend.StartAttempt(ctx, request)
}

func (backend recordingBackend) SaveCheckpoint(ctx context.Context, request SaveCheckpointRequest) (Execution, error) {
	backend.recorder.add("durable-boundary:" + string(request.Checkpoint.Continuation.Phase))
	return backend.Backend.SaveCheckpoint(ctx, request)
}

func TestRuntime_DurableInterceptorsAreOutermostAndBoundaryIsLast(t *testing.T) {
	recorder := &orderRecorder{}
	store := testbackend.New()
	runtime := newTestRuntime(t, recordingBackend{Backend: store, recorder: recorder}, Options{OwnerID: "ordering"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){func(context.Context, provider.Request) (provider.Stream, error) {
		recorder.add("provider")
		return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: "done"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
	}}}
	engine := testEngine(driver)
	engine.ModelInterceptor = provider.StreamInterceptorFunc(func(ctx context.Context, next provider.Driver, request provider.Request) (provider.Stream, error) {
		recorder.add("caller-effect")
		return next.Stream(ctx, request)
	})
	engine.Boundaries = agent.BoundaryObserverFunc(func(_ context.Context, continuation agent.Continuation) error {
		recorder.add("caller-boundary:" + string(continuation.Phase))
		return nil
	})

	if _, err := runtime.Start(context.Background(), "ordering", engine, testRequest("order"), agent.OutputPolicy{}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	values := recorder.snapshot()
	if len(values) < 5 {
		t.Fatalf("recorded order = %#v", values)
	}
	if values[0] != "caller-boundary:ready" || values[1] != "durable-boundary:ready" || values[2] != "durable-effect" || values[3] != "caller-effect" || values[4] != "provider" {
		t.Fatalf("recorded prefix = %#v", values)
	}
	for index, value := range values {
		if len(value) < len("caller-boundary:") || value[:len("caller-boundary:")] != "caller-boundary:" {
			continue
		}
		if index+1 >= len(values) || values[index+1] != "durable-boundary:"+value[len("caller-boundary:"):] {
			t.Fatalf("caller boundary %q not immediately followed by durable boundary in %#v", value, values)
		}
	}
}

type contextBlockingStream struct {
	ctx         context.Context
	started     chan<- struct{}
	startedOnce *sync.Once
}

func (stream contextBlockingStream) Recv() (provider.Event, error) {
	if stream.started != nil && stream.startedOnce != nil {
		stream.startedOnce.Do(func() { close(stream.started) })
	}
	<-stream.ctx.Done()
	return provider.Event{}, context.Cause(stream.ctx)
}

func (contextBlockingStream) Close() error { return nil }

func blockingProvider(started chan<- struct{}) *runtimeProvider {
	return &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){func(ctx context.Context, _ provider.Request) (provider.Stream, error) {
		return contextBlockingStream{ctx: ctx, started: started, startedOnce: &sync.Once{}}, nil
	}}}
}

func TestRuntime_SuspendCancelsAndPersistsSuspendedState(t *testing.T) {
	store := testbackend.New()
	runtime := newTestRuntime(t, store, Options{OwnerID: "suspender", SettlementTimeout: time.Second})
	started := make(chan struct{})
	driver := blockingProvider(started)
	startErr := make(chan error, 1)
	go func() {
		_, err := runtime.Start(context.Background(), "suspend", testEngine(driver), testRequest("wait"), agent.OutputPolicy{})
		startErr <- err
	}()
	<-started
	if err := runtime.Reconcile(context.Background(), "suspend", "turn:0:model", Reconciliation{
		AttemptNumber: 1, AttemptVersion: 1, Resolution: ReconcileResolutionRetry,
	}); !errors.Is(err, ErrBusy) {
		t.Fatalf("Reconcile() while active error = %v, want ErrBusy", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Suspend(ctx, "suspend"); err != nil {
		t.Fatalf("Suspend() error = %v", err)
	}
	if err := <-startErr; !errors.Is(err, ErrSuspended) {
		t.Fatalf("Start() error = %v, want ErrSuspended", err)
	}
	execution, err := store.LoadExecution(context.Background(), "suspend")
	if err != nil {
		t.Fatalf("LoadExecution() error = %v", err)
	}
	if execution.Status != ExecutionStatusSuspended || execution.Lease != nil {
		t.Fatalf("execution after Suspend = %#v", execution)
	}
}

func TestRuntime_CallerCancellationReleasesLease(t *testing.T) {
	store := testbackend.New()
	runtime := newTestRuntime(t, store, Options{OwnerID: "cancelled", SettlementTimeout: time.Second})
	started := make(chan struct{})
	driver := blockingProvider(started)
	ctx, cancel := context.WithCancel(context.Background())
	resultErr := make(chan error, 1)
	go func() {
		_, err := runtime.Start(ctx, "cancel", testEngine(driver), testRequest("wait"), agent.OutputPolicy{})
		resultErr <- err
	}()
	<-started
	cancel()
	if err := <-resultErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start() error = %v, want context.Canceled", err)
	}
	execution, err := store.LoadExecution(context.Background(), "cancel")
	if err != nil {
		t.Fatalf("LoadExecution() error = %v", err)
	}
	if execution.Lease != nil {
		t.Fatalf("caller cancellation retained lease %#v", execution.Lease)
	}

	resumer := newTestRuntime(t, store.Reopen(), Options{OwnerID: "resumer"})
	_, err = resumer.Resume(context.Background(), "cancel", testEngine(driver))
	if !errors.Is(err, ErrReconcileRequired) || errors.Is(err, ErrBusy) {
		t.Fatalf("Resume() error = %v, want reconciliation without busy lease", err)
	}
}

type closeAwareBackend struct {
	Backend
	closed atomic.Bool
}

func (backend *closeAwareBackend) Close() error {
	backend.closed.Store(true)
	return nil
}

func TestRuntime_CloseStopsWorkWithoutClosingBackend(t *testing.T) {
	store := testbackend.New()
	backend := &closeAwareBackend{Backend: store}
	runtime := newTestRuntime(t, backend, Options{OwnerID: "closer", SettlementTimeout: time.Second})
	started := make(chan struct{})
	driver := blockingProvider(started)
	startErr := make(chan error, 1)
	go func() {
		_, err := runtime.Start(context.Background(), "close", testEngine(driver), testRequest("wait"), agent.OutputPolicy{})
		startErr <- err
	}()
	<-started
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := <-startErr; !errors.Is(err, ErrClosed) {
		t.Fatalf("Start() error = %v, want ErrClosed", err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if backend.closed.Load() {
		t.Fatal("Runtime.Close closed caller-owned Backend")
	}
	if _, err := runtime.Start(context.Background(), "after-close", testEngine(driver), testRequest("no"), agent.OutputPolicy{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start() after Close error = %v, want ErrClosed", err)
	}
}

type failToolDriver struct {
	definition message.ToolDefinition
	err        error
	calls      atomic.Int32
}

func (driver *failToolDriver) Definition() message.ToolDefinition { return driver.definition }

func (driver *failToolDriver) Execute(context.Context, tool.Call, tool.UpdateSink) (tool.Result, error) {
	driver.calls.Add(1)
	return tool.Result{}, driver.err
}

func TestRuntime_DistinguishesKnownAndUnknownToolFailure(t *testing.T) {
	for _, test := range []struct {
		name          string
		err           error
		wantReconcile bool
	}{
		{name: "not executed", err: errors.Join(tool.ErrNotExecuted, errors.New("rejected before dispatch"))},
		{name: "uncertain", err: errors.New("connection lost after dispatch"), wantReconcile: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := testbackend.New()
			runtime := newTestRuntime(t, store, Options{OwnerID: "tool-failure"})
			driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){providerEvents(
				provider.Event{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "action", Arguments: json.RawMessage(`{}`)}},
				provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
			)}}
			action := &failToolDriver{definition: message.ToolDefinition{Name: "action", InputSchema: message.JSONSchema{Type: "object"}}, err: test.err}
			result, err := runtime.Start(context.Background(), "tool-failure-"+ExecutionID(test.name), testEngine(driver, action), testRequest("act"), agent.OutputPolicy{})
			if test.wantReconcile {
				if !errors.Is(err, ErrReconcileRequired) {
					t.Fatalf("Start() error = %v, want reconciliation", err)
				}
			} else {
				if err != nil || result.Failure == nil {
					t.Fatalf("known tool failure result = %#v, error = %v", result, err)
				}
				replayed := newTestRuntime(t, store.Reopen(), Options{OwnerID: "tool-replay"})
				if _, err := replayed.Resume(context.Background(), "tool-failure-"+ExecutionID(test.name), testEngine(driver, action)); err != nil {
					t.Fatalf("terminal Resume() error = %v", err)
				}
			}
			if action.calls.Load() != 1 {
				t.Fatalf("tool calls = %d, want 1", action.calls.Load())
			}
		})
	}
}

type heartbeatBackend struct {
	Backend
	renews         atomic.Int32
	versionChanged atomic.Bool
}

func (backend *heartbeatBackend) RenewExecution(ctx context.Context, request RenewExecutionRequest) (Lease, error) {
	before, beforeErr := backend.Backend.LoadExecution(ctx, request.ExecutionID)
	lease, err := backend.Backend.RenewExecution(ctx, request)
	after, afterErr := backend.Backend.LoadExecution(ctx, request.ExecutionID)
	if beforeErr == nil && afterErr == nil && before.Version != after.Version {
		backend.versionChanged.Store(true)
	}
	if err == nil {
		backend.renews.Add(1)
	}
	return lease, err
}

type delayedStream struct {
	delay    time.Duration
	done     bool
	textSent bool
}

func (stream *delayedStream) Recv() (provider.Event, error) {
	if !stream.textSent {
		stream.textSent = true
		return provider.Event{Kind: provider.EventTextDelta, Text: "done"}, nil
	}
	if stream.done {
		return provider.Event{}, io.EOF
	}
	time.Sleep(stream.delay)
	stream.done = true
	return provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonComplete}, nil
}

func (*delayedStream) Close() error { return nil }

func TestRuntime_HeartbeatDoesNotAdvanceExecutionVersion(t *testing.T) {
	store := testbackend.New()
	backend := &heartbeatBackend{Backend: store}
	runtime := newTestRuntime(t, backend, Options{OwnerID: "heartbeat", LeaseTTL: 30 * time.Millisecond})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){func(context.Context, provider.Request) (provider.Stream, error) {
		return &delayedStream{delay: 45 * time.Millisecond}, nil
	}}}
	if _, err := runtime.Start(context.Background(), "heartbeat", testEngine(driver), testRequest("wait"), agent.OutputPolicy{}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if backend.renews.Load() == 0 {
		t.Fatal("heartbeat never renewed the lease")
	}
	if backend.versionChanged.Load() {
		t.Fatal("heartbeat advanced execution version")
	}
}

type ambiguousStartBackend struct {
	Backend
	first atomic.Bool
	mu    sync.Mutex
	ids   []ClaimID
}

func (backend *ambiguousStartBackend) StartExecution(ctx context.Context, request StartExecutionRequest) (StartResult, error) {
	backend.mu.Lock()
	backend.ids = append(backend.ids, request.ClaimID)
	backend.mu.Unlock()
	result, err := backend.Backend.StartExecution(ctx, request)
	if err != nil {
		return StartResult{}, err
	}
	if !backend.first.Swap(true) {
		return StartResult{}, context.DeadlineExceeded
	}
	return result, nil
}

func TestRuntime_ReusesClaimIDAfterAmbiguousStart(t *testing.T) {
	store := testbackend.New()
	backend := &ambiguousStartBackend{Backend: store}
	runtime := newTestRuntime(t, backend, Options{OwnerID: "claim-retry"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){finalEvents("claimed")}}
	if _, err := runtime.Start(context.Background(), "ambiguous", testEngine(driver), testRequest("same"), agent.OutputPolicy{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Start() error = %v, want deadline", err)
	}
	result, err := runtime.Start(context.Background(), "ambiguous", testEngine(driver), testRequest("same"), agent.OutputPolicy{})
	if err != nil {
		t.Fatalf("retried Start() error = %v", err)
	}
	backend.mu.Lock()
	ids := append([]ClaimID(nil), backend.ids...)
	backend.mu.Unlock()
	if result.Text != "claimed" || len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("result = %#v, claim IDs = %#v", result, ids)
	}
}

type corruptResumeBackend struct{ Backend }

func (backend corruptResumeBackend) ResumeExecution(ctx context.Context, request ResumeExecutionRequest) (ResumeResult, error) {
	result, err := backend.Backend.ResumeExecution(ctx, request)
	if err == nil && result.Execution.Checkpoint != nil {
		result.Execution.Checkpoint.ContinuationHash[0] ^= 0xff
	}
	return result, err
}

func TestRuntime_RejectsCorruptCheckpointAndReleasesClaim(t *testing.T) {
	store := testbackend.New()
	fault := &failSaveBackend{Backend: store, phase: agent.ContinuationReady, afterCommit: true}
	first := newTestRuntime(t, fault, Options{OwnerID: "checkpoint-writer"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){finalEvents("valid")}}
	if _, err := first.Start(context.Background(), "corrupt", testEngine(driver), testRequest("persist"), agent.OutputPolicy{}); !errors.Is(err, errInjectedBackend) {
		t.Fatalf("Start() error = %v", err)
	}

	corrupt := newTestRuntime(t, corruptResumeBackend{Backend: store.Reopen()}, Options{OwnerID: "corrupt-reader"})
	_, err := corrupt.Resume(context.Background(), "corrupt", testEngine(driver))
	var executionFailure *ExecutionError
	if !errors.Is(err, ErrCorruptCheckpoint) || !errors.As(err, &executionFailure) || executionFailure.ExecutionID != "corrupt" {
		t.Fatalf("corrupt Resume() error = %v, want execution-scoped ErrCorruptCheckpoint", err)
	}
	if driver.callCount() != 0 {
		t.Fatalf("provider calls after corrupt checkpoint = %d, want 0", driver.callCount())
	}

	valid := newTestRuntime(t, store.Reopen(), Options{OwnerID: "valid-reader"})
	result, err := valid.Resume(context.Background(), "corrupt", testEngine(driver))
	if err != nil {
		t.Fatalf("valid Resume() after corrupt claim error = %v", err)
	}
	if result.Text != "valid" {
		t.Fatalf("result = %#v", result)
	}
}

type corruptLoadBackend struct {
	Backend
	reconcileCalls atomic.Int64
}

func (backend *corruptLoadBackend) LoadExecution(ctx context.Context, executionID ExecutionID) (Execution, error) {
	execution, err := backend.Backend.LoadExecution(ctx, executionID)
	if err == nil && execution.Checkpoint != nil {
		execution.Checkpoint.ContinuationHash[0] ^= 0xff
	}
	return execution, err
}

func (backend *corruptLoadBackend) ReconcileAttempt(ctx context.Context, request ReconcileAttemptRequest) (Attempt, error) {
	backend.reconcileCalls.Add(1)
	return backend.Backend.ReconcileAttempt(ctx, request)
}

func TestRuntime_ReconcileRejectsCorruptCheckpointBeforeMutation(t *testing.T) {
	store := testbackend.New()
	creator := newTestRuntime(t, store, Options{OwnerID: "reconcile-writer"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		func(context.Context, provider.Request) (provider.Stream, error) {
			return &partialFailureStream{events: []provider.Event{{Kind: provider.EventTextDelta, Text: "partial"}}, err: errors.New("outcome unknown")}, nil
		},
	}}
	_, err := creator.Start(context.Background(), "corrupt-reconcile", testEngine(driver), testRequest("work"), agent.OutputPolicy{})
	var required *ReconcileRequiredError
	if !errors.As(err, &required) || len(required.Attempts) != 1 {
		t.Fatalf("Start() error = %v, want one ReconcileRequiredError", err)
	}
	attempt := required.Attempts[0]
	before, err := store.LoadExecution(context.Background(), "corrupt-reconcile")
	if err != nil {
		t.Fatalf("LoadExecution(before) error = %v", err)
	}

	backend := &corruptLoadBackend{Backend: store.Reopen()}
	reconciler := newTestRuntime(t, backend, Options{OwnerID: "corrupt-reconciler"})
	err = reconciler.Reconcile(context.Background(), "corrupt-reconcile", attempt.OperationID, Reconciliation{
		AttemptNumber:  attempt.Number,
		AttemptVersion: attempt.Version,
		Resolution:     ReconcileResolutionSucceed,
		ModelEvents: []provider.Event{
			{Kind: provider.EventTextDelta, Text: "reconciled"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		},
	})
	var executionFailure *ExecutionError
	if !errors.Is(err, ErrCorruptCheckpoint) || !errors.As(err, &executionFailure) || executionFailure.ExecutionID != "corrupt-reconcile" {
		t.Fatalf("Reconcile() error = %v, want execution-scoped ErrCorruptCheckpoint", err)
	}
	if backend.reconcileCalls.Load() != 0 {
		t.Fatalf("ReconcileAttempt calls = %d, want 0", backend.reconcileCalls.Load())
	}
	after, err := store.LoadExecution(context.Background(), "corrupt-reconcile")
	if err != nil {
		t.Fatalf("LoadExecution(after) error = %v", err)
	}
	if after.Version != before.Version || after.Checkpoint.Sequence != before.Checkpoint.Sequence || after.Checkpoint.ContinuationHash != before.Checkpoint.ContinuationHash {
		t.Fatalf("corrupt reconciliation advanced execution: before=%#v after=%#v", before, after)
	}

	clean := newTestRuntime(t, store.Reopen(), Options{OwnerID: "clean-reconciler"})
	if err := clean.Reconcile(context.Background(), "corrupt-reconcile", attempt.OperationID, Reconciliation{
		AttemptNumber:  attempt.Number,
		AttemptVersion: attempt.Version,
		Resolution:     ReconcileResolutionSucceed,
		ModelEvents: []provider.Event{
			{Kind: provider.EventTextDelta, Text: "reconciled"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		},
	}); err != nil {
		t.Fatalf("clean Reconcile() after rejection error = %v", err)
	}
}
