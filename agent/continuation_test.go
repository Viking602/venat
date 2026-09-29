package agent

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

func continuationTestEngine(t *testing.T, observer BoundaryObserver) Engine {
	t.Helper()
	driver, err := kit.Tool("lookup", func(_ context.Context, input struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "result:" + input.Query, nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}
	return Engine{
		Model:      "test-model",
		Provider:   fakeProvider{},
		Tools:      tool.NewBus(driver),
		ToolMode:   tool.ModeSequential,
		LoopPolicy: LoopPolicy{MaxIterations: 3},
		Boundaries: observer,
	}
}

func TestEngineResumeMatchesUninterruptedResultAtEveryBoundary(t *testing.T) {
	var continuations []Continuation
	observer := BoundaryObserverFunc(func(_ context.Context, continuation Continuation) error {
		continuations = append(continuations, cloneContinuation(continuation))
		return nil
	})
	request := Request{Prompt: "find venat"}
	baseline := continuationTestEngine(t, observer).Run(context.Background(), request, OutputPolicy{})
	if baseline.Failure != nil {
		t.Fatalf("Run() failure = %v", baseline.Failure)
	}

	wantPhases := []ContinuationPhase{
		ContinuationReady,
		ContinuationModelComplete,
		ContinuationToolsComplete,
		ContinuationReady,
		ContinuationValidatingOutput,
	}
	if len(continuations) != len(wantPhases) {
		t.Fatalf("boundary count = %d, want %d (%v)", len(continuations), len(wantPhases), phasesOf(continuations))
	}
	for index, continuation := range continuations {
		if continuation.Phase != wantPhases[index] {
			t.Fatalf("boundary %d phase = %q, want %q", index, continuation.Phase, wantPhases[index])
		}
		encoded, err := EncodeContinuation(continuation)
		if err != nil {
			t.Fatalf("EncodeContinuation(boundary %d) error = %v", index, err)
		}
		decoded, err := DecodeContinuation(encoded)
		if err != nil {
			t.Fatalf("DecodeContinuation(boundary %d) error = %v", index, err)
		}
		resumed := continuationTestEngine(t, nil).Resume(context.Background(), decoded)
		if !reflect.DeepEqual(resumed, baseline) {
			t.Fatalf("Resume(boundary %d, %s) = %#v, want %#v", index, continuation.Phase, resumed, baseline)
		}
	}
}

func TestEngineResumeTerminalToolPreservesCompleteStopReason(t *testing.T) {
	var checkpoint Continuation
	driver := &scriptedProvider{turns: [][]provider.Event{{
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-1",
				Name:      "submit_report",
				Arguments: []byte(`{"answer":"done"}`),
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}}}
	engine := Engine{
		Model:    "test-model",
		Provider: driver,
		Tools:    tool.NewBus(terminalTool{}),
		Boundaries: BoundaryObserverFunc(func(_ context.Context, continuation Continuation) error {
			if continuation.Phase == ContinuationToolsComplete {
				checkpoint = cloneContinuation(continuation)
			}
			return nil
		}),
	}
	baseline := engine.Run(context.Background(), Request{Prompt: "finish"}, OutputPolicy{})
	if baseline.Failure != nil || baseline.StopReason != provider.StopReasonComplete {
		t.Fatalf("Run() = %#v, want complete terminal result", baseline)
	}
	if checkpoint.Phase != ContinuationToolsComplete {
		t.Fatalf("checkpoint phase = %q, want tools_complete", checkpoint.Phase)
	}

	resumed := (Engine{}).Resume(context.Background(), checkpoint)
	if resumed.Failure != nil || resumed.StopReason != provider.StopReasonComplete {
		t.Fatalf("Resume() = %#v, want complete terminal result", resumed)
	}
}

func TestEngineResumeEarlyFailurePreservesProviderStopReason(t *testing.T) {
	stopAtBoundary := errors.New("stop at validating-output boundary")
	var checkpoint Continuation
	driver := &scriptedProvider{turns: [][]provider.Event{{
		{Kind: provider.EventTextDelta, Text: "partial"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonLength},
	}}}
	engine := Engine{
		Model:    "test-model",
		Provider: driver,
		Boundaries: BoundaryObserverFunc(func(_ context.Context, continuation Continuation) error {
			if continuation.Phase != ContinuationValidatingOutput {
				return nil
			}
			checkpoint = cloneContinuation(continuation)
			return stopAtBoundary
		}),
	}
	interrupted := engine.Run(context.Background(), Request{Prompt: "answer"}, OutputPolicy{})
	if interrupted.Failure == nil || !errors.Is(interrupted.Failure, stopAtBoundary) {
		t.Fatalf("Run() failure = %#v, want boundary failure", interrupted.Failure)
	}
	if checkpoint.Phase != ContinuationValidatingOutput {
		t.Fatalf("checkpoint phase = %q, want validating_output", checkpoint.Phase)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resumed := (Engine{}).Resume(ctx, checkpoint)
	if resumed.Failure == nil || !errors.Is(resumed.Failure, context.Canceled) {
		t.Fatalf("Resume() failure = %#v, want context cancellation", resumed.Failure)
	}
	if resumed.StopReason != provider.StopReasonLength {
		t.Fatalf("Resume() stop reason = %q, want length", resumed.StopReason)
	}
}

func TestEngineEffectOperationIDsAreStableAcrossHooksAndInterceptors(t *testing.T) {
	var modelIDs, toolIDs []string
	engine := continuationTestEngine(t, nil)
	engine.Hooks = NewHookChain(operationIDMutatingHook{})
	engine.ModelInterceptor = provider.StreamInterceptorFunc(func(ctx context.Context, next provider.Driver, request provider.Request) (provider.Stream, error) {
		modelIDs = append(modelIDs, request.OperationID)
		return next.Stream(ctx, request)
	})
	engine.ToolInterceptor = tool.InterceptorFunc(func(ctx context.Context, next tool.Driver, call tool.Call, sink tool.UpdateSink) (tool.Result, error) {
		toolIDs = append(toolIDs, call.OperationID)
		return next.Execute(ctx, call, sink)
	})

	result := engine.Run(context.Background(), Request{Prompt: "find venat"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatalf("Run() failure = %v", result.Failure)
	}
	if want := []string{"turn:0:model", "turn:1:model"}; !reflect.DeepEqual(modelIDs, want) {
		t.Fatalf("model operation IDs = %v, want %v", modelIDs, want)
	}
	if want := []string{"turn:0:call:0"}; !reflect.DeepEqual(toolIDs, want) {
		t.Fatalf("tool operation IDs = %v, want %v", toolIDs, want)
	}
	var transcriptOperationID string
	for _, current := range result.Messages {
		if len(current.ToolCalls) > 0 {
			transcriptOperationID = current.ToolCalls[0].OperationID
			break
		}
	}
	if transcriptOperationID != "turn:0:call:0" {
		t.Fatalf("transcript tool operation ID = %q, want turn:0:call:0", transcriptOperationID)
	}
}
func TestValidateContinuationAccountsContextUsageSnapshots(t *testing.T) {
	assistant := message.NewText(message.RoleAssistant, "done")
	continuation := Continuation{
		SchemaVersion: ContinuationSchemaVersion,
		Request:       Request{Prompt: "hi"},
		Messages: []message.Message{
			message.NewText(message.RoleUser, "hi"),
			assistant,
		},
		Usage: provider.Usage{InputTokens: 2, OutputTokens: 1, TotalTokens: 3},
		Steps: []Step{{
			Index: 0,
			ModelCall: &ModelCall{
				Model:        "test-model",
				InputTokens:  2,
				OutputTokens: 1,
				TotalTokens:  3,
				StopReason:   provider.StopReasonComplete,
			},
			Decision:   StepDecisionFinish,
			BudgetUsed: BudgetUsage{Tokens: 3},
		}},
		Phase:             ContinuationValidatingOutput,
		NextOperationTurn: 1,
	}
	if err := ValidateContinuation(continuation); err != nil {
		t.Fatalf("ValidateContinuation() without context usage error = %v", err)
	}

	contextUsage := provider.Usage{InputTokens: 4, OutputTokens: 1, TotalTokens: 5}
	continuation.ContextUsage = contextUsage
	continuation.Usage = provider.Usage{InputTokens: 6, OutputTokens: 2, TotalTokens: 8}
	continuation.Steps[0].ContextUsage = contextUsage
	continuation.Steps[0].BudgetUsed.Tokens = 8
	if err := ValidateContinuation(continuation); err != nil {
		t.Fatalf("ValidateContinuation() with context usage error = %v", err)
	}

	continuation.Usage.TotalTokens = 7
	if err := ValidateContinuation(continuation); !errors.Is(err, ErrInvalidContinuation) {
		t.Fatalf("ValidateContinuation() error = %v, want ErrInvalidContinuation", err)
	}
}

func TestEngineResumeRejectsCorruptContinuationWithoutEffects(t *testing.T) {
	continuation := Continuation{
		SchemaVersion:     ContinuationSchemaVersion,
		Request:           Request{Prompt: "hi"},
		Messages:          []message.Message{message.NewText(message.RoleUser, "hi")},
		NextOperationTurn: 0,
		Phase:             ContinuationPhase("corrupt"),
	}
	providerCalls := 0
	engine := Engine{
		Model: "test-model",
		Provider: providerDriverFunc(func(context.Context, provider.Request) (provider.Stream, error) {
			providerCalls++
			return provider.NewSliceStream(nil), nil
		}),
	}
	result := engine.Resume(context.Background(), continuation)

	if result.Failure == nil || result.Failure.Kind != FailureKindEngineError || !errors.Is(result.Failure, ErrInvalidContinuation) {
		t.Fatalf("Resume() failure = %#v, want invalid-continuation engine failure", result.Failure)
	}
	if providerCalls != 0 {
		t.Fatalf("provider calls = %d, want 0", providerCalls)
	}
}

func TestEngineResumeExcludesCheckpointDowntimeFromWallClockBudget(t *testing.T) {
	stopAtReady := errors.New("stop at ready boundary")
	var continuation Continuation
	engine := continuationTestEngine(t, BoundaryObserverFunc(func(_ context.Context, current Continuation) error {
		if current.Phase == ContinuationReady {
			continuation = cloneContinuation(current)
			return stopAtReady
		}
		return nil
	}))
	request := Request{
		Prompt: "find venat",
		Budget: &Budget{MaxWallClock: 100 * time.Millisecond},
	}
	stopped := engine.Run(context.Background(), request, OutputPolicy{})
	if stopped.Failure == nil || !errors.Is(stopped.Failure, stopAtReady) {
		t.Fatalf("Run() failure = %#v, want ready-boundary stop", stopped.Failure)
	}

	time.Sleep(150 * time.Millisecond)
	resumed := continuationTestEngine(t, nil).Resume(context.Background(), continuation)
	if resumed.Failure != nil {
		t.Fatalf("Resume() failure = %v; checkpoint downtime consumed wall-clock budget", resumed.Failure)
	}
}

func TestJoinBoundaryObserversClonesOrdersAndStopsOnError(t *testing.T) {
	stop := errors.New("stop")
	var order []int
	continuation := Continuation{
		Messages: []message.Message{{
			Role:     message.RoleUser,
			Metadata: map[string]string{"owner": "original"},
		}},
	}
	joined := JoinBoundaryObservers(
		BoundaryObserverFunc(func(_ context.Context, current Continuation) error {
			order = append(order, 1)
			current.Messages[0].Metadata["owner"] = "changed"
			return nil
		}),
		BoundaryObserverFunc(func(_ context.Context, current Continuation) error {
			order = append(order, 2)
			if current.Messages[0].Metadata["owner"] != "original" {
				t.Fatalf("second observer saw aliased continuation: %#v", current.Messages[0].Metadata)
			}
			return stop
		}),
		BoundaryObserverFunc(func(context.Context, Continuation) error {
			order = append(order, 3)
			return nil
		}),
	)
	if err := joined.ObserveBoundary(context.Background(), continuation); !errors.Is(err, stop) {
		t.Fatalf("ObserveBoundary() error = %v, want stop", err)
	}
	if want := []int{1, 2}; !reflect.DeepEqual(order, want) {
		t.Fatalf("observer order = %v, want %v", order, want)
	}
	if continuation.Messages[0].Metadata["owner"] != "original" {
		t.Fatalf("source continuation was mutated: %#v", continuation.Messages[0].Metadata)
	}
}

func phasesOf(continuations []Continuation) []ContinuationPhase {
	phases := make([]ContinuationPhase, len(continuations))
	for index := range continuations {
		phases[index] = continuations[index].Phase
	}
	return phases
}

type operationIDMutatingHook struct{}

func (operationIDMutatingHook) TransformContext(_ context.Context, messages []message.Message) ([]message.Message, error) {
	return messages, nil
}

func (operationIDMutatingHook) BeforeModelCall(_ context.Context, request *provider.Request) error {
	request.OperationID = "changed:model"
	return nil
}

func (operationIDMutatingHook) BeforeToolCall(_ context.Context, call *tool.Call) error {
	call.OperationID = "changed:tool"
	return nil
}
func (operationIDMutatingHook) AfterToolCall(context.Context, *tool.Result) error { return nil }
func (operationIDMutatingHook) OnEvent(context.Context, provider.Event) error     { return nil }

type providerDriverFunc func(context.Context, provider.Request) (provider.Stream, error)

func (providerDriverFunc) Metadata() provider.Metadata { return provider.Metadata{Name: "test"} }
func (driver providerDriverFunc) Stream(ctx context.Context, request provider.Request) (provider.Stream, error) {
	return driver(ctx, request)
}
