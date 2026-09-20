package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Viking602/venat/agent"
	. "github.com/Viking602/venat/durable"
	"github.com/Viking602/venat/durable/internal/testbackend"
	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

func toolTurn(name string) func(context.Context, provider.Request) (provider.Stream, error) {
	return providerEvents(
		provider.Event{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-1",
				Name:      name,
				Arguments: json.RawMessage(`{}`),
			},
		},
		provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	)
}

func TestRuntime_ReplaysSettledToolRejectionThenContinuesCorrection(t *testing.T) {
	store := testbackend.New()
	fault := &failSaveBackend{Backend: store, phase: agent.ContinuationToolsComplete}
	first := newTestRuntime(t, fault, Options{OwnerID: "first"})
	var calls atomic.Int32
	action, err := kit.Tool("submit", func(context.Context, struct{}) (tool.Result, error) {
		if calls.Add(1) == 1 {
			return tool.Result{Content: "rejected: revise the result", IsError: true}, nil
		}
		return tool.Result{Content: "accepted"}, nil
	}, kit.Terminal())
	if err != nil {
		t.Fatal(err)
	}
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){toolTurn("submit")}}
	_, err = first.Start(context.Background(), "rejection", testEngine(driver, action), testRequest("submit"), agent.OutputPolicy{})
	if !errors.Is(err, errInjectedBackend) || calls.Load() != 1 {
		t.Fatalf("start error=%v calls=%d", err, calls.Load())
	}
	resumedModel := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		func(ctx context.Context, request provider.Request) (provider.Stream, error) {
			last := request.Messages[len(request.Messages)-1].ToolResult
			if last == nil || !last.IsError || !strings.Contains(last.Content, "revise") {
				return nil, errors.New("missing replayed rejection")
			}
			return toolTurn("submit")(ctx, request)
		},
	}}
	second := newTestRuntime(t, store.Reopen(), Options{OwnerID: "second"})
	result, err := second.Resume(context.Background(), "rejection", testEngine(resumedModel, action))
	if err != nil || result.Failure != nil || calls.Load() != 2 || resumedModel.callCount() != 1 || result.Steps[0].Decision != agent.StepDecisionContinue || result.Steps[1].Decision != agent.StepDecisionFinish {
		t.Fatalf("resume=%+v error=%v calls=%d", result, err, calls.Load())
	}
	execution, err := store.LoadExecution(context.Background(), "rejection")
	if err != nil || execution.Status != ExecutionStatusCompleted {
		t.Fatalf("execution=%+v error=%v", execution, err)
	}
}

func TestRuntime_ToolFailureAfterUpdateBecomesUnknown(t *testing.T) {
	store := testbackend.New()
	runtime := newTestRuntime(t, store, Options{OwnerID: "tool-update-unknown"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){toolTurn("action")}}
	effectErr := errors.New("effect outcome unavailable")
	var calls atomic.Int32
	action, err := kit.Tool("action", func(_ context.Context, _ struct{}, sink tool.UpdateSink) (string, error) {
		calls.Add(1)
		if updateErr := sink(tool.Update{
			Kind:  tool.UpdateOutput,
			Parts: []message.ContentPart{message.TextPart("partial")},
		}); updateErr != nil {
			return "", updateErr
		}
		return "", effectErr
	})
	if err != nil {
		t.Fatalf("kit.Tool() error = %v", err)
	}

	var frames []agent.Frame
	_, err = runtime.StartStream(
		context.Background(),
		"tool-update-unknown",
		testEngine(driver, action),
		testRequest("act"),
		agent.OutputPolicy{},
		agent.SinkFunc(func(_ context.Context, frame agent.Frame) error {
			frames = append(frames, frame)
			return nil
		}),
	)
	var required *ReconcileRequiredError
	if !errors.As(err, &required) || !errors.Is(err, effectErr) || errors.Is(err, tool.ErrNotExecuted) {
		t.Fatalf("StartStream() error = %v, want effect error and reconciliation without ErrNotExecuted", err)
	}
	if len(required.Attempts) != 1 || required.Attempts[0].Kind != AttemptKindTool || required.Attempts[0].Status != AttemptStatusUnknown {
		t.Fatalf("reconciliation attempts = %#v", required.Attempts)
	}
	if calls.Load() != 1 {
		t.Fatalf("tool calls = %d, want 1", calls.Load())
	}
	var updates, results int
	for _, frame := range frames {
		switch frame.Kind {
		case agent.FrameToolUpdate:
			updates++
			if frame.ToolUpdate == nil || frame.ToolUpdate.OperationID != "turn:0:call:0" || frame.ToolUpdate.Sequence != 1 {
				t.Fatalf("tool update frame = %#v", frame)
			}
		case agent.FrameToolResult:
			results++
		}
	}
	if updates != 1 || results != 0 {
		t.Fatalf("frames = %#v, want one transient update and no final result", frames)
	}
}

type toolSettlementRecordingBackend struct {
	Backend
	recorder *orderRecorder
}

func (backend toolSettlementRecordingBackend) FinishAttempt(ctx context.Context, request FinishAttemptRequest) (Attempt, error) {
	attempt, err := backend.Backend.FinishAttempt(ctx, request)
	if err == nil && strings.Contains(request.OperationID, ":call:") {
		backend.recorder.add("tool-settled")
	}
	return attempt, err
}

func TestRuntime_SettlesToolBeforeFinalResultFrame(t *testing.T) {
	recorder := &orderRecorder{}
	store := testbackend.New()
	runtime := newTestRuntime(t, toolSettlementRecordingBackend{Backend: store, recorder: recorder}, Options{OwnerID: "tool-update-order"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		toolTurn("action"),
		finalEvents("complete"),
	}}
	action, err := kit.Tool("action", func(_ context.Context, _ struct{}, sink tool.UpdateSink) (string, error) {
		if err := sink(tool.Update{Kind: tool.UpdateProgress, Message: "working"}); err != nil {
			return "", err
		}
		if err := sink(tool.Update{Kind: tool.UpdateOutput, Parts: []message.ContentPart{message.TextPart("done")}}); err != nil {
			return "", err
		}
		return "done", nil
	})
	if err != nil {
		t.Fatalf("kit.Tool() error = %v", err)
	}

	result, err := runtime.StartStream(
		context.Background(),
		"tool-update-order",
		testEngine(driver, action),
		testRequest("act"),
		agent.OutputPolicy{},
		agent.SinkFunc(func(_ context.Context, frame agent.Frame) error {
			switch frame.Kind {
			case agent.FrameToolUpdate:
				recorder.add("tool-update")
			case agent.FrameToolResult:
				recorder.add("tool-result")
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("StartStream() error = %v", err)
	}
	if result.Text != "complete" {
		t.Fatalf("result = %#v", result)
	}
	values := recorder.snapshot()
	lastUpdate := lastIndex(values, "tool-update")
	settled := lastIndex(values, "tool-settled")
	finalResult := lastIndex(values, "tool-result")
	if lastUpdate < 0 || settled <= lastUpdate || finalResult <= settled {
		t.Fatalf("tool lifecycle order = %#v", values)
	}
}

func TestRuntime_ReplaysSettledToolWithoutHistoricalUpdatesAfterReopen(t *testing.T) {
	store := testbackend.New()
	fault := &failSaveBackend{Backend: store, phase: agent.ContinuationToolsComplete}
	first := newTestRuntime(t, fault, Options{OwnerID: "tool-update-first"})
	driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){
		toolTurn("action"),
		finalEvents("complete"),
	}}
	var calls atomic.Int32
	action, err := kit.Tool("action", func(_ context.Context, _ struct{}, sink tool.UpdateSink) (string, error) {
		calls.Add(1)
		if err := sink(tool.Update{Kind: tool.UpdateProgress, Message: "working"}); err != nil {
			return "", err
		}
		if err := sink(tool.Update{Kind: tool.UpdateOutput, Parts: []message.ContentPart{message.TextPart("done")}}); err != nil {
			return "", err
		}
		return "done", nil
	})
	if err != nil {
		t.Fatalf("kit.Tool() error = %v", err)
	}

	var firstFrames []agent.Frame
	_, err = first.StartStream(
		context.Background(),
		"tool-update-reopen",
		testEngine(driver, action),
		testRequest("act"),
		agent.OutputPolicy{},
		agent.SinkFunc(func(_ context.Context, frame agent.Frame) error {
			firstFrames = append(firstFrames, frame)
			return nil
		}),
	)
	if !errors.Is(err, errInjectedBackend) {
		t.Fatalf("StartStream() error = %v, want injected tools checkpoint failure", err)
	}
	if countFrameKind(firstFrames, agent.FrameToolUpdate) != 2 || countFrameKind(firstFrames, agent.FrameToolResult) != 1 || calls.Load() != 1 {
		t.Fatalf("first frames = %#v, tool calls = %d", firstFrames, calls.Load())
	}

	second := newTestRuntime(t, store.Reopen(), Options{OwnerID: "tool-update-second"})
	var resumedFrames []agent.Frame
	result, err := second.ResumeStream(
		context.Background(),
		"tool-update-reopen",
		testEngine(driver, action),
		agent.SinkFunc(func(_ context.Context, frame agent.Frame) error {
			resumedFrames = append(resumedFrames, frame)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("ResumeStream() error = %v", err)
	}
	if result.Text != "complete" || calls.Load() != 1 || driver.callCount() != 2 {
		t.Fatalf("result = %#v, tool calls = %d, provider calls = %d", result, calls.Load(), driver.callCount())
	}
	if countFrameKind(resumedFrames, agent.FrameToolUpdate) != 0 || countFrameKind(resumedFrames, agent.FrameToolResult) != 1 {
		t.Fatalf("resumed frames = %#v, want final result replay without historical updates", resumedFrames)
	}
}

func lastIndex(values []string, target string) int {
	for index := len(values) - 1; index >= 0; index-- {
		if values[index] == target {
			return index
		}
	}
	return -1
}

func countFrameKind(frames []agent.Frame, kind agent.FrameKind) int {
	count := 0
	for _, frame := range frames {
		if frame.Kind == kind {
			count++
		}
	}
	return count
}

func TestRuntime_ContextSelectionReplaysAfterReopenWithoutNetwork(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/systemone" || r.Header.Get("Authorization") != "Bearer caller-secret" {
			t.Error("wrong evaluation request")
		}
		_, _ = io.WriteString(w, `{"model":"jev-version","answers":{"log":{"type":"noul","noul":0.9}},"usage":{"input_tokens":12,"output_tokens":1}}`)
	}))
	defer server.Close()
	action, err := kit.ContextSelectionTool("select_context", kit.ContextSelectionConfig{Protocol: "typesafe-system-one", BaseURL: server.URL + "/v1", APIKey: "caller-secret", Model: "jev-version"})
	if err != nil {
		t.Fatal(err)
	}
	model := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){providerEvents(
		provider.Event{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "selection", Name: "select_context", Arguments: json.RawMessage(`{"task":"fix","candidates":[{"id":"log","text":"failure","protected":false}]}`)}},
		provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	)}}
	store := testbackend.New()
	fault := &failSaveBackend{Backend: store, phase: agent.ContinuationToolsComplete}
	first := newTestRuntime(t, fault, Options{OwnerID: "first"})
	_, err = first.Start(context.Background(), "selection", testEngine(model, action), testRequest("select context"), agent.OutputPolicy{})
	if !errors.Is(err, errInjectedBackend) || calls.Load() != 1 {
		t.Fatalf("start err=%v calls=%d", err, calls.Load())
	}
	server.Close() // resumed execution must rely solely on the settled tool result
	resumedModel := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){func(ctx context.Context, req provider.Request) (provider.Stream, error) {
		last := req.Messages[len(req.Messages)-1].ToolResult
		if last == nil || !strings.Contains(last.Content, `"retainProbability":0.9`) || !strings.Contains(last.Content, `"totalTokens":13`) {
			t.Fatalf("missing settled scores: %+v", last)
		}
		return finalEvents("retained")(ctx, req)
	}}}
	second := newTestRuntime(t, store.Reopen(), Options{OwnerID: "second"})
	result, err := second.Resume(context.Background(), "selection", testEngine(resumedModel, action))
	if err != nil || result.Failure != nil || result.Text != "retained" || calls.Load() != 1 {
		t.Fatalf("resume=%+v err=%v calls=%d", result, err, calls.Load())
	}
	execution, err := store.LoadExecution(context.Background(), "selection")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(execution)
	if err != nil || strings.Contains(string(encoded), "caller-secret") {
		t.Fatalf("checkpoint leaked credential or could not serialize: %v", err)
	}
}

func TestRuntime_MalformedFullCallReplaysAsCorrection(t *testing.T) {
	store := testbackend.New()
	fault := &failSaveBackend{Backend: store, phase: agent.ContinuationValidatingOutput}
	first := newTestRuntime(t, fault, Options{OwnerID: "first"})
	model := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){providerEvents(
		provider.Event{Kind: provider.EventToolCallDelta, ToolCallDelta: &provider.ToolCallDelta{ID: "bad", Name: "never", ArgumentsDelta: `{"x"`}},
		provider.Event{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "bad", Name: "never", Arguments: json.RawMessage(`:1}`)}},
		provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	)}}
	_, err := first.Start(context.Background(), "bad-json", testEngine(model), testRequest("fix"), agent.OutputPolicy{})
	if !errors.Is(err, errInjectedBackend) {
		t.Fatalf("start err=%v", err)
	}
	resumedModel := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){func(ctx context.Context, req provider.Request) (provider.Stream, error) {
		last := req.Messages[len(req.Messages)-1]
		if last.Role != message.RoleUser || !strings.Contains(last.Text, "valid JSON") {
			t.Fatalf("missing correction: %+v", last)
		}
		return finalEvents("corrected")(ctx, req)
	}}}
	second := newTestRuntime(t, store.Reopen(), Options{OwnerID: "second"})
	result, err := second.Resume(context.Background(), "bad-json", testEngine(resumedModel))
	if err != nil || result.Failure != nil || result.Text != "corrected" || result.ToolCallsUsed != 0 || resumedModel.callCount() != 1 {
		t.Fatalf("resume=%+v err=%v", result, err)
	}
}
