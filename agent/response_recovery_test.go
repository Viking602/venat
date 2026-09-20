package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool/kit"
)

func TestResponseRecovery_CompletedResponsesAndBoundaries(t *testing.T) {
	for _, test := range []struct {
		name   string
		events []provider.Event
	}{
		{"empty", []provider.Event{{Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}},
		{"length", []provider.Event{{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "incomplete", Name: "do-not-execute", Arguments: json.RawMessage(`{}`)}}, {Kind: provider.EventDone, StopReason: provider.StopReasonLength}}},
		{"invalid arguments", []provider.Event{{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "bad", Name: "do-not-execute", Arguments: json.RawMessage(`{"x":`)}}, {Kind: provider.EventDone, StopReason: provider.StopReasonToolUse}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			model := &scriptedProvider{turns: [][]provider.Event{test.events, {{Kind: provider.EventTextDelta, Text: "fixed"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}}}
			engine := Engine{Provider: model, Boundaries: BoundaryObserverFunc(func(_ context.Context, c Continuation) error { _, err := EncodeContinuation(c); return err })}
			result := engine.Run(context.Background(), Request{Prompt: "finish"}, OutputPolicy{})
			if result.Failure != nil || result.Text != "fixed" || result.ToolCallsUsed != 0 || len(model.requests) != 2 {
				t.Fatalf("result=%+v", result)
			}
			if last := model.requests[1].Messages[len(model.requests[1].Messages)-1]; last.Role != message.RoleUser || !strings.Contains(last.Text, "previous") {
				t.Fatalf("missing correction: %+v", last)
			}
		})
	}
}

func TestMarkIncompleteResponse_PreservesProviderStateAndMetadata(t *testing.T) {
	assistant := message.Message{
		ProviderState: json.RawMessage(`{"type":"reasoning","id":"rs_1"}`),
		Metadata:      map[string]string{"provider": "openai"},
	}

	markIncompleteResponse(&assistant, provider.StopReasonLength, nil)

	if string(assistant.ProviderState) != `{"type":"reasoning","id":"rs_1"}` {
		t.Fatalf("provider state = %s, want opaque state preserved", assistant.ProviderState)
	}
	if assistant.Metadata["provider"] != "openai" || assistant.Metadata[responseRecoveryKey] != "output_length" {
		t.Fatalf("metadata = %#v, want original and recovery entries", assistant.Metadata)
	}
}

func TestResponseRecovery_ReplaysOpaqueProviderState(t *testing.T) {
	state := json.RawMessage(`[{"type":"reasoning","id":"rs_1"}]`)
	model := &scriptedProvider{turns: [][]provider.Event{
		{{Kind: provider.EventThinkingDelta, Thinking: "partial"}, {Kind: provider.EventDone, StopReason: provider.StopReasonLength, ProviderState: state}},
		{{Kind: provider.EventTextDelta, Text: "fixed"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}},
	}}

	result := (Engine{Provider: model}).Run(context.Background(), Request{Prompt: "finish"}, OutputPolicy{})
	if result.Failure != nil || result.Text != "fixed" || len(model.requests) != 2 {
		t.Fatalf("result=%+v requests=%d", result, len(model.requests))
	}
	var recovered message.Message
	for _, current := range model.requests[1].Messages {
		if current.Role == message.RoleAssistant {
			recovered = current
			break
		}
	}
	if string(recovered.ProviderState) != string(state) {
		t.Fatalf("recovered provider state = %s, want %s", recovered.ProviderState, state)
	}
}

func TestResponseRecovery_CapAndResume(t *testing.T) {
	for _, maxTurns := range []int{1, 20} {
		calls := 0
		model := agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
			calls++
			return provider.NewSliceStream([]provider.Event{{Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
		})
		var saved Continuation
		engine := Engine{Provider: model, LoopPolicy: LoopPolicy{MaxIterations: maxTurns}, Boundaries: BoundaryObserverFunc(func(_ context.Context, c Continuation) error {
			if c.Phase == ContinuationReady {
				saved = c
			}
			return ValidateContinuation(c)
		})}
		result := engine.Run(context.Background(), Request{Prompt: "answer"}, OutputPolicy{})
		if result.Failure == nil || result.Failure.Kind != FailureKindRepairFailed || !errors.Is(result.Failure, errIncompleteResponse) || calls != min(maxTurns, 4) {
			t.Fatalf("calls=%d result=%+v", calls, result)
		}
		if maxTurns == 20 {
			before := calls
			resumed := engine.Resume(context.Background(), saved)
			if resumed.Failure == nil || calls-before != 1 {
				t.Fatalf("resume reset cap: calls=%d result=%+v", calls-before, resumed)
			}
		}
	}
}

func TestBuild_ContextSelectionRequiresBothConfigurationAndToolSelection(t *testing.T) {
	model := singleTurnProvider("done")
	spec := Spec{Model: "model", Tools: []string{"select_context"}}
	deps := BuildDeps{Providers: provider.Single(model)}
	if _, err := Build(spec, deps); err == nil {
		t.Fatal("unconfigured capability accepted")
	}
	deps.ContextSelection = &kit.ContextSelectionConfig{Protocol: "typesafe-system-one", BaseURL: "http://127.0.0.1:1/v1", APIKey: "caller-secret", Model: "jev-version"}
	engine, err := Build(spec, deps)
	if err != nil || len(engine.Tools.Definitions()) != 1 {
		t.Fatalf("build: %v", err)
	}
	spec.Tools = nil
	engine, err = Build(spec, deps)
	if err != nil || (engine.Tools != nil && len(engine.Tools.Definitions()) != 0) {
		t.Fatalf("unselected capability exposed: %v", err)
	}
	if len(model.requests) != 0 {
		t.Fatal("Build contacted model")
	}
}
