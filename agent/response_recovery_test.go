package agent

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
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

func TestResponseRecovery_PlainTextToolCallRecoversWithoutFalsePositives(t *testing.T) {
	lookup, err := kit.Tool("lookup", func(_ context.Context, input struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "found:" + input.Query, nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}
	for _, tc := range []struct {
		name string
		text string
	}{
		{name: "xml tool_call tag", text: "<tool_call>\n{\"name\":\"lookup\",\"arguments\":{\"query\":\"venat\"}}\n</tool_call>"},
		{name: "function tag", text: "<function=lookup>{\"query\":\"venat\"}</function>"},
		{name: "fenced json envelope for registered tool", text: "I will call lookup:\n```json\n{\"name\":\"lookup\",\"arguments\":{\"query\":\"venat\"}}\n```"},
		{name: "inline fence before json tool fence", text: "See ```inline``` then:\n```json\n{\"type\":\"function\",\"function\":{\"name\":\"lookup\"}}\n```"},
		{name: "unfenced json envelope after prose", text: "Don't wait, calling lookup:\n{\"name\":\"lookup\",\"arguments\":{\"query\":\"venat\"}}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model := &scriptedProvider{turns: [][]provider.Event{
				{{Kind: provider.EventTextDelta, Text: tc.text}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}},
				{{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"query":"venat"}`)}}, {Kind: provider.EventDone, StopReason: provider.StopReasonToolUse}},
				{{Kind: provider.EventTextDelta, Text: "lookup complete"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}},
			}}
			engine := Engine{
				Provider:   model,
				Tools:      tool.NewBus(lookup),
				Boundaries: BoundaryObserverFunc(func(_ context.Context, c Continuation) error { return ValidateContinuation(c) }),
			}
			result := engine.Run(context.Background(), Request{Prompt: "search venat"}, OutputPolicy{})
			if result.Failure != nil || result.Text != "lookup complete" || result.ToolCallsUsed != 1 || len(model.requests) != 3 {
				t.Fatalf("result=%+v requests=%d", result, len(model.requests))
			}
			correction := model.requests[1].Messages[len(model.requests[1].Messages)-1]
			if correction.Role != message.RoleUser || !strings.Contains(correction.Text, "plain text") {
				t.Fatalf("correction = %+v, want plain-text tool call instruction", correction)
			}
		})
	}

	deepWrapper := strings.Repeat(`{"tool_calls":`, 128) + `{"name":"lookup","arguments":"` + strings.Repeat("x", 1024) + `"}` + strings.Repeat(`}`, 128)
	for _, safeText := range []string{
		`{"name":"lookup","status":"ok"}`,
		`{"name":"unregistered","arguments":{"query":"venat"}}`,
		"Use the lookup tool to search.",
		"The Llama tool-call prefix is <|python_tag|>; Mistral uses [TOOL_CALLS] and XML uses <function_calls> or <tool_call>.",
		"The <|python_tag|> prefix is used before calling lookup (for example, in Llama models).",
		deepWrapper,
	} {
		model := singleTurnProvider(safeText)
		engine := Engine{Provider: model, Tools: tool.NewBus(lookup)}
		result := engine.Run(context.Background(), Request{Prompt: "explain"}, OutputPolicy{})
		if result.Failure != nil || result.Text != safeText || len(model.requests) != 1 {
			t.Fatalf("safeText=%q falsely triggered recovery: result=%+v requests=%d", safeText, result, len(model.requests))
		}
	}

	replacedModel := singleTurnProvider("<tool_call>{\"name\":\"lookup\",\"arguments\":{\"query\":\"venat\"}}</tool_call>")
	replacedEngine := Engine{
		Provider: replacedModel,
		Tools:    tool.NewBus(lookup),
		OutputGuardrails: []OutputGuardrail{
			NewOutputGuardrail("sanitize", func(_ context.Context, in OutputGuardrailInput) (OutputGuardrailResult, error) {
				cleaned := in.Output
				cleaned.Content = []message.ContentPart{message.TextPart("sanitized answer")}
				cleaned.SyncLegacyContent()
				return ReplaceOutput(cleaned), nil
			}),
		},
	}
	if got := replacedEngine.Run(context.Background(), Request{Prompt: "search"}, OutputPolicy{}); got.Failure != nil || got.Text != "sanitized answer" || len(replacedModel.requests) != 1 {
		t.Fatalf("guardrail-replaced output triggered recovery: result=%+v requests=%d", got, len(replacedModel.requests))
	}

	schema := json.RawMessage(`{"type":"object","properties":{"name":{"type":"string","enum":["lookup"]},"arguments":{"type":"object"}},"required":["name","arguments"]}`)
	schemaModel := singleTurnProvider(`{"name":"lookup","arguments":{}}`)
	schemaEngine := Engine{Provider: schemaModel, Tools: tool.NewBus(lookup)}
	if got := schemaEngine.Run(context.Background(), Request{Prompt: "schema"}, OutputPolicy{Schema: schema, Validate: true}); got.Failure != nil || !got.Valid || len(schemaModel.requests) != 1 {
		t.Fatalf("schema-valid output was misclassified as text tool call: result=%+v requests=%d", got, len(schemaModel.requests))
	}
	formatModel := singleTurnProvider(`{"name":"lookup","arguments":{}}`)
	formatOut, err := (Engine{Provider: formatModel, Tools: tool.NewBus(lookup)}).RunMessages(context.Background(), LoopInput{
		Model:          "test-model",
		Messages:       []message.Message{message.NewText(message.RoleUser, "format")},
		ResponseFormat: &provider.ResponseFormat{Type: "json_schema", RawSchema: schema},
	})
	if err != nil || len(formatModel.requests) != 1 || formatOut.Iterations != 1 {
		t.Fatalf("ResponseFormat output was misclassified: out=%+v err=%v requests=%d", formatOut, err, len(formatModel.requests))
	}
}

func TestResume_ModelCompleteAttachesOutputStoreTools(t *testing.T) {
	store, err := tool.NewOutputStore(filepath.Join(t.TempDir(), "artifacts"), tool.WithPreviewThreshold(8))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	processed, err := store.Process(context.Background(), message.ToolResult{Content: strings.Repeat("venat-line\n", 16)})
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(processed.Content, "artifact://")
	end := strings.Index(processed.Content[start:], "]")
	ref := processed.Content[start : start+end]

	model := &scriptedProvider{turns: [][]provider.Event{
		{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "read-1", Name: "tool_output_read", Arguments: json.RawMessage(`{"reference":"` + ref + `","offset":0,"limit":32}`)}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse, Usage: usagePerTurn(5)},
		},
		{
			{Kind: provider.EventTextDelta, Text: "recovered artifact"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: usagePerTurn(5)},
		},
	}}
	var paused *Continuation
	engine := Engine{
		Provider:    model,
		OutputStore: store,
		Boundaries: BoundaryObserverFunc(func(_ context.Context, c Continuation) error {
			if paused == nil && c.Phase == ContinuationModelComplete {
				saved := c
				paused = &saved
				return errors.New("pause at model_complete")
			}
			return ValidateContinuation(c)
		}),
	}
	if first := engine.Run(context.Background(), Request{Prompt: "read artifact"}, OutputPolicy{}); first.Failure == nil || paused == nil {
		t.Fatalf("expected pause at model_complete, got failure=%v continuation=%v", first.Failure, paused)
	}
	resumed := engine.Resume(context.Background(), *paused)
	if resumed.Failure != nil || resumed.Text != "recovered artifact" || len(resumed.Steps) != 2 || resumed.Steps[0].ToolCalls[0].Error != "" {
		t.Fatalf("Resume result=%+v, want clean tool_output_read execution", resumed)
	}
}
