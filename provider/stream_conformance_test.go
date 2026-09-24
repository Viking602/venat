package provider_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/provider/anthropic"
	"github.com/Viking602/venat/provider/openai"
)

type streamConformanceAdapter struct {
	name          string
	supportsState bool
	open          func(context.Context, string, *http.Client) (provider.Stream, error)
}

type streamConformanceFixture struct {
	name              string
	chat              string
	anthropic         string
	responses         string
	wantKinds         []provider.EventKind
	wantText          string
	wantThinking      string
	wantTool          bool
	wantStop          provider.StopReason
	wantUsage         provider.Usage
	wantProviderState string
}

const (
	conformanceModel = "conformance-model"
	conformanceText  = "hello"
	conformanceThink = "reasoning"
	conformanceArgs  = `{"query":"venat"}`
)

func TestStreamAdaptersConformance(t *testing.T) {
	adapters := streamConformanceAdapters()
	fixtures := streamConformanceFixtures()

	for _, fixture := range fixtures {
		fixture := fixture
		for _, adapter := range adapters {
			adapter := adapter
			t.Run(fixture.name+"/"+adapter.name, func(t *testing.T) {
				stream, closeServer := openConformanceStream(t, adapter, fixture)
				defer closeServer()
				events, err := collectConformanceEvents(stream)
				if err != nil {
					t.Fatalf("stream returned error: %v", err)
				}
				assertEventSequence(t, events, fixture.wantKinds)
				normalized, err := provider.NormalizeEvents(events)
				if err != nil {
					t.Fatalf("NormalizeEvents() error = %v; events = %#v", err, events)
				}
				if fixture.wantText != "" && normalized.Text != fixture.wantText {
					t.Fatalf("normalized text = %q, want %q", normalized.Text, fixture.wantText)
				}
				if fixture.wantThinking != "" && normalized.Thinking != fixture.wantThinking {
					t.Fatalf("normalized thinking = %q, want %q", normalized.Thinking, fixture.wantThinking)
				}
				if fixture.wantTool {
					if len(normalized.ToolCalls) != 1 || normalized.ToolCalls[0].Name != "lookup" ||
						string(normalized.ToolCalls[0].Arguments) != conformanceArgs {
						t.Fatalf("normalized tool calls = %#v, want lookup(%s)", normalized.ToolCalls, conformanceArgs)
					}
				}
				if fixture.wantText != "" && normalizedText(events) != fixture.wantText {
					t.Fatalf("text = %q, want %q; events = %#v", normalizedText(events), fixture.wantText, events)
				}
				if fixture.wantThinking != "" && normalizedThinking(events) != fixture.wantThinking {
					t.Fatalf("thinking = %q, want %q; events = %#v", normalizedThinking(events), fixture.wantThinking, events)
				}
				terminal := assertOneTerminal(t, events, provider.EventDone)
				if terminal.StopReason != fixture.wantStop {
					t.Fatalf("stop reason = %q, want %q", terminal.StopReason, fixture.wantStop)
				}
				if terminal.Usage != fixture.wantUsage {
					t.Fatalf("usage = %#v, want %#v", terminal.Usage, fixture.wantUsage)
				}
				if fixture.wantTool {
					assertToolCallEvents(t, events)
				}
				if adapter.supportsState && fixture.wantProviderState != "" {
					if string(terminal.ProviderState) != fixture.wantProviderState {
						t.Fatalf("provider state = %s, want %s", terminal.ProviderState, fixture.wantProviderState)
					}
				}
			})
		}
	}
}

func TestResponsesProviderStateRoundTrip(t *testing.T) {
	var captured struct {
		Input []json.RawMessage `json:"input"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"output":[],"usage":{}}}` + "\n\n"))
	}))
	defer server.Close()

	state := json.RawMessage(`[{"type":"function_call","id":"call-1","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"venat\"}"}]`)
	driver := openai.New(openai.Config{
		APIKey: "test-key", BaseURL: server.URL, Client: server.Client(), WireAPI: openai.WireResponses,
	})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model: conformanceModel,
		Messages: []message.Message{
			message.NewText(message.RoleUser, "look it up"),
			{
				Role:          message.RoleAssistant,
				ProviderState: state,
				ToolCalls: []message.ToolCall{{
					ID: "call-1", Name: "lookup", Arguments: json.RawMessage(conformanceArgs),
				}},
			},
			message.NewToolResult(message.ToolResult{
				ToolCallID: "call-1", Name: "lookup", Content: "found",
			}),
		},
	})
	if err != nil {
		t.Fatalf("Stream() error: %v", err)
	}
	defer stream.Close()
	if _, err := collectConformanceEvents(stream); err != nil {
		t.Fatalf("collecting replay stream: %v", err)
	}
	if len(captured.Input) != 3 {
		t.Fatalf("request input length = %d, want user + provider state + tool output: %#v", len(captured.Input), captured.Input)
	}
	var expectedState []json.RawMessage
	if err := json.Unmarshal(state, &expectedState); err != nil {
		t.Fatalf("decode provider state fixture: %v", err)
	}
	if len(expectedState) != 1 || string(captured.Input[1]) != string(expectedState[0]) {
		t.Fatalf("replayed provider state = %s, want exact item %s", captured.Input[1], expectedState[0])
	}
	var toolOutput map[string]any
	if err := json.Unmarshal(captured.Input[2], &toolOutput); err != nil {
		t.Fatalf("decode replayed tool output: %v", err)
	}
	if toolOutput["type"] != "function_call_output" || toolOutput["call_id"] != "call-1" {
		t.Fatalf("replayed tool output = %#v", toolOutput)
	}
}

func TestStreamAdaptersEmitEventErrorAsTerminal(t *testing.T) {
	for _, adapter := range streamConformanceAdapters() {
		adapter := adapter
		t.Run(adapter.name, func(t *testing.T) {
			fixture := streamConformanceErrorFixture(adapter.name)
			stream, closeServer := openConformanceStream(t, adapter, fixture)
			defer closeServer()
			events, err := collectConformanceEvents(stream)
			if err != nil {
				t.Fatalf("stream returned transport error instead of EventError: %v", err)
			}
			terminal := assertOneTerminal(t, events, provider.EventError)
			if terminal.Err == nil {
				t.Fatal("EventError has nil Err")
			}
			var providerErr *provider.Error
			if !errors.As(terminal.Err, &providerErr) {
				t.Fatalf("EventError.Err = %T %v, want *provider.Error", terminal.Err, terminal.Err)
			}
			if providerErr.Message != "boom" || providerErr.Kind != provider.ErrorServer {
				t.Fatalf("provider error = %#v, want server error with message boom", providerErr)
			}
		})
	}
}

func TestStreamAdaptersRejectTruncatedStreams(t *testing.T) {
	for _, adapter := range streamConformanceAdapters() {
		adapter := adapter
		t.Run(adapter.name, func(t *testing.T) {
			fixture := streamConformanceTruncatedFixture(adapter.name)
			stream, closeServer := openConformanceStream(t, adapter, fixture)
			defer closeServer()
			_, err := collectConformanceEvents(stream)
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncated stream error = %v, want io.ErrUnexpectedEOF", err)
			}
		})
	}
}

func TestStreamAdaptersRejectEmptyStreams(t *testing.T) {
	for _, adapter := range streamConformanceAdapters() {
		adapter := adapter
		t.Run(adapter.name, func(t *testing.T) {
			fixture := streamConformanceFixture{name: "empty"}
			stream, closeServer := openConformanceStream(t, adapter, fixture)
			defer closeServer()
			_, err := collectConformanceEvents(stream)
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("empty stream error = %v, want io.ErrUnexpectedEOF", err)
			}
		})
	}
}

func streamConformanceAdapters() []streamConformanceAdapter {
	return []streamConformanceAdapter{
		{
			name: "openai-chat",
			open: func(ctx context.Context, baseURL string, client *http.Client) (provider.Stream, error) {
				return openai.New(openai.Config{
					APIKey: "test-key", BaseURL: baseURL, Client: client, WireAPI: openai.WireChatCompletions,
				}).Stream(ctx, conformanceRequest())
			},
		},
		{
			name: "anthropic",
			open: func(ctx context.Context, baseURL string, client *http.Client) (provider.Stream, error) {
				return anthropic.New(anthropic.Config{
					APIKey: "test-key", BaseURL: baseURL, Client: client,
				}).Stream(ctx, conformanceRequest())
			},
		},
		{
			name:          "openai-responses",
			supportsState: true,
			open: func(ctx context.Context, baseURL string, client *http.Client) (provider.Stream, error) {
				return openai.New(openai.Config{
					APIKey: "test-key", BaseURL: baseURL, Client: client, WireAPI: openai.WireResponses,
				}).Stream(ctx, conformanceRequest())
			},
		},
	}
}

func conformanceRequest() provider.Request {
	return provider.Request{
		Model:    conformanceModel,
		Messages: []message.Message{message.NewText(message.RoleUser, "test")},
		Tools: []message.ToolDefinition{{
			Name:        "lookup",
			Description: "Look up a value",
			InputSchema: message.JSONSchema{Type: "object"},
		}},
	}
}

func streamConformanceFixtures() []streamConformanceFixture {
	return []streamConformanceFixture{
		{
			name:      "text-only",
			chat:      chatTextFixture(false),
			anthropic: anthropicTextFixture(false),
			responses: responsesTextFixture(false),
			wantKinds: []provider.EventKind{provider.EventTextDelta, provider.EventDone},
			wantText:  conformanceText,
			wantStop:  provider.StopReasonComplete,
			wantUsage: provider.Usage{InputTokens: 4, OutputTokens: 3, TotalTokens: 7},
		},
		{
			name:         "thinking-and-text",
			chat:         chatThinkingTextFixture(),
			anthropic:    anthropicThinkingTextFixture(),
			responses:    responsesThinkingTextFixture(),
			wantKinds:    []provider.EventKind{provider.EventThinkingDelta, provider.EventTextDelta, provider.EventDone},
			wantText:     conformanceText,
			wantThinking: conformanceThink,
			wantStop:     provider.StopReasonComplete,
			wantUsage:    provider.Usage{InputTokens: 4, OutputTokens: 3, TotalTokens: 7},
		},
		{
			name:              "tool-call",
			chat:              chatToolFixture(),
			anthropic:         anthropicToolFixture(),
			responses:         responsesToolFixture(),
			wantKinds:         []provider.EventKind{provider.EventToolCallDelta, provider.EventToolCallDelta, provider.EventDone},
			wantStop:          provider.StopReasonToolUse,
			wantUsage:         provider.Usage{InputTokens: 4, OutputTokens: 3, TotalTokens: 7},
			wantTool:          true,
			wantProviderState: `[{"type":"function_call","id":"call-1","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"venat\"}"}]`,
		},
		{
			name:         "mixed-content",
			chat:         chatMixedFixture(),
			anthropic:    anthropicMixedFixture(),
			responses:    responsesMixedFixture(),
			wantKinds:    []provider.EventKind{provider.EventThinkingDelta, provider.EventTextDelta, provider.EventToolCallDelta, provider.EventToolCallDelta, provider.EventDone},
			wantText:     conformanceText,
			wantThinking: conformanceThink,
			wantStop:     provider.StopReasonToolUse,
			wantUsage:    provider.Usage{InputTokens: 4, OutputTokens: 3, TotalTokens: 7},
			wantTool:     true,
		},
		{
			chat:      chatTextFixture(true),
			anthropic: anthropicTextFixture(true),
			responses: responsesTextFixture(true),
			wantKinds: []provider.EventKind{provider.EventTextDelta, provider.EventDone},
			wantText:  conformanceText,
			wantStop:  provider.StopReasonComplete,
			wantUsage: provider.Usage{InputTokens: 4, OutputTokens: 3, TotalTokens: 7},
		},
	}
}

func streamConformanceErrorFixture(adapter string) streamConformanceFixture {
	fixture := streamConformanceFixture{name: "error"}
	switch adapter {
	case "openai-chat":
		fixture.chat = "data: {\"error\":{\"type\":\"server_error\",\"code\":\"server_error\",\"message\":\"boom\"}}\n\n"
	case "anthropic":
		fixture.anthropic = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"boom\"}}\n\n"
	case "openai-responses":
		fixture.responses = "event: error\ndata: {\"type\":\"error\",\"code\":\"server_error\",\"message\":\"boom\"}\n\n"
	}
	return fixture
}

func streamConformanceTruncatedFixture(adapter string) streamConformanceFixture {
	fixture := streamConformanceFixture{name: "truncated"}
	switch adapter {
	case "openai-chat":
		fixture.chat = `data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{"content":"partial"}}]}` + "\n\n"
	case "anthropic":
		fixture.anthropic = `event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}` + "\n\n"
	case "openai-responses":
		fixture.responses = `event: response.output_text.delta` + "\n" + `data: {"type":"response.output_text.delta","output_index":0,"delta":"partial"}` + "\n\n"
	}
	return fixture
}

func openConformanceStream(t *testing.T, adapter streamConformanceAdapter, fixture streamConformanceFixture) (provider.Stream, func()) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		switch adapter.name {
		case "openai-chat":
			_, _ = w.Write([]byte(fixture.chat))
		case "anthropic":
			_, _ = w.Write([]byte(fixture.anthropic))
		case "openai-responses":
			_, _ = w.Write([]byte(fixture.responses))
		}
	}))
	stream, err := adapter.open(context.Background(), server.URL, server.Client())
	if err != nil {
		server.Close()
		t.Fatalf("Stream() error: %v", err)
	}
	return stream, func() {
		_ = stream.Close()
		server.Close()
	}
}

func collectConformanceEvents(stream provider.Stream) ([]provider.Event, error) {
	events := make([]provider.Event, 0, 8)
	for {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				for _, event := range events {
					if event.Kind == provider.EventDone || event.Kind == provider.EventError {
						return events, nil
					}
				}
				return events, io.ErrUnexpectedEOF
			}
			return events, err
		}
		events = append(events, event)
	}
}

func assertEventSequence(t *testing.T, events []provider.Event, want []provider.EventKind) {
	t.Helper()
	if len(events) != len(want) {
		t.Fatalf("event count = %d, want %d; events = %#v", len(events), len(want), events)
	}
	for i, event := range events {
		if event.Kind != want[i] {
			t.Fatalf("event %d kind = %q, want %q; events = %#v", i, event.Kind, want[i], events)
		}
	}
}

func assertOneTerminal(t *testing.T, events []provider.Event, want provider.EventKind) provider.Event {
	t.Helper()
	terminals := make([]provider.Event, 0, 1)
	for _, event := range events {
		if event.Kind == provider.EventDone || event.Kind == provider.EventError {
			terminals = append(terminals, event)
		}
	}
	if len(terminals) != 1 {
		t.Fatalf("terminal event count = %d, want 1; events = %#v", len(terminals), events)
	}
	if terminals[0].Kind != want {
		t.Fatalf("terminal kind = %q, want %q; events = %#v", terminals[0].Kind, want, events)
	}
	if events[len(events)-1].Kind != terminals[0].Kind {
		t.Fatalf("terminal event was not last: events = %#v", events)
	}
	return terminals[0]
}

func assertToolCallEvents(t *testing.T, events []provider.Event) {
	t.Helper()
	for _, event := range events {
		if event.Kind != provider.EventToolCallDelta || event.ToolCallDelta == nil {
			continue
		}
		if event.ToolCallDelta.ID == "call-1" && event.ToolCallDelta.Name == "lookup" {
			return
		}
	}
	t.Fatalf("tool call metadata was not emitted: events = %#v", events)
}

func normalizedText(events []provider.Event) string {
	var text string
	for _, event := range events {
		if event.Kind == provider.EventTextDelta {
			text += event.Text
		}
	}
	return text
}

func normalizedThinking(events []provider.Event) string {
	var thinking string
	for _, event := range events {
		if event.Kind == provider.EventThinkingDelta {
			thinking += event.Thinking
		}
	}
	return thinking
}

func chatTextFixture(keepalive bool) string {
	prefix := ""
	if keepalive {
		prefix = ": keepalive\n\n"
	}
	return prefix + `data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{"content":"hello"}}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[],"usage":{"prompt_tokens":4,"completion_tokens":3,"total_tokens":7}}` + "\n\n" + "data: [DONE]\n\n"
}

func chatThinkingTextFixture() string {
	return `data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{"reasoning_content":"reasoning"}}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{"content":"hello"}}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[],"usage":{"prompt_tokens":4,"completion_tokens":3,"total_tokens":7}}` + "\n\n" + "data: [DONE]\n\n"
}

func chatToolFixture() string {
	return `data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"lookup"}}]}}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"query\":\"venat\"}"}}]}}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{},"finish_reason":"tool_calls"}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[],"usage":{"prompt_tokens":4,"completion_tokens":3,"total_tokens":7}}` + "\n\n" + "data: [DONE]\n\n"
}

func chatMixedFixture() string {
	return `data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{"reasoning_content":"reasoning"}}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{"content":"hello"}}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"lookup"}}]}}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"query\":\"venat\"}"}}]}}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[{"delta":{},"finish_reason":"tool_calls"}]}` + "\n\n" +
		`data: {"id":"chat-1","model":"conformance-model","choices":[],"usage":{"prompt_tokens":4,"completion_tokens":3,"total_tokens":7}}` + "\n\n" + "data: [DONE]\n\n"
}

func anthropicTextFixture(keepalive bool) string {
	prefix := ""
	if keepalive {
		prefix = ": keepalive\n\n"
	}
	return prefix + `event: message_start` + "\n" + `data: {"type":"message_start","message":{"id":"msg-1","model":"conformance-model"},"usage":{"input_tokens":4}}` + "\n\n" +
		`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}` + "\n\n" +
		`event: message_delta` + "\n" + `data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}` + "\n\n" +
		`event: message_stop` + "\n" + `data: {"type":"message_stop"}` + "\n\n"
}

func anthropicThinkingTextFixture() string {
	return `event: message_start` + "\n" + `data: {"type":"message_start","message":{"id":"msg-1","model":"conformance-model"},"usage":{"input_tokens":4}}` + "\n\n" +
		`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"reasoning"}}` + "\n\n" +
		`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"hello"}}` + "\n\n" +
		`event: message_delta` + "\n" + `data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}` + "\n\n" +
		`event: message_stop` + "\n" + `data: {"type":"message_stop"}` + "\n\n"
}

func anthropicToolFixture() string {
	return `event: message_start` + "\n" + `data: {"type":"message_start","message":{"id":"msg-1","model":"conformance-model"},"usage":{"input_tokens":4}}` + "\n\n" +
		`event: content_block_start` + "\n" + `data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call-1","name":"lookup"}}` + "\n\n" +
		`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"query\":\"venat\"}"}}` + "\n\n" +
		`event: message_delta` + "\n" + `data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":3}}` + "\n\n" +
		`event: message_stop` + "\n" + `data: {"type":"message_stop"}` + "\n\n"
}

func anthropicMixedFixture() string {
	return `event: message_start` + "\n" + `data: {"type":"message_start","message":{"id":"msg-1","model":"conformance-model"},"usage":{"input_tokens":4}}` + "\n\n" +
		`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"reasoning"}}` + "\n\n" +
		`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"hello"}}` + "\n\n" +
		`event: content_block_start` + "\n" + `data: {"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"call-1","name":"lookup"}}` + "\n\n" +
		`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"query\":\"venat\"}"}}` + "\n\n" +
		`event: message_delta` + "\n" + `data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":3}}` + "\n\n" +
		`event: message_stop` + "\n" + `data: {"type":"message_stop"}` + "\n\n"
}

func responsesTextFixture(keepalive bool) string {
	prefix := ""
	if keepalive {
		prefix = ": keepalive\n\n"
	}
	return prefix + `event: response.output_text.delta` + "\n" + `data: {"type":"response.output_text.delta","output_index":0,"delta":"hello"}` + "\n\n" +
		`event: response.completed` + "\n" + `data: {"type":"response.completed","response":{"id":"resp-1","model":"conformance-model","output":[],"usage":{"input_tokens":4,"output_tokens":3,"total_tokens":7}}}` + "\n\n"
}

func responsesThinkingTextFixture() string {
	return `event: response.reasoning_summary_text.delta` + "\n" + `data: {"type":"response.reasoning_summary_text.delta","output_index":0,"delta":"reasoning"}` + "\n\n" +
		`event: response.output_text.delta` + "\n" + `data: {"type":"response.output_text.delta","output_index":0,"delta":"hello"}` + "\n\n" +
		`event: response.completed` + "\n" + `data: {"type":"response.completed","response":{"id":"resp-1","model":"conformance-model","output":[],"usage":{"input_tokens":4,"output_tokens":3,"total_tokens":7}}}` + "\n\n"
}

func responsesToolFixture() string {
	return `event: response.output_item.added` + "\n" + `data: {"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"call-1","call_id":"call-1","name":"lookup"}}` + "\n\n" +
		`event: response.function_call_arguments.delta` + "\n" + `data: {"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"query\":"}` + "\n\n" +
		`event: response.function_call_arguments.delta` + "\n" + `data: {"type":"response.function_call_arguments.delta","output_index":0,"delta":"\"venat\"}"}` + "\n\n" +
		`event: response.output_item.done` + "\n" + `data: {"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"call-1","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"venat\"}"}}` + "\n\n" +
		`event: response.completed` + "\n" + `data: {"type":"response.completed","response":{"id":"resp-1","model":"conformance-model","output":[{"type":"function_call","id":"call-1","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"venat\"}"}],"usage":{"input_tokens":4,"output_tokens":3,"total_tokens":7}}}` + "\n\n"
}

func responsesMixedFixture() string {
	return `event: response.reasoning_summary_text.delta` + "\n" + `data: {"type":"response.reasoning_summary_text.delta","output_index":0,"delta":"reasoning"}` + "\n\n" +
		`event: response.output_text.delta` + "\n" + `data: {"type":"response.output_text.delta","output_index":0,"delta":"hello"}` + "\n\n" +
		`event: response.output_item.added` + "\n" + `data: {"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"call-1","call_id":"call-1","name":"lookup"}}` + "\n\n" +
		`event: response.function_call_arguments.delta` + "\n" + `data: {"type":"response.function_call_arguments.delta","output_index":1,"delta":"{\"query\":"}` + "\n\n" +
		`event: response.function_call_arguments.delta` + "\n" + `data: {"type":"response.function_call_arguments.delta","output_index":1,"delta":"\"venat\"}"}` + "\n\n" +
		`event: response.output_item.done` + "\n" + `data: {"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","id":"call-1","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"venat\"}"}}` + "\n\n" +
		`event: response.completed` + "\n" + `data: {"type":"response.completed","response":{"id":"resp-1","model":"conformance-model","output":[{"type":"message","id":"msg-1","phase":"final_answer"},{"type":"function_call","id":"call-1","call_id":"call-1","name":"lookup","arguments":"{\"query\":\"venat\"}"}],"usage":{"input_tokens":4,"output_tokens":3,"total_tokens":7}}}` + "\n\n"
}
