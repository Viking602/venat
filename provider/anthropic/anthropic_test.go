package anthropic

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/provider/shared"
)

type anthropicTestLogRecord struct {
	message string
	attrs   map[string]string
}

type anthropicTestLogRecorder struct {
	records []anthropicTestLogRecord
}

func (r *anthropicTestLogRecorder) Enabled(context.Context, slog.Level) bool {
	return true
}

func (r *anthropicTestLogRecorder) Handle(_ context.Context, record slog.Record) error {
	attrs := make(map[string]string)
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()
		return true
	})
	r.records = append(r.records, anthropicTestLogRecord{message: record.Message, attrs: attrs})
	return nil
}

func (r *anthropicTestLogRecorder) WithAttrs([]slog.Attr) slog.Handler {
	return r
}

func (r *anthropicTestLogRecorder) WithGroup(string) slog.Handler {
	return r
}

func (r *anthropicTestLogRecorder) has(field, reason string) bool {
	for _, record := range r.records {
		if record.message == "dropping unsupported request field" &&
			record.attrs["field"] == field && record.attrs["reason"] == reason {
			return true
		}
	}
	return false
}

func TestNewDefaultClientHasNoStreamLifetimeTimeout(t *testing.T) {
	driver := New(Config{})
	if driver.config.Client == nil {
		t.Fatal("expected default client")
	}
	if driver.config.Client.Timeout != 0 {
		t.Fatalf("default client timeout = %s, want 0", driver.config.Client.Timeout)
	}
	transport, ok := driver.config.Client.Transport.(*http.Transport)
	if !ok || transport.ResponseHeaderTimeout <= 0 {
		t.Fatalf("default response header timeout is not configured")
	}

	supplied := &http.Client{}
	driver = New(Config{Client: supplied})
	if driver.config.Client != supplied {
		t.Fatal("expected supplied client to be preserved")
	}
}

func TestDriverStreamParsesMessageSSE(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/messages" {
			t.Fatalf("unexpected path %s", request.URL.Path)
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-1\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":3,\"cache_read_input_tokens\":2,\"cache_creation_input_tokens\":1}}}\n\n"))
		_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello \"}}\n\n"))
		_, _ = writer.Write([]byte("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"lookup\",\"input\":{}}}\n\n"))
		_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"query\\\":\\\"ve\"}}\n\n"))
		_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"nat\\\"}\"}}\n\n"))
		_, _ = writer.Write([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":15}}\n\n"))
		_, _ = writer.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	driver := New(Config{
		APIKey:  "test",
		BaseURL: server.URL,
		Client:  server.Client(),
	})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model: "claude-test",
		Messages: []message.Message{
			message.NewText(message.RoleUser, "hello"),
		},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	events := collectAnthropicEvents(t, stream)
	if len(events) < 4 {
		t.Fatalf("expected streamed events, got %#v", events)
	}
	if events[0].Kind != provider.EventTextDelta || events[0].Text != "Hello " {
		t.Fatalf("unexpected first event %#v", events[0])
	}
	if events[1].Kind != provider.EventToolCallDelta || events[1].ToolCallDelta.Name != "lookup" {
		t.Fatalf("expected tool call start delta, got %#v", events[1])
	}
	last := events[len(events)-1]
	if last.Kind != provider.EventDone || last.StopReason != provider.StopReasonToolUse {
		t.Fatalf("expected tool-use done event, got %#v", last)
	}
	if last.Usage.OutputTokens != 15 {
		t.Fatalf("expected usage in final event, got %#v", last)
	}
	if last.Usage.InputTokens != 6 || last.Usage.CachedInputTokens != 2 || !last.Usage.CachedInputTokensReported ||
		last.Usage.CacheWriteInputTokens != 1 || !last.Usage.CacheWriteInputTokensReported {
		t.Fatalf("expected inclusive cache usage in final event, got %#v", last.Usage)
	}
	if last.Response.ID != "msg-1" || last.Response.Model != "claude-test" {
		t.Fatalf("response metadata = %#v", last.Response)
	}
}

func TestDriverStreamSkipsKeepaliveFrames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte(":keepalive\n\n"))
		_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n"))
		_, _ = writer.Write([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n"))
		_, _ = writer.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client()})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "claude-test",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	events := collectAnthropicEvents(t, stream)
	if len(events) == 0 || events[0].Kind != provider.EventTextDelta || events[0].Text != "ok" {
		t.Fatalf("keepalive should be skipped, got %#v", events)
	}
}

func TestDriverStreamForwardsStopAndThinking(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_ = json.NewDecoder(request.Body).Decode(&captured)
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"reasoning...\"}}\n\n"))
		_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"answer\"}}\n\n"))
		_, _ = writer.Write([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n"))
		_, _ = writer.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client()})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:          "claude-test",
		Messages:       []message.Message{message.NewText(message.RoleUser, "hi")},
		StopSequences:  []string{"Wait,"},
		ThinkingBudget: 500, // below 1024; driver should floor to 1024
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	events := collectAnthropicEvents(t, stream)

	stop, _ := captured["stop_sequences"].([]any)
	if len(stop) != 1 || stop[0] != "Wait," {
		t.Fatalf("expected stop_sequences forwarded, got %#v", captured["stop_sequences"])
	}
	thinking, _ := captured["thinking"].(map[string]any)
	if thinking["type"] != "enabled" {
		t.Fatalf("expected thinking enabled, got %#v", thinking)
	}
	if int(thinking["budget_tokens"].(float64)) != 1024 {
		t.Fatalf("expected budget_tokens floored to 1024, got %#v", thinking["budget_tokens"])
	}

	var sawThinking bool
	for _, ev := range events {
		if ev.Kind == provider.EventThinkingDelta && ev.Thinking == "reasoning..." {
			sawThinking = true
		}
	}
	if !sawThinking {
		t.Fatalf("expected EventThinkingDelta from thinking_delta, events=%#v", events)
	}
}

func TestToAnthropicRequestThinkingToolRoundTrip(t *testing.T) {
	history := []message.Message{
		{Role: message.RoleSystem, Text: "you are helpful"},
		message.NewText(message.RoleUser, "weather?"),
		{
			Role:              message.RoleAssistant,
			Thinking:          "let me check",
			ThinkingSignature: "sig-abc",
			ToolCalls: []message.ToolCall{
				{ID: "toolu_1", Name: "weather", Arguments: []byte(`{"city":"SF"}`)},
			},
		},
		message.NewToolResult(message.ToolResult{ToolCallID: "toolu_1", Name: "weather", Content: "sunny"}),
	}

	system, messages := toAnthropicRequest(history)
	if system != "you are helpful" {
		t.Fatalf("system = %q, want extracted system text", system)
	}
	assistant := messages[1]
	if assistant.Role != "assistant" || len(assistant.Content) != 2 {
		t.Fatalf("unexpected assistant message %#v", assistant)
	}
	if assistant.Content[0].Type != "thinking" || assistant.Content[0].Signature != "sig-abc" {
		t.Fatalf("expected leading signed thinking block, got %#v", assistant.Content[0])
	}
	if assistant.Content[1].Type != "tool_use" || assistant.Content[1].ID != "toolu_1" {
		t.Fatalf("expected tool_use block, got %#v", assistant.Content[1])
	}
	toolMsg := messages[2]
	if toolMsg.Role != "user" || len(toolMsg.Content) != 1 {
		t.Fatalf("unexpected tool-result message %#v", toolMsg)
	}
	block := toolMsg.Content[0]
	if block.Type != "tool_result" || block.ToolUseID != "toolu_1" || block.Content != "sunny" {
		t.Fatalf("expected tool_result carrying tool_use_id, got %#v", block)
	}
}

func TestToAnthropicRequestCoalescesToolResults(t *testing.T) {
	history := []message.Message{
		{
			Role: message.RoleAssistant,
			ToolCalls: []message.ToolCall{
				{ID: "a", Name: "t", Arguments: []byte(`{}`)},
				{ID: "b", Name: "t", Arguments: []byte(`{}`)},
			},
		},
		message.NewToolResult(message.ToolResult{ToolCallID: "a", Name: "t", Content: "one"}),
		message.NewToolResult(message.ToolResult{ToolCallID: "b", Name: "t", Content: "two", IsError: true}),
	}

	_, messages := toAnthropicRequest(history)

	user := messages[1]
	if user.Role != "user" || len(user.Content) != 2 {
		t.Fatalf("expected two tool_result blocks in one user message, got %#v", user)
	}
	if user.Content[0].ToolUseID != "a" || user.Content[1].ToolUseID != "b" {
		t.Fatalf("tool_use_id ordering wrong: %#v", user.Content)
	}
	if !user.Content[1].IsError {
		t.Fatalf("expected is_error on second tool result, got %#v", user.Content[1])
	}
}

func TestToAnthropicRequestDropsUnsignedThinking(t *testing.T) {
	history := []message.Message{
		{Role: message.RoleAssistant, Thinking: "no signature here", Text: "answer"},
	}
	recorder := &anthropicTestLogRecorder{}
	previous := slog.Default()
	slog.SetDefault(slog.New(recorder))
	defer slog.SetDefault(previous)
	_, messages := toAnthropicRequest(history)
	for _, block := range messages[0].Content {
		if block.Type == "thinking" {
			t.Fatalf("unsigned thinking block should be dropped, got %#v", messages[0].Content)
		}
	}
	if len(messages[0].Content) != 1 || messages[0].Content[0].Type != "text" {
		t.Fatalf("expected only the text block, got %#v", messages[0].Content)
	}
	if !recorder.has("thinking", "unsigned thinking block dropped") {
		t.Fatalf("missing unsigned thinking warning: %#v", recorder.records)
	}
}

func TestToAnthropicRequestDropsLateSignedThinking(t *testing.T) {
	recorder := &anthropicTestLogRecorder{}
	previous := slog.Default()
	slog.SetDefault(slog.New(recorder))
	defer slog.SetDefault(previous)

	_, messages := toAnthropicRequest([]message.Message{{
		Role: message.RoleAssistant,
		Content: []message.ContentPart{
			message.TextPart("answer"),
			message.ReasoningPart("late", "sig"),
		},
	}})
	if len(messages) != 1 || len(messages[0].Content) != 1 || messages[0].Content[0].Type != "text" {
		t.Fatalf("late thinking content = %#v", messages)
	}
	if !recorder.has("thinking", "signed thinking must precede visible assistant content") {
		t.Fatalf("missing late-thinking warning: %#v", recorder.records)
	}
}

func TestDriverStreamDropsMalformedAnthropicProviderState(t *testing.T) {
	var captured map[string]any
	server := anthropicContractServer(t, &captured)
	defer server.Close()
	recorder := &anthropicTestLogRecorder{}
	previous := slog.Default()
	slog.SetDefault(slog.New(recorder))
	defer slog.SetDefault(previous)

	driver := New(Config{APIKey: "test", BaseURL: server.URL})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model: "claude",
		Messages: []message.Message{{
			Role:          message.RoleAssistant,
			Text:          "fallback",
			ProviderState: json.RawMessage(`{"type":"reasoning"}`),
		}},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	_ = stream.Close()
	messages, _ := captured["messages"].([]any)
	content := messages[0].(map[string]any)["content"].([]any)
	if len(content) != 1 || content[0].(map[string]any)["text"] != "fallback" {
		t.Fatalf("fallback content = %#v", content)
	}
	if !recorder.has("providerState", "decode anthropic provider state") {
		t.Fatalf("missing provider state warning: %#v", recorder.records)
	}
}

func TestToAnthropicRequestEmptyToolInput(t *testing.T) {
	history := []message.Message{
		{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{{ID: "x", Name: "noop"}}},
	}

	_, messages := toAnthropicRequest(history)
	block := messages[0].Content[0]
	if block.Type != "tool_use" || string(block.Input) != "{}" {
		t.Fatalf("expected empty input rendered as {}, got %#v", block)
	}
	raw, err := json.Marshal(block)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), `"input":{}`) {
		t.Fatalf("expected input key present in %s", raw)
	}
}

// --- Request contract: every provider.Request field maps to the wire or is
// explicitly rejected; nothing is silently dropped. ---

func anthropicContractServer(t *testing.T, captured *map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_ = json.NewDecoder(request.Body).Decode(captured)
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude\",\"usage\":{\"input_tokens\":7,\"cache_read_input_tokens\":2,\"cache_creation_input_tokens\":1}}}\n\n"))
		_, _ = writer.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
}

func TestDriverStreamMapsResponseFormatToOutputConfig(t *testing.T) {
	tests := []struct {
		name       string
		format     *provider.ResponseFormat
		wantSchema string
	}{
		{
			name:       "raw schema takes precedence",
			format:     &provider.ResponseFormat{Type: "json_schema", Name: "ignored", RawSchema: json.RawMessage(`{"type":"object"}`), Schema: &message.JSONSchema{Type: "string"}},
			wantSchema: `{"type":"object"}`,
		},
		{
			name:       "typed schema marshaled",
			format:     &provider.ResponseFormat{Type: "json_schema", Schema: &message.JSONSchema{Type: "string"}},
			wantSchema: `{"type":"string"}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var captured map[string]any
			server := anthropicContractServer(t, &captured)
			defer server.Close()
			driver := New(Config{APIKey: "test", BaseURL: server.URL})
			stream, err := driver.Stream(context.Background(), provider.Request{
				Model:          "claude",
				Messages:       []message.Message{message.NewText(message.RoleUser, "hi")},
				ResponseFormat: test.format,
			})
			if err != nil {
				t.Fatalf("Stream() error = %v", err)
			}
			_ = stream.Close()
			format, _ := captured["output_config"].(map[string]any)["format"].(map[string]any)
			if format["type"] != "json_schema" {
				t.Fatalf("output_config.format = %#v", captured["output_config"])
			}
			encoded, _ := json.Marshal(format["schema"])
			if string(encoded) != test.wantSchema {
				t.Fatalf("schema = %s, want %s", encoded, test.wantSchema)
			}
		})
	}
}

func TestDriverStreamResponseFormatTextOmitsOutputConfig(t *testing.T) {
	var captured map[string]any
	server := anthropicContractServer(t, &captured)
	defer server.Close()
	driver := New(Config{APIKey: "test", BaseURL: server.URL})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:          "claude",
		Messages:       []message.Message{message.NewText(message.RoleUser, "hi")},
		ResponseFormat: &provider.ResponseFormat{Type: "text"},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	_ = stream.Close()
	if _, present := captured["output_config"]; present {
		t.Fatalf("text format must omit output_config, got %#v", captured["output_config"])
	}
}

func TestDriverStreamDropsUnsupportedResponseFormat(t *testing.T) {
	tests := []struct {
		name   string
		format *provider.ResponseFormat
		reason string
	}{
		{"json_object", &provider.ResponseFormat{Type: "json_object"}, "json_object is unsupported; use json_schema"},
		{"unknown type", &provider.ResponseFormat{Type: "yaml"}, "unsupported response format type"},
		{"empty type", &provider.ResponseFormat{Strict: true}, "unsupported response format type"},
		{"json_schema without schema", &provider.ResponseFormat{Type: "json_schema", Name: "n"}, "json_schema requires schema"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var captured map[string]any
			server := anthropicContractServer(t, &captured)
			defer server.Close()
			recorder := &anthropicTestLogRecorder{}
			previous := slog.Default()
			slog.SetDefault(slog.New(recorder))
			defer slog.SetDefault(previous)

			driver := New(Config{APIKey: "test", BaseURL: server.URL})
			stream, err := driver.Stream(context.Background(), provider.Request{
				Model:          "claude",
				Messages:       []message.Message{message.NewText(message.RoleUser, "hi")},
				ResponseFormat: test.format,
			})
			if err != nil {
				t.Fatalf("Stream() error = %v", err)
			}
			_ = stream.Close()
			if _, present := captured["output_config"]; present {
				t.Fatalf("dropped response format reached request: %#v", captured["output_config"])
			}
			if !recorder.has("responseFormat", test.reason) {
				t.Fatalf("warning missing responseFormat=%q: %#v", test.reason, recorder.records)
			}
		})
	}
}

func TestDriverStreamCacheBoundaryMarksBlocks(t *testing.T) {
	t.Run("system becomes block array", func(t *testing.T) {
		var captured map[string]any
		server := anthropicContractServer(t, &captured)
		defer server.Close()
		system := message.NewText(message.RoleSystem, "stable")
		system.CacheBoundary = true
		driver := New(Config{APIKey: "test", BaseURL: server.URL})
		stream, err := driver.Stream(context.Background(), provider.Request{
			Model:    "claude",
			Messages: []message.Message{system, message.NewText(message.RoleUser, "hi")},
		})
		if err != nil {
			t.Fatalf("Stream() error = %v", err)
		}
		_ = stream.Close()
		blocks, ok := captured["system"].([]any)
		if !ok || len(blocks) != 1 {
			t.Fatalf("system = %#v, want single-element block array", captured["system"])
		}
		block, _ := blocks[0].(map[string]any)
		cache, _ := block["cache_control"].(map[string]any)
		if block["text"] != "stable" || cache["type"] != "ephemeral" {
			t.Fatalf("system block = %#v", block)
		}
	})
	t.Run("user message marks last text block", func(t *testing.T) {
		var captured map[string]any
		server := anthropicContractServer(t, &captured)
		defer server.Close()
		user := message.Message{
			Role:          message.RoleUser,
			CacheBoundary: true,
			Content: []message.ContentPart{
				{Kind: message.ContentText, Text: "first"},
				{Kind: message.ContentText, Text: "second"},
			},
		}
		driver := New(Config{APIKey: "test", BaseURL: server.URL})
		stream, err := driver.Stream(context.Background(), provider.Request{
			Model:    "claude",
			Messages: []message.Message{user},
		})
		if err != nil {
			t.Fatalf("Stream() error = %v", err)
		}
		_ = stream.Close()
		messages, _ := captured["messages"].([]any)
		content, _ := messages[0].(map[string]any)["content"].([]any)
		first, _ := content[0].(map[string]any)
		second, _ := content[1].(map[string]any)
		if _, marked := first["cache_control"]; marked {
			t.Fatalf("first block must not carry cache_control: %#v", first)
		}
		if second["cache_control"].(map[string]any)["type"] != "ephemeral" {
			t.Fatalf("last block = %#v, want cache_control ephemeral", second)
		}
	})
	t.Run("tool result marks tool_result block", func(t *testing.T) {
		var captured map[string]any
		server := anthropicContractServer(t, &captured)
		defer server.Close()
		result := message.NewToolResult(message.ToolResult{ToolCallID: "call_1", Name: "lookup", Content: "found"})
		result.CacheBoundary = true
		driver := New(Config{APIKey: "test", BaseURL: server.URL})
		stream, err := driver.Stream(context.Background(), provider.Request{
			Model:    "claude",
			Messages: []message.Message{result},
		})
		if err != nil {
			t.Fatalf("Stream() error = %v", err)
		}
		_ = stream.Close()
		messages, _ := captured["messages"].([]any)
		content, _ := messages[0].(map[string]any)["content"].([]any)
		block, _ := content[0].(map[string]any)
		if block["type"] != "tool_result" || block["cache_control"].(map[string]any)["type"] != "ephemeral" {
			t.Fatalf("tool_result block = %#v, want cache_control ephemeral", block)
		}
	})
	t.Run("empty text drops boundary marker", func(t *testing.T) {
		var captured map[string]any
		server := anthropicContractServer(t, &captured)
		defer server.Close()
		recorder := &anthropicTestLogRecorder{}
		previous := slog.Default()
		slog.SetDefault(slog.New(recorder))
		defer slog.SetDefault(previous)

		empty := message.NewText(message.RoleUser, "")
		empty.CacheBoundary = true
		driver := New(Config{APIKey: "test", BaseURL: server.URL})
		stream, err := driver.Stream(context.Background(), provider.Request{
			Model: "claude", Messages: []message.Message{empty},
		})
		if err != nil {
			t.Fatalf("Stream() error = %v", err)
		}
		_ = stream.Close()
		messages, _ := captured["messages"].([]any)
		block := messages[0].(map[string]any)["content"].([]any)[0].(map[string]any)
		if _, marked := block["cache_control"]; marked {
			t.Fatalf("empty boundary must omit cache_control: %#v", block)
		}
		if !recorder.has("cacheBoundary", "cache boundary requires non-empty text") {
			t.Fatalf("missing cache boundary warning: %#v", recorder.records)
		}
	})
	t.Run("tool without result drops boundary marker", func(t *testing.T) {
		var captured map[string]any
		server := anthropicContractServer(t, &captured)
		defer server.Close()
		recorder := &anthropicTestLogRecorder{}
		previous := slog.Default()
		slog.SetDefault(slog.New(recorder))
		defer slog.SetDefault(previous)

		invalid := message.Message{Role: message.RoleTool, CacheBoundary: true}
		driver := New(Config{APIKey: "test", BaseURL: server.URL})
		stream, err := driver.Stream(context.Background(), provider.Request{
			Model: "claude", Messages: []message.Message{message.NewText(message.RoleUser, "keep"), invalid},
		})
		if err != nil {
			t.Fatalf("Stream() error = %v", err)
		}
		_ = stream.Close()
		messages, _ := captured["messages"].([]any)
		if len(messages) != 1 {
			t.Fatalf("tool without result changed content: %#v", messages)
		}
		if !recorder.has("cacheBoundary", "cache boundary requires a tool result") {
			t.Fatalf("missing tool-result boundary warning: %#v", recorder.records)
		}
	})
	t.Run("empty tool result text drops boundary marker", func(t *testing.T) {
		var captured map[string]any
		server := anthropicContractServer(t, &captured)
		defer server.Close()
		recorder := &anthropicTestLogRecorder{}
		previous := slog.Default()
		slog.SetDefault(slog.New(recorder))
		defer slog.SetDefault(previous)

		result := message.NewToolResult(message.ToolResult{ToolCallID: "call_1", Name: "lookup"})
		result.CacheBoundary = true
		driver := New(Config{APIKey: "test", BaseURL: server.URL})
		stream, err := driver.Stream(context.Background(), provider.Request{
			Model: "claude", Messages: []message.Message{result},
		})
		if err != nil {
			t.Fatalf("Stream() error = %v", err)
		}
		_ = stream.Close()
		messages, _ := captured["messages"].([]any)
		block := messages[0].(map[string]any)["content"].([]any)[0].(map[string]any)
		if _, marked := block["cache_control"]; marked {
			t.Fatalf("empty tool result boundary must omit cache_control: %#v", block)
		}
		if !recorder.has("cacheBoundary", "cache boundary requires non-empty tool result text") {
			t.Fatalf("missing empty tool-result warning: %#v", recorder.records)
		}
	})
}

func TestDriverStreamParallelToolCallsMapToToolChoice(t *testing.T) {
	tools := []message.ToolDefinition{{Name: "lookup", InputSchema: message.JSONSchema{Type: "object"}}}
	disabled := false
	enabled := true
	tests := []struct {
		name     string
		parallel *bool
		tools    []message.ToolDefinition
		want     map[string]any
	}{
		{"false with tools", &disabled, tools, map[string]any{"type": "auto", "disable_parallel_tool_use": true}},
		{"false without tools", &disabled, nil, nil},
		{"true with tools", &enabled, tools, nil},
		{"unset", nil, tools, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var captured map[string]any
			server := anthropicContractServer(t, &captured)
			defer server.Close()
			driver := New(Config{APIKey: "test", BaseURL: server.URL})
			stream, err := driver.Stream(context.Background(), provider.Request{
				Model:             "claude",
				Messages:          []message.Message{message.NewText(message.RoleUser, "hi")},
				Tools:             test.tools,
				ParallelToolCalls: test.parallel,
			})
			if err != nil {
				t.Fatalf("Stream() error = %v", err)
			}
			_ = stream.Close()
			choice, present := captured["tool_choice"]
			if test.want == nil {
				if present {
					t.Fatalf("tool_choice = %#v, want absent", choice)
				}
				return
			}
			got, _ := choice.(map[string]any)
			if got["type"] != test.want["type"] || got["disable_parallel_tool_use"] != test.want["disable_parallel_tool_use"] {
				t.Fatalf("tool_choice = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestDriverStreamMetadataRestrictedToUserID(t *testing.T) {
	var captured map[string]any
	server := anthropicContractServer(t, &captured)
	defer server.Close()
	driver := New(Config{APIKey: "test", BaseURL: server.URL})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "claude",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
		Metadata: map[string]string{"user_id": "usr_1"},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	_ = stream.Close()
	metadata, _ := captured["metadata"].(map[string]any)
	if metadata["user_id"] != "usr_1" || len(metadata) != 1 {
		t.Fatalf("metadata = %#v", metadata)
	}

	recorder := &anthropicTestLogRecorder{}
	previous := slog.Default()
	slog.SetDefault(slog.New(recorder))
	defer slog.SetDefault(previous)
	stream, err = driver.Stream(context.Background(), provider.Request{
		Model:    "claude",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
		Metadata: map[string]string{"user_id": "usr_1", "tenant": "t", "trace": "x"},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	_ = stream.Close()
	metadata, _ = captured["metadata"].(map[string]any)
	if metadata["user_id"] != "usr_1" || len(metadata) != 1 {
		t.Fatalf("metadata = %#v", metadata)
	}
	if !recorder.has("metadata", "metadata keys unsupported: tenant, trace") {
		t.Fatalf("missing metadata warning: %#v", recorder.records)
	}
}

func TestDriverStreamDropsPromptCacheKeyAndServiceTier(t *testing.T) {
	tests := []struct {
		name    string
		request provider.Request
		field   string
		reason  string
	}{
		{"prompt cache key", provider.Request{PromptCacheKey: "key"}, "promptCacheKey", "prompt cache key is unsupported"},
		{"service tier", provider.Request{ServiceTier: "priority"}, "serviceTier", "service tier is unsupported; use config.betas for beta tiers"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var captured map[string]any
			server := anthropicContractServer(t, &captured)
			defer server.Close()
			recorder := &anthropicTestLogRecorder{}
			previous := slog.Default()
			slog.SetDefault(slog.New(recorder))
			defer slog.SetDefault(previous)

			test.request.Model = "claude"
			test.request.Messages = []message.Message{message.NewText(message.RoleUser, "hi")}
			driver := New(Config{APIKey: "test", BaseURL: server.URL})
			stream, err := driver.Stream(context.Background(), test.request)
			if err != nil {
				t.Fatalf("Stream() error = %v", err)
			}
			_ = stream.Close()
			if _, present := captured[test.field]; present {
				t.Fatalf("dropped field %s reached request: %#v", test.field, captured)
			}
			if !recorder.has(test.field, test.reason) {
				t.Fatalf("missing %s warning: %#v", test.field, recorder.records)
			}
		})
	}
}

func TestDriverStreamMergesExtraBody(t *testing.T) {
	var captured map[string]any
	server := anthropicContractServer(t, &captured)
	defer server.Close()
	driver := New(Config{APIKey: "test", BaseURL: server.URL})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:       "claude",
		Messages:    []message.Message{message.NewText(message.RoleUser, "hi")},
		Temperature: 0.5,
		ExtraBody: map[string]any{
			"custom_flag": true,
			"stream":      false, // managed: stripped
			"temperature": 0.9,   // protected: typed value wins
			"top_p":       0.7,   // protected: typed unset, fills
		},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	_ = stream.Close()
	if captured["custom_flag"] != true {
		t.Fatalf("unmanaged extra body field dropped: %#v", captured)
	}
	if captured["stream"] != true {
		t.Fatalf("managed extra body field must not override: %#v", captured["stream"])
	}
	if captured["temperature"] != 0.5 {
		t.Fatalf("protected typed temperature overwritten: %#v", captured["temperature"])
	}
	if captured["top_p"] != 0.7 {
		t.Fatalf("protected empty top_p must fill from extra body: %#v", captured["top_p"])
	}
}

func TestDriverStreamReportsContextUsageOnce(t *testing.T) {
	var captured map[string]any
	server := anthropicContractServer(t, &captured)
	defer server.Close()
	calls := 0
	var observed provider.ContextUsage
	driver := New(Config{APIKey: "test", BaseURL: server.URL})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "claude",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
		ContextUsage: func(usage provider.ContextUsage) {
			calls++
			observed = usage
		},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
	}
	_ = stream.Close()
	if calls != 1 {
		t.Fatalf("ContextUsage called %d times, want 1", calls)
	}
	// input_tokens 7 + cache_read 2 + cache_creation 1 = 10
	if observed.UsedTokens != 10 || observed.MaxTokens != 0 {
		t.Fatalf("context usage = %#v, want UsedTokens 10", observed)
	}

	// nil observer must not panic
	stream, err = driver.Stream(context.Background(), provider.Request{
		Model:    "claude",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
	}
	_ = stream.Close()
}

func TestToAnthropicRequestPreservesCanonicalMultimodalContent(t *testing.T) {
	history := []message.Message{{
		Role: message.RoleUser,
		Content: []message.ContentPart{
			message.FinalAnswerPart("inspect"),
			{Kind: message.ContentImage, Data: []byte{1, 2, 3}, MediaType: "image/png"},
		},
	}}
	_, messages := toAnthropicRequest(history)
	if len(messages) != 1 || len(messages[0].Content) != 2 {
		t.Fatalf("multimodal messages = %#v", messages)
	}
	image := messages[0].Content[1]
	if image.Type != "image" || image.Source == nil || image.Source.Type != "base64" ||
		image.Source.MediaType != "image/png" || image.Source.Data != "AQID" {
		t.Fatalf("anthropic image block = %#v", image)
	}
}

func TestDriverStreamSendsSystemAndBlocks(t *testing.T) {
	var captured requestBody
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_ = json.NewDecoder(request.Body).Decode(&captured)
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client()})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:       "claude-test",
		Temperature: 0.5,
		TopP:        0.75,
		MaxTokens:   2048,
		Messages: []message.Message{
			{Role: message.RoleSystem, Text: "be terse"},
			message.NewText(message.RoleUser, "hi"),
		},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	_ = collectAnthropicEvents(t, stream)

	if captured.System != "be terse" {
		t.Fatalf("system = %q, want top-level system param", captured.System)
	}
	if len(captured.Messages) != 1 || captured.Messages[0].Role != "user" {
		t.Fatalf("expected single user message, got %#v", captured.Messages)
	}
	content := captured.Messages[0].Content
	if len(content) != 1 || content[0].Type != "text" || content[0].Text != "hi" {
		t.Fatalf("expected text block content, got %#v", content)
	}
	if captured.Temperature != 0.5 || captured.TopP != 0.75 || captured.MaxTokens != 2048 {
		t.Fatalf("model policy not forwarded: %#v", captured)
	}
}

func TestDriverStreamCapturesThinkingSignature(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"reasoning\"}}\n\n"))
		_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig-xyz\"}}\n\n"))
		_, _ = writer.Write([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n"))
		_, _ = writer.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client()})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "claude-test",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	normalized, err := provider.NormalizeEvents(collectAnthropicEvents(t, stream))
	if err != nil {
		t.Fatalf("NormalizeEvents() error = %v", err)
	}
	if normalized.Thinking != "reasoning" {
		t.Fatalf("thinking = %q, want accumulated reasoning", normalized.Thinking)
	}
	if normalized.Signature != "sig-xyz" {
		t.Fatalf("signature = %q, want captured signature", normalized.Signature)
	}
}

func TestDriverStreamCapturesRedactedThinking(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"redacted_thinking\",\"data\":\"enc-123\"}}\n\n"))
		_, _ = writer.Write([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n"))
		_, _ = writer.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client()})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "claude-test",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	normalized, err := provider.NormalizeEvents(collectAnthropicEvents(t, stream))
	if err != nil {
		t.Fatalf("NormalizeEvents() error = %v", err)
	}
	if normalized.RedactedThinking != "enc-123" {
		t.Fatalf("redacted thinking = %q, want captured payload", normalized.RedactedThinking)
	}
}

func collectAnthropicEvents(t *testing.T, stream provider.Stream) []provider.Event {
	t.Helper()
	defer func() { _ = stream.Close() }()
	events := make([]provider.Event, 0, 8)
	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Recv() error = %v", err)
		}
		events = append(events, event)
	}
	return events
}

func TestStreamRetriesTransientStatusOnInitiation(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		if calls < 3 {
			writer.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n"))
		_, _ = writer.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	driver := New(Config{
		APIKey:  "test-key",
		BaseURL: server.URL,
		Retry:   shared.RetryPolicy{BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond},
	})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "claude-sonnet-4-6",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v, want success after two 429 retries", err)
	}
	defer func() { _ = stream.Close() }()
	event, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv() error = %v", err)
	}
	if event.Text != "ok" {
		t.Fatalf("event text = %q, want ok", event.Text)
	}
	if calls != 3 {
		t.Fatalf("server saw %d calls, want 3", calls)
	}
}

// TestDriverStreamSurfacesTypedError is the regression for opaque mid-stream
// overload errors: adapters must preserve details and map the wire error to the
// provider-neutral retry contract.
func TestDriverStreamSurfacesTypedError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client()})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "claude-test",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	defer func() { _ = stream.Close() }()

	event, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv() error = %v", err)
	}
	if event.Kind != provider.EventError || event.Err == nil {
		t.Fatalf("Recv() event = %#v, want terminal EventError", event)
	}
	if !strings.Contains(event.Err.Error(), "overloaded_error") {
		t.Fatalf("error = %q, want it to contain the upstream type %q", event.Err.Error(), "overloaded_error")
	}
	if !strings.Contains(event.Err.Error(), "Overloaded") {
		t.Fatalf("error = %q, want it to contain the upstream message %q", event.Err.Error(), "Overloaded")
	}
	if provider.ErrorKindOf(event.Err) != provider.ErrorServer || !provider.IsRetryableError(event.Err) {
		t.Fatalf("error classification = %q retryable=%v", provider.ErrorKindOf(event.Err), provider.IsRetryableError(event.Err))
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("Recv() after terminal error = %v, want io.EOF", err)
	}
}

func TestDriverStreamReturnsUnexpectedEOFWhenMessageStopsMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client()})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "claude-test",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	defer func() { _ = stream.Close() }()
	event, err := stream.Recv()
	if err != nil || event.Text != "partial" {
		t.Fatalf("first Recv() = %#v, %v; want partial event", event, err)
	}
	if _, err := stream.Recv(); err != io.ErrUnexpectedEOF {
		t.Fatalf("second Recv() error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestDriverStreamReturnsUnexpectedEOFForEmptyStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client()})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "claude-test",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	defer func() { _ = stream.Close() }()
	if _, err := stream.Recv(); err != io.ErrUnexpectedEOF {
		t.Fatalf("Recv() error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestDriverStreamRejectsMalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("event: message_start\ndata: {not-json}\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client()})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "claude-test",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	defer func() { _ = stream.Close() }()
	if _, err := stream.Recv(); err == nil || !strings.Contains(err.Error(), "invalid character") {
		t.Fatalf("Recv() error = %v, want malformed JSON error", err)
	}
}

func TestDriverStreamProviderStateRoundTrip(t *testing.T) {
	var calls int
	var replayed requestBody
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		writer.Header().Set("Content-Type", "text/event-stream")
		if calls == 1 {
			_, _ = writer.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-state\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":2}}}\n\n"))
			_, _ = writer.Write([]byte("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n"))
			_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"answer\"}}\n\n"))
			_, _ = writer.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"citations_delta\",\"citation\":{\"type\":\"char_location\",\"cited_text\":\"source\"}}}\n\n"))
			_, _ = writer.Write([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n"))
			_, _ = writer.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
			return
		}
		if err := json.NewDecoder(request.Body).Decode(&replayed); err != nil {
			t.Fatalf("decode replay request: %v", err)
		}
		_, _ = writer.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client()})
	first, err := driver.Stream(context.Background(), provider.Request{
		Model:    "claude-test",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
	})
	if err != nil {
		t.Fatalf("first Stream() error = %v", err)
	}
	firstEvents := collectAnthropicEvents(t, first)
	if len(firstEvents) == 0 || firstEvents[len(firstEvents)-1].Kind != provider.EventDone {
		t.Fatalf("first events = %#v", firstEvents)
	}
	state := firstEvents[len(firstEvents)-1].ProviderState
	if len(state) == 0 || !strings.Contains(string(state), `"message"`) ||
		!strings.Contains(string(state), `"citations"`) {
		t.Fatalf("provider state = %s, want message and citation state", state)
	}

	second, err := driver.Stream(context.Background(), provider.Request{
		Model: "claude-test",
		Messages: []message.Message{
			message.NewText(message.RoleUser, "hi"),
			{Role: message.RoleAssistant, ProviderState: state},
			message.NewText(message.RoleUser, "next"),
		},
	})
	if err != nil {
		t.Fatalf("second Stream() error = %v", err)
	}
	defer func() { _ = second.Close() }()
	if _, err := second.Recv(); err != nil {
		t.Fatalf("second Recv() error = %v", err)
	}
	if len(replayed.Messages) != 3 || len(replayed.Messages[1].Content) != 1 ||
		replayed.Messages[1].Content[0].Text != "answer" ||
		len(replayed.Messages[1].Content[0].Citations) != 1 {
		t.Fatalf("replayed assistant content = %#v", replayed.Messages)
	}
}
