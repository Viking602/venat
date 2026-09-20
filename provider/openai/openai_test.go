package openai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

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

func TestDriver_RawOutputSchemaReachesBothHTTPProtocols(t *testing.T) {
	for _, api := range []WireAPI{WireChatCompletions, WireResponses} {
		t.Run(string(api), func(t *testing.T) {
			var captured map[string]json.RawMessage
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
					t.Error(err)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: [DONE]\n\n"))
			}))
			defer server.Close()
			schema := json.RawMessage(`{"type":"object","properties":{"n":{"type":"integer","enum":[1,9007199254740993]}}}`)
			driver := New(Config{APIKey: "test-key", BaseURL: server.URL, WireAPI: api})
			stream, err := driver.Stream(context.Background(), provider.Request{
				Model: "test", Messages: []message.Message{message.NewText(message.RoleUser, "score")},
				ResponseFormat: &provider.ResponseFormat{Type: "json_schema", Name: "score", RawSchema: schema, Schema: &message.JSONSchema{Type: "string"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = stream.Close() }()
			var outer, format map[string]json.RawMessage
			if api == WireChatCompletions {
				_ = json.Unmarshal(captured["response_format"], &outer)
				_ = json.Unmarshal(outer["json_schema"], &format)
			} else {
				_ = json.Unmarshal(captured["text"], &outer)
				_ = json.Unmarshal(outer["format"], &format)
			}
			var gotSchema, wantSchema map[string]any
			_ = json.Unmarshal(format["schema"], &gotSchema)
			_ = json.Unmarshal(schema, &wantSchema)
			if !reflect.DeepEqual(gotSchema, wantSchema) {
				t.Fatalf("wire schema=%s, want %s", format["schema"], schema)
			}
			if !strings.Contains(string(format["schema"]), "9007199254740993") {
				t.Fatal("large integer enum lost precision")
			}
		})
	}
}

func TestDriverStreamParsesChatCompletionSSE(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/chat/completions" {
			t.Fatalf("unexpected path %s", request.URL.Path)
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: {\"id\":\"chat-1\",\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello \"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"world\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"query\\\":\\\"ve\"}}]}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"nat\\\"}\"}}]}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"finish_reason\":\"tool_calls\"}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":5,\"total_tokens\":8,\"prompt_tokens_details\":{\"cached_tokens\":2,\"cache_write_tokens\":1},\"completion_tokens_details\":{\"reasoning_tokens\":3}}}\n\n"))
		_, _ = writer.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	driver := New(Config{
		APIKey:  "test",
		BaseURL: server.URL,
		Client:  server.Client(),
		WireAPI: WireChatCompletions,
	})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model: "gpt-test",
		Messages: []message.Message{
			message.NewText(message.RoleUser, "hello"),
		},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	events := collectEvents(t, stream)
	if len(events) < 5 {
		t.Fatalf("expected streamed events, got %#v", events)
	}
	if events[0].Kind != provider.EventTextDelta || events[0].Text != "Hello " {
		t.Fatalf("unexpected first event %#v", events[0])
	}
	if events[2].Kind != provider.EventToolCallDelta || events[2].ToolCallDelta.Name != "lookup" {
		t.Fatalf("expected tool call delta, got %#v", events[2])
	}
	last := events[len(events)-1]
	if last.Kind != provider.EventDone || last.StopReason != provider.StopReasonToolUse {
		t.Fatalf("expected tool-use done event, got %#v", last)
	}
	if last.Usage.TotalTokens != 8 || last.Usage.CachedInputTokens != 2 || !last.Usage.CachedInputTokensReported ||
		last.Usage.CacheWriteInputTokens != 1 || !last.Usage.CacheWriteInputTokensReported || last.Usage.ReasoningTokens != 3 {
		t.Fatalf("expected usage in final event, got %#v", last)
	}
	if last.Response.ID != "chat-1" || last.Response.Model != "gpt-test" {
		t.Fatalf("response metadata = %#v", last.Response)
	}
}

func TestDriverStreamExtractsReasoningContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"let me think\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\" harder\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = writer.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client(), WireAPI: WireChatCompletions})
	stream, err := driver.Stream(context.Background(), provider.Request{Model: "qwen", Messages: []message.Message{message.NewText(message.RoleUser, "hi")}})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	events := collectEvents(t, stream)
	var thinking, text string
	for _, ev := range events {
		switch ev.Kind {
		case provider.EventThinkingDelta:
			thinking += ev.Thinking
		case provider.EventTextDelta:
			text += ev.Text
		}
	}
	if thinking != "let me think harder" {
		t.Fatalf("thinking = %q, want %q", thinking, "let me think harder")
	}
	if text != "answer" {
		t.Fatalf("text = %q, want %q", text, "answer")
	}
}

func TestDriverStreamExtractsInlineThinkTags(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		// Split "<think>" across two chunks to exercise the cross-chunk buffer.
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"pre <thi\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"nk>hidden\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\" thoughts</thi\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"nk> visible\"}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = writer.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client(), WireAPI: WireChatCompletions})
	stream, err := driver.Stream(context.Background(), provider.Request{Model: "qwen", Messages: []message.Message{message.NewText(message.RoleUser, "hi")}})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	events := collectEvents(t, stream)
	var thinking, text string
	for _, ev := range events {
		switch ev.Kind {
		case provider.EventThinkingDelta:
			thinking += ev.Thinking
		case provider.EventTextDelta:
			text += ev.Text
		}
	}
	if thinking != "hidden thoughts" {
		t.Fatalf("thinking = %q, want %q", thinking, "hidden thoughts")
	}
	if text != "pre  visible" {
		t.Fatalf("text = %q, want %q", text, "pre  visible")
	}
}

func TestDriverStreamForwardsStopAndReasoning(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_ = json.NewDecoder(request.Body).Decode(&captured)
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client(), WireAPI: WireChatCompletions})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:          "gpt-5.4",
		Messages:       []message.Message{message.NewText(message.RoleUser, "hi")},
		Temperature:    0.3,
		TopP:           0.7,
		MaxTokens:      456,
		StopSequences:  []string{"Wait,", "Actually,"},
		ThinkingBudget: 5000,
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	_ = collectEvents(t, stream)

	stop, _ := captured["stop"].([]any)
	if len(stop) != 2 || stop[0] != "Wait," {
		t.Fatalf("expected stop sequences forwarded, got %#v", captured["stop"])
	}
	reasoning, _ := captured["reasoning"].(map[string]any)
	if reasoning["effort"] != "medium" {
		t.Fatalf("expected reasoning effort medium for budget=5000, got %#v", reasoning)
	}
	if captured["temperature"] != 0.3 || captured["top_p"] != 0.7 || captured["max_tokens"] != 456.0 {
		t.Fatalf("expected model policy forwarded, got %#v", captured)
	}
}

func TestDriverStreamForwardsStructuredResponseFormat(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_ = json.NewDecoder(request.Body).Decode(&captured)
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client(), WireAPI: WireChatCompletions})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "gpt-5.4",
		Messages: []message.Message{message.NewText(message.RoleUser, "hi")},
		ResponseFormat: &provider.ResponseFormat{
			Type:   "json_schema",
			Name:   "report",
			Strict: true,
			Schema: &message.JSONSchema{
				Type: "object",
				Properties: map[string]message.JSONSchema{
					"report": {Type: "object"},
				},
				Required: []string{"report"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	_ = collectEvents(t, stream)

	responseFormat, _ := captured["response_format"].(map[string]any)
	if responseFormat["type"] != "json_schema" {
		t.Fatalf("expected response_format.type json_schema, got %#v", responseFormat)
	}
	schemaEnvelope, _ := responseFormat["json_schema"].(map[string]any)
	if schemaEnvelope["name"] != "report" || schemaEnvelope["strict"] != true {
		t.Fatalf("expected json_schema envelope fields, got %#v", schemaEnvelope)
	}
}

func TestDriverStreamForwardsExtraBody(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_ = json.NewDecoder(request.Body).Decode(&captured)
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client(), WireAPI: WireChatCompletions})
	parallel := true
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:             "qwen",
		Messages:          []message.Message{message.NewText(message.RoleUser, "hi")},
		PromptCacheKey:    "typed-cache",
		ServiceTier:       "priority",
		ParallelToolCalls: &parallel,
		ExtraBody: map[string]any{
			"chat_template_kwargs": map[string]any{"thinking": true},
			"temperature":          0.2,
			"stream":               false,
			"prompt_cache_key":     "untyped-cache",
			"service_tier":         "default",
			"parallel_tool_calls":  false,
		},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	_ = collectEvents(t, stream)

	requireOpenAIExtraBody(t, captured)
}

func requireOpenAIExtraBody(t *testing.T, captured map[string]any) {
	t.Helper()
	requireChatTemplateThinkingEnabled(t, captured)
	requireCapturedField(t, captured, "temperature", 0.2)
	requireCapturedField(t, captured, "stream", true)
	requireCapturedField(t, captured, "prompt_cache_key", "typed-cache")
	requireCapturedField(t, captured, "service_tier", "priority")
	requireCapturedField(t, captured, "parallel_tool_calls", true)
}

func requireChatTemplateThinkingEnabled(t *testing.T, captured map[string]any) {
	t.Helper()
	extra, _ := captured["chat_template_kwargs"].(map[string]any)
	if extra["thinking"] != true {
		t.Fatalf("expected chat_template_kwargs forwarded, got %#v", captured["chat_template_kwargs"])
	}
}

func requireCapturedField(t *testing.T, captured map[string]any, key string, want any) {
	t.Helper()
	if captured[key] != want {
		t.Fatalf("expected %s=%#v, got %#v", key, want, captured[key])
	}
}

func collectEvents(t *testing.T, stream provider.Stream) []provider.Event {
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

func TestChatMessagesPreserveCanonicalMultimodalContent(t *testing.T) {
	items, err := toChatMessages([]message.Message{{
		Role: message.RoleUser,
		Content: []message.ContentPart{
			message.CommentaryPart("inspect"),
			{Kind: message.ContentImage, Data: []byte{1, 2, 3}, MediaType: "image/png"},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	blocks, ok := items[0].Content.([]chatContentBlock)
	if !ok || len(blocks) != 2 || blocks[0].Text != "inspect" ||
		blocks[1].ImageURL == nil || blocks[1].ImageURL.URL != "data:image/png;base64,AQID" {
		t.Fatalf("chat multimodal content = %#v", items[0].Content)
	}
}
