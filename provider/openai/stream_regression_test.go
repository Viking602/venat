package openai

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

func TestDriverStreamPreservesToolCallIndexAndNormalizesOpenAIStyleDeltas(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"query\\\":\\\"ve\"}}]}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"nat\\\"}\"}}]}}]}\n\n"))
		_, _ = writer.Write([]byte("data: {\"choices\":[{\"index\":0,\"finish_reason\":\"tool_calls\"}]}\n\n"))
		_, _ = writer.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client(), WireAPI: WireChatCompletions})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "gpt-test",
		Messages: []message.Message{message.NewText(message.RoleUser, "hello")},
	})
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	events := collectEvents(t, stream)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %#v", events)
	}
	if events[0].Kind != provider.EventToolCallDelta || events[0].ToolCallDelta == nil {
		t.Fatalf("expected first tool call delta, got %#v", events[0])
	}
	if events[0].ToolCallDelta.Index == nil || *events[0].ToolCallDelta.Index != 0 {
		t.Fatalf("expected first delta index 0, got %#v", events[0].ToolCallDelta)
	}
	if events[1].Kind != provider.EventToolCallDelta || events[1].ToolCallDelta == nil {
		t.Fatalf("expected second tool call delta, got %#v", events[1])
	}
	if events[1].ToolCallDelta.Index == nil || *events[1].ToolCallDelta.Index != 0 {
		t.Fatalf("expected second delta index 0, got %#v", events[1].ToolCallDelta)
	}
	if events[1].ToolCallDelta.ID != "" {
		t.Fatalf("expected second delta to omit id, got %#v", events[1].ToolCallDelta)
	}

	normalized, err := provider.NormalizeEvents(events)
	if err != nil {
		t.Fatalf("NormalizeEvents() error = %v", err)
	}
	if len(normalized.ToolCalls) != 1 {
		t.Fatalf("expected 1 normalized tool call, got %#v", normalized.ToolCalls)
	}
	if normalized.ToolCalls[0].ID != "call-1" || normalized.ToolCalls[0].Name != "lookup" {
		t.Fatalf("expected normalized tool call metadata, got %#v", normalized.ToolCalls[0])
	}
	if string(normalized.ToolCalls[0].Arguments) != `{"query":"venat"}` {
		t.Fatalf("expected normalized arguments, got %#v", normalized.ToolCalls[0])
	}
}

func TestDriverStreamNormalizesIndexlessToolCallDeltas(t *testing.T) {
	stream := newChatCompletionTestStream(t,
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"query\\\":\"}}]}}]}\n\n"+
			"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"function\":{\"arguments\":\"\\\"venat\\\"}\"}}]}}]}\n\n"+
			"data: {\"choices\":[{\"finish_reason\":\"tool_calls\"}]}\n\n"+
			"data: [DONE]\n\n",
	)
	normalized, err := provider.NormalizeEvents(collectEvents(t, stream))
	if err != nil {
		t.Fatalf("NormalizeEvents() error = %v", err)
	}
	if len(normalized.ToolCalls) != 1 || normalized.ToolCalls[0].ID != "call-1" || string(normalized.ToolCalls[0].Arguments) != `{"query":"venat"}` {
		t.Fatalf("tool calls = %#v, want one merged call-1", normalized.ToolCalls)
	}

	multi := newChatCompletionTestStream(t,
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"id\":\"call-1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"a\\\":1}\"}},{\"id\":\"call-2\",\"function\":{\"name\":\"fetch\",\"arguments\":\"{\\\"b\\\":2}\"}}]}}]}\n\n"+
			"data: {\"choices\":[{\"finish_reason\":\"tool_calls\"}]}\n\n"+
			"data: [DONE]\n\n",
	)
	normalizedMulti, err := provider.NormalizeEvents(collectEvents(t, multi))
	if err != nil {
		t.Fatalf("NormalizeEvents(multi) error = %v", err)
	}
	if len(normalizedMulti.ToolCalls) != 2 || normalizedMulti.ToolCalls[0].ID != "call-1" || normalizedMulti.ToolCalls[1].ID != "call-2" {
		t.Fatalf("multi tool calls = %#v, want call-1 and call-2", normalizedMulti.ToolCalls)
	}
}

func TestDriverStreamRejectsNon2xxStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client(), WireAPI: WireChatCompletions})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "gpt-test",
		Messages: []message.Message{message.NewText(message.RoleUser, "hello")},
	})
	if err == nil {
		if stream != nil {
			_ = stream.Close()
		}
		t.Fatal("expected non-2xx response to fail before streaming")
	}
}

func TestDriverStreamRejectsUnexpectedContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"message":"not an sse stream"}`))
	}))
	defer server.Close()

	driver := New(Config{APIKey: "test", BaseURL: server.URL, Client: server.Client(), WireAPI: WireChatCompletions})
	stream, err := driver.Stream(context.Background(), provider.Request{
		Model:    "gpt-test",
		Messages: []message.Message{message.NewText(message.RoleUser, "hello")},
	})
	if err == nil {
		if stream != nil {
			_ = stream.Close()
		}
		t.Fatal("expected non-SSE content type to fail before streaming")
	}
}
