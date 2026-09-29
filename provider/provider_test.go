package provider

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/Viking602/venat/message"
)

func TestUsageAdd(t *testing.T) {
	tests := []struct {
		name           string
		u1             Usage
		u2             Usage
		wantInput      int
		wantCached     int
		wantCacheWrite int
		wantOutput     int
		wantTotal      int
	}{
		{
			name:           "add two usages",
			u1:             Usage{InputTokens: 10, CachedInputTokens: 4, CacheWriteInputTokens: 2, OutputTokens: 20, TotalTokens: 30},
			u2:             Usage{InputTokens: 5, CachedInputTokens: 3, CacheWriteInputTokens: 1, OutputTokens: 10, TotalTokens: 15},
			wantInput:      15,
			wantCached:     7,
			wantCacheWrite: 3,
			wantOutput:     30,
			wantTotal:      45,
		},
		{
			name:           "negative counters cannot reduce accumulated usage",
			u1:             Usage{InputTokens: 10, CachedInputTokens: 4, CacheWriteInputTokens: 3, OutputTokens: 5, TotalTokens: 15},
			u2:             Usage{InputTokens: -20, CachedInputTokens: -3, CacheWriteInputTokens: -1, OutputTokens: -5, TotalTokens: -28},
			wantInput:      10,
			wantCached:     4,
			wantCacheWrite: 3,
			wantOutput:     5,
			wantTotal:      15,
		},
		{
			name:           "missing total is derived from input and output",
			u1:             Usage{},
			u2:             Usage{InputTokens: 7, CachedInputTokens: 9, CacheWriteInputTokens: 8, OutputTokens: 3},
			wantInput:      7,
			wantCached:     7,
			wantCacheWrite: 7,
			wantOutput:     3,
			wantTotal:      10,
		},
		{
			name:           "near-limit counters saturate instead of wrapping",
			u1:             Usage{InputTokens: int(^uint(0)>>1) - 2, CacheWriteInputTokens: int(^uint(0)>>1) - 1, OutputTokens: int(^uint(0)>>1) - 3},
			u2:             Usage{InputTokens: 10, CacheWriteInputTokens: 10, OutputTokens: 10},
			wantInput:      int(^uint(0) >> 1),
			wantCached:     0,
			wantCacheWrite: int(^uint(0) >> 1),
			wantOutput:     int(^uint(0) >> 1),
			wantTotal:      int(^uint(0) >> 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := tt.u1.Add(tt.u2)

			if u.InputTokens != tt.wantInput {
				t.Errorf("InputTokens = %v, want %v", u.InputTokens, tt.wantInput)
			}
			if u.CachedInputTokens != tt.wantCached {
				t.Errorf("CachedInputTokens = %v, want %v", u.CachedInputTokens, tt.wantCached)
			}
			if u.CacheWriteInputTokens != tt.wantCacheWrite {
				t.Errorf("CacheWriteInputTokens = %v, want %v", u.CacheWriteInputTokens, tt.wantCacheWrite)
			}
			if u.OutputTokens != tt.wantOutput {
				t.Errorf("OutputTokens = %v, want %v", u.OutputTokens, tt.wantOutput)
			}
			if u.TotalTokens != tt.wantTotal {
				t.Errorf("TotalTokens = %v, want %v", u.TotalTokens, tt.wantTotal)
			}
		})
	}
}

func TestUsageAddPreservesReportedZeroCacheAndReasoning(t *testing.T) {
	usage := (Usage{
		InputTokens:               10,
		CachedInputTokensReported: true,
		OutputTokens:              6,
		ReasoningTokens:           4,
	}).Add(Usage{
		InputTokens:                   5,
		CacheWriteInputTokensReported: true,
		OutputTokens:                  2,
		ReasoningTokens:               1,
	})
	if !usage.CachedInputTokensReported || !usage.CacheWriteInputTokensReported {
		t.Fatalf("reported cache flags were lost: %#v", usage)
	}
	if usage.ReasoningTokens != 5 || usage.OutputTokens != 8 {
		t.Fatalf("reasoning/output usage = %#v", usage)
	}
}

func TestNewSliceStream(t *testing.T) {
	events := []Event{
		{Kind: EventTextDelta, Text: "Hello"},
		{Kind: EventTextDelta, Text: " World"},
		{Kind: EventDone},
	}

	stream := NewSliceStream(events)
	if stream == nil {
		t.Fatal("NewSliceStream() returned nil")
	}

	// Test receiving all events
	for i, want := range events {
		got, err := stream.Recv()
		if err != nil {
			t.Errorf("Recv() at %d error = %v", i, err)
			continue
		}
		if got.Kind != want.Kind {
			t.Errorf("Event %d Kind = %v, want %v", i, got.Kind, want.Kind)
		}
		if got.Text != want.Text {
			t.Errorf("Event %d Text = %v, want %v", i, got.Text, want.Text)
		}
	}

	// Test EOF after all events
	_, err := stream.Recv()
	if !errors.Is(err, io.EOF) {
		t.Errorf("After all events, Recv() error = %v, want io.EOF", err)
	}

	// Test Close
	if err := stream.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestNewSliceStreamEmpty(t *testing.T) {
	stream := NewSliceStream([]Event{})

	_, err := stream.Recv()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Recv() error = %v, want io.EOF", err)
	}
}

func TestEventStruct(t *testing.T) {
	toolCall := &message.ToolCall{
		ID:   "call-1",
		Name: "search",
	}

	toolCallDelta := &ToolCallDelta{
		ID:             "call-1",
		Name:           "search",
		ArgumentsDelta: "{\"q\":\"test\"}",
	}

	event := Event{
		Kind:          EventToolCall,
		Text:          "",
		Thinking:      "thought",
		ToolCall:      toolCall,
		ToolCallDelta: toolCallDelta,
		Usage:         Usage{InputTokens: 10},
		StopReason:    StopReasonComplete,
		Err:           nil,
	}

	if event.Kind != EventToolCall {
		t.Errorf("Kind = %v, want %v", event.Kind, EventToolCall)
	}
	if event.Thinking != "thought" {
		t.Errorf("Thinking = %v, want thought", event.Thinking)
	}
	if event.ToolCall == nil {
		t.Error("ToolCall should not be nil")
	}
	if event.ToolCallDelta == nil {
		t.Error("ToolCallDelta should not be nil")
	}
	if event.StopReason != StopReasonComplete {
		t.Errorf("StopReason = %v, want %v", event.StopReason, StopReasonComplete)
	}
}

func TestRequestStruct(t *testing.T) {
	tools := []message.ToolDefinition{
		{Name: "search"},
	}

	req := Request{
		Model:          "gpt-4",
		Messages:       []message.Message{{Role: message.RoleUser, Text: "Hello"}},
		Tools:          tools,
		Metadata:       map[string]string{"key": "value"},
		StopSequences:  []string{"STOP"},
		ThinkingBudget: 1000,
		ResponseFormat: &ResponseFormat{
			Type: "json_object",
		},
	}

	if req.Model != "gpt-4" {
		t.Errorf("Model = %v, want gpt-4", req.Model)
	}
	if len(req.Messages) != 1 {
		t.Errorf("len(Messages) = %v, want 1", len(req.Messages))
	}
	if len(req.Tools) != 1 {
		t.Errorf("len(Tools) = %v, want 1", len(req.Tools))
	}
	if req.ThinkingBudget != 1000 {
		t.Errorf("ThinkingBudget = %v, want 1000", req.ThinkingBudget)
	}
	if req.ResponseFormat == nil || req.ResponseFormat.Type != "json_object" {
		t.Errorf("ResponseFormat = %#v, want json_object", req.ResponseFormat)
	}
}

func TestMetadataStruct(t *testing.T) {
	meta := Metadata{
		Name:    "openai",
		Models:  []string{"gpt-4", "gpt-3.5-turbo"},
		Version: "1.0.0",
	}

	if meta.Name != "openai" {
		t.Errorf("Name = %v, want openai", meta.Name)
	}
	if len(meta.Models) != 2 {
		t.Errorf("len(Models) = %v, want 2", len(meta.Models))
	}
	if meta.Version != "1.0.0" {
		t.Errorf("Version = %v, want 1.0.0", meta.Version)
	}
}

func TestStopReasonConstants(t *testing.T) {
	reasons := []StopReason{
		StopReasonUnknown,
		StopReasonComplete,
		StopReasonPause,
		StopReasonToolUse,
		StopReasonLength,
		StopReasonContentFilter,
		StopReasonMaxTurns,
		StopReasonAborted,
		StopReasonError,
	}
	expected := []string{"unknown", "complete", "pause", "tool_use", "length", "content_filter", "max_turns", "aborted", "error"}

	for i, reason := range reasons {
		if string(reason) != expected[i] {
			t.Errorf("StopReason %d = %v, want %v", i, reason, expected[i])
		}
	}
}

func TestEventKindConstants(t *testing.T) {
	kinds := []EventKind{
		EventTextDelta,
		EventThinkingDelta,
		EventToolCallDelta,
		EventToolCall,
		EventDone,
		EventError,
	}
	expected := []string{
		"text_delta",
		"thinking_delta",
		"tool_call_delta",
		"tool_call",
		"done",
		"error",
	}

	for i, kind := range kinds {
		if string(kind) != expected[i] {
			t.Errorf("EventKind %d = %v, want %v", i, kind, expected[i])
		}
	}
}

func TestToolCallDeltaStruct(t *testing.T) {
	delta := ToolCallDelta{
		ID:             "call-1",
		Name:           "search",
		ArgumentsDelta: "{\"query\":\"test\"}",
	}

	if delta.ID != "call-1" {
		t.Errorf("ID = %v, want call-1", delta.ID)
	}
	if delta.Name != "search" {
		t.Errorf("Name = %v, want search", delta.Name)
	}
	if delta.ArgumentsDelta != "{\"query\":\"test\"}" {
		t.Errorf("ArgumentsDelta = %v, want {\"query\":\"test\"}", delta.ArgumentsDelta)
	}
}

// MockDriver is a test implementation of Driver

type MockDriver struct {
	metadata Metadata
	events   []Event
	err      error
}

func (m *MockDriver) Metadata() Metadata {
	return m.metadata
}

func (m *MockDriver) Stream(_ context.Context, _ Request) (Stream, error) {
	if m.err != nil {
		return nil, m.err
	}
	return NewSliceStream(m.events), nil
}

func TestDriverInterface(t *testing.T) {
	driver := &MockDriver{
		metadata: Metadata{Name: "test"},
		events:   []Event{{Kind: EventDone}},
	}

	if driver.Metadata().Name != "test" {
		t.Errorf("Metadata().Name = %v, want test", driver.Metadata().Name)
	}

	stream, err := driver.Stream(context.Background(), Request{})
	if err != nil {
		t.Errorf("Stream() error = %v", err)
	}

	event, err := stream.Recv()
	if err != nil {
		t.Errorf("Recv() error = %v", err)
	}
	if event.Kind != EventDone {
		t.Errorf("Event.Kind = %v, want %v", event.Kind, EventDone)
	}
}

func TestProviderErrorClassification(t *testing.T) {
	tests := []struct {
		status    int
		kind      ErrorKind
		retryable bool
	}{
		{status: 400, kind: ErrorInvalidRequest},
		{status: 401, kind: ErrorAuthentication},
		{status: 403, kind: ErrorPermission},
		{status: 404, kind: ErrorNotFound},
		{status: 429, kind: ErrorRateLimit, retryable: true},
		{status: 503, kind: ErrorServer, retryable: true},
	}
	for _, test := range tests {
		failure := NewHTTPError("test", test.status, "failed")
		wrapped := errors.Join(errors.New("request failed"), failure)
		if got := ErrorKindOf(wrapped); got != test.kind {
			t.Fatalf("status %d kind = %q, want %q", test.status, got, test.kind)
		}
		if got := IsRetryableError(wrapped); got != test.retryable {
			t.Fatalf("status %d retryable = %v, want %v", test.status, got, test.retryable)
		}
	}
}
