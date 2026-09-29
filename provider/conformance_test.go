package provider

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/Viking602/venat/message"
)

func TestNormalizeEventsConformanceCases(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	tests := []struct {
		name      string
		events    []Event
		wantErr   error
		assertion func(t *testing.T, response NormalizedResponse)
	}{
		{
			name: "text only stream",
			events: []Event{
				{Kind: EventTextDelta, Text: "hello "},
				{Kind: EventTextDelta, Text: "world"},
				{Kind: EventDone, StopReason: StopReasonComplete},
			},
			assertion: func(t *testing.T, response NormalizedResponse) {
				if response.Text != "hello world" || response.StopReason != StopReasonComplete {
					t.Fatalf("unexpected response %#v", response)
				}
			},
		},
		{
			name: "thinking and text",
			events: []Event{
				{Kind: EventThinkingDelta, Thinking: "plan"},
				{Kind: EventTextDelta, Text: "answer"},
				{Kind: EventDone, StopReason: StopReasonComplete},
			},
			assertion: func(t *testing.T, response NormalizedResponse) {
				if response.Thinking != "plan" || response.Text != "answer" {
					t.Fatalf("unexpected response %#v", response)
				}
			},
		},
		{
			name: "full tool call event",
			events: []Event{
				{Kind: EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"venat"}`)}},
				{Kind: EventDone, StopReason: StopReasonToolUse},
			},
			assertion: func(t *testing.T, response NormalizedResponse) {
				if len(response.ToolCalls) != 1 || response.ToolCalls[0].Name != "lookup" {
					t.Fatalf("unexpected response %#v", response)
				}
			},
		},
		{
			name: "delta tool call event",
			events: []Event{
				{Kind: EventToolCallDelta, ToolCallDelta: &ToolCallDelta{ID: "call-1", Name: "lookup", ArgumentsDelta: "{\"q\":\"ve"}},
				{Kind: EventToolCallDelta, ToolCallDelta: &ToolCallDelta{ID: "call-1", ArgumentsDelta: "nat\"}"}},
				{Kind: EventDone, StopReason: StopReasonToolUse},
			},
			assertion: func(t *testing.T, response NormalizedResponse) {
				if len(response.ToolCalls) != 1 || string(response.ToolCalls[0].Arguments) != "{\"q\":\"venat\"}" {
					t.Fatalf("unexpected response %#v", response)
				}
			},
		},
		{
			name: "mixed full and delta tool event",
			events: []Event{
				{Kind: EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "lookup"}},
				{Kind: EventToolCallDelta, ToolCallDelta: &ToolCallDelta{ID: "call-1", ArgumentsDelta: "{\"q\":\"venat\"}"}},
				{Kind: EventDone, StopReason: StopReasonToolUse},
			},
			assertion: func(t *testing.T, response NormalizedResponse) {
				if len(response.ToolCalls) != 1 || response.ToolCalls[0].Name != "lookup" || string(response.ToolCalls[0].Arguments) != "{\"q\":\"venat\"}" {
					t.Fatalf("unexpected response %#v", response)
				}
			},
		},
		{
			name: "invalid json delta",
			events: []Event{
				{Kind: EventToolCallDelta, ToolCallDelta: &ToolCallDelta{ID: "call-1", Name: "lookup", ArgumentsDelta: "{\"q\":"}},
				{Kind: EventDone, StopReason: StopReasonToolUse},
			},
			wantErr: ErrInvalidToolCallArguments,
		},
		{
			name: "duplicate tool call id",
			events: []Event{
				{Kind: EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{}`)}},
				{Kind: EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{}`)}},
			},
			wantErr: ErrDuplicateToolCallID,
		},
		{
			name: "event error before done",
			events: []Event{
				{Kind: EventTextDelta, Text: "hello"},
				{Kind: EventError, Err: boom},
			},
			wantErr: boom,
		},
		{
			name: "done without usage",
			events: []Event{
				{Kind: EventTextDelta, Text: "answer"},
				{Kind: EventDone, StopReason: StopReasonComplete},
			},
			assertion: func(t *testing.T, response NormalizedResponse) {
				if response.Usage != (Usage{}) || response.StopReason != StopReasonComplete {
					t.Fatalf("unexpected response %#v", response)
				}
			},
		},
		{
			name: "text and tool mixed response",
			events: []Event{
				{Kind: EventTextDelta, Text: "first "},
				{Kind: EventToolCallDelta, ToolCallDelta: &ToolCallDelta{ID: "call-1", Name: "lookup", ArgumentsDelta: "{\"q\":\"venat\"}"}},
				{Kind: EventDone, StopReason: StopReasonToolUse, Usage: Usage{InputTokens: 3, OutputTokens: 5, TotalTokens: 8}},
			},
			assertion: func(t *testing.T, response NormalizedResponse) {
				if response.Text != "first " || len(response.ToolCalls) != 1 || response.Usage.TotalTokens != 8 {
					t.Fatalf("unexpected response %#v", response)
				}
			},
		},
		{
			name: "opaque provider state",
			events: []Event{
				{Kind: EventTextDelta, Text: "answer"},
				{Kind: EventDone, StopReason: StopReasonComplete, ProviderState: json.RawMessage(`[{"type":"reasoning","id":"rs_1"}]`)},
			},
			assertion: func(t *testing.T, response NormalizedResponse) {
				if string(response.ProviderState) != `[{"type":"reasoning","id":"rs_1"}]` {
					t.Fatalf("provider state = %s", response.ProviderState)
				}
			},
		},
		{
			name: "pause preserves terminal state and usage",
			events: []Event{
				{Kind: EventTextDelta, Text: "working"},
				{Kind: EventDone, StopReason: StopReasonPause, Usage: Usage{InputTokens: 3, OutputTokens: 4, ReasoningTokens: 2, TotalTokens: 7}, ProviderState: json.RawMessage(`[{"type":"reasoning","id":"rs_pause"}]`)},
			},
			assertion: func(t *testing.T, response NormalizedResponse) {
				if response.StopReason != StopReasonPause || response.Usage.TotalTokens != 7 ||
					response.Usage.ReasoningTokens != 2 || string(response.ProviderState) != `[{"type":"reasoning","id":"rs_pause"}]` {
					t.Fatalf("pause response = %#v", response)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := NormalizeEvents(tt.events)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("NormalizeEvents() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NormalizeEvents() error = %v", err)
			}
			tt.assertion(t, response)
		})
	}
}
