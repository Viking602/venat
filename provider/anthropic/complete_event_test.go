package anthropic

import (
	"encoding/json"
	"testing"

	"github.com/Viking602/venat/provider"
)

func TestAnthropicStream_EmitsCompleteToolCallAtBlockStop(t *testing.T) {
	stream := anthropicStream{state: streamState{
		toolCalls:         map[int]provider.ToolCallDelta{0: {ID: "call-1", Name: "lookup"}},
		blocks:            map[int]contentBlock{0: {Type: "tool_use", Input: json.RawMessage(`{"q":"ok"}`)}},
		emittedToolCalls:  map[int]bool{},
		completeToolCalls: true,
	}}
	_, _, err := stream.consume(eventEnvelope{Type: "content_block_stop", Index: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(stream.state.pending) != 1 || stream.state.pending[0].Kind != provider.EventToolCall || stream.state.pending[0].ToolCall == nil {
		t.Fatalf("pending=%#v", stream.state.pending)
	}
}

func TestAnthropicStream_StopBeforeStartKeepsOneProviderStateBlock(t *testing.T) {
	stream := limitedAnthropicStream(`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":"first"}}

data: {"type":"content_block_stop","index":1}

data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"call-1","name":"lookup","input":{}}}

data: {"type":"content_block_stop","index":9}

data: {"type":"message_stop"}

`)
	stream.state.completeToolCalls = true
	var done provider.Event
	for {
		event, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv error = %v", err)
		}
		if event.Kind == provider.EventDone {
			done = event
			break
		}
	}
	var state struct {
		Content []json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(done.ProviderState, &state); err != nil {
		t.Fatalf("provider state %s: %v", done.ProviderState, err)
	}
	if len(state.Content) != 2 {
		t.Fatalf("provider state content = %s, want text then one tool_use", done.ProviderState)
	}
}

func TestAnthropicStream_DeltaBeforeStartMergesByIndex(t *testing.T) {
	stream := limitedAnthropicStream(`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":\"ok\"}"}}

data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call-1","name":"lookup","input":{}}}

data: {"type":"content_block_stop","index":0}

data: {"type":"message_delta","delta":{"stop_reason":"tool_use"}}

data: {"type":"message_stop"}

`)
	var events []provider.Event
	for {
		event, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv error = %v", err)
		}
		events = append(events, event)
		if event.Kind == provider.EventDone {
			break
		}
	}
	normalized, err := provider.NormalizeEvents(events)
	if err != nil {
		t.Fatalf("NormalizeEvents error = %v", err)
	}
	if len(normalized.ToolCalls) != 1 || normalized.ToolCalls[0].ID != "call-1" || string(normalized.ToolCalls[0].Arguments) != `{"q":"ok"}` {
		t.Fatalf("tool calls = %#v, want merged call-1", normalized.ToolCalls)
	}
}
