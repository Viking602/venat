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
