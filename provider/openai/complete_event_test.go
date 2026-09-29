package openai

import (
	"encoding/json"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

func TestChatStream_EmitsCompleteToolCallOnlyAfterArgumentsFinish(t *testing.T) {
	state := streamState{
		toolCalls:         make(map[int]*message.ToolCall),
		emittedToolCalls:  make(map[int]bool),
		completeToolCalls: true,
	}
	stream := openAIStream{state: state}
	index := 0
	choice := choiceChunk{}
	choice.Delta.ToolCalls = []toolCallDeltaItem{{Index: &index, ID: "call-1", Function: struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}{Name: "lookup", Arguments: `{"q":"ok"}`}}}
	choice.FinishReason = "tool_calls"
	stream.processChoiceDelta(choice)
	var complete *message.ToolCall
	for _, event := range stream.state.pending {
		if event.Kind == provider.EventToolCall {
			complete = event.ToolCall
		}
	}
	if complete == nil || complete.ID != "call-1" || !json.Valid(complete.Arguments) {
		t.Fatalf("complete event=%#v, pending=%#v", complete, stream.state.pending)
	}
}

func TestResponsesStream_EmitsCompleteToolCallAtOutputItemDone(t *testing.T) {
	stream := responsesStream{items: map[int]*responsesOutputState{0: {callID: "call-1", name: "lookup"}}, completeToolCalls: true}
	event := stream.outputItemDone(0, responsesOutputItem{Type: "function_call", Arguments: `{"q":"ok"}`})
	if event.Kind != provider.EventToolCall || event.ToolCall == nil || event.ToolCall.ID != "call-1" {
		t.Fatalf("event=%#v", event)
	}
}
