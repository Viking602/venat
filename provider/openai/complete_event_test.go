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

func TestResponsesStream_DeliversCompleteToolCallWithoutArgumentDeltas(t *testing.T) {
	for _, complete := range []bool{false, true} {
		stream := newResponsesTestStream(`data: {"type":"response.output_item.added","output_index":0,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":""}}

data: {"type":"response.output_item.done","output_index":0,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"q\":\"x\"}"}}

data: {"type":"response.completed","response":{"output":[{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"q\":\"x\"}"}],"usage":{}}}

`)
		stream.completeToolCalls = complete
		normalized, err := provider.NormalizeEvents(collectEvents(t, stream))
		if err != nil {
			t.Fatalf("complete=%v NormalizeEvents error = %v", complete, err)
		}
		if len(normalized.ToolCalls) != 1 || normalized.ToolCalls[0].ID != "call_1" || string(normalized.ToolCalls[0].Arguments) != `{"q":"x"}` {
			t.Fatalf("complete=%v tool calls = %#v, want call_1 with arguments", complete, normalized.ToolCalls)
		}
	}
}

func TestResponsesStream_BindsDelayedCallMetadataAtOutputItemDone(t *testing.T) {
	for _, delta := range []string{`"{\"q\":\"x\"}"`, `""`} {
		for _, complete := range []bool{false, true} {
			stream := newResponsesTestStream(`data: {"type":"response.function_call_arguments.delta","output_index":0,"delta":` + delta + `}

data: {"type":"response.output_item.done","output_index":0,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"q\":\"x\"}"}}

data: {"type":"response.completed","response":{"output":[{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"q\":\"x\"}"}],"usage":{}}}

`)
			stream.completeToolCalls = complete
			events := collectEvents(t, stream)
			normalized, err := provider.NormalizeEvents(events)
			if err != nil {
				t.Fatalf("delta=%s complete=%v NormalizeEvents error = %v", delta, complete, err)
			}
			if len(normalized.ToolCalls) != 1 || normalized.ToolCalls[0].ID != "call_1" || normalized.ToolCalls[0].Name != "lookup" || string(normalized.ToolCalls[0].Arguments) != `{"q":"x"}` {
				t.Fatalf("delta=%s complete=%v tool calls = %#v, want call_1 lookup with arguments", delta, complete, normalized.ToolCalls)
			}
			if complete {
				sawComplete := false
				for _, ev := range events {
					if ev.Kind == provider.EventToolCall && ev.ToolCall != nil && ev.ToolCall.ID == "call_1" {
						sawComplete = true
					}
				}
				if !sawComplete {
					t.Fatalf("delta=%s complete=true did not emit EventToolCall: %#v", delta, events)
				}
			}
		}
	}
}
