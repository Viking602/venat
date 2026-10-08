package openai

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/provider/shared"
)

func limitedChatStream(data string) *openAIStream {
	body := io.NopCloser(strings.NewReader(data))
	return &openAIStream{body: body, state: streamState{
		reader: shared.NewReader(body), toolCalls: make(map[int]*message.ToolCall), emittedToolCalls: make(map[int]bool),
	}}
}

func TestChatStream_RejectsDenseArraysAndOversizedNames(t *testing.T) {
	for _, data := range []string{
		`{"choices":[` + strings.Repeat("{},", shared.MaxStreamItems) + `{}]}`,
		`{"choices":[{"delta":{"tool_calls":[` + strings.Repeat("{},", shared.MaxStreamItems) + `{}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"function":{"name":"` + strings.Repeat("a", message.MaxToolNameBytes+1) + `"}}]}}]}`,
	} {
		stream := limitedChatStream("data: " + data + "\n\n")
		if event, err := stream.Recv(); err == nil || event.Kind != "" || len(stream.state.pending) != 0 || len(stream.state.toolCalls) != 0 {
			t.Fatalf("unbounded frame allocated state: event=%s error=%v", event.Kind, err)
		}
	}
}

func TestChatStream_CountsSilentFrames(t *testing.T) {
	stream := limitedChatStream(strings.Repeat("data: {}\n\n", shared.MaxStreamFrames+1))
	if _, err := stream.Recv(); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("silent stream = %v", err)
	}
}

func TestResponsesStream_BoundsSilentStateAndTerminalArrays(t *testing.T) {
	var frames strings.Builder
	for i := 0; i <= shared.MaxStreamItems; i++ {
		fmt.Fprintf(&frames, "data: {\"type\":\"response.output_item.added\",\"output_index\":%d,\"item\":{\"type\":\"message\"}}\n\n", i)
	}
	body := io.NopCloser(strings.NewReader(frames.String()))
	stream := &responsesStream{body: body, reader: shared.NewReader(body), items: make(map[int]*responsesOutputState)}
	if _, err := stream.Recv(); err == nil || !strings.Contains(err.Error(), "limit") || len(stream.items) != shared.MaxStreamItems {
		t.Fatalf("silent output: error=%v items=%d", err, len(stream.items))
	}
	terminal := `{"type":"response.completed","response":{"output":[` + strings.Repeat("{},", shared.MaxStreamItems) + `{}]}}`
	body = io.NopCloser(strings.NewReader("data: " + terminal + "\n\n"))
	stream = &responsesStream{body: body, reader: shared.NewReader(body), items: make(map[int]*responsesOutputState)}
	if event, err := stream.Recv(); err == nil || event.Kind == provider.EventDone {
		t.Fatalf("dense terminal output accepted: event=%s error=%v", event.Kind, err)
	}
}
