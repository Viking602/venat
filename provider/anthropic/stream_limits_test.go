package anthropic

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/provider/shared"
)

func limitedAnthropicStream(data string) *anthropicStream {
	body := io.NopCloser(strings.NewReader(data))
	return &anthropicStream{body: body, state: streamState{
		reader: shared.NewReader(body), blocks: make(map[int]contentBlock), toolCalls: make(map[int]provider.ToolCallDelta), emittedToolCalls: make(map[int]bool),
	}}
}

func TestAnthropicStream_BoundsSilentBlocksAndCitations(t *testing.T) {
	var frames strings.Builder
	for i := 0; i <= shared.MaxStreamItems; i++ {
		fmt.Fprintf(&frames, "data: {\"type\":\"content_block_start\",\"index\":%d,\"content_block\":{\"type\":\"text\"}}\n\n", i)
	}
	stream := limitedAnthropicStream(frames.String())
	if _, err := stream.Recv(); err == nil || !strings.Contains(err.Error(), "limit") || len(stream.state.blocks) != shared.MaxStreamItems {
		t.Fatalf("silent blocks: error=%v blocks=%d", err, len(stream.state.blocks))
	}
	stream = limitedAnthropicStream(strings.Repeat("data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"citations_delta\",\"citation\":{}}}\n\n", shared.MaxStreamItems+1))
	if _, err := stream.Recv(); err == nil || !strings.Contains(err.Error(), "limit") || len(stream.state.blocks[0].Citations) != shared.MaxStreamItems {
		t.Fatalf("silent citations: error=%v citations=%d", err, len(stream.state.blocks[0].Citations))
	}
}
