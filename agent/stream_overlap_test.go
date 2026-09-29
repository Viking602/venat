package agent

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

type blockedOverlapProvider struct {
	readDone <-chan struct{}
	mu       sync.Mutex
	calls    int
}

func (p *blockedOverlapProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "blocked-overlap"}
}
func (p *blockedOverlapProvider) Stream(context.Context, provider.Request) (provider.Stream, error) {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.mu.Unlock()
	if call > 1 {
		return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: "finished"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
	}
	return &blockedOverlapStream{readDone: p.readDone}, nil
}

type blockedOverlapStream struct {
	readDone <-chan struct{}
	step     int
}

func (s *blockedOverlapStream) Recv() (provider.Event, error) {
	s.step++
	switch s.step {
	case 1:
		return provider.Event{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "read-1", Name: "lookup", Arguments: json.RawMessage(`{"query":"venat"}`)}}, nil
	case 2:
		<-s.readDone
		return provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse}, nil
	default:
		return provider.Event{}, io.EOF
	}
}
func (*blockedOverlapStream) Close() error { return nil }

func TestRunMessages_OverlapsOnlyOptedInCompleteRead(t *testing.T) {
	readDone := make(chan struct{})
	providerDriver := &blockedOverlapProvider{readDone: readDone}
	read, err := kit.Tool("lookup", func(context.Context, struct {
		Query string `json:"query"`
	}) (string, error) {
		close(readDone)
		return "read result", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	output, runErr := (Engine{Provider: providerDriver, Tools: tool.NewBus(read), SafeStreamingTools: []string{"lookup"}}).RunMessages(context.Background(), LoopInput{
		Messages: []message.Message{message.NewText(message.RoleUser, "read")}, MaxIterations: 3, SafeStreamingTools: []string{"lookup"},
	})
	if runErr != nil || output.StopReason != provider.StopReasonComplete || output.ToolCallsUsed != 1 {
		t.Fatalf("output=%+v err=%v", output, runErr)
	}
	if providerDriver.calls != 2 {
		t.Fatalf("provider calls=%d, want 2", providerDriver.calls)
	}
}
