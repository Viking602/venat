package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

type blockedProvider struct {
	readDone <-chan struct{}
	turn     int
}

func (p *blockedProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "streaming-example"}
}
func (p *blockedProvider) Stream(context.Context, provider.Request) (provider.Stream, error) {
	p.turn++
	if p.turn > 1 {
		return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: "overlap complete"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
	}
	return &blockedStream{readDone: p.readDone}, nil
}

type blockedStream struct {
	readDone <-chan struct{}
	step     int
}

func (s *blockedStream) Recv() (provider.Event, error) {
	s.step++
	if s.step == 1 {
		return provider.Event{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "read", Name: "read", Arguments: json.RawMessage(`{"query":"context"}`)}}, nil
	}
	if s.step == 2 {
		<-s.readDone
		return provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse}, nil
	}
	return provider.Event{}, io.EOF
}
func (*blockedStream) Close() error { return nil }

func main() {
	readDone := make(chan struct{})
	read, err := kit.Tool("read", func(context.Context, struct {
		Query string `json:"query"`
	}) (string, error) {
		close(readDone)
		return "context was read while the model stream was blocked", nil
	})
	if err != nil {
		panic(err)
	}
	engine := agent.Engine{Provider: &blockedProvider{readDone: readDone}, Tools: tool.NewBus(read), SafeStreamingTools: []string{"read"}}
	output, err := engine.RunMessages(context.Background(), agent.LoopInput{
		Messages: []message.Message{message.NewText(message.RoleUser, "read context")}, MaxIterations: 3,
		SafeStreamingTools: []string{"read"},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(output.Messages[len(output.Messages)-1].Text)
}
