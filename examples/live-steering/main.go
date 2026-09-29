package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/provider"
)

type steeringProvider struct {
	started chan struct{}
	once    sync.Once
}

func (p *steeringProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "live-steering-example"}
}

func (p *steeringProvider) Stream(ctx context.Context, request provider.Request) (provider.Stream, error) {
	last := request.Messages[len(request.Messages)-1]
	if last.Text == "correct the answer to blue" {
		return provider.NewSliceStream([]provider.Event{
			{Kind: provider.EventTextDelta, Text: "The answer is blue."},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		}), nil
	}
	p.once.Do(func() { close(p.started) })
	return &blockedStream{ctx: ctx}, nil
}

type blockedStream struct {
	ctx context.Context
}

func (stream *blockedStream) Recv() (provider.Event, error) {
	<-stream.ctx.Done()
	return provider.Event{}, stream.ctx.Err()
}

func (*blockedStream) Close() error { return nil }

func main() {
	control := &agent.Control{}
	model := &steeringProvider{started: make(chan struct{})}
	engine := agent.Engine{Provider: model, Model: "example-model", Control: control}
	done := make(chan agent.Result, 1)
	go func() {
		done <- engine.Run(context.Background(), agent.Request{Prompt: "answer the question"}, agent.OutputPolicy{})
	}()
	<-model.started
	ack, err := control.Steer(agent.Request{Prompt: "correct the answer to blue"})
	if err != nil {
		panic(err)
	}
	if err := <-ack; err != nil {
		panic(err)
	}
	result := <-done
	if result.Failure != nil {
		panic(result.Failure)
	}
	if result.Text == "" {
		panic(errors.New("steering produced an empty answer"))
	}
	fmt.Println(result.Text)
}
