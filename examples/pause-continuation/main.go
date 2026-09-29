package main

import (
	"context"
	"fmt"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/provider"
)

// pauseProvider is a credential-free provider that exercises the explicit
// pause protocol. The Engine resumes the same task without a synthetic user
// "continue" message.
type pauseProvider struct {
	calls int
}

func (*pauseProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "pause-continuation-local"}
}

func (p *pauseProvider) Stream(_ context.Context, _ provider.Request) (provider.Stream, error) {
	p.calls++
	if p.calls <= 2 {
		return provider.NewSliceStream([]provider.Event{
			{Kind: provider.EventTextDelta, Text: fmt.Sprintf("pause %d: continuing the same task", p.calls)},
			{Kind: provider.EventDone, StopReason: provider.StopReasonPause, Usage: provider.Usage{InputTokens: 4, OutputTokens: 3, TotalTokens: 7}},
		}), nil
	}
	return provider.NewSliceStream([]provider.Event{
		{Kind: provider.EventTextDelta, Text: "final answer after explicit pauses"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: provider.Usage{InputTokens: 5, OutputTokens: 6, TotalTokens: 11}},
	}), nil
}

func main() {
	provider := &pauseProvider{}
	engine := agent.Engine{
		Provider: provider,
		Model:    "pause-model",
		LoopPolicy: agent.LoopPolicy{
			MaxIterations: 4,
		},
		Boundaries: agent.BoundaryObserverFunc(func(_ context.Context, continuation agent.Continuation) error {
			return agent.ValidateContinuation(continuation)
		}),
	}
	result := engine.Run(context.Background(), agent.Request{Prompt: "finish the assigned task"}, agent.OutputPolicy{})
	if result.Failure != nil {
		panic(result.Failure)
	}
	if provider.calls != 3 || result.Text != "final answer after explicit pauses" {
		panic(fmt.Sprintf("pause continuation failed: calls=%d text=%q", provider.calls, result.Text))
	}
	fmt.Printf("pause-continuation: calls=%d result=%q usage=%d\n", provider.calls, result.Text, result.Usage.TotalTokens)
}
