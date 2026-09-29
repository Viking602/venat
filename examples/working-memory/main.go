package main

import (
	"context"
	"fmt"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

// localProvider makes both the auxiliary summary call and the real Engine call
// deterministic. It never contacts a network service.
type localProvider struct{}

func (localProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "working-memory-local"}
}

func (localProvider) Stream(_ context.Context, request provider.Request) (provider.Stream, error) {
	if request.Metadata["venat-purpose"] == "working-memory-summary" {
		return provider.NewSliceStream([]provider.Event{
			{Kind: provider.EventTextDelta, Text: "Goal\nfinish the local example\nConstraints\nno network\nEvidence\nverified /tmp/report.txt\nFiles and artifact references\n/tmp/report.txt\nFailures and decisions\nnone\nTodos\ncontinue with the current request"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: provider.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15}},
		}), nil
	}
	for _, current := range request.Messages {
		if current.Kind == message.KindCompactionSummary {
			return provider.NewSliceStream([]provider.Event{
				{Kind: provider.EventTextDelta, Text: "continued after working-memory summary"},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: provider.Usage{InputTokens: 5, OutputTokens: 5, TotalTokens: 10}},
			}), nil
		}
	}
	return provider.NewSliceStream([]provider.Event{
		{Kind: provider.EventTextDelta, Text: "unexpected request without working-memory summary"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}), nil
}

// seededContext gives the runnable Engine a few completed historical turns;
// applications normally load these from their own durable conversation store.
type seededContext struct {
	memory *agent.WorkingMemory
	seed   []message.Message
}

func (c seededContext) Build(ctx context.Context, request agent.Request) ([]message.Message, error) {
	built, err := c.memory.Build(ctx, request)
	if err != nil {
		return nil, err
	}
	last := built[len(built)-1]
	out := append(message.CloneMessages(built[:len(built)-1]), message.CloneMessages(c.seed)...)
	out = append(out, last)
	return out, nil
}

func (c seededContext) Compact(ctx context.Context, history []message.Message) ([]message.Message, error) {
	return c.memory.Compact(ctx, history)
}

func (c seededContext) CompactTo(ctx context.Context, history []message.Message, target int) ([]message.Message, error) {
	return c.memory.CompactTo(ctx, history, target)
}

func main() {
	memory, err := agent.NewWorkingMemory(agent.WorkingMemoryConfig{
		Provider:           localProvider{},
		Model:              "local-summary-model",
		SystemInstructions: "You are a local example agent.",
		RecentTurns:        1,
		Estimator: agent.WorkingMemoryEstimatorFunc(func(_ context.Context, _ string, messages []message.Message) (int, error) {
			// Deterministic stand-in for a model tokenizer: each message reserves
			// the same amount, while the production fallback counts UTF-8 text.
			return len(messages) * 40, nil
		}),
	})
	if err != nil {
		panic(err)
	}
	seed := []message.Message{
		message.NewText(message.RoleUser, "inspect /tmp/report.txt and retain the artifact reference"),
		message.NewText(message.RoleAssistant, "I inspected the report."),
		message.NewText(message.RoleUser, "The verified result is important for the next step."),
		message.NewText(message.RoleAssistant, "The result is recorded for continuation."),
	}
	engine := agent.Engine{
		Provider:       localProvider{},
		Model:          "local-summary-model",
		ContextBuilder: seededContext{memory: memory, seed: seed},
		LoopPolicy:     agent.LoopPolicy{MaxIterations: 1, ContextTokenTarget: 180},
		Boundaries: agent.BoundaryObserverFunc(func(_ context.Context, continuation agent.Continuation) error {
			return agent.ValidateContinuation(continuation)
		}),
	}
	result := engine.Run(context.Background(), agent.Request{Prompt: "continue the report task"}, agent.OutputPolicy{})
	if result.Failure != nil {
		panic(result.Failure)
	}
	if result.Text != "continued after working-memory summary" || result.Usage.TotalTokens != 25 {
		panic(fmt.Sprintf("summary continuation/accounting failed: text=%q usage=%+v", result.Text, result.Usage))
	}
	fmt.Printf("working-memory: %s; summary_and_main_tokens=%d\n", result.Text, result.Usage.TotalTokens)
}
