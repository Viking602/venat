package agent

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

func TestWorkingMemoryPreservesEveryNewlyAdmittedInput(t *testing.T) {
	control := &Control{}
	inputs := []string{"first correction", "second detail", "third constraint"}
	mainCalls, summaries := 0, 0
	model := agentToolProviderFunc(func(_ context.Context, request provider.Request) (provider.Stream, error) {
		text := ""
		if request.Metadata["venat-purpose"] == "working-memory-summary" {
			summaries++
			text = "Earlier work is summarized."
		} else {
			mainCalls++
			text = strings.Repeat("old work ", 600)
			if mainCalls == 2 {
				for _, input := range inputs {
					found := false
					for _, current := range request.Messages {
						found = found || current.Role == message.RoleUser && current.Text == input
					}
					if !found {
						return nil, fmt.Errorf("new input %q was summarized before the model saw it", input)
					}
				}
				text = "all new inputs consumed"
			}
		}
		return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: text}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: provider.Usage{OutputTokens: 1, TotalTokens: 1}}}), nil
	})
	memory, err := NewWorkingMemory(WorkingMemoryConfig{Provider: model, Model: "summary", RecentTurns: 1})
	if err != nil {
		t.Fatal(err)
	}
	var receipts []<-chan error
	engine := Engine{
		Provider: model, WorkingMemory: memory, Control: control,
		LoopPolicy: LoopPolicy{ContextTokenTarget: 1000},
		Boundaries: BoundaryObserverFunc(func(_ context.Context, continuation Continuation) error {
			if err := ValidateContinuation(continuation); err != nil {
				return err
			}
			if continuation.Phase == ContinuationReady && len(receipts) == 0 {
				for _, input := range inputs {
					receipt, err := control.Send(Request{Prompt: input})
					if err != nil {
						return err
					}
					receipts = append(receipts, receipt)
				}
			}
			return nil
		}),
	}
	result := engine.Run(context.Background(), Request{Prompt: "original task"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatal(result.Failure)
	}
	if result.Text != "all new inputs consumed" || mainCalls != 2 || summaries != 1 {
		t.Fatalf("result=%s main=%d summaries=%d", result.Text, mainCalls, summaries)
	}
	for _, receipt := range receipts {
		if err := <-receipt; err != nil {
			t.Fatalf("input not consumed: %v", err)
		}
	}
}
