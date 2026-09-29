package agent

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

func TestWorkingMemoryArchivedToolsAreEvidenceNotLiveProviderCalls(t *testing.T) {
	model := agentToolProviderFunc(func(_ context.Context, request provider.Request) (provider.Stream, error) {
		if len(request.Messages) != 2 || request.Messages[1].Role != message.RoleUser {
			return nil, fmt.Errorf("summary request must begin with user evidence, not an orphaned assistant tool turn")
		}
		for _, current := range request.Messages {
			if len(current.ToolCalls) != 0 || current.ToolResult != nil {
				return nil, fmt.Errorf("archived tools require unavailable live definitions")
			}
		}
		if !strings.Contains(request.Messages[1].Text, "artifact://saved-evidence") {
			return nil, fmt.Errorf("archived evidence reference was lost")
		}
		return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: "Evidence remains at artifact://saved-evidence"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
	})
	memory, err := NewWorkingMemory(WorkingMemoryConfig{Provider: model, Model: "summary"})
	if err != nil {
		t.Fatal(err)
	}
	history := []message.Message{
		{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{{ID: "archived-read", Name: "read", Arguments: []byte(`{}`)}}},
		message.NewToolResult(message.ToolResult{ToolCallID: "archived-read", Name: "read", Content: "artifact://saved-evidence"}),
	}
	summary, _, err := memory.summarize(context.Background(), history)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Role != message.RoleUser || summary.Kind != message.KindCompactionSummary || !strings.Contains(summary.Text, "artifact://saved-evidence") {
		t.Fatalf("invalid working state: %+v", summary)
	}
}
