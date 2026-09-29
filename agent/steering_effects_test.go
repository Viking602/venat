package agent

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

func TestSteerKeepsStartedWriteAndSkipsObsoletePendingWrite(t *testing.T) {
	control := &Control{}
	path := filepath.Join(t.TempDir(), "completed.txt")
	var receipt <-chan error
	firstCalls, secondCalls := 0, 0
	first, err := kit.Tool("first_write", func(context.Context, struct{}) (string, error) {
		firstCalls++
		if err := os.WriteFile(path, []byte("completed once"), 0600); err != nil {
			return "", err
		}
		var err error
		receipt, err = control.Steer(Request{Prompt: "Stop editing; report what already changed."})
		return "first write completed", err
	}, kit.Concurrency(tool.ConcurrencySequential))
	if err != nil {
		t.Fatal(err)
	}
	second, err := kit.Tool("obsolete_write", func(context.Context, struct{}) (string, error) { secondCalls++; return "should not run", nil }, kit.Concurrency(tool.ConcurrencySequential))
	if err != nil {
		t.Fatal(err)
	}
	model := &scriptedProvider{turns: [][]provider.Event{
		{{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "first", Name: "first_write", Arguments: []byte(`{}`)}}, {Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "second", Name: "obsolete_write", Arguments: []byte(`{}`)}}, {Kind: provider.EventDone, StopReason: provider.StopReasonToolUse}},
		{{Kind: provider.EventTextDelta, Text: "One file was changed; further editing stopped."}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}},
	}}
	engine := Engine{Provider: model, Tools: tool.NewBus(first, second), Control: control, ToolMode: tool.ModeParallel,
		Boundaries: BoundaryObserverFunc(func(_ context.Context, value Continuation) error { return ValidateContinuation(value) }),
	}
	result := engine.Run(context.Background(), Request{Prompt: "Edit both files."}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatal(result.Failure)
	}
	if firstCalls != 1 || secondCalls != 0 {
		t.Fatalf("writes=%d,%d", firstCalls, secondCalls)
	}
	content, err := os.ReadFile(path)
	if err != nil || string(content) != "completed once" {
		t.Fatalf("completed effect lost: %q %v", content, err)
	}
	if receipt == nil {
		t.Fatal("steering was not admitted")
	}
	if err := <-receipt; err != nil {
		t.Fatal(err)
	}
	completed, skipped := false, false
	for _, current := range result.Messages {
		if current.ToolResult == nil {
			continue
		}
		if current.ToolResult.ToolCallID == "first" {
			completed = !current.ToolResult.IsError
		}
		if current.ToolResult.ToolCallID == "second" {
			skipped = current.ToolResult.IsError
		}
	}
	if !completed || !skipped {
		t.Fatalf("completed=%v skipped=%v", completed, skipped)
	}
}
