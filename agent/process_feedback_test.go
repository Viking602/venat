package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

func TestEngine_ProcessFailureCanBeCorrectedWithinOneExecution(t *testing.T) {
	if os.Getenv("VENAT_CORRECTION_HELPER") == "1" {
		var input struct {
			Fixed bool `json:"fixed"`
		}
		if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
			os.Exit(2)
		}
		if !input.Fixed {
			_, _ = os.Stderr.WriteString("test failed: fix required")
			os.Exit(1)
		}
		_, _ = os.Stdout.WriteString("test passed")
		os.Exit(0)
	}
	driver := kit.ProcessTool("test", tool.Schema{Type: "object"}, kit.ProcessToolConfig{
		Command: os.Args[0], Args: []string{"-test.run=^TestEngine_ProcessFailureCanBeCorrectedWithinOneExecution$"},
		Env: append(os.Environ(), "VENAT_CORRECTION_HELPER=1"), StdinJSON: true,
	})
	wrapped, err := kit.Tool("test", func(ctx context.Context, input struct {
		Fixed bool `json:"fixed"`
	}) (string, error) {
		payload, err := json.Marshal(input)
		if err != nil {
			return "", err
		}
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEngine_ProcessFailureCanBeCorrectedWithinOneExecution$")
		command.Env = append(os.Environ(), "VENAT_CORRECTION_HELPER=1")
		command.Stdin = bytes.NewReader(payload)
		output, err := command.CombinedOutput()
		if err != nil {
			return string(output), fmt.Errorf("command: %w", err)
		}
		return string(output), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, current := range []struct {
		name   string
		driver tool.Driver
	}{{"ProcessTool", driver}, {"Go function", wrapped}} {
		t.Run(current.name, func(t *testing.T) {
			model := &scriptedProvider{turns: [][]provider.Event{
				{{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "failed", Name: "test", Arguments: json.RawMessage(`{"fixed":false}`)}}, {Kind: provider.EventDone, StopReason: provider.StopReasonToolUse}},
				{{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "fixed", Name: "test", Arguments: json.RawMessage(`{"fixed":true}`)}}, {Kind: provider.EventDone, StopReason: provider.StopReasonToolUse}},
				{{Kind: provider.EventTextDelta, Text: "verified"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}},
			}}
			result := (Engine{Provider: model, Tools: tool.NewBus(current.driver)}).Run(context.Background(), Request{Prompt: "fix the test"}, OutputPolicy{})
			if result.Failure != nil || result.Text != "verified" || result.ToolCallsUsed != 2 || len(model.requests) != 3 {
				t.Fatalf("result=%+v", result)
			}
			failed := model.requests[1].Messages[len(model.requests[1].Messages)-1].ToolResult
			if failed == nil || !failed.IsError || !strings.Contains(failed.Content, "test failed") || !strings.Contains(failed.Content, "code 1") {
				t.Fatalf("feedback=%+v", failed)
			}
		})
	}
}
