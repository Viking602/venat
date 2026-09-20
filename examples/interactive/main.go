// This local smoke scenario uses a deterministic provider and real child
// processes. It requires no model credentials or external services.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

type localProvider struct{ calls int }

func (*localProvider) Metadata() provider.Metadata { return provider.Metadata{Name: "local-smoke"} }

func (driver *localProvider) Stream(_ context.Context, request provider.Request) (provider.Stream, error) {
	driver.calls++
	if request.ResponseFormat == nil || len(request.ResponseFormat.RawSchema) == 0 {
		return nil, fmt.Errorf("native schema was not forwarded")
	}
	if driver.calls == 2 {
		failed, steered := false, false
		for _, current := range request.Messages {
			if current.ToolResult != nil && current.ToolResult.IsError && strings.Contains(current.ToolResult.Content, "code 1") {
				failed = true
			}
			if current.Role == message.RoleUser && current.Text == "Keep the final report short." {
				steered = true
			}
		}
		if !failed || !steered {
			return nil, fmt.Errorf("missing process feedback or live input")
		}
	}
	if driver.calls <= 2 {
		arguments := json.RawMessage(`{"fixed":false}`)
		if driver.calls == 2 {
			arguments = json.RawMessage(`{"fixed":true}`)
		}
		return provider.NewSliceStream([]provider.Event{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: fmt.Sprintf("check-%d", driver.calls), Name: "check", Arguments: arguments}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		}), nil
	}
	return provider.NewSliceStream([]provider.Event{
		{Kind: provider.EventTextDelta, Text: `{"status":"ok"}`},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}), nil
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--check" {
		var input struct {
			Fixed bool `json:"fixed"`
		}
		if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
			os.Exit(2)
		}
		if !input.Fixed {
			_, _ = fmt.Fprint(os.Stderr, "check failed: fix required")
			os.Exit(1)
		}
		fmt.Print("check passed")
		return
	}
	executable, err := os.Executable()
	if err != nil {
		panic(err)
	}
	check := kit.ProcessTool("check", tool.Schema{Type: "object"}, kit.ProcessToolConfig{Command: executable, Args: []string{"--check"}, StdinJSON: true})
	control := &agent.Control{}
	driver := &localProvider{}
	boundaries := 0
	engine := agent.Engine{
		Provider: driver, Tools: tool.NewBus(check), Control: control,
		LoopPolicy: agent.LoopPolicy{ContextTokenTarget: 2000},
		Boundaries: agent.BoundaryObserverFunc(func(_ context.Context, value agent.Continuation) error {
			_, err := agent.EncodeContinuation(value)
			boundaries++
			return err
		}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var ack <-chan error
	result := engine.RunStream(ctx, agent.Request{Prompt: "Run the check, correct its input on failure, then report."}, agent.OutputPolicy{
		Native: true, Validate: true, Schema: json.RawMessage(`{"type":"object","properties":{"status":{"type":"string","enum":["ok"]}},"required":["status"],"additionalProperties":false}`),
	}, agent.SinkFunc(func(_ context.Context, frame agent.Frame) error {
		if frame.Kind == agent.FrameToolResult && frame.ToolResult.IsError {
			var err error
			ack, err = control.Send(agent.Request{Prompt: "Keep the final report short."})
			return err
		}
		return nil
	}))
	if result.Failure != nil {
		panic(result.Failure)
	}
	if ack == nil {
		panic("missing input receipt")
	}
	if err := <-ack; err != nil {
		panic(err)
	}
	if result.ToolCallsUsed != 2 || !result.Valid {
		panic("incomplete correction")
	}
	fmt.Printf("interactive: failed command -> live input -> corrected command -> validated JSON; tools=%d; model_calls=%d; in_memory_boundaries=%d; result=%s\n", result.ToolCallsUsed, driver.calls, boundaries, result.Structured)
}
