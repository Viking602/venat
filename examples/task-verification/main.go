// This scenario verifies a real file with a real child process. A deterministic
// local provider first claims completion too early, then repairs the file after
// the verifier returns the failed process diagnostics. No credentials needed.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

type localProvider struct{ turns int }

func (*localProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "local-verification"}
}
func (driver *localProvider) Stream(_ context.Context, request provider.Request) (provider.Stream, error) {
	driver.turns++
	if driver.turns == 2 {
		feedback := false
		for _, current := range request.Messages {
			feedback = feedback || strings.Contains(current.Text, "expected port=8080")
		}
		if !feedback {
			return nil, fmt.Errorf("missing observed check diagnostics")
		}
		return provider.NewSliceStream([]provider.Event{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "repair-config", Name: "repair_config", Arguments: []byte(`{}`)}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		}), nil
	}
	answer := "Configuration is complete."
	if driver.turns == 3 {
		answer = "Configured port 8080; acceptance check passed."
	}
	return provider.NewSliceStream([]provider.Event{{Kind: provider.EventTextDelta, Text: answer}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}}), nil
}

func main() {
	if len(os.Args) == 3 && os.Args[1] == "--verify" {
		data, err := os.ReadFile(os.Args[2])
		if err != nil || string(data) != "port=8080\n" {
			fmt.Fprintln(os.Stderr, "expected port=8080 in configuration file")
			os.Exit(1)
		}
		fmt.Println("configuration verified")
		return
	}
	dir, err := os.MkdirTemp("", "venat-verification-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "app.conf")
	if err := os.WriteFile(path, []byte("port=80\n"), 0600); err != nil {
		panic(err)
	}
	executable, err := os.Executable()
	if err != nil {
		panic(err)
	}
	check := kit.ProcessTool("verify_config", tool.Schema{Type: "object"}, kit.ProcessToolConfig{Command: executable, Args: []string{"--verify", path}})
	verifier, err := agent.NewToolVerifier("configuration acceptance", tool.NewBus(check), []tool.Call{{Name: "verify_config", Arguments: []byte(`{}`)}})
	if err != nil {
		panic(err)
	}
	repair, err := kit.Tool("repair_config", func(context.Context, struct{}) (string, error) {
		return "wrote requested configuration", os.WriteFile(path, []byte("port=8080\n"), 0600)
	})
	if err != nil {
		panic(err)
	}
	model := &localProvider{}
	engine := agent.Engine{Provider: model, Tools: tool.NewBus(repair), OutputGuardrails: []agent.OutputGuardrail{verifier}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := engine.Run(ctx, agent.Request{Prompt: "Configure port 8080 and verify the result."}, agent.OutputPolicy{})
	if result.Failure != nil {
		panic(result.Failure)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "port=8080\n" || model.turns != 3 || result.ToolCallsUsed != 1 {
		panic("acceptance repair was not completed")
	}
	fmt.Printf("task-verification: premature completion rejected -> real check diagnostics -> repaired file -> check passed; model_calls=%d; repairs=%d\n", model.turns, result.ToolCallsUsed)
}
