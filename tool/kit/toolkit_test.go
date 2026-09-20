package kit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/tooltest"
)

func TestTool_CompletedProcessExitIsFeedbackWithoutMaskingOtherFailures(t *testing.T) {
	if os.Getenv("VENAT_FUNCTION_EXIT_HELPER") == "1" {
		_, _ = os.Stdout.WriteString("command failed")
		_, _ = os.Stderr.WriteString("diagnostic")
		os.Exit(3)
	}
	for _, scenario := range []string{"returned", "wrapped", "streamed", "stderr", "joined", "cancelled", "sink failure"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			infrastructureErr := errors.New("storage unavailable")
			driver, err := Tool("command", func(ctx context.Context, _ struct{}, sink tool.UpdateSink) (string, error) {
				command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTool_CompletedProcessExitIsFeedbackWithoutMaskingOtherFailures$")
				command.Env = append(os.Environ(), "VENAT_FUNCTION_EXIT_HELPER=1")
				var output []byte
				var err error
				if scenario == "stderr" {
					output, err = command.Output()
				} else {
					output, err = command.CombinedOutput()
				}
				switch scenario {
				case "wrapped":
					err = fmt.Errorf("check: %w", err)
				case "joined":
					err = errors.Join(err, infrastructureErr)
				case "cancelled":
					cancel()
				case "streamed", "sink failure":
					_ = sink(tool.Update{Kind: tool.UpdateOutput, Parts: []message.ContentPart{message.TextPart(string(output[:3]))}})
					_ = sink(tool.Update{Kind: tool.UpdateOutput, Parts: []message.ContentPart{message.TextPart(string(output[3:]))}})
				}
				return string(output), err
			})
			if err != nil {
				t.Fatal(err)
			}
			var streamed strings.Builder
			result, err := tool.NewBus(driver).Execute(ctx, tool.Call{ID: "check", Name: "command"}, tool.ExecuteOptions{Sink: func(update tool.Update) error {
				if scenario == "sink failure" {
					return infrastructureErr
				}
				for _, part := range update.Parts {
					streamed.WriteString(part.Text)
				}
				return nil
			}})
			switch scenario {
			case "joined", "sink failure":
				if !errors.Is(err, infrastructureErr) {
					t.Fatalf("masked infrastructure error: %v", err)
				}
			case "cancelled":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("masked cancellation: %v", err)
				}
			default:
				if err != nil || !result.IsError || !strings.Contains(result.Content, "code 3") || !strings.Contains(result.Content, "command failed") || !strings.Contains(result.Content, "diagnostic") {
					t.Fatalf("result=%+v error=%v", result, err)
				}
				if scenario == "streamed" && result.Content != streamed.String() {
					t.Fatalf("stream=%q final=%q", streamed.String(), result.Content)
				}
			}
		})
	}
}

func TestTool_TypedRejectionAndExplicitResult(t *testing.T) {
	returned := tool.Result{Content: "use a nonnegative value", IsError: true, Structured: json.RawMessage(`{"reason":"negative"}`)}
	called := 0
	driver, err := Tool("check", func(_ context.Context, _ struct {
		Value int8 `json:"value"`
	}) (tool.Result, error) {
		called++
		return returned, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	bus := tool.NewBus(driver)
	for _, arguments := range []string{`{"value":999}`, `{"value":-1}`} {
		result, err := bus.Execute(context.Background(), tool.Call{ID: "call", Name: "check", Arguments: json.RawMessage(arguments)}, tool.ExecuteOptions{})
		if err != nil || !result.IsError || result.ToolCallID != "call" || result.Name != "check" {
			t.Fatalf("result=%+v error=%v", result, err)
		}
		if len(result.Structured) > 0 {
			result.Structured[0] = '['
		}
	}
	if called != 1 || string(returned.Structured) != `{"reason":"negative"}` {
		t.Fatalf("function calls=%d; result aliased=%s", called, returned.Structured)
	}
}

func TestSchemaForRecursiveTypeDoesNotOverflow(t *testing.T) {
	type Node struct {
		Name string `json:"name"`
		Next *Node  `json:"next,omitempty"`
	}
	schema, err := schemaFor(reflect.TypeOf(Node{}))
	if err != nil {
		t.Fatalf("schemaFor() error = %v", err)
	}
	if schema.Type != "object" {
		t.Fatalf("schema type = %q, want object", schema.Type)
	}
}

func TestDecodeInputRequiresFields(t *testing.T) {
	type input struct {
		Query string `json:"query"`
	}
	if _, err := decodeInput(reflect.TypeOf(input{}), json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected missing required field error")
	}
}

func TestToolWrapsFunctionAndGeneratesSchema(t *testing.T) {
	type input struct {
		Query string `json:"query" description:"search query"`
		Limit int    `json:"limit,omitempty"`
	}
	driver, err := Tool("search", func(_ context.Context, in input) (string, error) {
		return in.Query, nil
	}, Description("search the corpus"))
	if err != nil {
		t.Fatalf("Tool() error = %v", err)
	}
	schema := tooltest.MustSchema(t, driver)
	if schema.Type != "object" {
		t.Fatalf("expected object schema, got %q", schema.Type)
	}
	if schema.Properties["query"].Description != "search query" {
		t.Fatalf("expected field description, got %q", schema.Properties["query"].Description)
	}
	result := tooltest.MustCall(t, driver, map[string]any{"query": "venat"})
	if result.Content != "venat" {
		t.Fatalf("unexpected content: %q", result.Content)
	}
}

func TestToolCarriesExecutionSettings(t *testing.T) {
	type input struct {
		Target string `json:"target"`
	}
	driver, err := Tool("deploy", func(context.Context, input) (string, error) {
		return "ok", nil
	},
		Terminal(),
		Timeout(5*time.Second),
		Concurrency(tool.ConcurrencySequential),
		ConcurrencyGroup("deployments"),
		MaxConcurrency(1),
	)
	if err != nil {
		t.Fatalf("Tool() error = %v", err)
	}
	def := driver.Definition()
	if !def.Terminal || def.Timeout != 5*time.Second {
		t.Fatalf("terminal/timeout settings = %#v", def)
	}
	if def.Concurrency != tool.ConcurrencySequential || def.ConcurrencyGroup != "deployments" || def.MaxConcurrency != 1 {
		t.Fatalf("concurrency settings = %#v", def)
	}
}

func TestToolSupportsStreamingUpdates(t *testing.T) {
	type input struct {
		Name string `json:"name"`
	}
	driver, err := Tool("greeter", func(_ context.Context, in input, sink tool.UpdateSink) (map[string]string, error) {
		if err := sink(tool.Update{Kind: tool.UpdateProgress, Message: "started"}); err != nil {
			return nil, err
		}
		if err := sink(tool.Update{Kind: tool.UpdateOutput, Parts: []message.ContentPart{message.TextPart("hello " + in.Name)}}); err != nil {
			return nil, err
		}
		return map[string]string{"message": "hello " + in.Name}, nil
	})
	if err != nil {
		t.Fatalf("Tool() error = %v", err)
	}
	updates := make([]tool.Update, 0, 2)
	result, err := tool.NewBus(driver).Execute(context.Background(), tool.Call{
		ID:          "call-1",
		Name:        "greeter",
		OperationID: "turn:0:call:0",
		Arguments:   json.RawMessage(`{"name":"mcp"}`),
	}, tool.ExecuteOptions{Sink: func(update tool.Update) error {
		updates = append(updates, tool.CloneUpdate(update))
		return nil
	}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(updates) != 2 || updates[0].Kind != tool.UpdateProgress || updates[1].Kind != tool.UpdateOutput {
		t.Fatalf("unexpected updates: %#v", updates)
	}
	for index, update := range updates {
		if update.ToolCallID != "call-1" || update.OperationID != "turn:0:call:0" || update.Sequence != uint64(index+1) {
			t.Fatalf("update[%d] identity = %#v", index, update)
		}
	}
	if result.Name != "greeter" || result.Content != "hello mcp" || len(result.Parts) != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
}
