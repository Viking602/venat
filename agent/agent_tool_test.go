package agent

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
)

type agentToolProviderFunc func(context.Context, provider.Request) (provider.Stream, error)

func (agentToolProviderFunc) Metadata() provider.Metadata {
	return provider.Metadata{Name: "agent-tool-test"}
}

func (driver agentToolProviderFunc) Stream(ctx context.Context, request provider.Request) (provider.Stream, error) {
	return driver(ctx, request)
}

func TestNewAgentTool_ValidatesAndClonesConfig(t *testing.T) {
	validDefinition := func() tool.Definition {
		return tool.Definition{Name: "researcher", Description: "delegate research"}
	}
	tests := []struct {
		name   string
		mutate func(*AgentToolConfig)
	}{
		{
			name: "empty name",
			mutate: func(config *AgentToolConfig) {
				config.Definition.Name = ""
			},
		},
		{
			name: "blank description",
			mutate: func(config *AgentToolConfig) {
				config.Definition.Description = " \t\n"
			},
		},
		{
			name: "terminal definition",
			mutate: func(config *AgentToolConfig) {
				config.Definition.Terminal = true
			},
		},
		{
			name: "negative depth",
			mutate: func(config *AgentToolConfig) {
				config.MaxDepth = -1
			},
		},
		{
			name: "negative token budget",
			mutate: func(config *AgentToolConfig) {
				config.Budget = &Budget{MaxTokens: -1}
			},
		},
		{
			name: "negative tool-call budget",
			mutate: func(config *AgentToolConfig) {
				config.Budget = &Budget{MaxToolCalls: -1}
			},
		},
		{
			name: "negative step budget",
			mutate: func(config *AgentToolConfig) {
				config.Budget = &Budget{MaxSteps: -1}
			},
		},
		{
			name: "negative wall-clock budget",
			mutate: func(config *AgentToolConfig) {
				config.Budget = &Budget{MaxWallClock: -time.Second}
			},
		},
		{
			name: "negative repair attempts",
			mutate: func(config *AgentToolConfig) {
				config.OutputPolicy.MaxRepairAttempts = -1
			},
		},
		{
			name: "invalid input schema",
			mutate: func(config *AgentToolConfig) {
				config.Definition.InputSchema = tool.Schema{Type: "not-a-json-schema-type"}
			},
		},
		{
			name: "invalid output schema",
			mutate: func(config *AgentToolConfig) {
				config.OutputPolicy = OutputPolicy{
					Validate: true,
					Schema:   json.RawMessage(`{"type":"not-supported"}`),
				}
			},
		},
		{
			name: "invalid concurrency",
			mutate: func(config *AgentToolConfig) {
				config.Definition.Concurrency = tool.ConcurrencySequential
				config.Definition.MaxConcurrency = 2
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := AgentToolConfig{Definition: validDefinition()}
			test.mutate(&config)
			_, err := NewAgentTool(Engine{}, config)
			if !errors.Is(err, tool.ErrInvalidToolDefinition) {
				t.Fatalf("NewAgentTool() error = %v, want ErrInvalidToolDefinition", err)
			}
		})
	}

	t.Run("strict default schema", func(t *testing.T) {
		driver, err := NewAgentTool(Engine{}, AgentToolConfig{Definition: validDefinition()})
		if err != nil {
			t.Fatalf("NewAgentTool() error = %v", err)
		}
		definition := driver.Definition()
		if definition.InputSchema.Type != "object" {
			t.Fatalf("schema type = %q, want object", definition.InputSchema.Type)
		}
		if !reflect.DeepEqual(definition.InputSchema.Required, []string{"task"}) {
			t.Fatalf("required = %#v, want [task]", definition.InputSchema.Required)
		}
		task, ok := definition.InputSchema.Properties["task"]
		if !ok || task.Type != "string" || task.Description != defaultAgentToolTaskDescription {
			t.Fatalf("task schema = %#v", task)
		}
		if definition.InputSchema.AdditionalProperties == nil || *definition.InputSchema.AdditionalProperties {
			t.Fatalf("additionalProperties = %#v, want false", definition.InputSchema.AdditionalProperties)
		}
	})

	t.Run("mutable values do not alias", func(t *testing.T) {
		additionalProperties := false
		items := tool.Schema{Type: "string", Enum: []string{"alpha"}}
		definition := tool.Definition{
			Name:        "researcher",
			Description: "delegate research",
			InputSchema: tool.Schema{
				Type: "object",
				Properties: map[string]tool.Schema{
					"task": {Type: "string"},
					"tags": {Type: "array", Items: &items},
				},
				Required:             []string{"task"},
				AdditionalProperties: &additionalProperties,
			},
		}
		budget := &Budget{MaxTokens: 17, MaxToolCalls: 3, MaxSteps: 4, MaxWallClock: time.Minute}
		outputSchema := json.RawMessage(`{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}`)
		policy := OutputPolicy{Schema: outputSchema, Validate: true}

		var observed Request
		child := Engine{
			Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
				return provider.NewSliceStream([]provider.Event{
					{Kind: provider.EventTextDelta, Text: `{"ok":true}`},
					{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: provider.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}},
				}), nil
			}),
			ContextBuilder: ContextBuilderFunc(func(_ context.Context, request Request) ([]message.Message, error) {
				observed = request
				observed.Budget = cloneBudget(request.Budget)
				return []message.Message{message.NewText(message.RoleUser, request.Prompt)}, nil
			}),
		}
		driver, err := NewAgentTool(child, AgentToolConfig{
			Definition:   definition,
			Budget:       budget,
			OutputPolicy: policy,
		})
		if err != nil {
			t.Fatalf("NewAgentTool() error = %v", err)
		}

		definition.InputSchema.Required[0] = "mutated"
		definition.InputSchema.Properties["task"] = tool.Schema{Type: "number"}
		definition.InputSchema.Properties["tags"].Items.Enum[0] = "mutated"
		additionalProperties = true
		budget.MaxTokens = 999
		outputSchema[0] = '['

		first := driver.Definition()
		first.InputSchema.Required[0] = "changed"
		first.InputSchema.Properties["task"] = tool.Schema{Type: "boolean"}
		first.InputSchema.Properties["tags"].Items.Enum[0] = "changed"
		*first.InputSchema.AdditionalProperties = true
		second := driver.Definition()
		if !reflect.DeepEqual(second.InputSchema.Required, []string{"task"}) {
			t.Fatalf("cloned required = %#v", second.InputSchema.Required)
		}
		if second.InputSchema.Properties["task"].Type != "string" {
			t.Fatalf("cloned task type = %q", second.InputSchema.Properties["task"].Type)
		}
		if got := second.InputSchema.Properties["tags"].Items.Enum[0]; got != "alpha" {
			t.Fatalf("cloned nested enum = %q, want alpha", got)
		}
		if second.InputSchema.AdditionalProperties == nil || *second.InputSchema.AdditionalProperties {
			t.Fatalf("cloned additionalProperties = %#v, want false", second.InputSchema.AdditionalProperties)
		}

		result, err := driver.Execute(context.Background(), tool.Call{
			ID:        "outer-call",
			Name:      "researcher",
			Arguments: json.RawMessage(`{"task":"review","tags":["alpha"]}`),
		}, nil)
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if result.IsError {
			t.Fatalf("Execute() result = %#v", result)
		}
		if observed.Budget == nil || observed.Budget.MaxTokens != 17 || observed.Budget.MaxToolCalls != 3 || observed.Budget.MaxSteps != 4 || observed.Budget.MaxWallClock != time.Minute {
			t.Fatalf("observed budget = %#v", observed.Budget)
		}
		if observed.Prompt != `{"task":"review","tags":["alpha"]}` {
			t.Fatalf("observed prompt = %q", observed.Prompt)
		}
		if string(result.Structured) != `{"ok":true}` {
			t.Fatalf("structured result = %s", result.Structured)
		}
		if strings.TrimSpace(result.Content) != `{"ok":true}` {
			t.Fatalf("result content = %q", result.Content)
		}
	})
}

func TestAgentTool_MapsDefaultAndCustomInputs(t *testing.T) {
	t.Run("default task schema", func(t *testing.T) {
		providerCalls := 0
		var requests []Request
		child := Engine{
			Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
				providerCalls++
				return provider.NewSliceStream([]provider.Event{
					{Kind: provider.EventTextDelta, Text: "child answer"},
					{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
				}), nil
			}),
			ContextBuilder: ContextBuilderFunc(func(_ context.Context, request Request) ([]message.Message, error) {
				requests = append(requests, request)
				return []message.Message{message.NewText(message.RoleUser, request.Prompt)}, nil
			}),
		}
		driver, err := NewAgentTool(child, AgentToolConfig{
			Definition: tool.Definition{Name: "researcher", Description: "delegate research"},
		})
		if err != nil {
			t.Fatalf("NewAgentTool() error = %v", err)
		}

		result, err := driver.Execute(context.Background(), tool.Call{
			ID:        "valid",
			Name:      "researcher",
			Arguments: json.RawMessage(`{"task":"review"}`),
		}, nil)
		if err != nil {
			t.Fatalf("Execute(valid) error = %v", err)
		}
		if result.IsError || result.Content != "child answer" {
			t.Fatalf("Execute(valid) result = %#v", result)
		}
		if len(requests) != 1 || requests[0].Prompt != "review" || requests[0].Budget != nil {
			t.Fatalf("child requests = %#v", requests)
		}

		rejected := []struct {
			name      string
			arguments json.RawMessage
		}{
			{name: "empty arguments"},
			{name: "missing task", arguments: json.RawMessage(`{}`)},
			{name: "blank task", arguments: json.RawMessage(`{"task":" \t "}`)},
			{name: "additional property", arguments: json.RawMessage(`{"task":"review","extra":true}`)},
		}
		for _, test := range rejected {
			t.Run(test.name, func(t *testing.T) {
				result, err := driver.Execute(context.Background(), tool.Call{
					ID:        test.name,
					Name:      "researcher",
					Arguments: test.arguments,
				}, nil)
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				if !result.IsError || !strings.HasPrefix(result.Content, `subagent "researcher" input rejected: `) {
					t.Fatalf("Execute() result = %#v", result)
				}
				if result.ToolCallID != test.name || result.Name != "researcher" {
					t.Fatalf("Execute() identity = %#v", result)
				}
			})
		}
		if providerCalls != 1 || len(requests) != 1 {
			t.Fatalf("rejected input started child: provider calls = %d, requests = %d", providerCalls, len(requests))
		}
	})

	t.Run("custom schema preserves raw JSON", func(t *testing.T) {
		additionalProperties := false
		var observed Request
		providerCalls := 0
		child := Engine{
			Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
				providerCalls++
				return provider.NewSliceStream([]provider.Event{
					{Kind: provider.EventTextDelta, Text: "custom answer"},
					{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
				}), nil
			}),
			ContextBuilder: ContextBuilderFunc(func(_ context.Context, request Request) ([]message.Message, error) {
				observed = request
				return []message.Message{message.NewText(message.RoleUser, request.Prompt)}, nil
			}),
		}
		driver, err := NewAgentTool(child, AgentToolConfig{Definition: tool.Definition{
			Name:        "researcher",
			Description: "delegate research",
			InputSchema: tool.Schema{
				Type: "object",
				Properties: map[string]tool.Schema{
					"task": {Type: "string"},
					"options": {
						Type: "object",
						Properties: map[string]tool.Schema{
							"depth": {Type: "integer"},
						},
						Required:             []string{"depth"},
						AdditionalProperties: &additionalProperties,
					},
				},
				Required:             []string{"task", "options"},
				AdditionalProperties: &additionalProperties,
			},
		}})
		if err != nil {
			t.Fatalf("NewAgentTool() error = %v", err)
		}
		arguments := json.RawMessage(`{"task":"review","options":{"depth":2}}`)
		result, err := driver.Execute(context.Background(), tool.Call{
			ID:        "custom",
			Name:      "researcher",
			Arguments: arguments,
		}, nil)
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if result.IsError || result.Content != "custom answer" {
			t.Fatalf("Execute() result = %#v", result)
		}
		if providerCalls != 1 || observed.Prompt != string(arguments) || observed.Budget != nil {
			t.Fatalf("child call = %d, request = %#v", providerCalls, observed)
		}
	})
}

type agentToolDelegatingProvider struct {
	target  string
	results []tool.Result
}

func (providerDriver *agentToolDelegatingProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "delegate-" + providerDriver.target}
}

func (providerDriver *agentToolDelegatingProvider) Stream(_ context.Context, request provider.Request) (provider.Stream, error) {
	if len(request.Messages) > 0 {
		last := request.Messages[len(request.Messages)-1]
		if last.ToolResult != nil {
			result := message.CloneToolResult(*last.ToolResult)
			providerDriver.results = append(providerDriver.results, result)
			return provider.NewSliceStream([]provider.Event{
				{Kind: provider.EventTextDelta, Text: result.Content},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
			}), nil
		}
	}
	return provider.NewSliceStream([]provider.Event{
		{
			Kind: provider.EventToolCall,
			ToolCall: &message.ToolCall{
				ID:        "call-" + providerDriver.target,
				Name:      providerDriver.target,
				Arguments: json.RawMessage(`{"task":"continue"}`),
			},
		},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}), nil
}

func TestAgentTool_EnforcesStrictestNestedDepth(t *testing.T) {
	mustAgentTool := func(name string, child Engine, maxDepth int) tool.Driver {
		t.Helper()
		driver, err := NewAgentTool(child, AgentToolConfig{
			Definition: tool.Definition{Name: name, Description: "delegate to " + name},
			MaxDepth:   maxDepth,
		})
		if err != nil {
			t.Fatalf("NewAgentTool(%s) error = %v", name, err)
		}
		return driver
	}

	t.Run("default depth rejects fifth child", func(t *testing.T) {
		level5Calls := 0
		level5 := mustAgentTool("level5", Engine{
			Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
				level5Calls++
				return provider.NewSliceStream([]provider.Event{
					{Kind: provider.EventTextDelta, Text: "unexpected"},
					{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
				}), nil
			}),
		}, 0)
		level4Provider := &agentToolDelegatingProvider{target: "level5"}
		level4 := mustAgentTool("level4", Engine{Provider: level4Provider, Tools: tool.NewBus(level5)}, 0)
		level3Provider := &agentToolDelegatingProvider{target: "level4"}
		level3 := mustAgentTool("level3", Engine{Provider: level3Provider, Tools: tool.NewBus(level4)}, 0)
		level2Provider := &agentToolDelegatingProvider{target: "level3"}
		level2 := mustAgentTool("level2", Engine{Provider: level2Provider, Tools: tool.NewBus(level3)}, 0)
		level1Provider := &agentToolDelegatingProvider{target: "level2"}
		level1 := mustAgentTool("level1", Engine{Provider: level1Provider, Tools: tool.NewBus(level2)}, 0)

		result, err := level1.Execute(context.Background(), tool.Call{
			ID:        "outer",
			Name:      "level1",
			Arguments: json.RawMessage(`{"task":"start"}`),
		}, nil)
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		want := `subagent "level5" refused: max nesting depth 4 reached`
		if level5Calls != 0 {
			t.Fatalf("level5 provider calls = %d, want 0", level5Calls)
		}
		if len(level4Provider.results) != 1 || !level4Provider.results[0].IsError || level4Provider.results[0].Content != want {
			t.Fatalf("level4 observed results = %#v", level4Provider.results)
		}
		if result.Content != want {
			t.Fatalf("outer result content = %q, want %q", result.Content, want)
		}
	})

	t.Run("ancestor cap cannot be loosened", func(t *testing.T) {
		level3Calls := 0
		level3 := mustAgentTool("level3", Engine{
			Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
				level3Calls++
				return provider.NewSliceStream([]provider.Event{
					{Kind: provider.EventTextDelta, Text: "unexpected"},
					{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
				}), nil
			}),
		}, 4)
		level2Provider := &agentToolDelegatingProvider{target: "level3"}
		level2 := mustAgentTool("level2", Engine{Provider: level2Provider, Tools: tool.NewBus(level3)}, 4)
		level1Provider := &agentToolDelegatingProvider{target: "level2"}
		level1 := mustAgentTool("level1", Engine{Provider: level1Provider, Tools: tool.NewBus(level2)}, 2)

		result, err := level1.Execute(context.Background(), tool.Call{
			ID:        "outer",
			Name:      "level1",
			Arguments: json.RawMessage(`{"task":"start"}`),
		}, nil)
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		want := `subagent "level3" refused: max nesting depth 2 reached`
		if level3Calls != 0 {
			t.Fatalf("level3 provider calls = %d, want 0", level3Calls)
		}
		if len(level2Provider.results) != 1 || !level2Provider.results[0].IsError || level2Provider.results[0].Content != want {
			t.Fatalf("level2 observed results = %#v", level2Provider.results)
		}
		if result.Content != want {
			t.Fatalf("outer result content = %q, want %q", result.Content, want)
		}
	})
}

type agentToolStaticDriver struct {
	definition tool.Definition
	result     tool.Result
	calls      int
}

func (driver *agentToolStaticDriver) Definition() tool.Definition {
	return cloneAgentToolDefinition(driver.definition)
}

func (driver *agentToolStaticDriver) Execute(_ context.Context, call tool.Call, _ tool.UpdateSink) (tool.Result, error) {
	driver.calls++
	result := message.CloneToolResult(driver.result)
	result.ToolCallID = call.ID
	result.Name = call.Name
	return result, nil
}

func TestAgentTool_ReturnsTextStructuredAndTerminalFallback(t *testing.T) {
	t.Run("text and structured output", func(t *testing.T) {
		outputSchema := json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}`)
		child := Engine{Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
			return provider.NewSliceStream([]provider.Event{
				{Kind: provider.EventTextDelta, Text: `{"answer":"ok"}`},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
			}), nil
		})}
		driver, err := NewAgentTool(child, AgentToolConfig{
			Definition:   tool.Definition{Name: "researcher", Description: "delegate research"},
			OutputPolicy: OutputPolicy{Schema: outputSchema, Validate: true},
		})
		if err != nil {
			t.Fatalf("NewAgentTool() error = %v", err)
		}
		call := tool.Call{ID: "outer", Name: "researcher", Arguments: json.RawMessage(`{"task":"review"}`)}
		first, err := driver.Execute(context.Background(), call, nil)
		if err != nil {
			t.Fatalf("Execute(first) error = %v", err)
		}
		if first.IsError || first.Content != `{"answer":"ok"}` || string(first.Structured) != `{"answer":"ok"}` {
			t.Fatalf("Execute(first) result = %#v", first)
		}
		if len(first.Parts) != 1 || first.Parts[0].Text != first.Content {
			t.Fatalf("Execute(first) parts = %#v", first.Parts)
		}
		first.Structured[0] = '['
		first.Parts[0].Text = "mutated"

		second, err := driver.Execute(context.Background(), call, nil)
		if err != nil {
			t.Fatalf("Execute(second) error = %v", err)
		}
		if second.Content != `{"answer":"ok"}` || string(second.Structured) != `{"answer":"ok"}` || second.Parts[0].Text != second.Content {
			t.Fatalf("Execute(second) result aliases first = %#v", second)
		}
	})

	t.Run("terminal child result fallback", func(t *testing.T) {
		terminal := &agentToolStaticDriver{
			definition: tool.Definition{Name: "submit", Description: "submit answer", Terminal: true},
			result: tool.Result{
				Parts: []message.ContentPart{
					message.TextPart("terminal "),
					message.FinalAnswerPart("answer"),
				},
				Structured: json.RawMessage(`{"accepted":true}`),
			},
		}
		child := Engine{
			Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
				return provider.NewSliceStream([]provider.Event{
					{
						Kind: provider.EventToolCall,
						ToolCall: &message.ToolCall{
							ID:        "submit-call",
							Name:      "submit",
							Arguments: json.RawMessage(`{}`),
						},
					},
					{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
				}), nil
			}),
			Tools: tool.NewBus(terminal),
		}
		driver, err := NewAgentTool(child, AgentToolConfig{
			Definition: tool.Definition{Name: "researcher", Description: "delegate research"},
		})
		if err != nil {
			t.Fatalf("NewAgentTool() error = %v", err)
		}
		result, err := driver.Execute(context.Background(), tool.Call{
			ID:        "outer",
			Name:      "researcher",
			Arguments: json.RawMessage(`{"task":"review"}`),
		}, nil)
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if terminal.calls != 1 || result.IsError || result.Content != "terminal answer" {
			t.Fatalf("Execute() result = %#v, terminal calls = %d", result, terminal.calls)
		}
		if len(result.Parts) != 2 || result.Parts[0].Text != "terminal " || result.Parts[1].Text != "answer" {
			t.Fatalf("fallback parts = %#v", result.Parts)
		}
		if string(result.Structured) != `{"accepted":true}` {
			t.Fatalf("fallback structured = %s", result.Structured)
		}
		terminal.result.Parts[0].Text = "mutated"
		terminal.result.Structured[0] = '['
		if result.Parts[0].Text != "terminal " || string(result.Structured) != `{"accepted":true}` {
			t.Fatalf("fallback aliases child result = %#v", result)
		}
	})

	t.Run("non-terminal observation is not final output", func(t *testing.T) {
		observation := &agentToolStaticDriver{
			definition: tool.Definition{Name: "lookup", Description: "look up data"},
			result:     tool.Result{Content: "private observation"},
		}
		turn := 0
		child := Engine{
			Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
				turn++
				if turn == 1 {
					return provider.NewSliceStream([]provider.Event{
						{
							Kind: provider.EventToolCall,
							ToolCall: &message.ToolCall{
								ID:        "lookup-call",
								Name:      "lookup",
								Arguments: json.RawMessage(`{}`),
							},
						},
						{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
					}), nil
				}
				return provider.NewSliceStream([]provider.Event{
					{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
				}), nil
			}),
			Tools: tool.NewBus(observation),
		}
		driver, err := NewAgentTool(child, AgentToolConfig{
			Definition: tool.Definition{Name: "researcher", Description: "delegate research"},
		})
		if err != nil {
			t.Fatalf("NewAgentTool() error = %v", err)
		}
		result, err := driver.Execute(context.Background(), tool.Call{
			ID:        "outer",
			Name:      "researcher",
			Arguments: json.RawMessage(`{"task":"review"}`),
		}, nil)
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		want := `agent response remained incomplete`
		if !result.IsError || !strings.Contains(result.Content, want) || strings.Contains(result.Content, observation.result.Content) {
			t.Fatalf("Execute() result = %#v", result)
		}
	})
}

func TestAgentTool_MapsFailureAndIncompleteRuns(t *testing.T) {
	newDriver := func(t *testing.T, child Engine) tool.Driver {
		t.Helper()
		driver, err := NewAgentTool(child, AgentToolConfig{
			Definition: tool.Definition{Name: "researcher", Description: "delegate research"},
		})
		if err != nil {
			t.Fatalf("NewAgentTool() error = %v", err)
		}
		return driver
	}
	execute := func(t *testing.T, driver tool.Driver) tool.Result {
		t.Helper()
		result, err := driver.Execute(context.Background(), tool.Call{
			ID:        "outer",
			Name:      "researcher",
			Arguments: json.RawMessage(`{"task":"review"}`),
		}, nil)
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if result.ToolCallID != "outer" || result.Name != "researcher" {
			t.Fatalf("Execute() identity = %#v", result)
		}
		return result
	}

	t.Run("typed child failure", func(t *testing.T) {
		providerErr := errors.New("child provider failed")
		driver := newDriver(t, Engine{Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
			return nil, providerErr
		})})
		result := execute(t, driver)
		if !result.IsError || !strings.Contains(result.Content, providerErr.Error()) {
			t.Fatalf("Execute() result = %#v", result)
		}
		var failure AgentFailure
		if err := json.Unmarshal(result.Structured, &failure); err != nil {
			t.Fatalf("unmarshal failure: %v", err)
		}
		if failure.Kind != FailureKindEngineError || !strings.Contains(failure.Reason, providerErr.Error()) {
			t.Fatalf("failure = %#v", failure)
		}
	})

	for _, test := range []struct {
		name    string
		partial string
		want    string
	}{
		{
			name:    "max turns with partial text",
			partial: "partial answer",
			want:    `subagent "researcher" stopped before completion (max_turns): partial answer`,
		},
		{
			name: "max turns without partial text",
			want: `subagent "researcher" stopped before completion (max_turns): child ran out of iterations`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			work := &agentToolStaticDriver{
				definition: tool.Definition{Name: "work", Description: "continue work"},
				result:     tool.Result{Content: "observation"},
			}
			driver := newDriver(t, Engine{
				Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
					events := make([]provider.Event, 0, 3)
					if test.partial != "" {
						events = append(events, provider.Event{Kind: provider.EventTextDelta, Text: test.partial})
					}
					events = append(events,
						provider.Event{
							Kind: provider.EventToolCall,
							ToolCall: &message.ToolCall{
								ID:        "work-call",
								Name:      "work",
								Arguments: json.RawMessage(`{}`),
							},
						},
						provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
					)
					return provider.NewSliceStream(events), nil
				}),
				Tools:      tool.NewBus(work),
				LoopPolicy: LoopPolicy{MaxIterations: 1},
			})
			result := execute(t, driver)
			if !result.IsError || result.Content != test.want {
				t.Fatalf("Execute() result = %#v", result)
			}
		})
	}

	t.Run("complete without answer", func(t *testing.T) {
		driver := newDriver(t, Engine{Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
			return provider.NewSliceStream([]provider.Event{
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
			}), nil
		})})
		result := execute(t, driver)
		want := `agent response remained incomplete`
		if !result.IsError || !strings.Contains(result.Content, want) {
			t.Fatalf("Execute() result = %#v", result)
		}
	})
}

type agentToolStreamingDriver struct{}

func (agentToolStreamingDriver) Definition() tool.Definition {
	return tool.Definition{
		Name:        "lookup",
		Description: "look up child data",
		InputSchema: tool.Schema{Type: "object"},
	}
}

func (agentToolStreamingDriver) Execute(_ context.Context, _ tool.Call, sink tool.UpdateSink) (tool.Result, error) {
	if err := sink(tool.Update{
		Kind:    tool.UpdateProgress,
		Message: "nested progress",
		Data: map[string]string{
			"safe":                      "kept",
			"subagent.path":             "spoofed",
			"subagent.frameKind":        "spoofed",
			"subagent.childToolCallId":  "spoofed",
			"subagent.childOperationId": "spoofed",
		},
	}); err != nil {
		return tool.Result{}, err
	}
	if err := sink(tool.Update{
		Kind:  tool.UpdateOutput,
		Parts: []message.ContentPart{message.TextPart("nested secret output")},
	}); err != nil {
		return tool.Result{}, err
	}
	return tool.Result{}, nil
}

func TestAgentTool_EmitsSafeProgress(t *testing.T) {
	childTurn := 0
	child := Engine{
		Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
			childTurn++
			if childTurn == 1 {
				return provider.NewSliceStream([]provider.Event{
					{Kind: provider.EventTextDelta, Text: "working", TextPhase: provider.TextPhaseCommentary},
					{Kind: provider.EventThinkingDelta, Thinking: "hidden reasoning"},
					{
						Kind: provider.EventToolCall,
						ToolCall: &message.ToolCall{
							ID:   "child-tool-call",
							Name: "lookup",
						},
					},
					{
						Kind: provider.EventToolCallDelta,
						ToolCallDelta: &provider.ToolCallDelta{
							ID:             "child-tool-call",
							ArgumentsDelta: `{"secret":"classified"}`,
						},
					},
					{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
				}), nil
			}
			return provider.NewSliceStream([]provider.Event{
				{Kind: provider.EventTextDelta, Text: "child answer", TextPhase: provider.TextPhaseFinalAnswer},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
			}), nil
		}),
		Tools: tool.NewBus(agentToolStreamingDriver{}),
	}
	delegate, err := NewAgentTool(child, AgentToolConfig{
		Definition: tool.Definition{Name: "researcher", Description: "delegate research"},
	})
	if err != nil {
		t.Fatalf("NewAgentTool() error = %v", err)
	}

	parentTurn := 0
	parent := Engine{
		Provider: agentToolProviderFunc(func(_ context.Context, request provider.Request) (provider.Stream, error) {
			parentTurn++
			if parentTurn == 1 {
				return provider.NewSliceStream([]provider.Event{
					{
						Kind: provider.EventToolCall,
						ToolCall: &message.ToolCall{
							ID:        "delegate-call",
							Name:      "researcher",
							Arguments: json.RawMessage(`{"task":"analyze"}`),
						},
					},
					{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
				}), nil
			}
			last := request.Messages[len(request.Messages)-1]
			return provider.NewSliceStream([]provider.Event{
				{Kind: provider.EventTextDelta, Text: "parent received: " + last.ToolResult.Content},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
			}), nil
		}),
		Tools:         tool.NewBus(delegate),
		OperationTurn: 7,
	}
	var frames []Frame
	result := parent.RunStream(context.Background(), Request{Prompt: "delegate"}, OutputPolicy{}, SinkFunc(func(_ context.Context, frame Frame) error {
		frames = append(frames, frame)
		return nil
	}))
	if result.Failure != nil {
		t.Fatalf("parent result failure = %v", result.Failure)
	}
	if result.Text != "parent received: child answer" {
		t.Fatalf("parent result text = %q", result.Text)
	}

	var updates []tool.Update
	for _, frame := range frames {
		if frame.Kind == FrameToolUpdate && frame.ToolUpdate != nil {
			updates = append(updates, tool.CloneUpdate(*frame.ToolUpdate))
		}
	}
	wantKinds := []string{
		string(FrameText),
		string(FrameToolCall),
		string(FrameDone),
		string(FrameToolUpdate),
		string(FrameToolUpdate),
		string(FrameToolResult),
		string(FrameText),
		string(FrameDone),
	}
	if len(updates) != len(wantKinds) {
		t.Fatalf("outer updates = %#v, want %d", updates, len(wantKinds))
	}
	for index, update := range updates {
		if update.Kind != tool.UpdateProgress || len(update.Parts) != 0 {
			t.Fatalf("update[%d] = %#v, want progress without parts", index, update)
		}
		if update.ToolCallID != "delegate-call" || update.OperationID != "turn:7:call:0" || update.Sequence != uint64(index+1) {
			t.Fatalf("update[%d] identity = %#v", index, update)
		}
		if update.Data["subagent.path"] != `["researcher"]` || update.Data["subagent.frameKind"] != wantKinds[index] {
			t.Fatalf("update[%d] metadata = %#v", index, update.Data)
		}
	}
	if updates[0].Message != "working" || updates[0].Data["subagent.textPhase"] != string(provider.TextPhaseCommentary) {
		t.Fatalf("text update = %#v", updates[0])
	}
	if updates[3].Message != "nested progress" ||
		updates[3].Data["safe"] != "kept" ||
		updates[3].Data["subagent.childToolCallId"] != "child-tool-call" ||
		updates[3].Data["subagent.childOperationId"] != "turn:0:call:0" {
		t.Fatalf("nested progress update = %#v", updates[3])
	}
	if updates[4].Message != "child tool emitted output" || updates[4].Data["subagent.outputPartCount"] != "1" {
		t.Fatalf("nested output update = %#v", updates[4])
	}
	if updates[5].Data["subagent.childToolName"] != "lookup" || updates[5].Data["subagent.childToolIsError"] != "false" {
		t.Fatalf("nested result update = %#v", updates[5])
	}
	if updates[6].Message != "child answer" || updates[6].Data["subagent.textPhase"] != string(provider.TextPhaseFinalAnswer) {
		t.Fatalf("final text update = %#v", updates[6])
	}

	updateJSON, err := json.Marshal(updates)
	if err != nil {
		t.Fatalf("marshal updates: %v", err)
	}
	for _, secret := range []string{"hidden reasoning", "classified", "nested secret output"} {
		if strings.Contains(string(updateJSON), secret) {
			t.Fatalf("outer updates leaked %q: %s", secret, updateJSON)
		}
	}

	toolMessages := 0
	for _, current := range result.Messages {
		if current.Role == message.RoleTool {
			toolMessages++
			if current.ToolResult == nil || current.ToolResult.Name != "researcher" || current.ToolResult.Content != "child answer" {
				t.Fatalf("parent tool message = %#v", current)
			}
		}
	}
	if toolMessages != 1 {
		t.Fatalf("parent tool messages = %d, want 1", toolMessages)
	}
	messageJSON, err := json.Marshal(result.Messages)
	if err != nil {
		t.Fatalf("marshal parent messages: %v", err)
	}
	for _, secret := range []string{"hidden reasoning", "classified", "nested secret output", `"name":"lookup"`} {
		if strings.Contains(string(messageJSON), secret) {
			t.Fatalf("parent transcript leaked child state %q: %s", secret, messageJSON)
		}
	}
}

type agentToolCancelableStream struct {
	ctx       context.Context
	emitted   bool
	emittedCh chan struct{}
}

func (stream *agentToolCancelableStream) Recv() (provider.Event, error) {
	if !stream.emitted {
		stream.emitted = true
		close(stream.emittedCh)
		return provider.Event{
			Kind:  provider.EventTextDelta,
			Text:  "partial",
			Usage: provider.Usage{InputTokens: 2, OutputTokens: 3, TotalTokens: 5},
		}, nil
	}
	<-stream.ctx.Done()
	return provider.Event{}, stream.ctx.Err()
}

func (*agentToolCancelableStream) Close() error { return nil }

func TestAgentTool_PropagatesCancellationAndSinkFailure(t *testing.T) {
	newDriver := func(t *testing.T, providerDriver provider.Driver) tool.Driver {
		t.Helper()
		driver, err := NewAgentTool(Engine{Provider: providerDriver}, AgentToolConfig{
			Definition: tool.Definition{Name: "researcher", Description: "delegate research"},
		})
		if err != nil {
			t.Fatalf("NewAgentTool() error = %v", err)
		}
		return driver
	}
	call := tool.Call{ID: "outer", Name: "researcher", Arguments: json.RawMessage(`{"task":"review"}`)}

	t.Run("parent cancellation after child starts", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		dispatchCtx, collector := withAgentToolDispatchContext(parentCtx, 0, provider.Usage{})
		emitted := make(chan struct{})
		driver := newDriver(t, agentToolProviderFunc(func(ctx context.Context, _ provider.Request) (provider.Stream, error) {
			return &agentToolCancelableStream{ctx: ctx, emittedCh: emitted}, nil
		}))
		go func() {
			<-emitted
			cancel()
		}()
		_, err := driver.Execute(dispatchCtx, call, nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context.Canceled", err)
		}
		if errors.Is(err, tool.ErrNotExecuted) {
			t.Fatalf("post-start cancellation matched ErrNotExecuted: %v", err)
		}
		if got := collector.snapshot().TotalTokens; got != 5 {
			t.Fatalf("reported usage = %d, want 5", got)
		}
	})

	t.Run("sink error", func(t *testing.T) {
		sentinel := errors.New("update sink failed")
		dispatchCtx, collector := withAgentToolDispatchContext(context.Background(), 0, provider.Usage{})
		driver := newDriver(t, agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
			return provider.NewSliceStream([]provider.Event{
				{
					Kind:  provider.EventTextDelta,
					Text:  "partial",
					Usage: provider.Usage{InputTokens: 2, OutputTokens: 3, TotalTokens: 5},
				},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
			}), nil
		}))
		_, err := driver.Execute(dispatchCtx, call, func(tool.Update) error { return sentinel })
		if !errors.Is(err, sentinel) {
			t.Fatalf("Execute() error = %v, want sentinel", err)
		}
		if errors.Is(err, tool.ErrNotExecuted) {
			t.Fatalf("post-start sink error matched ErrNotExecuted: %v", err)
		}
		if got := collector.snapshot().TotalTokens; got != 5 {
			t.Fatalf("reported usage = %d, want 5", got)
		}
	})

	t.Run("sink panic", func(t *testing.T) {
		dispatchCtx, collector := withAgentToolDispatchContext(context.Background(), 0, provider.Usage{})
		driver := newDriver(t, agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
			return provider.NewSliceStream([]provider.Event{
				{
					Kind:  provider.EventTextDelta,
					Text:  "partial",
					Usage: provider.Usage{InputTokens: 2, OutputTokens: 3, TotalTokens: 5},
				},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
			}), nil
		}))
		_, err := driver.Execute(dispatchCtx, call, func(tool.Update) error {
			panic("sink exploded")
		})
		if err == nil || !strings.Contains(err.Error(), `agent tool "researcher" update sink panicked: sink exploded`) {
			t.Fatalf("Execute() error = %v", err)
		}
		if errors.Is(err, tool.ErrNotExecuted) {
			t.Fatalf("post-start sink panic matched ErrNotExecuted: %v", err)
		}
		if got := collector.snapshot().TotalTokens; got != 5 {
			t.Fatalf("reported usage = %d, want 5", got)
		}
	})
}

func TestAgentTool_RollsUsageIntoParentExactlyOnce(t *testing.T) {
	grandchildUsage := provider.Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5}
	grandchild, err := NewAgentTool(Engine{
		Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
			return provider.NewSliceStream([]provider.Event{
				{Kind: provider.EventTextDelta, Text: "grandchild answer"},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: grandchildUsage},
			}), nil
		}),
	}, AgentToolConfig{
		Definition: tool.Definition{Name: "grandchild", Description: "delegate to grandchild"},
	})
	if err != nil {
		t.Fatalf("NewAgentTool(grandchild) error = %v", err)
	}

	var childToolsComplete provider.Usage
	childTurn := 0
	child := Engine{
		Provider: agentToolProviderFunc(func(_ context.Context, request provider.Request) (provider.Stream, error) {
			childTurn++
			if childTurn == 1 {
				return provider.NewSliceStream([]provider.Event{
					{
						Kind: provider.EventToolCall,
						ToolCall: &message.ToolCall{
							ID:        "grandchild-call",
							Name:      "grandchild",
							Arguments: json.RawMessage(`{"task":"continue"}`),
						},
					},
					{
						Kind:       provider.EventDone,
						StopReason: provider.StopReasonToolUse,
						Usage:      provider.Usage{InputTokens: 4, OutputTokens: 1, TotalTokens: 5},
					},
				}), nil
			}
			last := request.Messages[len(request.Messages)-1]
			return provider.NewSliceStream([]provider.Event{
				{Kind: provider.EventTextDelta, Text: "child received: " + last.ToolResult.Content},
				{
					Kind:       provider.EventDone,
					StopReason: provider.StopReasonComplete,
					Usage:      provider.Usage{InputTokens: 2, OutputTokens: 3, TotalTokens: 5},
				},
			}), nil
		}),
		Tools: tool.NewBus(grandchild),
		Boundaries: BoundaryObserverFunc(func(_ context.Context, continuation Continuation) error {
			if continuation.Phase == ContinuationToolsComplete {
				childToolsComplete = continuation.Usage
			}
			return nil
		}),
	}
	childTool, err := NewAgentTool(child, AgentToolConfig{
		Definition: tool.Definition{Name: "child", Description: "delegate to child"},
	})
	if err != nil {
		t.Fatalf("NewAgentTool(child) error = %v", err)
	}

	var parentToolsComplete provider.Usage
	parentTurn := 0
	parent := Engine{
		Provider: agentToolProviderFunc(func(_ context.Context, request provider.Request) (provider.Stream, error) {
			parentTurn++
			if parentTurn == 1 {
				return provider.NewSliceStream([]provider.Event{
					{
						Kind: provider.EventToolCall,
						ToolCall: &message.ToolCall{
							ID:        "child-call",
							Name:      "child",
							Arguments: json.RawMessage(`{"task":"start"}`),
						},
					},
					{
						Kind:       provider.EventDone,
						StopReason: provider.StopReasonToolUse,
						Usage:      provider.Usage{InputTokens: 5, OutputTokens: 2, TotalTokens: 7},
					},
				}), nil
			}
			last := request.Messages[len(request.Messages)-1]
			return provider.NewSliceStream([]provider.Event{
				{Kind: provider.EventTextDelta, Text: "parent received: " + last.ToolResult.Content},
				{
					Kind:       provider.EventDone,
					StopReason: provider.StopReasonComplete,
					Usage:      provider.Usage{InputTokens: 2, OutputTokens: 1, TotalTokens: 3},
				},
			}), nil
		}),
		Tools: tool.NewBus(childTool),
		Boundaries: BoundaryObserverFunc(func(_ context.Context, continuation Continuation) error {
			if continuation.Phase == ContinuationToolsComplete {
				parentToolsComplete = continuation.Usage
			}
			return nil
		}),
	}
	result := parent.Run(context.Background(), Request{Prompt: "delegate"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatalf("parent result failure = %v", result.Failure)
	}
	if childToolsComplete.InputTokens != 7 || childToolsComplete.OutputTokens != 3 || childToolsComplete.TotalTokens != 10 {
		t.Fatalf("child tools-complete usage = %#v", childToolsComplete)
	}
	if parentToolsComplete.InputTokens != 14 || parentToolsComplete.OutputTokens != 8 || parentToolsComplete.TotalTokens != 22 {
		t.Fatalf("parent tools-complete usage = %#v", parentToolsComplete)
	}
	if result.Usage.InputTokens != 16 || result.Usage.OutputTokens != 9 || result.Usage.TotalTokens != 25 {
		t.Fatalf("parent result usage = %#v", result.Usage)
	}
	if len(result.Steps) < 1 || result.Steps[0].BudgetUsed.Tokens != 22 {
		t.Fatalf("parent tool-step budget = %#v", result.Steps)
	}
	if result.ToolCallsUsed != 1 {
		t.Fatalf("parent tool calls used = %d, want 1", result.ToolCallsUsed)
	}
}

type agentToolConcurrencyProbe struct {
	current atomic.Int32
	maximum atomic.Int32
	started chan string
	release <-chan struct{}
}

func (probe *agentToolConcurrencyProbe) enter(name string) {
	current := probe.current.Add(1)
	for {
		maximum := probe.maximum.Load()
		if current <= maximum || probe.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	probe.started <- name
	<-probe.release
	probe.current.Add(-1)
}

type agentToolBudgetObservation struct {
	name   string
	budget *Budget
}

func newAgentToolParallelParent(tools *tool.Bus, names []string, firstUsage, finalUsage provider.Usage) (Engine, *atomic.Int32) {
	var turns atomic.Int32
	return Engine{
		Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
			turn := turns.Add(1)
			if turn == 1 {
				events := make([]provider.Event, 0, len(names)+1)
				for _, name := range names {
					events = append(events, provider.Event{
						Kind: provider.EventToolCall,
						ToolCall: &message.ToolCall{
							ID:        "call-" + name,
							Name:      name,
							Arguments: json.RawMessage(`{"task":"work"}`),
						},
					})
				}
				events = append(events, provider.Event{
					Kind:       provider.EventDone,
					StopReason: provider.StopReasonToolUse,
					Usage:      firstUsage,
				})
				return provider.NewSliceStream(events), nil
			}
			return provider.NewSliceStream([]provider.Event{
				{Kind: provider.EventTextDelta, Text: "parent complete"},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: finalUsage},
			}), nil
		}),
		Tools:    tools,
		ToolMode: tool.ModeParallel,
	}, &turns
}

func TestAgentTool_BoundedParentSerializesAndCapsParallelChildren(t *testing.T) {
	newConcurrentTool := func(
		t *testing.T,
		name string,
		usage provider.Usage,
		probe *agentToolConcurrencyProbe,
		budgets chan<- agentToolBudgetObservation,
	) tool.Driver {
		t.Helper()
		child := Engine{
			Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
				probe.enter(name)
				return provider.NewSliceStream([]provider.Event{
					{Kind: provider.EventTextDelta, Text: name + " answer"},
					{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: usage},
				}), nil
			}),
			ContextBuilder: ContextBuilderFunc(func(_ context.Context, request Request) ([]message.Message, error) {
				if budgets != nil {
					budgets <- agentToolBudgetObservation{name: name, budget: cloneBudget(request.Budget)}
				}
				return []message.Message{message.NewText(message.RoleUser, request.Prompt)}, nil
			}),
		}
		driver, err := NewAgentTool(child, AgentToolConfig{
			Definition: tool.Definition{Name: name, Description: "delegate to " + name},
		})
		if err != nil {
			t.Fatalf("NewAgentTool(%s) error = %v", name, err)
		}
		return driver
	}

	t.Run("bounded parent serializes a parallel batch", func(t *testing.T) {
		release := make(chan struct{})
		probe := &agentToolConcurrencyProbe{
			started: make(chan string, 2),
			release: release,
		}
		budgets := make(chan agentToolBudgetObservation, 2)
		usageByName := map[string]provider.Usage{
			"child-a": {InputTokens: 12, OutputTokens: 8, TotalTokens: 20},
			"child-b": {InputTokens: 18, OutputTokens: 12, TotalTokens: 30},
		}
		childA := newConcurrentTool(t, "child-a", usageByName["child-a"], probe, budgets)
		childB := newConcurrentTool(t, "child-b", usageByName["child-b"], probe, budgets)
		parent, _ := newAgentToolParallelParent(
			tool.NewBus(childA, childB),
			[]string{"child-a", "child-b"},
			provider.Usage{InputTokens: 6, OutputTokens: 4, TotalTokens: 10},
			provider.Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5},
		)
		done := make(chan Result, 1)
		go func() {
			done <- parent.Run(context.Background(), Request{
				Prompt: "parallel",
				Budget: &Budget{MaxTokens: 100},
			}, OutputPolicy{})
		}()

		var first string
		select {
		case first = <-probe.started:
		case <-time.After(time.Second):
			t.Fatal("first bounded child did not start")
		}
		select {
		case second := <-probe.started:
			t.Fatalf("bounded sibling %q started while %q was running", second, first)
		case <-time.After(50 * time.Millisecond):
		}
		close(release)
		var second string
		select {
		case second = <-probe.started:
		case <-time.After(time.Second):
			t.Fatal("second bounded child did not start")
		}
		result := <-done
		if result.Failure != nil {
			t.Fatalf("parent result failure = %v", result.Failure)
		}
		if probe.maximum.Load() != 1 {
			t.Fatalf("bounded max concurrency = %d, want 1", probe.maximum.Load())
		}
		if result.Usage.TotalTokens != 65 {
			t.Fatalf("bounded parent usage = %#v", result.Usage)
		}
		observed := map[string]int64{}
		for range 2 {
			current := <-budgets
			if current.budget == nil {
				t.Fatalf("%s budget is nil", current.name)
			}
			observed[current.name] = current.budget.MaxTokens
		}
		if observed[first] != 90 {
			t.Fatalf("first child %q max tokens = %d, want 90", first, observed[first])
		}
		wantSecond := int64(90 - usageByName[first].TotalTokens)
		if observed[second] != wantSecond {
			t.Fatalf("second child %q max tokens = %d, want %d", second, observed[second], wantSecond)
		}
	})

	t.Run("settlement returns unused claim before the next child", func(t *testing.T) {
		dispatchCtx, _ := withAgentToolDispatchContext(
			context.Background(),
			100,
			provider.Usage{TotalTokens: 10},
		)
		releaseA := make(chan struct{})
		aBudget := make(chan *Budget, 1)
		bBudget := make(chan *Budget, 1)
		childA, err := NewAgentTool(Engine{
			Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
				<-releaseA
				return provider.NewSliceStream([]provider.Event{
					{Kind: provider.EventTextDelta, Text: "a answer"},
					{
						Kind:       provider.EventDone,
						StopReason: provider.StopReasonComplete,
						Usage:      provider.Usage{InputTokens: 12, OutputTokens: 8, TotalTokens: 20},
					},
				}), nil
			}),
			ContextBuilder: ContextBuilderFunc(func(_ context.Context, request Request) ([]message.Message, error) {
				aBudget <- cloneBudget(request.Budget)
				return []message.Message{message.NewText(message.RoleUser, request.Prompt)}, nil
			}),
		}, AgentToolConfig{
			Definition: tool.Definition{Name: "child-a", Description: "delegate to child a"},
			Budget:     &Budget{MaxTokens: 50, MaxSteps: 7},
		})
		if err != nil {
			t.Fatalf("NewAgentTool(child-a) error = %v", err)
		}
		childB, err := NewAgentTool(Engine{
			Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
				return provider.NewSliceStream([]provider.Event{
					{Kind: provider.EventTextDelta, Text: "b answer"},
					{
						Kind:       provider.EventDone,
						StopReason: provider.StopReasonComplete,
						Usage:      provider.Usage{InputTokens: 6, OutputTokens: 4, TotalTokens: 10},
					},
				}), nil
			}),
			ContextBuilder: ContextBuilderFunc(func(_ context.Context, request Request) ([]message.Message, error) {
				bBudget <- cloneBudget(request.Budget)
				return []message.Message{message.NewText(message.RoleUser, request.Prompt)}, nil
			}),
			LoopPolicy: LoopPolicy{Budget: &Budget{
				MaxToolCalls: 4,
				MaxSteps:     5,
				MaxWallClock: time.Minute,
			}},
		}, AgentToolConfig{
			Definition: tool.Definition{Name: "child-b", Description: "delegate to child b"},
		})
		if err != nil {
			t.Fatalf("NewAgentTool(child-b) error = %v", err)
		}

		aDone := make(chan error, 1)
		go func() {
			_, executeErr := childA.Execute(dispatchCtx, tool.Call{
				ID:        "a",
				Name:      "child-a",
				Arguments: json.RawMessage(`{"task":"a"}`),
			}, nil)
			aDone <- executeErr
		}()
		observedA := <-aBudget
		if observedA == nil || observedA.MaxTokens != 50 || observedA.MaxSteps != 7 {
			t.Fatalf("child A budget = %#v", observedA)
		}

		bDone := make(chan error, 1)
		go func() {
			_, executeErr := childB.Execute(dispatchCtx, tool.Call{
				ID:        "b",
				Name:      "child-b",
				Arguments: json.RawMessage(`{"task":"b"}`),
			}, nil)
			bDone <- executeErr
		}()
		select {
		case budget := <-bBudget:
			t.Fatalf("child B started before A settled with budget %#v", budget)
		case <-time.After(50 * time.Millisecond):
		}
		close(releaseA)
		var observedB *Budget
		select {
		case observedB = <-bBudget:
		case <-time.After(time.Second):
			t.Fatal("child B did not start after A settled")
		}
		if observedB == nil ||
			observedB.MaxTokens != 70 ||
			observedB.MaxToolCalls != 4 ||
			observedB.MaxSteps != 5 ||
			observedB.MaxWallClock != time.Minute {
			t.Fatalf("child B budget = %#v", observedB)
		}
		if err := <-aDone; err != nil {
			t.Fatalf("child A Execute() error = %v", err)
		}
		if err := <-bDone; err != nil {
			t.Fatalf("child B Execute() error = %v", err)
		}
	})

	t.Run("unbounded parent preserves parallelism", func(t *testing.T) {
		release := make(chan struct{})
		probe := &agentToolConcurrencyProbe{
			started: make(chan string, 2),
			release: release,
		}
		childA := newConcurrentTool(t, "child-a", provider.Usage{TotalTokens: 1}, probe, nil)
		childB := newConcurrentTool(t, "child-b", provider.Usage{TotalTokens: 1}, probe, nil)
		parent, _ := newAgentToolParallelParent(
			tool.NewBus(childA, childB),
			[]string{"child-a", "child-b"},
			provider.Usage{},
			provider.Usage{},
		)
		done := make(chan Result, 1)
		go func() {
			done <- parent.Run(context.Background(), Request{Prompt: "parallel"}, OutputPolicy{})
		}()
		started := map[string]bool{}
		for len(started) < 2 {
			select {
			case name := <-probe.started:
				started[name] = true
			case <-time.After(time.Second):
				t.Fatal("unbounded children did not overlap")
			}
		}
		if probe.maximum.Load() != 2 {
			t.Fatalf("unbounded max concurrency = %d, want 2", probe.maximum.Load())
		}
		close(release)
		result := <-done
		if result.Failure != nil {
			t.Fatalf("parent result failure = %v", result.Failure)
		}
	})

	t.Run("exhausted pool refuses the next child before start", func(t *testing.T) {
		var childStarts atomic.Int32
		newExhaustingTool := func(name string) tool.Driver {
			child := Engine{
				Provider: agentToolProviderFunc(func(context.Context, provider.Request) (provider.Stream, error) {
					return provider.NewSliceStream([]provider.Event{
						{Kind: provider.EventTextDelta, Text: name + " answer"},
						{
							Kind:       provider.EventDone,
							StopReason: provider.StopReasonComplete,
							Usage:      provider.Usage{InputTokens: 12, OutputTokens: 8, TotalTokens: 20},
						},
					}), nil
				}),
				ContextBuilder: ContextBuilderFunc(func(_ context.Context, request Request) ([]message.Message, error) {
					childStarts.Add(1)
					return []message.Message{message.NewText(message.RoleUser, request.Prompt)}, nil
				}),
			}
			driver, buildErr := NewAgentTool(child, AgentToolConfig{
				Definition: tool.Definition{Name: name, Description: "delegate to " + name},
			})
			if buildErr != nil {
				t.Fatalf("NewAgentTool(%s) error = %v", name, buildErr)
			}
			return driver
		}
		childA := newExhaustingTool("child-a")
		childB := newExhaustingTool("child-b")
		parent, turns := newAgentToolParallelParent(
			tool.NewBus(childA, childB),
			[]string{"child-a", "child-b"},
			provider.Usage{InputTokens: 6, OutputTokens: 4, TotalTokens: 10},
			provider.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2},
		)
		result := parent.Run(context.Background(), Request{
			Prompt: "parallel",
			Budget: &Budget{MaxTokens: 30},
		}, OutputPolicy{})
		if result.Failure == nil || result.Failure.Kind != FailureKindBudgetExhausted {
			t.Fatalf("parent failure = %#v", result.Failure)
		}
		if !errors.Is(result.Failure, ErrBudgetExhausted) || !errors.Is(result.Failure, tool.ErrNotExecuted) {
			t.Fatalf("parent failure chain = %v", result.Failure)
		}
		if childStarts.Load() != 1 {
			t.Fatalf("child starts = %d, want 1", childStarts.Load())
		}
		if turns.Load() != 1 {
			t.Fatalf("parent provider turns = %d, want 1", turns.Load())
		}
		if result.Usage.TotalTokens != 30 {
			t.Fatalf("parent usage = %#v", result.Usage)
		}
		if len(result.Steps) != 1 || result.Steps[0].BudgetUsed.Tokens != 30 {
			t.Fatalf("parent steps = %#v", result.Steps)
		}
		var batchErr *tool.BatchExecutionError
		if !errors.As(result.Failure, &batchErr) || len(batchErr.Failures) != 1 {
			t.Fatalf("batch failure = %#v", batchErr)
		}
		refused := batchErr.Failures[0].Err.Error()
		if !strings.HasSuffix(refused, "refused: parent token budget exhausted") {
			t.Fatalf("refusal error = %q", refused)
		}
	})
}
