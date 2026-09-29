package agent

import (
	"context"
	"fmt"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
)

func attachOutputStoreTools(bus *tool.Bus, store *tool.OutputStore) (*tool.Bus, error) {
	if store == nil {
		return bus, nil
	}
	if bus == nil {
		bus = tool.NewBus()
	} else {
		bus = bus.Clone()
	}
	for _, driver := range store.Tools() {
		name := driver.Definition().Name
		if existing, exists := bus.Driver(name); exists {
			if existing != driver {
				return nil, fmt.Errorf("agent: output artifact tool %q conflicts with a registered tool", name)
			}
			continue
		}
		if err := bus.Register(driver); err != nil {
			return nil, fmt.Errorf("agent: register output artifact tool %q: %w", name, err)
		}
	}
	return bus, bus.Validate()
}

func (e Engine) contextManager() ContextManager {
	if e.ContextBuilder != nil {
		return e.ContextBuilder
	}
	if e.WorkingMemory != nil {
		return e.WorkingMemory
	}
	return nil
}

func isOutputArtifactTool(name string) bool {
	return name == "tool_output_read" || name == "tool_output_search"
}

func prepareIterationContext(ctx context.Context, input LoopInput, current []message.Message, usage provider.Usage, steps []Step, iteration, toolCallsUsed int) ([]message.Message, LoopOutput, bool, error) {
	if iteration > 0 {
		return loopTurnPreamble(ctx, input, current, usage, steps, iteration, toolCallsUsed)
	}
	if input.ContextTokenTarget <= 0 {
		return current, LoopOutput{}, false, nil
	}
	prepared, err := maybeCompactHistory(ctx, input, current, usage)
	if err != nil {
		return current, loopErrorOutput(current, usage, steps, iteration, toolCallsUsed), true, err
	}
	return prepared, LoopOutput{}, false, nil
}

func mergePreparedToolCalls(bus *tool.Bus, calls []message.ToolCall, early map[string]streamOverlapResult, pending []tool.Call) ([]tool.Call, bool) {
	prepared := make([]tool.Call, len(calls))
	pendingIndex := 0
	terminal := false
	for index, call := range calls {
		if completed, ok := early[call.ID]; ok {
			prepared[index] = completed.prepared
		} else if pendingIndex < len(pending) {
			prepared[index] = pending[pendingIndex]
			pendingIndex++
		}
		if bus != nil && bus.IsTerminal(prepared[index].Name) {
			terminal = true
		}
	}
	return prepared, terminal
}

func mergeCompletedToolResults(calls []message.ToolCall, early map[string]streamOverlapResult, normal []message.ToolResult) ([]message.ToolResult, []message.ToolResult, provider.Usage) {
	results := make([]message.ToolResult, len(calls))
	recorded := make([]message.ToolResult, 0, len(calls))
	usage := provider.Usage{}
	normalIndex := 0
	for index, call := range calls {
		if completed, ok := early[call.ID]; ok {
			results[index] = completed.result
			usage = usage.Add(completed.usage)
		} else if normalIndex < len(normal) {
			results[index] = normal[normalIndex]
			normalIndex++
		}
		if results[index].ToolCallID != "" || results[index].Name != "" {
			recorded = append(recorded, results[index])
		}
	}
	return results, recorded, usage
}

func snapshotContextUsage(steps []Step, total, context provider.Usage) {
	if len(steps) == 0 {
		return
	}
	latest := &steps[len(steps)-1]
	latest.ContextUsage = context
	latest.BudgetUsed.Tokens = int64(total.TotalTokens)
}
