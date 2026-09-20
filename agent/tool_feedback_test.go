package agent

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

func TestEngine_MixedRejectedCallsContinueUntilTerminalToolSucceeds(t *testing.T) {
	for _, mode := range []tool.Mode{tool.ModeSequential, tool.ModeParallel} {
		t.Run(string(mode), func(t *testing.T) {
			var checkCalls, lookupCalls atomic.Int32
			check, err := kit.Tool("check", func(_ context.Context, input struct {
				Value int8 `json:"value"`
			}) (tool.Result, error) {
				checkCalls.Add(1)
				if input.Value < 0 {
					return tool.Result{Content: "value must be nonnegative", IsError: true}, nil
				}
				return tool.Result{Content: "accepted"}, nil
			}, kit.Terminal())
			if err != nil {
				t.Fatal(err)
			}
			lookup, err := kit.Tool("lookup", func(context.Context, struct{}) (string, error) { lookupCalls.Add(1); return "available", nil })
			if err != nil {
				t.Fatal(err)
			}
			calls := []message.ToolCall{
				{ID: "missing", Name: "ghost", Arguments: json.RawMessage(`{}`)},
				{ID: "schema", Name: "check", Arguments: json.RawMessage(`{"value":"bad"}`)},
				{ID: "overflow", Name: "check", Arguments: json.RawMessage(`{"value":999}`)},
				{ID: "domain", Name: "check", Arguments: json.RawMessage(`{"value":-1}`)},
				{ID: "valid", Name: "lookup", Arguments: json.RawMessage(`{}`)},
			}
			var firstTurn []provider.Event
			for _, call := range calls {
				current := call
				firstTurn = append(firstTurn, provider.Event{Kind: provider.EventToolCall, ToolCall: &current})
			}
			firstTurn = append(firstTurn, provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse})
			model := &scriptedProvider{turns: [][]provider.Event{firstTurn, {
				{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "fixed", Name: "check", Arguments: json.RawMessage(`{"value":1}`)}},
				{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
			}}}
			engine := Engine{Provider: model, Tools: tool.NewBus(check, lookup), ToolMode: mode, Boundaries: BoundaryObserverFunc(func(_ context.Context, continuation Continuation) error { return ValidateContinuation(continuation) })}
			result := engine.Run(context.Background(), Request{Prompt: "check"}, OutputPolicy{})
			if result.Failure != nil || len(model.requests) != 2 || result.ToolCallsUsed != 6 || checkCalls.Load() != 2 || lookupCalls.Load() != 1 {
				t.Fatalf("result=%+v check=%d lookup=%d", result, checkCalls.Load(), lookupCalls.Load())
			}
			history := model.requests[1].Messages
			feedback := history[len(history)-len(calls):]
			for index, current := range feedback {
				if current.ToolResult == nil || current.ToolResult.ToolCallID != calls[index].ID || current.ToolResult.IsError != (index < 4) {
					t.Fatalf("feedback[%d]=%+v", index, current)
				}
			}
			if result.Steps[0].Decision != StepDecisionContinue || result.Steps[1].Decision != StepDecisionFinish {
				t.Fatalf("decisions=%+v", result.Steps)
			}
		})
	}
}

func TestEngine_RepeatedRejectedCallsStillExhaustBudget(t *testing.T) {
	model := &scriptedProvider{turns: [][]provider.Event{{
		{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "missing", Name: "ghost", Arguments: json.RawMessage(`{}`)}},
		{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
	}}}
	result := (Engine{Provider: model}).Run(context.Background(), Request{Prompt: "check", Budget: &Budget{MaxToolCalls: 1}}, OutputPolicy{})
	if result.Failure == nil || !errors.Is(result.Failure, ErrBudgetExhausted) || result.ToolCallsUsed != 1 || len(model.requests) != 1 {
		t.Fatalf("result=%+v", result)
	}
}
