package agent

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

func TestRunMessagesEmitsStepPerIterationToolThenFinish(t *testing.T) {
	driver, err := kit.Tool("lookup", func(_ context.Context, input struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "result:" + input.Query, nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}
	engine := Engine{Provider: fakeProvider{}, Tools: tool.NewBus(driver)}

	output, err := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "find venat")},
		MaxIterations: 3,
		ToolMode:      tool.ModeSequential,
	})
	if err != nil {
		t.Fatalf("RunMessages() error = %v", err)
	}

	// fakeProvider emits one tool call (turn 1) then prose (turn 2): two
	// iterations, so two steps.
	if output.Iterations != 2 {
		t.Fatalf("Iterations = %d, want 2", output.Iterations)
	}
	if len(output.Steps) != output.Iterations {
		t.Fatalf("len(Steps) = %d, want one per iteration (%d)", len(output.Steps), output.Iterations)
	}
	if output.ToolCallsUsed != 1 {
		t.Fatalf("ToolCallsUsed = %d, want 1", output.ToolCallsUsed)
	}

	tool0 := output.Steps[0]
	if tool0.Index != 0 || tool0.Decision != StepDecisionContinue {
		t.Fatalf("step 0 = {Index:%d Decision:%s}, want {0 continue}", tool0.Index, tool0.Decision)
	}
	if tool0.ModelCall == nil || tool0.ModelCall.Model != "test-model" {
		t.Fatalf("step 0 ModelCall = %#v, want model test-model", tool0.ModelCall)
	}
	if len(tool0.ToolCalls) != 1 || tool0.ToolCalls[0].Name != "lookup" {
		t.Fatalf("step 0 ToolCalls = %#v, want one lookup trace", tool0.ToolCalls)
	}
	if len(tool0.ToolCalls[0].Output) == 0 {
		t.Fatalf("step 0 tool trace output is empty, want the tool result")
	}
	if tool0.BudgetUsed.ToolCalls != 1 {
		t.Fatalf("step 0 BudgetUsed.ToolCalls = %d, want 1", tool0.BudgetUsed.ToolCalls)
	}

	finish := output.Steps[1]
	if finish.Index != 1 || finish.Decision != StepDecisionFinish {
		t.Fatalf("step 1 = {Index:%d Decision:%s}, want {1 finish}", finish.Index, finish.Decision)
	}
	if len(finish.ToolCalls) != 0 {
		t.Fatalf("step 1 ToolCalls = %#v, want none on the finishing turn", finish.ToolCalls)
	}
}

func TestRunMessagesEmitsStepPerIterationAtMaxTurns(t *testing.T) {
	driver, err := kit.Tool("lookup", func(_ context.Context, _ struct {
		Query string `json:"query"`
	},
	) (string, error) {
		return "result", nil
	})
	if err != nil {
		t.Fatalf("tool setup: %v", err)
	}
	engine := Engine{Provider: &alwaysToolProvider{}, Tools: tool.NewBus(driver)}

	output, err := engine.RunMessages(context.Background(), LoopInput{
		Model:    "test-model",
		Messages: []message.Message{message.NewText(message.RoleUser, "loop forever")},
		// MaxIterations unset -> default ceiling of 12.
	})
	if err != nil {
		t.Fatalf("RunMessages() error = %v", err)
	}

	if len(output.Steps) != output.Iterations || output.Iterations != 12 {
		t.Fatalf("len(Steps)=%d Iterations=%d, want both 12", len(output.Steps), output.Iterations)
	}
	if output.ToolCallsUsed != 12 {
		t.Fatalf("ToolCallsUsed = %d, want 12", output.ToolCallsUsed)
	}
	for i, step := range output.Steps {
		if step.Index != i {
			t.Fatalf("step %d has non-contiguous Index %d", i, step.Index)
		}
		if step.Decision != StepDecisionContinue {
			t.Fatalf("step %d Decision = %s, want continue (never reached a terminal turn)", i, step.Decision)
		}
		if step.BudgetUsed.ToolCalls != i+1 {
			t.Fatalf("step %d BudgetUsed.ToolCalls = %d, want cumulative %d", i, step.BudgetUsed.ToolCalls, i+1)
		}
	}
}

func TestRunMessagesStepDecisionFinishOnTerminalTool(t *testing.T) {
	driver := &scriptedProvider{
		turns: [][]provider.Event{{
			{
				Kind: provider.EventToolCall,
				ToolCall: &message.ToolCall{
					ID:        "call-1",
					Name:      "submit_report",
					Arguments: json.RawMessage(`{"answer":"done"}`),
				},
			},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		}},
	}
	engine := Engine{Provider: driver, Tools: tool.NewBus(terminalTool{})}

	output, err := engine.RunMessages(context.Background(), LoopInput{
		Model:         "test-model",
		Messages:      []message.Message{message.NewText(message.RoleUser, "finish")},
		MaxIterations: 3,
	})
	if err != nil {
		t.Fatalf("RunMessages() error = %v", err)
	}

	if len(output.Steps) != 1 {
		t.Fatalf("len(Steps) = %d, want 1 (terminal tool stops after first turn)", len(output.Steps))
	}
	step := output.Steps[0]
	if step.Decision != StepDecisionFinish {
		t.Fatalf("terminal-tool step Decision = %s, want finish", step.Decision)
	}
	if len(step.ToolCalls) != 1 || step.ToolCalls[0].Name != "submit_report" {
		t.Fatalf("terminal-tool step ToolCalls = %#v, want one submit_report trace", step.ToolCalls)
	}
	if string(step.ToolCalls[0].Output) != `{"answer":"done"}` {
		t.Fatalf("terminal-tool trace Output = %s, want the submitted arguments", step.ToolCalls[0].Output)
	}
	if output.ToolCallsUsed != 1 {
		t.Fatalf("ToolCallsUsed = %d, want 1", output.ToolCallsUsed)
	}
}

func TestEngineRepairConcatenatesStepsWithGlobalReindex(t *testing.T) {
	// Initial run returns enum-invalid output; the single repair run fixes it.
	// The initial output continues into repair, so the merged trace is two steps
	// with globally continuous indices 0 and 1 — not two index-0 collisions.
	_, engine := newOutputPolicyEngine(
		`{"status":"blocked","score":0.75,"count":2,"tags":["risk"],"accepted":true}`,
		`{"status":"ok","score":0.75,"count":2,"tags":["risk"],"accepted":true}`,
	)
	policy := outputPolicyReport()
	policy.Repair = true
	policy.MaxRepairAttempts = 1

	result := engine.Run(context.Background(), Request{Prompt: "classify risk"}, policy)

	if !result.Valid {
		t.Fatalf("result.Valid = false, failure = %#v", result.Failure)
	}
	if result.RepairCount != 1 {
		t.Fatalf("RepairCount = %d, want 1", result.RepairCount)
	}
	if len(result.Steps) != 2 {
		t.Fatalf("len(Steps) = %d, want 2 (initial + repair turn)", len(result.Steps))
	}
	for i, step := range result.Steps {
		if step.Index != i {
			t.Fatalf("step %d has Index %d; repair steps must be globally reindexed", i, step.Index)
		}
		want := StepDecisionFinish
		if i == 0 {
			want = StepDecisionContinue
		}
		if step.Decision != want {
			t.Fatalf("step %d Decision = %s, want %s", i, step.Decision, want)
		}
	}
}

func TestRunMessagesObservesFinalizedSteps(t *testing.T) {
	engine := newLoopToolEngine(t, &alwaysToolProvider{})
	var observed []Step
	output, err := engine.RunMessages(context.Background(), LoopInput{
		Model:    "test-model",
		Messages: []message.Message{message.NewText(message.RoleUser, "loop")},
		StepDecider: stepDeciderFunc(func(LoopSnapshot) (StepDecision, error) {
			return StepDecisionFinish, nil
		}),
		StepObserver: StepObserverFunc(func(_ context.Context, step Step) error {
			observed = append(observed, step)
			return nil
		}),
	})
	if err != nil {
		t.Fatalf("RunMessages() error = %v", err)
	}
	if len(observed) != 1 {
		t.Fatalf("observed %d steps, want 1", len(observed))
	}
	if observed[0].Decision != StepDecisionFinish {
		t.Fatalf("observed decision = %q, want finish", observed[0].Decision)
	}
	if !reflect.DeepEqual(observed, output.Steps) {
		t.Fatalf("observed steps = %#v, output steps = %#v", observed, output.Steps)
	}
}

func TestRunMessagesObserverFailureReturnsPartialTrace(t *testing.T) {
	t.Run("observer failure", func(t *testing.T) {
		observeErr := errors.New("observer unavailable")
		engine := Engine{Provider: singleTurnProvider("done")}
		output, err := engine.RunMessages(context.Background(), LoopInput{
			Model:    "test-model",
			Messages: []message.Message{message.NewText(message.RoleUser, "finish")},
			StepObserver: StepObserverFunc(func(context.Context, Step) error {
				return observeErr
			}),
		})
		if !errors.Is(err, observeErr) {
			t.Fatalf("RunMessages() error = %v, want observer cause", err)
		}
		if !strings.Contains(err.Error(), "agent: observe step 0:") {
			t.Fatalf("RunMessages() error = %q, want indexed observer prefix", err)
		}
		if len(output.Steps) != 1 || output.Steps[0].Decision != StepDecisionFinish {
			t.Fatalf("partial Steps = %#v, want finalized finish step", output.Steps)
		}
		if output.StopReason != provider.StopReasonError {
			t.Fatalf("partial StopReason = %q, want error", output.StopReason)
		}
	})

	t.Run("decider and observer failures join", func(t *testing.T) {
		decideErr := errors.New("decider failed")
		observeErr := errors.New("observer failed")
		engine := newLoopToolEngine(t, &alwaysToolProvider{})
		output, err := engine.RunMessages(context.Background(), LoopInput{
			Model:    "test-model",
			Messages: []message.Message{message.NewText(message.RoleUser, "loop")},
			StepDecider: stepDeciderFunc(func(LoopSnapshot) (StepDecision, error) {
				return "", decideErr
			}),
			StepObserver: StepObserverFunc(func(context.Context, Step) error {
				return observeErr
			}),
		})
		if !errors.Is(err, decideErr) || !errors.Is(err, observeErr) || !errors.Is(err, ErrStepAborted) {
			t.Fatalf("RunMessages() error = %v, want decider, observer, and ErrStepAborted causes", err)
		}
		if len(output.Steps) != 1 {
			t.Fatalf("partial Steps = %#v, want one finalized step", output.Steps)
		}
	})
}

func TestEngineRepairStepObserverUsesGlobalIndexes(t *testing.T) {
	_, engine := newOutputPolicyEngine(
		`{"status":"blocked","score":0.75,"count":2,"tags":["risk"],"accepted":true}`,
		`{"status":"ok","score":0.75,"count":2,"tags":["risk"],"accepted":true}`,
	)
	var observed []Step
	engine.StepObserver = StepObserverFunc(func(_ context.Context, step Step) error {
		observed = append(observed, step)
		return nil
	})
	policy := outputPolicyReport()
	policy.Repair = true
	policy.MaxRepairAttempts = 1

	result := engine.Run(context.Background(), Request{Prompt: "classify risk"}, policy)
	if result.Failure != nil {
		t.Fatalf("Run() failure = %#v", result.Failure)
	}
	if len(observed) != 2 {
		t.Fatalf("observed %d steps, want initial and repair steps", len(observed))
	}
	for index, step := range observed {
		if step.Index != index {
			t.Fatalf("observed step %d has Index %d, want %d", index, step.Index, index)
		}
	}
}

type operationRecordingDriver struct {
	calls []tool.Call
}

func (d *operationRecordingDriver) Definition() tool.Definition {
	return tool.Definition{Name: "write"}
}

func (d *operationRecordingDriver) Execute(_ context.Context, call tool.Call, _ tool.UpdateSink) (tool.Result, error) {
	d.calls = append(d.calls, call)
	return tool.Result{ToolCallID: call.ID, Name: call.Name, Content: "ok"}, nil
}

func TestEngineAssignsStableDistinctToolOperationSlots(t *testing.T) {
	driver := &operationRecordingDriver{}
	engine := Engine{
		Provider: &scriptedProvider{turns: [][]provider.Event{
			{
				{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "provider-a", Name: "write", Arguments: json.RawMessage(`{"same":true}`)}},
				{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "provider-b", Name: "write", Arguments: json.RawMessage(`{"same":true}`)}},
				{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
			},
			{
				{Kind: provider.EventTextDelta, Text: "done"},
				{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
			},
		}},
		Tools:         tool.NewBus(driver),
		LoopPolicy:    LoopPolicy{MaxIterations: 2},
		OperationTurn: 2,
	}
	result := engine.Run(context.Background(), Request{Prompt: "write twice"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatalf("Engine.Run() failure = %v", result.Failure)
	}
	if len(driver.calls) != 2 {
		t.Fatalf("driver calls = %d, want 2", len(driver.calls))
	}
	if driver.calls[0].OperationID != "turn:2:call:0" || driver.calls[1].OperationID != "turn:2:call:1" {
		t.Fatalf("operation IDs = %q, %q", driver.calls[0].OperationID, driver.calls[1].OperationID)
	}
	if next := nextToolOperationTurn(result.Messages); next != 3 {
		t.Fatalf("next operation turn = %d, want 3", next)
	}
	if driver.calls[0].OperationID == driver.calls[1].OperationID {
		t.Fatal("identical repeated calls collapsed onto one logical operation slot")
	}
	for _, current := range result.Messages {
		if current.Role == message.RoleAssistant && len(current.ToolCalls) == 2 {
			if current.ToolCalls[0].OperationID != driver.calls[0].OperationID ||
				current.ToolCalls[1].OperationID != driver.calls[1].OperationID {
				t.Fatalf("checkpoint transcript lost operation IDs: %#v", current.ToolCalls)
			}
			return
		}
	}
	t.Fatal("assistant tool-call turn missing from result")
}
