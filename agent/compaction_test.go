package agent

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

// recordingCompactor counts how many times the loop invokes it and, when
// replace is set, returns that slice as the compacted history. A non-nil fail is
// returned instead, to exercise the loop's compaction-error path.
type recordingCompactor struct {
	calls    int
	replace  []message.Message
	fail     error
	received []message.Message
}

func (c *recordingCompactor) compact(_ context.Context, history []message.Message) ([]message.Message, error) {
	c.calls++
	c.received = append(c.received[:0], history...)
	if c.fail != nil {
		return history, c.fail
	}
	if c.replace != nil {
		return append([]message.Message{}, c.replace...), nil
	}
	return history, nil
}

func usagePerTurn(total int) provider.Usage {
	// Split across input/output so the usage mirrors a real turn; TotalTokens is
	// what the budget and compaction trigger read.
	return provider.Usage{InputTokens: total - total/2, OutputTokens: total / 2, TotalTokens: total}
}

func TestRunMessagesCompactsWhenTokenBudgetApproached(t *testing.T) {
	// 10 tokens/turn against a 45-token budget (headroom band = 45/5 = 9): the
	// loop enters the band once 36+ tokens are spent and compacts before the next
	// turn.
	comp := &recordingCompactor{}
	engine := newLoopToolEngine(t, &usageToolProvider{perTurn: usagePerTurn(10)})
	output, _ := engine.RunMessages(context.Background(), LoopInput{
		Model:     "test-model",
		Messages:  []message.Message{message.NewText(message.RoleUser, "loop")},
		MaxTokens: 45,
		Compact:   comp.compact,
	})
	if comp.calls == 0 {
		t.Fatalf("Compact was never invoked; want at least one call once the token budget is approached (Iterations=%d)", output.Iterations)
	}
	// The first compaction lands only after the budget enters the headroom band,
	// never on the opening turns.
	if output.Iterations < 4 {
		t.Fatalf("loop ran only %d turns; compaction should trigger deep into the budget, not early", output.Iterations)
	}
}

func TestRunMessagesDoesNotCompactWithoutTokenBudget(t *testing.T) {
	comp := &recordingCompactor{}
	engine := newLoopToolEngine(t, &usageToolProvider{perTurn: usagePerTurn(10)})
	output, err := engine.RunMessages(context.Background(), LoopInput{
		Model:    "test-model",
		Messages: []message.Message{message.NewText(message.RoleUser, "loop")},
		// MaxTokens unset -> no token boundary to approach.
		Compact: comp.compact,
	})
	if err != nil {
		t.Fatalf("RunMessages() error = %v", err)
	}
	if comp.calls != 0 {
		t.Fatalf("Compact ran %d times with no token budget; it must never compact when MaxTokens is zero", comp.calls)
	}
	if output.Iterations != 12 {
		t.Fatalf("Iterations = %d, want the 12 ceiling", output.Iterations)
	}
}

func TestRunMessagesDoesNotCompactWhenBudgetRoomy(t *testing.T) {
	comp := &recordingCompactor{}
	engine := newLoopToolEngine(t, &usageToolProvider{perTurn: usagePerTurn(10)})
	_, err := engine.RunMessages(context.Background(), LoopInput{
		Model:     "test-model",
		Messages:  []message.Message{message.NewText(message.RoleUser, "loop")},
		MaxTokens: 1_000_000, // 120 tokens over 12 turns never approaches this
		Compact:   comp.compact,
	})
	if err != nil {
		t.Fatalf("RunMessages() error = %v", err)
	}
	if comp.calls != 0 {
		t.Fatalf("Compact ran %d times with a roomy budget; it must only fire near the ceiling", comp.calls)
	}
}

func TestRunMessagesAdoptsCompactedHistory(t *testing.T) {
	// When Compact fires it replaces the working history, so the marker it
	// returns must survive into the loop's output messages.
	marker := message.NewText(message.RoleSystem, "COMPACTED-MARKER")
	comp := &recordingCompactor{replace: []message.Message{marker, message.NewText(message.RoleUser, "continue")}}
	engine := newLoopToolEngine(t, &usageToolProvider{perTurn: usagePerTurn(10)})
	output, _ := engine.RunMessages(context.Background(), LoopInput{
		Model:     "test-model",
		Messages:  []message.Message{message.NewText(message.RoleUser, "loop")},
		MaxTokens: 45,
		Compact:   comp.compact,
	})
	if comp.calls == 0 {
		t.Fatal("Compact never fired; cannot assert the loop adopted the compacted history")
	}
	if len(output.Messages) == 0 || output.Messages[0].Text != "COMPACTED-MARKER" {
		t.Fatalf("loop did not adopt the compacted history; first message = %#v", output.Messages)
	}
}

func TestRunMessagesCompactionErrorAbortsLoop(t *testing.T) {
	boom := errors.New("compact boom")
	comp := &recordingCompactor{fail: boom}
	engine := newLoopToolEngine(t, &usageToolProvider{perTurn: usagePerTurn(10)})
	_, err := engine.RunMessages(context.Background(), LoopInput{
		Model:     "test-model",
		Messages:  []message.Message{message.NewText(message.RoleUser, "loop")},
		MaxTokens: 45,
		Compact:   comp.compact,
	})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want it to wrap the compaction error", err)
	}
}

func TestRunMessagesRejectsIncompleteCompactedToolTurn(t *testing.T) {
	comp := &recordingCompactor{replace: []message.Message{
		message.NewText(message.RoleSystem, "compacted"),
		{
			Role: message.RoleAssistant,
			ToolCalls: []message.ToolCall{{
				ID:   "missing-result",
				Name: "tool",
			}},
		},
	}}
	engine := newLoopToolEngine(t, &usageToolProvider{perTurn: usagePerTurn(10)})
	output, err := engine.RunMessages(context.Background(), LoopInput{
		Model:     "test-model",
		Messages:  []message.Message{message.NewText(message.RoleUser, "original")},
		MaxTokens: 45,
		Compact:   comp.compact,
	})
	if !errors.Is(err, message.ErrIncompleteToolTurn) {
		t.Fatalf("error = %v, want errors.Is(err, message.ErrIncompleteToolTurn)", err)
	}
	if comp.calls == 0 {
		t.Fatal("Compact was never invoked")
	}
	if !reflect.DeepEqual(output.Messages, comp.received) {
		t.Fatalf("partial output did not preserve the pre-compaction history:\noutput: %#v\noriginal: %#v", output.Messages, comp.received)
	}
}

// recordingContextManager is a ContextManager whose Compact records calls, used
// to prove Engine.Run wires ContextManager.Compact into the loop.
type recordingContextManager struct {
	compactCalls int
}

func (*recordingContextManager) Build(_ context.Context, task Request) ([]message.Message, error) {
	return []message.Message{
		message.NewText(message.RoleSystem, "You are a test agent."),
		message.NewText(message.RoleUser, task.Prompt),
	}, nil
}

func (c *recordingContextManager) Compact(_ context.Context, history []message.Message) ([]message.Message, error) {
	c.compactCalls++
	return history, nil
}

func TestEngineRunWiresContextManagerCompact(t *testing.T) {
	cm := &recordingContextManager{}
	engine := newLoopToolEngine(t, &usageToolProvider{perTurn: usagePerTurn(10)})
	engine.ContextBuilder = cm
	engine.LoopPolicy.Budget = &Budget{MaxTokens: 45}

	result := engine.Run(context.Background(), Request{Prompt: "loop"}, OutputPolicy{})
	if cm.compactCalls == 0 {
		t.Fatalf("Engine.Run did not invoke ContextManager.Compact; failure=%#v", result.Failure)
	}
}

func TestEngineRunUsesLegacyCompactAsContextTargetFallback(t *testing.T) {
	cm := &recordingContextManager{}
	engine := newLoopToolEngine(t, &usageToolProvider{})
	engine.ContextBuilder = cm
	engine.LoopPolicy = LoopPolicy{MaxIterations: 2, ContextTokenTarget: 1_000}

	result := engine.Run(context.Background(), Request{Prompt: "loop"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatalf("Engine.Run failure = %#v", result.Failure)
	}
	if cm.compactCalls != 2 {
		t.Fatalf("legacy Compact calls = %d, want one before each request", cm.compactCalls)
	}
}

type recordingTargetContextManager struct {
	legacyCalls int
	targets     []int
	histories   [][]message.Message
	replace     []message.Message
	fail        error
}

func (*recordingTargetContextManager) Build(_ context.Context, task Request) ([]message.Message, error) {
	return []message.Message{message.NewText(message.RoleUser, task.Prompt)}, nil
}

func (c *recordingTargetContextManager) Compact(_ context.Context, history []message.Message) ([]message.Message, error) {
	c.legacyCalls++
	return history, nil
}

func (c *recordingTargetContextManager) CompactTo(_ context.Context, history []message.Message, targetTokens int) ([]message.Message, error) {
	c.targets = append(c.targets, targetTokens)
	c.histories = append(c.histories, append([]message.Message(nil), history...))
	if c.fail != nil {
		return history, c.fail
	}
	if c.replace != nil {
		return append([]message.Message(nil), c.replace...), nil
	}
	return history, nil
}

func TestEngineRunCompactsToContextTargetBeforeFirstRequest(t *testing.T) {
	marker := message.NewText(message.RoleUser, "COMPACTED-BEFORE-FIRST-REQUEST")
	cm := &recordingTargetContextManager{replace: []message.Message{marker}}
	driver := &scriptedProvider{turns: [][]provider.Event{{
		{Kind: provider.EventTextDelta, Text: "done"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}}}
	engine := Engine{
		Provider:       driver,
		ContextBuilder: cm,
		LoopPolicy:     LoopPolicy{ContextTokenTarget: 1_000},
	}

	result := engine.Run(context.Background(), Request{Prompt: "oversized history"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatalf("Engine.Run failure = %#v", result.Failure)
	}
	if !reflect.DeepEqual(cm.targets, []int{1_000}) {
		t.Fatalf("CompactTo targets = %v, want [1000]", cm.targets)
	}
	if cm.legacyCalls != 0 {
		t.Fatalf("legacy Compact calls = %d, want 0 when CompactTo is available", cm.legacyCalls)
	}
	if len(driver.requests) != 1 || len(driver.requests[0].Messages) != 1 || driver.requests[0].Messages[0].Text != marker.Text {
		t.Fatalf("first provider request messages = %#v, want compacted marker", driver.requests)
	}
}

func TestEngineRunPreparesContextAfterToolResult(t *testing.T) {
	cm := &recordingTargetContextManager{}
	driver := &scriptedProvider{turns: [][]provider.Event{
		{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"query":"x"}`)}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		},
		{
			{Kind: provider.EventTextDelta, Text: "done"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		},
	}}
	engine := newLoopToolEngine(t, driver)
	engine.ContextBuilder = cm
	engine.LoopPolicy = LoopPolicy{ContextTokenTarget: 2_000}

	result := engine.Run(context.Background(), Request{Prompt: "use a tool"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatalf("Engine.Run failure = %#v", result.Failure)
	}
	if !reflect.DeepEqual(cm.targets, []int{2_000, 2_000}) {
		t.Fatalf("CompactTo targets = %v, want one call before each request", cm.targets)
	}
	if len(cm.histories) != 2 {
		t.Fatalf("CompactTo history count = %d, want 2", len(cm.histories))
	}
	foundToolResult := false
	for _, current := range cm.histories[1] {
		if current.Role == message.RoleTool {
			foundToolResult = true
			break
		}
	}
	if !foundToolResult {
		t.Fatalf("second CompactTo history omitted tool result: %#v", cm.histories[1])
	}
}
func TestRunMessagesContextUsageBoundariesAndResume(t *testing.T) {
	summaryUsage := provider.Usage{InputTokens: 4, OutputTokens: 1, TotalTokens: 5}
	summariesRemaining := 2
	summaryCalls := 0
	compactTo := func(ctx context.Context, history []message.Message, _ int) ([]message.Message, error) {
		if summariesRemaining > 0 {
			summariesRemaining--
			summaryCalls++
			if observer := WorkingMemoryObserverFromContext(ctx); observer != nil {
				observer(ctx, WorkingMemoryProgress{Phase: "complete", SummaryUsage: summaryUsage})
			}
		}
		return history, nil
	}
	driver := &scriptedProvider{turns: [][]provider.Event{
		{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{
				ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"query":"x"}`),
			}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse, Usage: usagePerTurn(10)},
		},
		{
			{Kind: provider.EventTextDelta, Text: "done"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: usagePerTurn(10)},
		},
	}}
	var ready Continuation
	engine := newLoopToolEngine(t, driver)
	engine.Boundaries = BoundaryObserverFunc(func(_ context.Context, continuation Continuation) error {
		if err := ValidateContinuation(continuation); err != nil {
			return err
		}
		if continuation.Phase == ContinuationReady && len(continuation.Steps) == 1 {
			ready = cloneContinuation(continuation)
		}
		return nil
	})
	output, err := engine.RunMessages(context.Background(), LoopInput{
		Model:              "test-model",
		Messages:           []message.Message{message.NewText(message.RoleUser, "use a tool")},
		ContextTokenTarget: 100,
		CompactTo:          compactTo,
	})
	if err != nil {
		t.Fatalf("RunMessages() error = %v", err)
	}
	if summaryCalls != 2 || output.Usage.TotalTokens != 30 {
		t.Fatalf("summary calls=%d usage=%#v, want calls=2 total=30", summaryCalls, output.Usage)
	}
	if ready.Phase != ContinuationReady {
		t.Fatalf("ready checkpoint = %#v, want between-round ready boundary", ready)
	}

	resumedDriver := &scriptedProvider{turns: [][]provider.Event{{
		{Kind: provider.EventTextDelta, Text: "done"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: usagePerTurn(10)},
	}}}
	resumed := newLoopToolEngine(t, resumedDriver)
	resumed.LoopPolicy = LoopPolicy{ContextTokenTarget: 100}
	resumed.ContextBuilder = accountedContextBuilder{compact: compactTo}
	resumed.Boundaries = BoundaryObserverFunc(func(_ context.Context, continuation Continuation) error {
		return ValidateContinuation(continuation)
	})
	resumedResult := resumed.Resume(context.Background(), ready)
	if resumedResult.Failure != nil || resumedResult.Usage.TotalTokens != 30 {
		t.Fatalf("Resume() failure=%v usage=%#v, want nil and total=30", resumedResult.Failure, resumedResult.Usage)
	}
	if len(resumedDriver.requests) != 1 {
		t.Fatalf("resume model calls = %d, want 1 (no summary replay)", len(resumedDriver.requests))
	}
}

func TestEngineRunContextPreparationFailureSkipsProvider(t *testing.T) {
	boom := errors.New("target compact boom")
	cm := &recordingTargetContextManager{fail: boom}
	driver := &scriptedProvider{}
	engine := Engine{
		Provider:       driver,
		ContextBuilder: cm,
		LoopPolicy:     LoopPolicy{ContextTokenTarget: 1_000},
	}

	result := engine.Run(context.Background(), Request{Prompt: "oversized history"}, OutputPolicy{})
	if result.Failure == nil || !errors.Is(result.Failure, boom) {
		t.Fatalf("Engine.Run failure = %#v, want wrapped compaction error", result.Failure)
	}
	if len(driver.requests) != 0 {
		t.Fatalf("provider received %d requests after context preparation failed", len(driver.requests))
	}
}

func TestTargetedCompactionMustPreserveSkillContext(t *testing.T) {
	skillMessage := message.NewText(message.RoleSystem, "required skill")
	skillMessage.Metadata = map[string]string{skillContextMetadataKey: "active"}
	before := []message.Message{skillMessage, message.NewText(message.RoleUser, "task")}
	after := []message.Message{message.NewText(message.RoleUser, "task")}

	if err := validateSkillContextPreserved(before, after); err == nil {
		t.Fatal("targeted compaction accepted history without required skill context")
	}
	if err := validateSkillContextPreserved(before, before); err != nil {
		t.Fatalf("targeted compaction rejected preserved skill context: %v", err)
	}
}

func TestTargetedCompactionMustPreserveCachePrefix(t *testing.T) {
	stable := message.NewText(message.RoleSystem, "stable")
	stable.CacheBoundary = true
	before := []message.Message{stable, message.NewText(message.RoleUser, "task")}

	if err := validateCachePrefixPreserved(before, before[1:]); err == nil {
		t.Fatal("targeted compaction accepted history without the explicit cache prefix")
	}
	mutated := append([]message.Message(nil), before...)
	mutated[0].Text = "changed"
	if err := validateCachePrefixPreserved(before, mutated); err == nil {
		t.Fatal("targeted compaction accepted a changed explicit cache prefix")
	}
	if err := validateCachePrefixPreserved(before, before); err != nil {
		t.Fatalf("targeted compaction rejected preserved cache prefix: %v", err)
	}
}

func TestTargetedCompactionRejectsInPlaceCachePrefixMutation(t *testing.T) {
	stable := message.NewText(message.RoleSystem, "stable")
	stable.CacheBoundary = true
	stable.Metadata = map[string]string{"scope": "shared"}
	current := []message.Message{stable, message.NewText(message.RoleUser, "task")}

	returned, err := maybeCompactHistory(context.Background(), LoopInput{
		ContextTokenTarget: 1_000,
		CompactTo: func(_ context.Context, history []message.Message, _ int) ([]message.Message, error) {
			history[0].Text = "changed"
			history[0].Metadata["scope"] = "changed"
			return history, nil
		},
	}, current, provider.Usage{})

	if err == nil {
		t.Fatal("targeted compaction accepted an in-place cache-prefix mutation")
	}
	if current[0].Text != "stable" || current[0].Metadata["scope"] != "shared" {
		t.Fatalf("compactor mutated source cache prefix: %#v", current[0])
	}
	if !reflect.DeepEqual(returned, current) {
		t.Fatalf("error path returned mutated history:\nreturned: %#v\nsource:   %#v", returned, current)
	}
}

type accountedContextBuilder struct {
	compact func(context.Context, []message.Message, int) ([]message.Message, error)
}

func (builder accountedContextBuilder) Build(_ context.Context, request Request) ([]message.Message, error) {
	return []message.Message{message.NewText(message.RoleUser, request.Prompt)}, nil
}

func (builder accountedContextBuilder) Compact(ctx context.Context, history []message.Message) ([]message.Message, error) {
	return builder.compact(ctx, history, 0)
}

func (builder accountedContextBuilder) CompactTo(ctx context.Context, history []message.Message, target int) ([]message.Message, error) {
	return builder.compact(ctx, history, target)
}
