package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

type workingMemoryProvider struct {
	streams [][]provider.Event
	calls   []provider.Request
	err     error
}

func (p *workingMemoryProvider) Metadata() provider.Metadata {
	return provider.Metadata{Name: "working-memory-test"}
}

func (p *workingMemoryProvider) Stream(_ context.Context, request provider.Request) (provider.Stream, error) {
	p.calls = append(p.calls, request)
	if p.err != nil {
		return nil, p.err
	}
	if len(p.streams) == 0 {
		return nil, errors.New("unexpected summary call")
	}
	stream := provider.NewSliceStream(p.streams[0])
	p.streams = p.streams[1:]
	return stream, nil
}

func workingMemoryHistory() []message.Message {
	return []message.Message{
		{Role: message.RoleSystem, Kind: message.KindStandard, Text: "system", Content: []message.ContentPart{message.TextPart("system")}, CacheBoundary: true},
		message.NewText(message.RoleUser, "old goal; artifact ref artifact://old"),
		{Role: message.RoleAssistant, Kind: message.KindStandard, Text: "old answer", ToolCalls: []message.ToolCall{{ID: "lookup-1", Name: "lookup", Arguments: json.RawMessage(`{"query":"old"}`)}}, Content: []message.ContentPart{message.TextPart("old answer")}},
		message.NewToolResult(message.ToolResult{ToolCallID: "lookup-1", Name: "lookup", Content: "verified /tmp/old.txt"}),
		message.NewText(message.RoleUser, "CORRECTION: use artifact://new and preserve constraint no network"),
		message.NewText(message.RoleAssistant, "latest answer"),
	}
}

func TestWorkingMemory_CompactsSemanticSummaryAndKeepsRecentCompleteTurn(t *testing.T) {
	model := &workingMemoryProvider{streams: [][]provider.Event{{
		{Kind: provider.EventTextDelta, Text: "Goal\nupdated task\nConstraints\nno network\nEvidence\nverified /tmp/old.txt\nFiles and artifact references\nartifact://old\nFailures and decisions\nnone\nTodos\ncontinue"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete, Usage: provider.Usage{InputTokens: 20, OutputTokens: 15, TotalTokens: 35}},
	}}}
	memory, err := NewWorkingMemory(WorkingMemoryConfig{
		Provider:    model,
		Model:       "summary-model",
		RecentTurns: 1,
		Estimator: WorkingMemoryEstimatorFunc(func(_ context.Context, _ string, messages []message.Message) (int, error) {
			return len(messages) * 10, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	history := workingMemoryHistory()
	compacted, err := memory.CompactTo(context.Background(), history, 50)
	if err != nil {
		t.Fatal(err)
	}
	if err := message.ValidateCompleteTurns(compacted); err != nil {
		t.Fatalf("compacted history split a tool turn: %v", err)
	}
	archivedEvidence := false
	for _, current := range compacted {
		if current.ContextArchived && current.ToolResult != nil && current.ToolResult.ToolCallID == "lookup-1" {
			archivedEvidence = true
		}
	}
	if !archivedEvidence {
		t.Fatal("compaction deleted execution evidence instead of archiving it")
	}
	compacted = message.ContextView(compacted)
	if err := message.ValidateCompleteTurns(compacted); err != nil {
		t.Fatalf("model view split a tool turn: %v", err)
	}
	if len(model.calls) != 1 {
		t.Fatalf("summary calls = %d, want 1", len(model.calls))
	}
	if compacted[0].Role != message.RoleSystem || !compacted[0].CacheBoundary {
		t.Fatalf("system/cache prefix was not preserved: %#v", compacted[0])
	}
	if compacted[1].Role != message.RoleUser || compacted[1].Kind != message.KindCompactionSummary || !strings.Contains(compacted[1].Text, "artifact://old") {
		t.Fatalf("summary = %#v, want untrusted compaction summary retaining artifact reference", compacted[1])
	}
	if compacted[len(compacted)-2].Text != "CORRECTION: use artifact://new and preserve constraint no network" {
		t.Fatalf("newest user correction was not retained: %#v", compacted[len(compacted)-2])
	}
	if compacted[len(compacted)-1].Text != "latest answer" {
		t.Fatalf("recent answer was not retained: %#v", compacted[len(compacted)-1])
	}
}

func TestWorkingMemory_DoesNotResummarizeWhenAlreadyFits(t *testing.T) {
	model := &workingMemoryProvider{}
	memory, err := NewWorkingMemory(WorkingMemoryConfig{Provider: model, Model: "summary-model", Estimator: WorkingMemoryEstimatorFunc(func(context.Context, string, []message.Message) (int, error) { return 2, nil })})
	if err != nil {
		t.Fatal(err)
	}
	history := workingMemoryHistory()
	got, err := memory.CompactTo(context.Background(), history, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(model.calls) != 0 {
		t.Fatalf("summary calls = %d, want 0", len(model.calls))
	}
	if got[0].Text != history[0].Text || len(got) != len(history) {
		t.Fatalf("fitting history changed: got %d messages", len(got))
	}
}

func TestWorkingMemory_SummaryFailureIsNotDestructive(t *testing.T) {
	transport := errors.New("transport down")
	model := &workingMemoryProvider{err: transport}
	memory, err := NewWorkingMemory(WorkingMemoryConfig{
		Provider:  model,
		Model:     "summary-model",
		Estimator: WorkingMemoryEstimatorFunc(func(_ context.Context, _ string, messages []message.Message) (int, error) { return len(messages), nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	original := workingMemoryHistory()
	_, err = memory.CompactTo(context.Background(), original, 1)
	if !errors.Is(err, transport) {
		t.Fatalf("error = %v, want transport failure", err)
	}
	if original[1].Text != "old goal; artifact ref artifact://old" || original[3].ToolResult.Content != "verified /tmp/old.txt" {
		t.Fatalf("input history was mutated after failure: %#v", original)
	}
}

func TestWorkingMemory_RejectsTruncatedSummary(t *testing.T) {
	model := &workingMemoryProvider{streams: [][]provider.Event{{
		{Kind: provider.EventTextDelta, Text: "partial"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonLength},
	}}}
	memory, err := NewWorkingMemory(WorkingMemoryConfig{Provider: model, Model: "summary-model", Estimator: WorkingMemoryEstimatorFunc(func(context.Context, string, []message.Message) (int, error) { return 10, nil })})
	if err != nil {
		t.Fatal(err)
	}
	_, err = memory.CompactTo(context.Background(), workingMemoryHistory(), 1)
	if !errors.Is(err, ErrWorkingMemorySummaryTruncated) {
		t.Fatalf("error = %v, want ErrWorkingMemorySummaryTruncated", err)
	}
}

func TestWorkingMemory_RetainsRecentCompleteToolGroup(t *testing.T) {
	model := &workingMemoryProvider{streams: [][]provider.Event{{
		{Kind: provider.EventTextDelta, Text: "Goal\nkeep tool evidence\nConstraints\nnone\nEvidence\nlatest\nFiles and artifact references\n/tmp/latest\nFailures and decisions\nnone\nTodos\ncontinue"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}}}
	memory, err := NewWorkingMemory(WorkingMemoryConfig{
		Provider:    model,
		Model:       "summary-model",
		RecentTurns: 1,
		Estimator: WorkingMemoryEstimatorFunc(func(_ context.Context, _ string, messages []message.Message) (int, error) {
			return len(messages) * 10, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	history := []message.Message{
		{Role: message.RoleSystem, Kind: message.KindStandard, Text: "system", CacheBoundary: true},
		message.NewText(message.RoleUser, "task"),
		{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{{ID: "old", Name: "lookup"}}},
		message.NewToolResult(message.ToolResult{ToolCallID: "old", Name: "lookup", Content: "old evidence"}),
		{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{{ID: "latest", Name: "lookup"}}},
		message.NewToolResult(message.ToolResult{ToolCallID: "latest", Name: "lookup", Content: "latest evidence"}),
	}
	compacted, err := memory.CompactTo(context.Background(), history, 50)
	if err != nil {
		t.Fatal(err)
	}
	if err := message.ValidateCompleteTurns(compacted); err != nil {
		t.Fatal(err)
	}
	if compacted[len(compacted)-1].ToolResult == nil || compacted[len(compacted)-1].ToolResult.ToolCallID != "latest" {
		t.Fatalf("latest complete tool group was not retained: %#v", compacted)
	}
}
