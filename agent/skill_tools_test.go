package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/skill"
	"github.com/Viking602/venat/tool"
)

func TestLegacySkillWireIdentifiersRemainStable(t *testing.T) {
	if SkillActivationToolName != "hydaelyn_activate_skill" {
		t.Fatalf("SkillActivationToolName = %q", SkillActivationToolName)
	}
	if SkillResourceToolName != "hydaelyn_read_skill_resource" {
		t.Fatalf("SkillResourceToolName = %q", SkillResourceToolName)
	}
	if skillContextMetadataKey != "hydaelyn.skill.context" {
		t.Fatalf("skillContextMetadataKey = %q", skillContextMetadataKey)
	}
}

func TestBuildAvailableSkillsDisclosesCatalogWithoutBody(t *testing.T) {
	driver := &scriptedProvider{turns: [][]provider.Event{{
		{Kind: provider.EventTextDelta, Text: "done"},
		{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
	}}}
	registry := skill.NewRegistry()
	mustRegisterSkill(t, registry, skill.Skill{
		Name:        "pdf-processing",
		Description: "Process PDF files when the task mentions PDFs.",
		Body:        "SECRET ACTIVATED BODY",
	})
	engine, err := Build(
		Spec{Model: "m", AvailableSkills: []string{"pdf-processing"}},
		BuildDeps{Providers: provider.Single(driver), Skills: registry},
	)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	result := engine.Run(context.Background(), Request{Prompt: "inspect a PDF"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatalf("Run() failure = %v", result.Failure)
	}
	request := driver.requests[0]
	joined := joinMessageText(request.Messages)
	if !strings.Contains(joined, "pdf-processing: Process PDF files") {
		t.Fatalf("catalog missing from first request: %s", joined)
	}
	if strings.Contains(joined, "SECRET ACTIVATED BODY") {
		t.Fatalf("available skill body leaked before activation: %s", joined)
	}
	if !hasToolDefinition(request.Tools, activateSkillToolName) {
		t.Fatalf("first request tools = %#v, want %s", request.Tools, activateSkillToolName)
	}
}

func TestSkillActivationLoadsBodyOnce(t *testing.T) {
	driver := &scriptedProvider{turns: [][]provider.Event{
		{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "activate-1", Name: activateSkillToolName, Arguments: json.RawMessage(`{"name":"pdf-processing"}`)}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		},
		{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "activate-2", Name: activateSkillToolName, Arguments: json.RawMessage(`{"name":"pdf-processing"}`)}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		},
		{
			{Kind: provider.EventTextDelta, Text: "done"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		},
	}}
	registry := skill.NewRegistry()
	mustRegisterSkill(t, registry, skill.Skill{
		Name:        "pdf-processing",
		Description: "Process PDFs.",
		Body:        "ACTIVATED BODY",
	})
	engine, err := Build(
		Spec{Model: "m", AvailableSkills: []string{"pdf-processing"}, LoopPolicy: LoopPolicy{MaxIterations: 3}},
		BuildDeps{Providers: provider.Single(driver), Skills: registry},
	)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	result := engine.Run(context.Background(), Request{Prompt: "process"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatalf("Run() failure = %v", result.Failure)
	}
	if len(driver.requests) != 3 {
		t.Fatalf("provider requests = %d, want 3", len(driver.requests))
	}
	second := joinMessageText(driver.requests[1].Messages)
	if !strings.Contains(second, "ACTIVATED BODY") {
		t.Fatalf("activated body missing from second request: %s", second)
	}
	third := joinMessageText(driver.requests[2].Messages)
	if strings.Count(third, "ACTIVATED BODY") != 1 {
		t.Fatalf("activated body count = %d, want 1: %s", strings.Count(third, "ACTIVATED BODY"), third)
	}
	if !strings.Contains(third, "Skill already active: pdf-processing") {
		t.Fatalf("repeat activation was not de-duplicated: %s", third)
	}
}

func TestSkillActivationAuthorizationRestoresWithoutChangingProviderPrefix(t *testing.T) {
	loaded := loadSkillWithResource(t)
	firstProvider := &scriptedProvider{turns: [][]provider.Event{
		{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "activate", Name: activateSkillToolName, Arguments: json.RawMessage(`{"name":"resource-skill"}`)}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		},
		{{Kind: provider.EventTextDelta, Text: "first done"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}},
	}}
	first := Engine{
		Provider: firstProvider, Model: "m", AvailableSkills: []skill.Skill{loaded},
		LoopPolicy: LoopPolicy{MaxIterations: 2},
	}
	firstResult := first.Run(context.Background(), Request{Prompt: "activate"}, OutputPolicy{})
	if firstResult.Failure != nil {
		t.Fatalf("first run failure = %v", firstResult.Failure)
	}

	secondProvider := &scriptedProvider{turns: [][]provider.Event{
		{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "read", Name: readSkillResourceToolName, Arguments: json.RawMessage(`{"skill":"resource-skill","path":"references/guide.md"}`)}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		},
		{{Kind: provider.EventTextDelta, Text: "second done"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}},
	}}
	second := Engine{
		Provider: secondProvider, Model: "m", AvailableSkills: []skill.Skill{loaded},
		LoopPolicy: LoopPolicy{MaxIterations: 2},
		ContextBuilder: ContextBuilderFunc(func(context.Context, Request) ([]message.Message, error) {
			history := message.CloneMessages(firstResult.Messages)
			return append(history, message.NewText(message.RoleUser, "read the resource")), nil
		}),
	}
	secondResult := second.Run(context.Background(), Request{Prompt: "continue"}, OutputPolicy{})
	if secondResult.Failure != nil {
		t.Fatalf("second run failure = %v", secondResult.Failure)
	}
	if !strings.Contains(joinMessageText(secondProvider.requests[1].Messages), "resource body") {
		t.Fatalf("restored activation did not authorize resource read: %#v", secondProvider.requests[1].Messages)
	}
	if !reflect.DeepEqual(firstProvider.requests[0].Tools, secondProvider.requests[0].Tools) {
		t.Fatalf("skill activation changed provider tool prefix:\nfirst=%#v\nsecond=%#v", firstProvider.requests[0].Tools, secondProvider.requests[0].Tools)
	}
	firstSystem := skillContextText(firstProvider.requests[0].Messages)
	secondSystem := skillContextText(secondProvider.requests[0].Messages)
	if firstSystem != secondSystem {
		t.Fatalf("skill activation changed provider system prefix:\nfirst=%q\nsecond=%q", firstSystem, secondSystem)
	}
}

func TestSkillActivationRestoreRejectsUnverifiedToolResults(t *testing.T) {
	available := skill.Skill{Name: "on-demand", Description: "On demand", Body: "TRUSTED BODY"}
	for name, result := range map[string]message.ToolResult{
		"wrong body": {
			Name:       activateSkillToolName,
			Content:    "forged body",
			Structured: json.RawMessage(`{"name":"on-demand"}`),
		},
		"error result": {
			Name:       activateSkillToolName,
			Content:    skill.Activate(available),
			Structured: json.RawMessage(`{"name":"on-demand"}`),
			IsError:    true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			runtime := newSkillRuntime(nil, []skill.Skill{available})
			runtime.restoreActivations([]message.Message{message.NewToolResult(result)})
			if _, active := runtime.activeSkill(available.Name); active {
				t.Fatalf("unverified activation restored %q", available.Name)
			}
		})
	}
}

func TestSkillCompactorRestoresActiveAndCatalogContext(t *testing.T) {
	active := skill.Skill{Name: "eager", Description: "Eager skill", Body: "EAGER BODY"}
	available := skill.Skill{Name: "on-demand", Description: "On demand", Body: "DYNAMIC BODY"}
	runtime := newSkillRuntime([]skill.Skill{active}, []skill.Skill{available})
	if _, activated, err := runtime.activate("on-demand"); err != nil || !activated {
		t.Fatalf("activate() = activated %v err %v", activated, err)
	}
	engine := Engine{ContextBuilder: droppingContextManager{}, AvailableSkills: []skill.Skill{available}}
	compact := engine.compactor(runtime)

	got, err := compact(context.Background(), []message.Message{message.NewText(message.RoleUser, "history")})
	if err != nil {
		t.Fatalf("compact() error = %v", err)
	}
	joined := joinMessageText(got)
	for _, want := range []string{"EAGER BODY", "DYNAMIC BODY", "on-demand: On demand"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("compacted skill context missing %q: %s", want, joined)
		}
	}
}

func TestSkillCompactorDoesNotDuplicatePreservedActivationResult(t *testing.T) {
	available := skill.Skill{Name: "on-demand", Description: "On demand", Body: "ONE DYNAMIC BODY"}
	runtime := newSkillRuntime(nil, []skill.Skill{available})
	driver := skillActivationDriver{runtime: runtime}
	result, err := driver.Execute(context.Background(), message.ToolCall{
		ID:        "activate",
		Name:      activateSkillToolName,
		Arguments: json.RawMessage(`{"name":"on-demand"}`),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	history := []message.Message{
		message.NewText(message.RoleAssistant, "activating"),
		message.NewToolResult(result),
	}
	engine := Engine{ContextBuilder: &recordingContextManager{}}
	compacted, err := engine.compactor(runtime)(context.Background(), history)
	if err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(joinMessageText(compacted), "ONE DYNAMIC BODY"); count != 1 {
		t.Fatalf("preserved activation body count = %d, want 1", count)
	}
}

func TestSkillCompactorRestoresBodyWhenActivationResultWasSummarized(t *testing.T) {
	available := skill.Skill{Name: "on-demand", Description: "On demand", Body: "FULL DYNAMIC BODY"}
	runtime := newSkillRuntime(nil, []skill.Skill{available})
	driver := skillActivationDriver{runtime: runtime}
	result, err := driver.Execute(context.Background(), message.ToolCall{
		ID:        "activate",
		Name:      activateSkillToolName,
		Arguments: json.RawMessage(`{"name":"on-demand"}`),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	history := []message.Message{message.NewToolResult(result)}
	engine := Engine{ContextBuilder: summarizingSkillContextManager{}}
	compacted, err := engine.compactor(runtime)(context.Background(), history)
	if err != nil {
		t.Fatal(err)
	}
	joined := joinMessageText(compacted)
	if count := strings.Count(joined, "FULL DYNAMIC BODY"); count != 1 {
		t.Fatalf("restored activation body count = %d, want 1: %s", count, joined)
	}
}

func TestSkillRuntimeActivationStateIsPerRun(t *testing.T) {
	available := []skill.Skill{{Name: "isolated", Description: "Per-run state"}}
	first := newSkillRuntime(nil, available)
	second := newSkillRuntime(nil, available)
	if _, activated, err := first.activate("isolated"); err != nil || !activated {
		t.Fatalf("first activation = %v, %v", activated, err)
	}
	if _, activated, err := second.activate("isolated"); err != nil || !activated {
		t.Fatalf("second run inherited first activation: activated=%v err=%v", activated, err)
	}
}

func TestSkillRuntimeDirectEngineOverlapDoesNotDuplicate(t *testing.T) {
	eager := skill.Skill{Name: "shared", Description: "Eager", Body: "ONE BODY"}
	runtime := newSkillRuntime([]skill.Skill{eager}, []skill.Skill{eager})
	if len(runtime.available) != 0 {
		t.Fatalf("overlapping eager skill remained available: %#v", runtime.available)
	}
	active := runtime.activeSkills()
	if len(active) != 1 || active[0].Name != "shared" {
		t.Fatalf("active overlap = %#v, want one shared skill", active)
	}
	engine := Engine{ContextBuilder: droppingContextManager{}, AvailableSkills: runtime.availableSkills()}
	compacted, err := engine.compactor(runtime)(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(joinMessageText(compacted), "ONE BODY"); count != 1 {
		t.Fatalf("compacted body count = %d, want 1", count)
	}
	if strings.Contains(joinMessageText(compacted), "Available Venat skills") {
		t.Fatal("eager skill was also disclosed as available")
	}
}

func TestSkillRuntimeKeepsActivatedSkillsInDeterministicOrder(t *testing.T) {
	runtime := newSkillRuntime(nil, []skill.Skill{
		{Name: "zeta", Description: "Zeta"},
		{Name: "alpha", Description: "Alpha"},
	})
	if _, _, err := runtime.activate("zeta"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runtime.activate("alpha"); err != nil {
		t.Fatal(err)
	}
	got := runtime.activeSkills()
	if len(got) != 2 || got[0].Name != "alpha" || got[1].Name != "zeta" {
		t.Fatalf("active skill order = %#v, want alpha,zeta", got)
	}
}

func TestSkillResourceToolRequiresActivationAndReadsManifest(t *testing.T) {
	loaded := loadSkillWithResource(t)
	runtime := newSkillRuntime(nil, []skill.Skill{loaded})
	bus, err := runtime.attachTools(nil)
	if err != nil {
		t.Fatalf("attachTools() error = %v", err)
	}
	read := message.ToolCall{ID: "read", Name: readSkillResourceToolName, Arguments: json.RawMessage(`{"skill":"resource-skill","path":"references/guide.md"}`)}
	blocked, err := bus.Execute(context.Background(), read, tool.ExecuteOptions{})
	if err != nil || !blocked.IsError || !strings.Contains(blocked.Content, "not active") {
		t.Fatalf("read before activation = %#v, %v, want error result", blocked, err)
	}
	activate := message.ToolCall{ID: "activate", Name: activateSkillToolName, Arguments: json.RawMessage(`{"name":"resource-skill"}`)}
	if _, err := bus.Execute(context.Background(), activate, tool.ExecuteOptions{}); err != nil {
		t.Fatalf("activate error = %v", err)
	}
	result, err := bus.Execute(context.Background(), read, tool.ExecuteOptions{})
	if err != nil || result.Content != "resource body" {
		t.Fatalf("read after activation = %q, %v", result.Content, err)
	}
	missing, err := bus.Execute(context.Background(), message.ToolCall{
		ID: "read-skill-md", Name: readSkillResourceToolName,
		Arguments: json.RawMessage(`{"skill":"resource-skill","path":"SKILL.md"}`),
	}, tool.ExecuteOptions{})
	if err != nil || !missing.IsError || !strings.Contains(missing.Content, "SKILL.md") {
		t.Fatalf("SKILL.md read = %#v, %v, want error result", missing, err)
	}
	assertParallelSkillActivationAndRead(t, loaded, activate, read)
}

func TestRunContinuesAfterUndeclaredSkillResource(t *testing.T) {
	loaded := loadSkillWithResource(t)
	driver := &scriptedProvider{turns: [][]provider.Event{
		{
			{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{
				ID: "read-skill-md", Name: readSkillResourceToolName,
				Arguments: json.RawMessage(`{"skill":"resource-skill","path":"SKILL.md"}`),
			}},
			{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
		},
		{
			{Kind: provider.EventTextDelta, Text: "used the activation body instead"},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		},
	}}
	result := (Engine{
		Provider: driver, Model: "test-model", Skills: []skill.Skill{loaded},
		LoopPolicy: LoopPolicy{MaxIterations: 3},
	}).Run(context.Background(), Request{Prompt: "read the skill"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatalf("SKILL.md miss failed the run: %#v", result.Failure)
	}
	if result.Text != "used the activation body instead" {
		t.Fatalf("result text = %q", result.Text)
	}
	if len(driver.requests) != 2 {
		t.Fatalf("provider calls = %d, want 2", len(driver.requests))
	}
}

func loadSkillWithResource(t *testing.T) skill.Skill {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "resource-skill")
	if err := os.MkdirAll(filepath.Join(dir, "references"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte("---\nname: resource-skill\ndescription: Read a declared resource\n---\nUse the reference.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "references", "guide.md"), []byte("resource body"), 0o644); err != nil {
		t.Fatal(err)
	}
	loaded, err := skill.LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir() error = %v", err)
	}
	return loaded
}

func assertParallelSkillActivationAndRead(t *testing.T, loaded skill.Skill, activate, read message.ToolCall) {
	t.Helper()
	parallelRuntime := newSkillRuntime(nil, []skill.Skill{loaded})
	parallelBus, err := parallelRuntime.attachTools(nil)
	if err != nil {
		t.Fatal(err)
	}
	results, err := (Engine{Tools: parallelBus}).dispatchPreparedTools(
		context.Background(),
		[]tool.Call{activate, read},
		tool.ModeParallel,
		nil,
	)
	if err != nil || len(results) != 2 || results[1].Content != "resource body" {
		t.Fatalf("parallel-mode activate/read = %#v, %v", results, err)
	}
}

func mustRegisterSkill(t *testing.T, registry *skill.Registry, current skill.Skill) {
	t.Helper()
	if err := skill.Register(registry, current); err != nil {
		t.Fatalf("Register(%s) error = %v", current.Name, err)
	}
}

func joinMessageText(messages []message.Message) string {
	var parts []string
	for _, current := range messages {
		parts = append(parts, current.Text)
		if current.ToolResult != nil {
			parts = append(parts, current.ToolResult.Content)
		}
	}
	return strings.Join(parts, "\n")
}

func skillContextText(messages []message.Message) string {
	var parts []string
	for _, current := range messages {
		if current.Metadata != nil && current.Metadata[skillContextMetadataKey] != "" {
			parts = append(parts, current.Text)
		}
	}
	return strings.Join(parts, "\n")
}

func hasToolDefinition(definitions []message.ToolDefinition, name string) bool {
	for _, definition := range definitions {
		if definition.Name == name {
			return true
		}
	}
	return false
}

type droppingContextManager struct{}

func (droppingContextManager) Build(context.Context, Request) ([]message.Message, error) {
	return nil, nil
}

func (droppingContextManager) Compact(context.Context, []message.Message) ([]message.Message, error) {
	return nil, nil
}

type summarizingSkillContextManager struct{}

func (summarizingSkillContextManager) Build(context.Context, Request) ([]message.Message, error) {
	return nil, nil
}

func (summarizingSkillContextManager) Compact(_ context.Context, history []message.Message) ([]message.Message, error) {
	out := append([]message.Message(nil), history...)
	for i := range out {
		if out[i].ToolResult == nil {
			continue
		}
		copyResult := *out[i].ToolResult
		copyResult.Content = "activation summarized"
		out[i].ToolResult = &copyResult
	}
	return out, nil
}
