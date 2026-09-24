package tool

import (
	"context"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
)

type staticDriver struct {
	name        string
	description string
}

func (d staticDriver) Definition() Definition {
	return Definition{
		Name:        d.name,
		Description: d.description,
		InputSchema: Schema{
			Type: "object",
		},
	}
}

func (d staticDriver) Execute(context.Context, Call, UpdateSink) (Result, error) {
	return Result{Name: d.name}, nil
}

type argumentMutatingDriver struct{}

func (argumentMutatingDriver) Definition() Definition {
	return Definition{Name: "mutate", InputSchema: Schema{Type: "object"}}
}

func (argumentMutatingDriver) Execute(_ context.Context, call Call, _ UpdateSink) (Result, error) {
	call.Arguments[0] = '['
	return Result{ToolCallID: call.ID, Name: call.Name, Content: "ok"}, nil
}

func TestBusExecuteRejectsUnknownNameListingAvailableTools(t *testing.T) {
	bus := NewBus(staticDriver{name: "beta"}, staticDriver{name: "alpha"})
	result, err := bus.Execute(context.Background(), Call{ID: "call-1", Name: "ghost"}, ExecuteOptions{})
	if err != nil {
		t.Fatalf("unknown name must be a completed rejection, got error %v", err)
	}
	if !result.IsError {
		t.Fatalf("expected IsError rejection, got %#v", result)
	}
	// The feedback lists the dispatchable names so the model can self-correct
	// on its next turn; sorted for deterministic content.
	if !strings.Contains(result.Content, "choose an available tool (available: alpha, beta)") {
		t.Fatalf("rejection content = %q, want available tool list", result.Content)
	}
	// Unrelated names must not attract a misleading "did you mean".
	if strings.Contains(result.Content, "did you mean") {
		t.Fatalf("rejection content = %q, want no suggestion for unrelated names", result.Content)
	}
	if result.ToolCallID != "call-1" {
		t.Fatalf("rejection lost the call id: %#v", result)
	}
}

func TestBusExecuteRejectsUnknownNameSuggestsNearestTool(t *testing.T) {
	bus := NewBus(staticDriver{name: "update_plan"}, staticDriver{name: "list_files"})
	result, err := bus.Execute(context.Background(), Call{ID: "call-1", Name: "agent_update_plan"}, ExecuteOptions{})
	if err != nil || !result.IsError {
		t.Fatalf("expected completed rejection, got %#v, %v", result, err)
	}
	// Vendor-prior names carry affixes (agent_): containment must surface the
	// real tool as the suggestion and rank it first.
	if !strings.Contains(result.Content, "did you mean: update_plan?") {
		t.Fatalf("rejection content = %q, want nearest-name suggestion", result.Content)
	}
	if !strings.Contains(result.Content, "available: update_plan, list_files") {
		t.Fatalf("rejection content = %q, want similarity-ranked list", result.Content)
	}
}

func TestBusExecuteRejectsUnknownNameSuggestsFromDescriptionVocabulary(t *testing.T) {
	bus := NewBus(
		staticDriver{name: "edit_file", description: "Edit file contents. Replaces apply_patch and str-replace style tools."},
		staticDriver{name: "list_files"},
	)
	result, err := bus.Execute(context.Background(), Call{ID: "call-1", Name: "apply_patch"}, ExecuteOptions{})
	if err != nil || !result.IsError {
		t.Fatalf("expected completed rejection, got %#v, %v", result, err)
	}
	// apply_patch and edit_file share no spelling, but the description names
	// the displaced tool family, so token overlap must bridge them.
	if !strings.Contains(result.Content, "did you mean: edit_file?") {
		t.Fatalf("rejection content = %q, want description-vocabulary suggestion", result.Content)
	}
}

func TestBusExecuteRejectsUnknownNameOnEmptyBus(t *testing.T) {
	bus := NewBus()
	result, err := bus.Execute(context.Background(), Call{Name: "ghost"}, ExecuteOptions{})
	if err != nil || !result.IsError {
		t.Fatalf("expected completed rejection, got %#v, %v", result, err)
	}
	if !strings.Contains(result.Content, "no tools are registered") {
		t.Fatalf("rejection content = %q, want empty-bus notice", result.Content)
	}
}

func TestBusSubsetRejectsDeniedNameListingOnlyGrantedTools(t *testing.T) {
	bus := NewBus(staticDriver{name: "alpha"}, staticDriver{name: "beta"})
	subset := bus.Subset([]string{"beta"})
	result, err := subset.Execute(context.Background(), Call{Name: "alpha"}, ExecuteOptions{})
	if err != nil || !result.IsError {
		t.Fatalf("expected denied tool rejection, got %#v, %v", result, err)
	}
	// Only the subset's dispatchable names may be disclosed.
	if !strings.Contains(result.Content, "choose an available tool (available: beta)") {
		t.Fatalf("rejection content = %q, want granted tools only", result.Content)
	}
	if strings.Contains(result.Content, "alpha, beta") {
		t.Fatalf("rejection leaked denied tool names: %q", result.Content)
	}
}

func TestBusSubsetDefaultsToDenyByDefault(t *testing.T) {
	bus := NewBus(staticDriver{name: "alpha"}, staticDriver{name: "beta"})
	subset := bus.Subset(nil)
	if len(subset.Definitions()) != 0 {
		t.Fatalf("expected no tools when no names are granted, got %#v", subset.Definitions())
	}
}

func TestBusSubsetKeepsExplicitlyGrantedTools(t *testing.T) {
	bus := NewBus(staticDriver{name: "alpha"}, staticDriver{name: "beta"})
	subset := bus.Subset([]string{"beta"})
	definitions := subset.Definitions()
	if len(definitions) != 1 {
		t.Fatalf("expected one granted tool, got %#v", definitions)
	}
	if definitions[0].Name != "beta" {
		t.Fatalf("expected granted tool beta, got %#v", definitions[0])
	}
	if result, err := subset.Execute(context.Background(), Call{Name: "alpha"}, ExecuteOptions{}); err != nil || !result.IsError {
		t.Fatalf("expected denied tool to be rejected: %+v, %v", result, err)
	}
	result, err := subset.Execute(context.Background(), Call{Name: "beta", Arguments: message.ToolCall{}.Arguments}, ExecuteOptions{})
	if err != nil {
		t.Fatalf("expected granted tool to execute, got %v", err)
	}
	if result.Name != "beta" {
		t.Fatalf("unexpected tool result %#v", result)
	}
}

func TestBusExecuteIsolatesMutableCallArguments(t *testing.T) {
	bus := NewBus(argumentMutatingDriver{})
	call := Call{ID: "call-1", Name: "mutate", Arguments: []byte(`{"value":true}`)}
	want := string(call.Arguments)

	if _, err := bus.Execute(context.Background(), call, ExecuteOptions{}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if string(call.Arguments) != want {
		t.Fatalf("caller arguments = %q, want %q", call.Arguments, want)
	}
}

func TestBusDefinitionsAreStableAndSorted(t *testing.T) {
	bus := NewBus(staticDriver{name: "zeta"}, staticDriver{name: "alpha"}, staticDriver{name: "middle"})
	for range 100 {
		definitions := bus.Definitions()
		if len(definitions) != 3 || definitions[0].Name != "alpha" || definitions[1].Name != "middle" || definitions[2].Name != "zeta" {
			t.Fatalf("definitions are not sorted: %#v", definitions)
		}
	}
}
