package tool

import (
	"context"
	"testing"

	"github.com/Viking602/venat/message"
)

type staticDriver struct {
	name string
}

func (d staticDriver) Definition() Definition {
	return Definition{
		Name: d.name,
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
