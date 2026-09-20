package tool

import (
	"context"
	"testing"
)

type safetyDriver struct {
	name  string
	calls int
	text  string
}

func (d *safetyDriver) Definition() Definition {
	return Definition{Name: d.name, InputSchema: Schema{Type: "object"}}
}

func (d *safetyDriver) Execute(context.Context, Call, UpdateSink) (Result, error) {
	d.calls++
	return Result{Name: d.name, Content: d.text}, nil
}

func TestUnsafeToolSelection(t *testing.T) {
	t.Parallel()

	safe := &safetyDriver{name: "safe", text: "ok"}
	dangerous := &safetyDriver{name: "dangerous", text: "boom"}
	bus := NewBus(safe, dangerous)
	restricted := bus.Subset([]string{"safe"})

	if result, err := restricted.Execute(context.Background(), Call{Name: "dangerous"}, ExecuteOptions{}); err != nil || !result.IsError {
		t.Fatalf("expected dangerous tool to be rejected, got %+v, %v", result, err)
	}
	if dangerous.calls != 0 {
		t.Fatalf("expected dangerous tool to remain uncalled, got %d", dangerous.calls)
	}
	if results, err := restricted.ExecuteBatch(context.Background(), []Call{{Name: "safe"}, {Name: "dangerous"}}, ModeSequential, ExecuteOptions{}); err != nil || len(results) != 2 || !results[1].IsError {
		t.Fatalf("expected rejected slot in mixed batch, got %+v, %v", results, err)
	}
	if safe.calls != 1 {
		t.Fatalf("expected safe tool to run once before denial, got %d", safe.calls)
	}
	if dangerous.calls != 0 {
		t.Fatalf("expected dangerous tool misuse to stay blocked, got %d", dangerous.calls)
	}
}
