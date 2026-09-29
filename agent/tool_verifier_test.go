package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
	"github.com/Viking602/venat/tool/kit"
)

func TestToolVerifierRequiresActualRepairBeforeCompletion(t *testing.T) {
	fixed, checks := false, 0
	check, err := kit.Tool("verify", func(context.Context, struct{}) (tool.Result, error) {
		checks++
		return tool.Result{Content: "expected config port 8080", IsError: !fixed}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	repair, err := kit.Tool("repair", func(context.Context, struct{}) (string, error) {
		fixed = true
		return "configured port 8080", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewToolVerifier("acceptance", tool.NewBus(check), []tool.Call{{Name: "verify", Arguments: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	model := &scriptedProvider{turns: [][]provider.Event{
		{{Kind: provider.EventTextDelta, Text: "done"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}},
		{{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "fix", Name: "repair", Arguments: []byte(`{}`)}}, {Kind: provider.EventDone, StopReason: provider.StopReasonToolUse}},
		{{Kind: provider.EventTextDelta, Text: "verified"}, {Kind: provider.EventDone, StopReason: provider.StopReasonComplete}},
	}}
	engine := Engine{Provider: model, Tools: tool.NewBus(repair), OutputGuardrails: []OutputGuardrail{verifier}}
	result := engine.Run(context.Background(), Request{Prompt: "configure the application"}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatal(result.Failure)
	}
	if !fixed || checks != 2 || result.Text != "verified" {
		t.Fatalf("fixed=%v checks=%d result=%+v", fixed, checks, result)
	}
	found := false
	for _, current := range model.requests[1].Messages {
		found = found || strings.Contains(current.Text, "expected config port 8080")
	}
	if !found {
		t.Fatal("repair turn did not receive failed check diagnostics")
	}
}

func TestToolVerifierDoesNotConcealInfrastructureFailure(t *testing.T) {
	failure := errors.New("check transport lost")
	check, err := kit.Tool("verify", func(context.Context, struct{}) (string, error) { return "", failure })
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewToolVerifier("acceptance", tool.NewBus(check), []tool.Call{{Name: "verify", Arguments: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifier.Check(context.Background(), OutputGuardrailInput{})
	if !errors.Is(err, failure) {
		t.Fatalf("got %v, want original failure", err)
	}
}

func TestToolVerifierCollectsFailuresWithoutRunningAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	later := false
	first, err := kit.Tool("first", func(context.Context, struct{}) (tool.Result, error) {
		cancel()
		return tool.Result{IsError: true, Content: "failed"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := kit.Tool("second", func(context.Context, struct{}) (string, error) { later = true; return "ok", nil })
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewToolVerifier("acceptance", tool.NewBus(first, second), []tool.Call{{Name: "first", Arguments: []byte(`{}`)}, {Name: "second", Arguments: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifier.Check(ctx, OutputGuardrailInput{})
	if !errors.Is(err, context.Canceled) || later {
		t.Fatalf("err=%v later=%v", err, later)
	}
}

func TestToolVerifierBoundsDiagnosticsAndKeepsFinalEvidence(t *testing.T) {
	check, err := kit.Tool("verify", func(context.Context, struct{}) (tool.Result, error) {
		return tool.Result{IsError: true, Content: "START " + strings.Repeat("诊断", 10000) + " END exit=1"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewToolVerifier("acceptance", tool.NewBus(check), []tool.Call{{Name: "verify", Arguments: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	decision, err := verifier.Check(context.Background(), OutputGuardrailInput{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != OutputGuardrailActionRetry {
		t.Fatalf("decision=%+v", decision)
	}
	text := decision.RetryMessages[0].Text
	if !strings.Contains(text, "START") || !strings.Contains(text, "END exit=1") || !utf8.ValidString(text) || len(text) > 17<<10 {
		t.Fatalf("invalid diagnostic excerpt: size=%d valid=%v", len(text), utf8.ValidString(text))
	}
}
