package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
)

func TestContextWindow_RetainsTaskInstructionsAndCompleteLatestExchange(t *testing.T) {
	history := []message.Message{
		message.NewText(message.RoleSystem, "instructions"),
		message.NewText(message.RoleUser, "original task"),
		message.NewText(message.RoleAssistant, strings.Repeat("old", 400)),
		{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{{ID: "old", Name: "read"}}},
		message.NewToolResult(message.ToolResult{ToolCallID: "old", Name: "read", Content: strings.Repeat("log", 400)}),
		message.NewText(message.RoleUser, "new constraint"),
		{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{{ID: "skill", Name: activateSkillToolName}}},
		message.NewToolResult(message.ToolResult{ToolCallID: "skill", Name: activateSkillToolName, Content: "skill instructions"}),
		{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{{ID: "latest", Name: "check"}}},
		message.NewToolResult(message.ToolResult{ToolCallID: "latest", Name: "check", Content: "failed"}),
	}
	history[0].CacheBoundary = true
	want := append(message.CloneMessages(history[:2]), message.CloneMessages(history[5:])...)
	target := 0
	for _, current := range want {
		cost, err := contextMessageCost(current)
		if err != nil {
			t.Fatal(err)
		}
		target += cost
	}
	got, err := fitContext(context.Background(), history, target)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("fit=%+v error=%v", got, err)
	}
	again, err := fitContext(context.Background(), got, target)
	if err != nil || !reflect.DeepEqual(got, again) {
		t.Fatalf("not idempotent: %v", err)
	}
	got[0].Content[0].Text = "mutated"
	if history[0].Content[0].Text != "instructions" {
		t.Fatal("retained caller memory")
	}
	if _, err := fitContext(context.Background(), history, target-1); !errors.Is(err, errContextLimit) {
		t.Fatalf("protected overflow error=%v", err)
	}
}

func TestContextWindow_ProviderViewDoesNotEraseCheckpointEvidence(t *testing.T) {
	driver := &usageToolProvider{perTurn: usagePerTurn(10)}
	engine := newLoopToolEngine(t, driver)
	var checkpoint Continuation
	engine.Boundaries = BoundaryObserverFunc(func(_ context.Context, value Continuation) error {
		if value.Phase == ContinuationReady {
			checkpoint = value
		}
		return nil
	})
	output, err := engine.RunMessages(context.Background(), LoopInput{
		Messages:           []message.Message{message.NewText(message.RoleUser, "loop")},
		ContextTokenTarget: 1500, MaxIterations: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Steps) != 6 || len(checkpoint.Steps) != 5 {
		t.Fatalf("steps=%d checkpoint=%d", len(output.Steps), len(checkpoint.Steps))
	}
	if err := ValidateContinuation(checkpoint); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeContinuation(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := DecodeContinuation(encoded)
	if err != nil {
		t.Fatal(err)
	}
	resumedDriver := singleTurnProvider("finished")
	resumed := (Engine{Provider: resumedDriver, LoopPolicy: LoopPolicy{ContextTokenTarget: 1500}}).Resume(context.Background(), restored)
	if resumed.Failure != nil || len(resumed.Steps) != 6 || len(resumedDriver.requests[0].Messages) >= len(restored.Messages) {
		t.Fatalf("resume failure=%v steps=%d", resumed.Failure, len(resumed.Steps))
	}
}

func TestContextWindow_FailsBeforeModelForIrreducibleOrUnknownMediaCost(t *testing.T) {
	for _, parts := range [][]message.ContentPart{
		{message.TextPart(strings.Repeat("x", 1000))},
		{{Kind: message.ContentImage, URI: "https://example.invalid/image.png"}},
	} {
		driver := singleTurnProvider("never")
		result := (Engine{Provider: driver, LoopPolicy: LoopPolicy{ContextTokenTarget: 100}}).Run(context.Background(), Request{Content: parts}, OutputPolicy{})
		if result.Failure == nil || result.Failure.Kind != FailureKindContextBuildFailed || len(driver.requests) != 0 {
			t.Fatalf("failure=%v calls=%d", result.Failure, len(driver.requests))
		}
	}
}

func TestRequest_MultimodalBuildAndContinuationOwnership(t *testing.T) {
	parts := []message.ContentPart{{Kind: message.ContentImage, Data: []byte("image"), MediaType: "image/png"}}
	driver := singleTurnProvider("described")
	var saved Continuation
	engine := Engine{Provider: driver, Boundaries: BoundaryObserverFunc(func(_ context.Context, value Continuation) error {
		if value.Phase == ContinuationReady {
			saved = value
		}
		return nil
	})}
	result := engine.Run(context.Background(), Request{Prompt: "describe", Content: parts}, OutputPolicy{})
	if result.Failure != nil {
		t.Fatal(result.Failure)
	}
	user := driver.requests[0].Messages[1]
	if len(user.Content) != 2 || user.Content[0].Text != "describe" || string(user.Content[1].Data) != "image" {
		t.Fatalf("input=%+v", user)
	}
	parts[0].Data[0] = 'X'
	if string(saved.Request.Content[0].Data) != "image" {
		t.Fatal("request storage aliased")
	}
	encoded, err := EncodeContinuation(saved)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := DecodeContinuation(encoded)
	if err != nil || string(restored.Request.Content[0].Data) != "image" {
		t.Fatalf("restore=%+v error=%v", restored.Request, err)
	}
	for _, kind := range []message.ContentKind{message.ContentReasoning, message.ContentProviderData} {
		driver := singleTurnProvider("never")
		result := (Engine{Provider: driver}).Run(context.Background(), Request{Content: []message.ContentPart{{Kind: kind, Text: "injected"}}}, OutputPolicy{})
		if result.Failure == nil || len(driver.requests) != 0 {
			t.Fatal("non-user content accepted")
		}
	}
}

func TestContinuationCodec_PreservesLegacyCanonicalBytes(t *testing.T) {
	fixture, err := os.ReadFile("testdata/continuation-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	before := []byte(strings.TrimSpace(string(fixture)))
	decoded, err := DecodeContinuation(before)
	if err != nil {
		t.Fatal(err)
	}
	after, err := EncodeContinuation(decoded)
	if err != nil || string(before) != string(after) || decoded.SchemaVersion != 1 {
		t.Fatalf("legacy bytes changed: %v", err)
	}
	for field, value := range map[string]string{"request": `{"content":null}`, "outputPolicy": `{"native":false}`} {
		candidate := mutateContinuationJSON(t, before, func(fields map[string]json.RawMessage) {
			fields[field] = json.RawMessage(value)
		})
		assertInvalidContinuationJSON(t, candidate)
	}
	decoded.Request.Content = []message.ContentPart{message.TextPart("new")}
	if _, err := EncodeContinuation(decoded); !errors.Is(err, ErrInvalidContinuation) {
		t.Fatalf("v1 accepted v2 content: %v", err)
	}
}

func TestContextWindow_BoundsLatestToolOutputWithoutChangingTranscript(t *testing.T) {
	text := "first-diagnostic" + strings.Repeat("界", 4000) + "last-diagnostic"
	history := []message.Message{
		message.NewText(message.RoleUser, "fix"),
		{Role: message.RoleAssistant, ToolCalls: []message.ToolCall{{ID: "call", Name: "check"}}},
		message.NewToolResult(message.ToolResult{ToolCallID: "call", Name: "check", IsError: true, Content: text}),
	}
	got, err := fitContext(context.Background(), history, 2000)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, current := range got {
		cost, err := contextMessageCost(current)
		if err != nil {
			t.Fatal(err)
		}
		total += cost
	}
	result := got[2].ToolResult
	if total > 2000 || !result.IsError || !strings.HasPrefix(result.Content, "first-diagnostic") || !strings.HasSuffix(result.Content, "last-diagnostic") || !strings.Contains(result.Content, "omitted") || history[2].ToolResult.Content != text {
		t.Fatalf("cost=%d result=%+v", total, result)
	}
	if err := message.ValidateCompleteTurns(got); err != nil {
		t.Fatal(err)
	}
}

func TestContextWindow_BuilderFuncUsesDefaultFitting(t *testing.T) {
	model := singleTurnProvider("done")
	engine := Engine{Provider: model, LoopPolicy: LoopPolicy{ContextTokenTarget: 1000}, ContextBuilder: ContextBuilderFunc(func(context.Context, Request) ([]message.Message, error) {
		return []message.Message{message.NewText(message.RoleUser, "task"), message.NewText(message.RoleAssistant, strings.Repeat("old", 1000)), message.NewText(message.RoleUser, "continue")}, nil
	})}
	result := engine.Run(context.Background(), Request{Prompt: "task"}, OutputPolicy{})
	if result.Failure != nil || len(model.requests) != 1 || len(model.requests[0].Messages) != 2 {
		t.Fatalf("result=%+v", result)
	}
}
