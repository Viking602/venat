package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

const outputPolicyReportSchema = `{
	"type": "object",
	"properties": {
		"status": {"type": "string", "enum": ["ok", "warn"]},
		"score": {"type": "number"},
		"count": {"type": "integer"},
		"tags": {"type": "array", "items": {"type": "string"}},
		"accepted": {"type": "boolean"}
	},
	"required": ["status", "score", "count", "tags", "accepted"],
	"additionalProperties": false
}`

func TestEngineOutputPolicyValidObjectPassesAndSetsStructured(t *testing.T) {
	text := `{"status":"ok","score":0.75,"count":2,"tags":["risk"],"accepted":true}`
	_, engine := newOutputPolicyEngine(text)

	result := engine.Run(context.Background(), Request{Prompt: "classify risk"}, outputPolicyReport())

	if !result.Valid {
		t.Fatalf("result.Valid = false, failure = %#v", result.Failure)
	}
	if result.Failure != nil {
		t.Fatalf("result.Failure = %#v, want nil", result.Failure)
	}
	if string(result.Structured) != text {
		t.Fatalf("Structured = %s, want %s", result.Structured, text)
	}
	var structured map[string]any
	if err := json.Unmarshal(result.Structured, &structured); err != nil {
		t.Fatalf("Structured is not JSON: %v", err)
	}
	if structured["status"] != "ok" {
		t.Fatalf("structured status = %#v, want ok", structured["status"])
	}
}

func TestEngineOutputPolicyMissingRequiredFieldFails(t *testing.T) {
	_, engine := newOutputPolicyEngine(`{"status":"ok","score":0.75,"count":2,"tags":["risk"]}`)

	result := engine.Run(context.Background(), Request{Prompt: "classify risk"}, outputPolicyReport())

	requireOutputPolicyFailure(t, result, FailureKindSchemaInvalid)
}

func TestEngineOutputPolicyWrongTypeFails(t *testing.T) {
	_, engine := newOutputPolicyEngine(`{"status":"ok","score":0.75,"count":"2","tags":["risk"],"accepted":true}`)

	result := engine.Run(context.Background(), Request{Prompt: "classify risk"}, outputPolicyReport())

	requireOutputPolicyFailure(t, result, FailureKindSchemaInvalid)
}

func TestEngineOutputPolicyEnumMismatchFails(t *testing.T) {
	_, engine := newOutputPolicyEngine(`{"status":"blocked","score":0.75,"count":2,"tags":["risk"],"accepted":true}`)

	result := engine.Run(context.Background(), Request{Prompt: "classify risk"}, outputPolicyReport())

	requireOutputPolicyFailure(t, result, FailureKindSchemaInvalid)
}

func TestEngineOutputPolicyNumericEnumAcceptsEquivalentJSONNumbers(t *testing.T) {
	schema := json.RawMessage(`{"type":"number","enum":[1]}`)
	for _, output := range []string{`1.0`, `1e0`} {
		t.Run(output, func(t *testing.T) {
			_, engine := newOutputPolicyEngine(output)

			result := engine.Run(context.Background(), Request{Prompt: "classify score"}, OutputPolicy{
				Schema:   schema,
				Validate: true,
			})

			if !result.Valid {
				t.Fatalf("result.Valid = false, failure = %#v", result.Failure)
			}
			if result.Failure != nil {
				t.Fatalf("result.Failure = %#v, want nil", result.Failure)
			}
			if string(result.Structured) != output {
				t.Fatalf("Structured = %s, want %s", result.Structured, output)
			}
		})
	}
}

func TestOutputPolicy_NativeSchemaSurvivesRepairAndResume(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"score":{"type":"integer","enum":[1,2]}},"required":["score"]}`)
	driver, engine := newOutputPolicyEngine(`{"score":3}`, `{"score":2}`)
	var ready Continuation
	engine.Boundaries = BoundaryObserverFunc(func(_ context.Context, value Continuation) error {
		if ready.SchemaVersion == 0 && value.Phase == ContinuationReady {
			ready = value
		}
		return nil
	})
	result := engine.Run(context.Background(), Request{Prompt: "score"}, OutputPolicy{Schema: schema, Native: true, Validate: true, Repair: true, MaxRepairAttempts: 1})
	if result.Failure != nil || result.RepairCount != 1 || len(driver.requests) != 2 {
		t.Fatalf("result=%+v calls=%d", result, len(driver.requests))
	}
	for _, request := range driver.requests {
		if request.ResponseFormat == nil || string(request.ResponseFormat.RawSchema) != string(schema) {
			t.Fatalf("schema not forwarded: %+v", request.ResponseFormat)
		}
	}
	driver, engine = newOutputPolicyEngine(`{"score":1}`)
	result = engine.Resume(context.Background(), ready)
	if result.Failure != nil || len(driver.requests) != 1 || string(driver.requests[0].ResponseFormat.RawSchema) != string(schema) {
		t.Fatalf("resume=%+v requests=%+v", result, driver.requests)
	}
	driver.requests[0].ResponseFormat.RawSchema[0] = '['
	if schema[0] != '{' || ready.OutputPolicy.Schema[0] != '{' {
		t.Fatal("provider received retained schema storage")
	}
}

func TestEngineOutputPolicyAdditionalPropertiesFailWhenDisallowed(t *testing.T) {
	_, engine := newOutputPolicyEngine(`{"status":"ok","score":0.75,"count":2,"tags":["risk"],"accepted":true,"extra":true}`)

	result := engine.Run(context.Background(), Request{Prompt: "classify risk"}, outputPolicyReport())

	requireOutputPolicyFailure(t, result, FailureKindSchemaInvalid)
}

func TestEngineOutputPolicyInvalidSchemaWithRepairFailsWithoutRepairAttempts(t *testing.T) {
	tests := []struct {
		name   string
		schema json.RawMessage
	}{
		{name: "malformed", schema: json.RawMessage(`{"type":`)},
		{name: "null", schema: json.RawMessage(`null`)},
		{name: "unsupported type", schema: json.RawMessage(`{"type":"decimal"}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, engine := newOutputPolicyEngine(
				`{"status":"ok"}`,
				`{"status":"ok"}`,
				`{"status":"ok"}`,
			)
			result := engine.Run(context.Background(), Request{Prompt: "classify risk"}, OutputPolicy{
				Schema:            tt.schema,
				Validate:          true,
				Repair:            true,
				MaxRepairAttempts: 2,
			})

			requireOutputPolicyFailure(t, result, FailureKindSchemaInvalid)
			if result.RepairCount != 0 {
				t.Fatalf("RepairCount = %d, want 0", result.RepairCount)
			}
			if len(driver.requests) > 1 {
				t.Fatalf("provider calls = %d, want no repeated repair calls", len(driver.requests))
			}
		})
	}
}

func TestEngineOutputPolicyUnsupportedSchemaKeywordsFailBeforeRepair(t *testing.T) {
	tests := []struct {
		name   string
		schema json.RawMessage
		output string
	}{
		{
			name:   "root pattern keyword",
			schema: json.RawMessage(`{"type":"string","pattern":"^ok$"}`),
			output: `"not-ok"`,
		},
		{
			name:   "root const keyword",
			schema: json.RawMessage(`{"const":"ok"}`),
			output: `"ok"`,
		},
		{
			name:   "nested property anyOf keyword",
			schema: json.RawMessage(`{"type":"object","properties":{"status":{"anyOf":[{"type":"string"}]}}}`),
			output: `{"status":"ok"}`,
		},
		{
			name:   "nested item minimum keyword",
			schema: json.RawMessage(`{"type":"array","items":{"type":"number","minimum":1}}`),
			output: `[0]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, engine := newOutputPolicyEngine(
				tt.output,
				tt.output,
				tt.output,
			)

			result := engine.Run(context.Background(), Request{Prompt: "produce structured output"}, OutputPolicy{
				Schema:            tt.schema,
				Validate:          true,
				Repair:            true,
				MaxRepairAttempts: 2,
			})

			requireOutputPolicyFailure(t, result, FailureKindSchemaInvalid)
			if result.RepairCount != 0 {
				t.Fatalf("RepairCount = %d, want 0", result.RepairCount)
			}
			if len(driver.requests) != 0 {
				t.Fatalf("provider calls = %d, want zero calls for an invalid schema", len(driver.requests))
			}
		})
	}
}

func TestEngineOutputPolicyNestedNullSchemaFailsBeforeRepair(t *testing.T) {
	tests := []struct {
		name   string
		schema json.RawMessage
		output string
	}{
		{
			name:   "null property schema",
			schema: json.RawMessage(`{"type":"object","properties":{"status":null}}`),
			output: `{"status":"ok"}`,
		},
		{
			name:   "null items schema",
			schema: json.RawMessage(`{"type":"array","items":null}`),
			output: `["ok"]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, engine := newOutputPolicyEngine(
				tt.output,
				tt.output,
				tt.output,
			)

			result := engine.Run(context.Background(), Request{Prompt: "produce structured output"}, OutputPolicy{
				Schema:            tt.schema,
				Validate:          true,
				Repair:            true,
				MaxRepairAttempts: 2,
			})

			requireOutputPolicyFailure(t, result, FailureKindSchemaInvalid)
			if result.RepairCount != 0 {
				t.Fatalf("RepairCount = %d, want 0", result.RepairCount)
			}
			if len(driver.requests) != 0 {
				t.Fatalf("provider calls = %d, want zero calls for an invalid schema", len(driver.requests))
			}
		})
	}
}

func TestEngineOutputPolicyRepairSuccessUsesRepairPrompt(t *testing.T) {
	driver, engine := newOutputPolicyEngine(
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
	if result.Failure != nil {
		t.Fatalf("result.Failure = %#v, want nil", result.Failure)
	}
	if result.RepairCount != 1 {
		t.Fatalf("RepairCount = %d, want 1", result.RepairCount)
	}
	if string(result.Structured) != `{"status":"ok","score":0.75,"count":2,"tags":["risk"],"accepted":true}` {
		t.Fatalf("Structured = %s", result.Structured)
	}
	if len(driver.requests) != 2 {
		t.Fatalf("provider calls = %d, want 2", len(driver.requests))
	}
	repairRequest := driver.requests[1]
	if len(repairRequest.Messages) == 0 {
		t.Fatal("repair request had no messages")
	}
	last := repairRequest.Messages[len(repairRequest.Messages)-1]
	if last.Role != message.RoleUser {
		t.Fatalf("repair prompt role = %s, want user", last.Role)
	}
	if !strings.Contains(last.Text, "Repair the previous JSON output") {
		t.Fatalf("repair prompt = %q, want repair instruction", last.Text)
	}
}

func TestEngineOutputPolicyRepairExhaustedReturnsRepairFailed(t *testing.T) {
	driver, engine := newOutputPolicyEngine(
		`{"status":"blocked","score":0.75,"count":2,"tags":["risk"],"accepted":true}`,
		`{"status":"still-blocked","score":0.75,"count":2,"tags":["risk"],"accepted":true}`,
	)
	policy := outputPolicyReport()
	policy.Repair = true
	policy.MaxRepairAttempts = 1

	result := engine.Run(context.Background(), Request{Prompt: "classify risk"}, policy)

	requireOutputPolicyFailure(t, result, FailureKindRepairFailed)
	if result.RepairCount != 1 {
		t.Fatalf("RepairCount = %d, want 1", result.RepairCount)
	}
	if len(driver.requests) != 2 {
		t.Fatalf("provider calls = %d, want 2", len(driver.requests))
	}
}

func outputPolicyReport() OutputPolicy {
	return OutputPolicy{
		Schema:   json.RawMessage(outputPolicyReportSchema),
		Validate: true,
	}
}

func newOutputPolicyEngine(responses ...string) (*scriptedProvider, Engine) {
	turns := make([][]provider.Event, 0, len(responses))
	for _, response := range responses {
		turns = append(turns, []provider.Event{
			{Kind: provider.EventTextDelta, Text: response},
			{Kind: provider.EventDone, StopReason: provider.StopReasonComplete},
		})
	}
	driver := &scriptedProvider{turns: turns}
	return driver, Engine{Provider: driver}
}

func requireOutputPolicyFailure(t *testing.T, result Result, kind FailureKind) {
	t.Helper()
	if result.Valid {
		t.Fatalf("result.Valid = true, want false")
	}
	if result.Failure == nil {
		t.Fatal("result.Failure = nil, want failure")
	}
	if result.Failure.Kind != kind {
		t.Fatalf("Failure.Kind = %s, want %s (reason: %s)", result.Failure.Kind, kind, result.Failure.Reason)
	}
	if len(result.Structured) != 0 {
		t.Fatalf("Structured = %s, want empty", result.Structured)
	}
}
