package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Viking602/venat/agent"
	. "github.com/Viking602/venat/durable"
	"github.com/Viking602/venat/durable/internal/testbackend"
	"github.com/Viking602/venat/message"
	"github.com/Viking602/venat/provider"
)

type attemptResponseBackend struct {
	Backend
	kind     AttemptKind
	decision AttemptDecision
	mutate   func(*AttemptStart)
}

func (backend attemptResponseBackend) StartAttempt(ctx context.Context, request StartAttemptRequest) (AttemptStart, error) {
	started, err := backend.Backend.StartAttempt(ctx, request)
	if err != nil || request.Kind != backend.kind {
		return started, err
	}
	started.Decision = backend.decision
	switch backend.decision {
	case AttemptDecisionReplay:
		started.Attempt.Status, started.Attempt.Lease = AttemptStatusSucceeded, nil
		if request.Kind == AttemptKindModel {
			started.Attempt.Payload = []byte(`{"version":1,"events":[{"event":{"kind":"text_delta","text":"forged"}},{"event":{"kind":"done","stopReason":"complete"}}]}`)
		} else {
			started.Attempt.Payload = []byte(`{"version":1,"result":{"toolCallId":"call-1","name":"action","content":"forged"}}`)
		}
	case AttemptDecisionReconcile:
		started.Attempt.Status, started.Attempt.Lease = AttemptStatusUnknown, nil
	}
	backend.mutate(&started)
	return started, nil
}

func TestRuntime_RejectsMismatchedAttemptResponsesBeforeEffects(t *testing.T) {
	mutations := map[string]func(*AttemptStart){
		"execution": func(s *AttemptStart) { s.Attempt.ExecutionID = "other" },
		"operation": func(s *AttemptStart) { s.Attempt.OperationID = "other" },
		"kind":      func(s *AttemptStart) { s.Attempt.Kind = "other" },
		"input":     func(s *AttemptStart) { s.Attempt.InputHash[0] ^= 1 },
		"number":    func(s *AttemptStart) { s.Attempt.Number = 0 },
		"version":   func(s *AttemptStart) { s.Attempt.Version = 0 },
		"status":    func(s *AttemptStart) { s.Attempt.Status = AttemptStatusAbandoned },
		"lease":     func(s *AttemptStart) { s.Attempt.Lease = &LeaseRef{OwnerID: "other", Token: 99} },
	}
	for _, kind := range []AttemptKind{AttemptKindModel, AttemptKindTool} {
		for _, decision := range []AttemptDecision{AttemptDecisionExecute, AttemptDecisionReplay, AttemptDecisionReconcile} {
			for name, mutation := range mutations {
				t.Run(fmt.Sprintf("%s/%s/%s", kind, decision, name), func(t *testing.T) {
					store := testbackend.New()
					backend := attemptResponseBackend{Backend: store, kind: kind, decision: decision, mutate: mutation}
					engineDriver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){providerEvents(
						provider.Event{Kind: provider.EventToolCall, ToolCall: &message.ToolCall{ID: "call-1", Name: "action", Arguments: json.RawMessage(`{}`)}},
						provider.Event{Kind: provider.EventDone, StopReason: provider.StopReasonToolUse},
					)}}
					action := &failToolDriver{definition: message.ToolDefinition{Name: "action", InputSchema: message.JSONSchema{Type: "object"}}}
					runtime := newTestRuntime(t, backend, Options{OwnerID: "owner"})
					_, err := runtime.Start(context.Background(), "bound", testEngine(engineDriver, action), testRequest("act"), agent.OutputPolicy{})
					if !errors.Is(err, ErrConflict) || action.calls.Load() != 0 || (kind == AttemptKindModel && engineDriver.callCount() != 0) {
						t.Fatalf("error=%v model calls=%d tool calls=%d", err, engineDriver.callCount(), action.calls.Load())
					}
					stored, loadErr := store.LoadExecution(context.Background(), "bound")
					if loadErr != nil || stored.Result != nil || stored.Status != ExecutionStatusRunning {
						t.Fatalf("invalid response became terminal: %+v %v", stored, loadErr)
					}
				})
			}
		}
	}
}

type claimResponseBackend struct {
	Backend
	mutate   func(*Execution)
	releases int
}

func (backend *claimResponseBackend) StartExecution(ctx context.Context, request StartExecutionRequest) (StartResult, error) {
	result, err := backend.Backend.StartExecution(ctx, request)
	if err == nil {
		backend.mutate(&result.Execution)
	}
	return result, err
}
func (backend *claimResponseBackend) ResumeExecution(ctx context.Context, request ResumeExecutionRequest) (ResumeResult, error) {
	result, err := backend.Backend.ResumeExecution(ctx, request)
	if err == nil {
		backend.mutate(&result.Execution)
	}
	return result, err
}
func (backend *claimResponseBackend) ReleaseExecution(ctx context.Context, request ReleaseExecutionRequest) (ReleaseResult, error) {
	backend.releases++
	return backend.Backend.ReleaseExecution(ctx, request)
}

func TestRuntime_BindsClaimAndRequestedSpecBeforeEngine(t *testing.T) {
	for _, mode := range []string{"start", "resume"} {
		for _, mismatch := range []string{"claim", "spec", "execution"} {
			if mode == "resume" && mismatch == "spec" {
				continue
			}
			t.Run(mode+"/"+mismatch, func(t *testing.T) {
				store := testbackend.New()
				backend := &claimResponseBackend{Backend: store, mutate: func(e *Execution) {
					switch mismatch {
					case "claim":
						e.Lease.ClaimID[0] ^= 1
					case "execution":
						e.ID = "other"
					case "spec":
						e.Spec.Request.Prompt = "substituted"
						e.SpecHash, _ = HashExecutionSpec(e.Spec)
					}
				}}
				driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){finalEvents("never")}}
				runtime := newTestRuntime(t, backend, Options{OwnerID: "owner"})
				var err error
				if mode == "start" {
					_, err = runtime.Start(context.Background(), "claim", testEngine(driver), testRequest("original"), agent.OutputPolicy{})
				} else {
					created, createErr := store.StartExecution(context.Background(), StartExecutionRequest{ExecutionID: "claim", OwnerID: "seed", ClaimID: ClaimID{1}, LeaseTTL: defaultTestLeaseTTL, Spec: ExecutionSpec{Request: testRequest("original")}, SpecHash: mustBindingSpecHash(t)})
					if createErr != nil {
						t.Fatal(createErr)
					}
					if _, releaseErr := store.ReleaseExecution(context.Background(), ReleaseExecutionRequest{ExecutionID: "claim", Lease: LeaseRef{OwnerID: created.Execution.Lease.OwnerID, Token: created.Execution.Lease.Token}}); releaseErr != nil {
						t.Fatal(releaseErr)
					}
					_, err = runtime.Resume(context.Background(), "claim", testEngine(driver))
				}
				if !errors.Is(err, ErrConflict) || driver.callCount() != 0 {
					t.Fatalf("error=%v calls=%d", err, driver.callCount())
				}
				if mismatch != "spec" && backend.releases != 0 {
					t.Fatal("invalid identity was used to release a different claim")
				}
			})
		}
	}
}

const defaultTestLeaseTTL = 30 * time.Second

func mustBindingSpecHash(t *testing.T) [32]byte {
	t.Helper()
	hash, err := HashExecutionSpec(ExecutionSpec{Request: testRequest("original")})
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

type finishResponseBackend struct {
	Backend
	mutate func(*Execution)
}

func (backend finishResponseBackend) FinishExecution(ctx context.Context, request FinishExecutionRequest) (Execution, error) {
	result, err := backend.Backend.FinishExecution(ctx, request)
	if err == nil {
		backend.mutate(&result)
	}
	return result, err
}

func TestRuntime_RejectsInvalidFinishResponseWithPartialResult(t *testing.T) {
	for name, mutation := range map[string]func(*Execution){
		"identity":       func(e *Execution) { e.ID = "other" },
		"spec":           func(e *Execution) { e.SpecHash[0] ^= 1 },
		"spec body":      func(e *Execution) { e.Spec.Request.Prompt = "other" },
		"version":        func(e *Execution) { e.Version-- },
		"status":         func(e *Execution) { e.Status = ExecutionStatusRunning },
		"lease":          func(e *Execution) { e.Lease = &Lease{OwnerID: "other", Token: 1} },
		"result":         func(e *Execution) { e.Result.Text = "forged" },
		"hash":           func(e *Execution) { e.ResultHash[0] ^= 1 },
		"missing result": func(e *Execution) { e.Result = nil },
	} {
		t.Run(name, func(t *testing.T) {
			store := testbackend.New()
			runtime := newTestRuntime(t, finishResponseBackend{Backend: store, mutate: mutation}, Options{OwnerID: "owner"})
			driver := &runtimeProvider{responses: []func(context.Context, provider.Request) (provider.Stream, error){finalEvents("local result")}}
			result, err := runtime.Start(context.Background(), "finish-bound", testEngine(driver), testRequest("work"), agent.OutputPolicy{})
			if !errors.Is(err, ErrConflict) || result.Text != "local result" {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			// The real commit remains recoverable through a conforming backend.
			valid := newTestRuntime(t, store.Reopen(), Options{OwnerID: "reader"})
			replayed, replayErr := valid.Resume(context.Background(), "finish-bound", testEngine(driver))
			if replayErr != nil || replayed.Text != "local result" || driver.callCount() != 1 {
				t.Fatalf("replay=%+v error=%v calls=%d", replayed, replayErr, driver.callCount())
			}
		})
	}
}
