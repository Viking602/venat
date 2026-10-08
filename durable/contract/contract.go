// Package contract publishes conformance tests for external durable.Backend
// implementations.
package contract

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/durable"
	"github.com/Viking602/venat/message"
)

// BackendFactory creates a clean backend, a process-reopen handle, and cleanup.
type BackendFactory func(t *testing.T) (backend durable.Backend, reopen func(*testing.T) durable.Backend, cleanup func())

// RunBackendContractTests runs the complete execution-semantic Backend
// contract against factory.
func RunBackendContractTests(t *testing.T, factory BackendFactory) {
	t.Helper()
	t.Run("claim", func(t *testing.T) {
		t.Run("start exact replay and conflict", func(t *testing.T) { testStartClaimAndConflict(t, factory) })
		t.Run("concurrent start fencing", func(t *testing.T) { testConcurrentStart(t, factory) })
		t.Run("start and resume response-loss reopen", func(t *testing.T) { testClaimResponseLossReopen(t, factory) })
	})
	t.Run("lease", func(t *testing.T) {
		t.Run("renew expiry fencing and resume replay", func(t *testing.T) { testLeaseExpiryFencingAndRenew(t, factory) })
		t.Run("release and suspend unknown convergence", func(t *testing.T) { testReleaseAndSuspendConvergence(t, factory) })
		t.Run("renew release and suspend response-loss reopen", func(t *testing.T) { testLeaseMutationResponseLossReopen(t, factory) })
	})
	t.Run("checkpoint", func(t *testing.T) {
		t.Run("record identity and sequence binding", func(t *testing.T) { testRecordBinding(t, factory) })
		t.Run("CAS replacement codec hash and reopen", func(t *testing.T) { testCheckpointCASReplacementAndReopen(t, factory) })
		t.Run("response-loss exact replay after reopen", func(t *testing.T) { testCheckpointResponseLossReopen(t, factory) })
	})
	t.Run("attempt", func(t *testing.T) {
		t.Run("start finish unknown and retry decisions", func(t *testing.T) { testAttemptReplayUnknownAndRetry(t, factory) })
		t.Run("mutation response-loss replay after reopen", func(t *testing.T) { testAttemptResponseLossReopen(t, factory) })
		t.Run("stale lease and version preserve state", func(t *testing.T) { testAttemptStaleMutationPreservesState(t, factory) })
	})
	t.Run("terminal", func(t *testing.T) {
		t.Run("completion rules and exact replay", func(t *testing.T) { testFinishTerminalContract(t, factory) })
		t.Run("finish response-loss exact replay after reopen", func(t *testing.T) { testFinishResponseLossReopen(t, factory) })
	})
	t.Run("error", func(t *testing.T) {
		t.Run("typed execution and attempt identity", func(t *testing.T) { testTypedErrors(t, factory) })
		t.Run("failed mutations do not advance versions", func(t *testing.T) { testRejectedMutationPreservesVersions(t, factory) })
	})
}

func testStartClaimAndConflict(t *testing.T, factory BackendFactory) {
	backend, _, cleanup := openBackend(t, factory)
	defer cleanup()
	ctx := context.Background()
	spec := testSpec("hello")
	request := startRequest(t, "start", spec, claimID(1), time.Second)
	contractFacts(t, "StartExecution", request.ExecutionID, request.ClaimID, 0, 0, 0)
	created, err := backend.StartExecution(ctx, request)
	if err != nil {
		t.Fatalf("StartExecution() error = %v", err)
	}
	if !created.Created || created.Execution.Status != durable.ExecutionStatusRunning || created.Execution.Lease == nil || created.Execution.Version == 0 {
		t.Fatalf("StartExecution() = %#v, want created running execution", created)
	}
	replayed, err := backend.StartExecution(ctx, request)
	if err != nil {
		t.Fatalf("exact StartExecution() retry error = %v", err)
	}
	if !reflect.DeepEqual(replayed, created) {
		t.Fatalf("exact StartExecution() retry = %#v, want %#v", replayed, created)
	}

	busy := request
	busy.ClaimID = claimID(2)
	if _, err := backend.StartExecution(ctx, busy); !errors.Is(err, durable.ErrBusy) {
		t.Fatalf("competing StartExecution() error = %v, want ErrBusy", err)
	}
	conflict := request
	conflict.Spec = testSpec("different")
	conflict.SpecHash = mustSpecHash(t, conflict.Spec)
	if _, err := backend.StartExecution(ctx, conflict); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("conflicting StartExecution() error = %v, want ErrConflict", err)
	}
}

func testConcurrentStart(t *testing.T, factory BackendFactory) {
	backend, _, cleanup := openBackend(t, factory)
	defer cleanup()
	spec := testSpec("concurrent")
	requests := []durable.StartExecutionRequest{
		startRequest(t, "concurrent", spec, claimID(1), time.Second),
		startRequest(t, "concurrent", spec, claimID(2), time.Second),
	}
	for _, request := range requests {
		contractFacts(t, "StartExecution", request.ExecutionID, request.ClaimID, 0, 0, 0)
	}
	type outcome struct {
		result durable.StartResult
		err    error
	}
	outcomes := make(chan outcome, len(requests))
	var wait sync.WaitGroup
	for _, request := range requests {
		request := request
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := backend.StartExecution(context.Background(), request)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	wait.Wait()
	close(outcomes)
	created := 0
	busy := 0
	for outcome := range outcomes {
		switch {
		case outcome.err == nil && outcome.result.Created:
			created++
		case errors.Is(outcome.err, durable.ErrBusy):
			busy++
		default:
			t.Fatalf("concurrent StartExecution() = %#v, %v", outcome.result, outcome.err)
		}
	}
	if created != 1 || busy != 1 {
		t.Fatalf("concurrent starts: created=%d busy=%d, want 1/1", created, busy)
	}
}

func testLeaseExpiryFencingAndRenew(t *testing.T, factory BackendFactory) {
	backend, _, cleanup := openBackend(t, factory)
	defer cleanup()
	ctx := context.Background()
	created := mustStart(t, backend, "lease", testSpec("lease"), claimID(1), 40*time.Millisecond)
	oldRef := reference(created.Execution)
	version := created.Execution.Version
	contractFacts(t, "RenewExecution/ResumeExecution", created.Execution.ID, created.Execution.Lease.ClaimID, oldRef.Token, version, version)
	renewed, err := backend.RenewExecution(ctx, durable.RenewExecutionRequest{ExecutionID: "lease", Lease: oldRef, LeaseTTL: 40 * time.Millisecond})
	if err != nil {
		t.Fatalf("RenewExecution() error = %v", err)
	}
	loaded, err := backend.LoadExecution(ctx, "lease")
	if err != nil {
		t.Fatalf("LoadExecution() error = %v", err)
	}
	if loaded.Version != version || loaded.Lease == nil || !loaded.Lease.ExpiresAt.Equal(renewed.ExpiresAt) {
		t.Fatalf("renew changed version or lease mismatch: %#v", loaded)
	}

	inputHash := sha256.Sum256([]byte("lease-attempt"))
	attempt, err := backend.StartAttempt(ctx, durable.StartAttemptRequest{ExecutionID: "lease", Lease: oldRef, OperationID: "turn:0:model", Kind: durable.AttemptKindModel, InputHash: inputHash})
	if err != nil || attempt.Decision != durable.AttemptDecisionExecute {
		t.Fatalf("StartAttempt() = %#v, %v", attempt, err)
	}
	resumed := resumeAfterExpiry(t, backend, "lease", claimID(2), 40*time.Millisecond)
	if resumed.Execution.Lease == nil || resumed.Execution.Lease.Token <= oldRef.Token || len(resumed.Reconcile) != 1 || resumed.Reconcile[0].Status != durable.AttemptStatusUnknown {
		t.Fatalf("ResumeExecution() after expiry = %#v, want higher token and unknown attempt", resumed)
	}
	newRef := reference(resumed.Execution)
	if _, err := backend.RenewExecution(ctx, durable.RenewExecutionRequest{ExecutionID: "lease", Lease: oldRef, LeaseTTL: time.Second}); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("old RenewExecution() error = %v, want ErrLeaseLost", err)
	}
	if _, err := backend.ReleaseExecution(ctx, durable.ReleaseExecutionRequest{ExecutionID: "lease", Lease: oldRef}); !errors.Is(err, durable.ErrLeaseLost) {
		t.Fatalf("old ReleaseExecution() error = %v, want ErrLeaseLost", err)
	}
	exact, err := backend.ResumeExecution(ctx, durable.ResumeExecutionRequest{ExecutionID: "lease", OwnerID: newRef.OwnerID, ClaimID: resumed.Execution.Lease.ClaimID, LeaseTTL: 40 * time.Millisecond})
	if err != nil || !reflect.DeepEqual(exact, resumed) {
		t.Fatalf("exact ResumeExecution() retry = %#v, %v, want %#v", exact, err, resumed)
	}
	sameOwnerDifferentClaim := durable.ResumeExecutionRequest{ExecutionID: "lease", OwnerID: newRef.OwnerID, ClaimID: claimID(3), LeaseTTL: time.Second}
	if _, err := backend.ResumeExecution(ctx, sameOwnerDifferentClaim); !errors.Is(err, durable.ErrBusy) {
		t.Fatalf("same-owner competing ResumeExecution() error = %v, want ErrBusy", err)
	}
}

func testCheckpointCASReplacementAndReopen(t *testing.T, factory BackendFactory) {
	backend, reopen, cleanup := openBackend(t, factory)
	defer cleanup()
	ctx := context.Background()
	created := mustStart(t, backend, "checkpoint", testSpec("checkpoint"), claimID(1), time.Second)
	lease := reference(created.Execution)
	contractFacts(t, "SaveCheckpoint", created.Execution.ID, created.Execution.Lease.ClaimID, lease.Token, created.Execution.Version, created.Execution.Version)
	first := testCheckpoint(t, created.Execution, 1, "first")
	saved, err := backend.SaveCheckpoint(ctx, durable.SaveCheckpointRequest{ExecutionID: "checkpoint", Lease: lease, ExpectedVersion: created.Execution.Version, Checkpoint: first})
	if err != nil {
		t.Fatalf("SaveCheckpoint(first) error = %v", err)
	}
	if saved.Version != created.Execution.Version+1 || saved.Checkpoint == nil || saved.Checkpoint.Sequence != 1 {
		t.Fatalf("SaveCheckpoint(first) = %#v", saved)
	}
	exact, err := backend.SaveCheckpoint(ctx, durable.SaveCheckpointRequest{ExecutionID: "checkpoint", Lease: lease, ExpectedVersion: created.Execution.Version, Checkpoint: first})
	if err != nil || !reflect.DeepEqual(exact, saved) {
		t.Fatalf("exact SaveCheckpoint() retry = %#v, %v, want %#v", exact, err, saved)
	}
	conflicting := testCheckpoint(t, created.Execution, 1, "different")
	if _, err := backend.SaveCheckpoint(ctx, durable.SaveCheckpointRequest{ExecutionID: "checkpoint", Lease: lease, ExpectedVersion: saved.Version, Checkpoint: conflicting}); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("conflicting sequence SaveCheckpoint() error = %v, want ErrConflict", err)
	}
	second := testCheckpoint(t, created.Execution, 2, "second")
	if _, err := backend.SaveCheckpoint(ctx, durable.SaveCheckpointRequest{ExecutionID: "checkpoint", Lease: lease, ExpectedVersion: created.Execution.Version, Checkpoint: second}); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("stale-version SaveCheckpoint() error = %v, want ErrConflict", err)
	}
	saved, err = backend.SaveCheckpoint(ctx, durable.SaveCheckpointRequest{ExecutionID: "checkpoint", Lease: lease, ExpectedVersion: saved.Version, Checkpoint: second})
	if err != nil {
		t.Fatalf("SaveCheckpoint(second) error = %v", err)
	}
	if saved.Checkpoint == nil || saved.Checkpoint.Sequence != 2 || saved.Checkpoint.Continuation.Request.Prompt != "second" {
		t.Fatalf("checkpoint was not fully replaced: %#v", saved.Checkpoint)
	}
	loaded, err := reopen(t).LoadExecution(ctx, "checkpoint")
	if err != nil || !reflect.DeepEqual(loaded, saved) {
		t.Fatalf("reopened LoadExecution() = %#v, %v, want %#v", loaded, err, saved)
	}
	corrupt := testCheckpoint(t, created.Execution, 3, "corrupt")
	corrupt.ContinuationHash[0] ^= 0xff
	if _, err := backend.SaveCheckpoint(ctx, durable.SaveCheckpointRequest{ExecutionID: "checkpoint", Lease: lease, ExpectedVersion: saved.Version, Checkpoint: corrupt}); !errors.Is(err, durable.ErrCorruptCheckpoint) {
		t.Fatalf("corrupt SaveCheckpoint() error = %v, want ErrCorruptCheckpoint", err)
	}
}

func testReleaseAndSuspendConvergence(t *testing.T, factory BackendFactory) {
	t.Run("release", func(t *testing.T) {
		backend, _, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "release", testSpec("release"), claimID(1), time.Second)
		lease := reference(created.Execution)
		contractFacts(t, "ReleaseExecution", created.Execution.ID, created.Execution.Lease.ClaimID, lease.Token, created.Execution.Version, created.Execution.Version)
		startAttempt(t, backend, "release", lease, "turn:0:model", durable.AttemptKindModel)
		released, err := backend.ReleaseExecution(context.Background(), durable.ReleaseExecutionRequest{ExecutionID: "release", Lease: lease})
		if err != nil {
			t.Fatalf("ReleaseExecution() error = %v", err)
		}
		if released.Execution.Lease != nil || released.Execution.Status != durable.ExecutionStatusRunning || released.Execution.Version != created.Execution.Version || len(released.Reconcile) != 1 || released.Reconcile[0].Status != durable.AttemptStatusUnknown {
			t.Fatalf("ReleaseExecution() = %#v", released)
		}
		exact, err := backend.ReleaseExecution(context.Background(), durable.ReleaseExecutionRequest{ExecutionID: "release", Lease: lease})
		if err != nil || !reflect.DeepEqual(exact, released) {
			t.Fatalf("exact ReleaseExecution() retry = %#v, %v, want %#v", exact, err, released)
		}
		resumed, err := backend.ResumeExecution(context.Background(), durable.ResumeExecutionRequest{ExecutionID: "release", OwnerID: "owner", ClaimID: claimID(2), LeaseTTL: time.Second})
		if err != nil {
			t.Fatalf("ResumeExecution() error = %v", err)
		}
		if resumed.Execution.Lease.Token <= lease.Token {
			t.Fatalf("new token = %d, want > %d", resumed.Execution.Lease.Token, lease.Token)
		}
		if _, err := backend.ReleaseExecution(context.Background(), durable.ReleaseExecutionRequest{ExecutionID: "release", Lease: lease}); !errors.Is(err, durable.ErrLeaseLost) {
			t.Fatalf("old exact release after new claim error = %v, want ErrLeaseLost", err)
		}
	})

	t.Run("suspend", func(t *testing.T) {
		backend, _, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "suspend", testSpec("suspend"), claimID(1), time.Second)
		lease := reference(created.Execution)
		startAttempt(t, backend, "suspend", lease, "turn:0:model", durable.AttemptKindModel)
		request := durable.SuspendExecutionRequest{ExecutionID: "suspend", Lease: lease, ExpectedVersion: created.Execution.Version}
		contractFacts(t, "SuspendExecution", created.Execution.ID, created.Execution.Lease.ClaimID, lease.Token, request.ExpectedVersion, created.Execution.Version)
		suspended, err := backend.SuspendExecution(context.Background(), request)
		if err != nil {
			t.Fatalf("SuspendExecution() error = %v", err)
		}
		if suspended.Status != durable.ExecutionStatusSuspended || suspended.Lease != nil || suspended.Version != created.Execution.Version+1 {
			t.Fatalf("SuspendExecution() = %#v", suspended)
		}
		exact, err := backend.SuspendExecution(context.Background(), request)
		if err != nil || !reflect.DeepEqual(exact, suspended) {
			t.Fatalf("exact SuspendExecution() retry = %#v, %v, want %#v", exact, err, suspended)
		}
	})
}

func testAttemptReplayUnknownAndRetry(t *testing.T, factory BackendFactory) {
	backend, _, cleanup := openBackend(t, factory)
	defer cleanup()
	created := mustStart(t, backend, "attempt", testSpec("attempt"), claimID(1), time.Second)
	lease := reference(created.Execution)
	contractFacts(t, "StartAttempt/FinishAttempt/MarkAttemptUnknown/ReconcileAttempt", created.Execution.ID, created.Execution.Lease.ClaimID, lease.Token, 1, 1)
	verifySettledAttemptReplay(t, backend, lease)
	unknown := verifyUnknownAttempt(t, backend, lease)
	verifyRetryResolution(t, backend, lease, unknown)

	loaded, err := backend.LoadExecution(context.Background(), "attempt")
	if err != nil || loaded.Version != created.Execution.Version {
		t.Fatalf("attempts changed execution version: %#v, %v", loaded, err)
	}
}

func verifySettledAttemptReplay(t *testing.T, backend durable.Backend, lease durable.LeaseRef) {
	t.Helper()
	ctx := context.Background()
	started := startAttempt(t, backend, "attempt", lease, "turn:0:call:0", durable.AttemptKindTool)
	exactStart, err := backend.StartAttempt(ctx, durable.StartAttemptRequest{ExecutionID: "attempt", Lease: lease, OperationID: started.Attempt.OperationID, Kind: started.Attempt.Kind, InputHash: started.Attempt.InputHash})
	if err != nil || exactStart.Decision != durable.AttemptDecisionExecute || !reflect.DeepEqual(exactStart.Attempt, started.Attempt) {
		t.Fatalf("exact StartAttempt() replay = %#v, %v, want execute %#v", exactStart, err, started.Attempt)
	}
	mismatch := started.Attempt.InputHash
	mismatch[0] ^= 0xff
	if _, err := backend.StartAttempt(ctx, durable.StartAttemptRequest{ExecutionID: "attempt", Lease: lease, OperationID: started.Attempt.OperationID, Kind: started.Attempt.Kind, InputHash: mismatch}); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("mismatched StartAttempt() error = %v, want ErrConflict", err)
	}
	finishRequest := durable.FinishAttemptRequest{
		ExecutionID:            "attempt",
		Lease:                  lease,
		OperationID:            started.Attempt.OperationID,
		AttemptNumber:          started.Attempt.Number,
		ExpectedAttemptVersion: started.Attempt.Version,
		Payload:                []byte("terminal-payload"),
	}
	finished, err := backend.FinishAttempt(ctx, finishRequest)
	if err != nil || finished.Status != durable.AttemptStatusSucceeded {
		t.Fatalf("FinishAttempt() = %#v, %v", finished, err)
	}
	exact, err := backend.FinishAttempt(ctx, finishRequest)
	if err != nil || !reflect.DeepEqual(exact, finished) {
		t.Fatalf("exact FinishAttempt() retry = %#v, %v, want %#v", exact, err, finished)
	}
	replay, err := backend.StartAttempt(ctx, durable.StartAttemptRequest{ExecutionID: "attempt", Lease: lease, OperationID: started.Attempt.OperationID, Kind: started.Attempt.Kind, InputHash: started.Attempt.InputHash})
	if err != nil || replay.Decision != durable.AttemptDecisionReplay || !reflect.DeepEqual(replay.Attempt, finished) {
		t.Fatalf("replay StartAttempt() = %#v, %v", replay, err)
	}
}

func verifyUnknownAttempt(t *testing.T, backend durable.Backend, lease durable.LeaseRef) durable.Attempt {
	t.Helper()
	ctx := context.Background()
	started := startAttempt(t, backend, "attempt", lease, "turn:1:model", durable.AttemptKindModel)
	request := durable.MarkAttemptUnknownRequest{
		ExecutionID:            "attempt",
		Lease:                  lease,
		OperationID:            started.Attempt.OperationID,
		AttemptNumber:          started.Attempt.Number,
		ExpectedAttemptVersion: started.Attempt.Version,
		Payload:                []byte("partial"),
		Failure:                &durable.FailureRecord{Code: "transport", Message: "uncertain"},
	}
	unknown, err := backend.MarkAttemptUnknown(ctx, request)
	if err != nil || unknown.Status != durable.AttemptStatusUnknown {
		t.Fatalf("MarkAttemptUnknown() = %#v, %v", unknown, err)
	}
	exact, err := backend.MarkAttemptUnknown(ctx, request)
	if err != nil || !reflect.DeepEqual(exact, unknown) {
		t.Fatalf("exact MarkAttemptUnknown() retry = %#v, %v, want %#v", exact, err, unknown)
	}
	decision, err := backend.StartAttempt(ctx, durable.StartAttemptRequest{ExecutionID: "attempt", Lease: lease, OperationID: unknown.OperationID, Kind: unknown.Kind, InputHash: unknown.InputHash})
	if err != nil || decision.Decision != durable.AttemptDecisionReconcile {
		t.Fatalf("unknown StartAttempt() = %#v, %v", decision, err)
	}
	return unknown
}

func verifyRetryResolution(t *testing.T, backend durable.Backend, lease durable.LeaseRef, unknown durable.Attempt) {
	t.Helper()
	ctx := context.Background()
	request := durable.ReconcileAttemptRequest{
		ExecutionID:            "attempt",
		OperationID:            unknown.OperationID,
		AttemptNumber:          unknown.Number,
		ExpectedAttemptVersion: unknown.Version,
		Resolution:             durable.ReconcileResolutionRetry,
	}
	if _, err := backend.ReconcileAttempt(ctx, request); !errors.Is(err, durable.ErrBusy) {
		t.Fatalf("leased ReconcileAttempt() error = %v, want ErrBusy", err)
	}
	if _, err := backend.ReleaseExecution(ctx, durable.ReleaseExecutionRequest{ExecutionID: "attempt", Lease: lease}); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
	abandoned, err := backend.ReconcileAttempt(ctx, request)
	if err != nil || abandoned.Status != durable.AttemptStatusAbandoned {
		t.Fatalf("ReconcileAttempt(retry) = %#v, %v", abandoned, err)
	}
	exact, err := backend.ReconcileAttempt(ctx, request)
	if err != nil || !reflect.DeepEqual(exact, abandoned) {
		t.Fatalf("exact ReconcileAttempt() retry = %#v, %v, want %#v", exact, err, abandoned)
	}
	resumed, err := backend.ResumeExecution(ctx, durable.ResumeExecutionRequest{ExecutionID: "attempt", OwnerID: "owner", ClaimID: claimID(2), LeaseTTL: time.Second})
	if err != nil {
		t.Fatalf("ResumeExecution() error = %v", err)
	}
	second, err := backend.StartAttempt(ctx, durable.StartAttemptRequest{ExecutionID: "attempt", Lease: reference(resumed.Execution), OperationID: unknown.OperationID, Kind: unknown.Kind, InputHash: unknown.InputHash})
	if err != nil || second.Decision != durable.AttemptDecisionExecute || second.Attempt.Number != unknown.Number+1 {
		t.Fatalf("StartAttempt() after retry = %#v, %v", second, err)
	}
}

func testFinishTerminalContract(t *testing.T, factory BackendFactory) {
	backend, _, cleanup := openBackend(t, factory)
	defer cleanup()
	ctx := context.Background()
	created := mustStart(t, backend, "finish", testSpec("finish"), claimID(1), time.Second)
	lease := reference(created.Execution)
	uncertain := startAttempt(t, backend, "finish", lease, "turn:0:model", durable.AttemptKindModel)
	result := agent.Result{Text: "done", Valid: true}
	finish := durable.FinishExecutionRequest{ExecutionID: "finish", Lease: lease, ExpectedVersion: created.Execution.Version, Result: result, ResultHash: mustResultHash(t, created.Execution, result)}
	contractFacts(t, "FinishExecution", created.Execution.ID, created.Execution.Lease.ClaimID, lease.Token, finish.ExpectedVersion, created.Execution.Version)
	if _, err := backend.FinishExecution(ctx, finish); !errors.Is(err, durable.ErrReconcileRequired) {
		t.Fatalf("FinishExecution() with running attempt error = %v, want ErrReconcileRequired", err)
	}
	failedAttempt, err := backend.FinishAttempt(ctx, durable.FinishAttemptRequest{
		ExecutionID:            "finish",
		Lease:                  lease,
		OperationID:            uncertain.Attempt.OperationID,
		AttemptNumber:          uncertain.Attempt.Number,
		ExpectedAttemptVersion: uncertain.Attempt.Version,
		Failure:                &durable.FailureRecord{Code: "not_started", Message: "request rejected before dispatch"},
	})
	if err != nil || failedAttempt.Status != durable.AttemptStatusFailed {
		t.Fatalf("FinishAttempt(failed) = %#v, %v", failedAttempt, err)
	}
	finished, err := backend.FinishExecution(ctx, finish)
	if err != nil {
		t.Fatalf("FinishExecution() error = %v", err)
	}
	if finished.Status != durable.ExecutionStatusCompleted || finished.Result == nil || finished.Lease != nil || finished.Version != created.Execution.Version+1 {
		t.Fatalf("FinishExecution() = %#v", finished)
	}
	exact, err := backend.FinishExecution(ctx, finish)
	if err != nil || !reflect.DeepEqual(exact, finished) {
		t.Fatalf("exact FinishExecution() retry = %#v, %v, want %#v", exact, err, finished)
	}
	terminalStart := startRequest(t, "finish", testSpec("finish"), claimID(9), time.Second)
	terminal, err := backend.StartExecution(ctx, terminalStart)
	if err != nil || terminal.Created || terminal.Execution.Lease != nil || !reflect.DeepEqual(terminal.Execution, finished) {
		t.Fatalf("StartExecution() terminal replay = %#v, %v", terminal, err)
	}
	different := finish
	different.Result = agent.Result{Text: "different"}
	different.ResultHash = mustResultHash(t, created.Execution, different.Result)
	if _, err := backend.FinishExecution(ctx, different); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("conflicting FinishExecution() error = %v, want ErrConflict", err)
	}

	failedCreated := mustStart(t, backend, "failed", testSpec("failed"), claimID(10), time.Second)
	failedResult := agent.Result{Failure: &agent.AgentFailure{Kind: agent.FailureKindEngineError, Reason: "failed"}}
	failed, err := backend.FinishExecution(ctx, durable.FinishExecutionRequest{ExecutionID: "failed", Lease: reference(failedCreated.Execution), ExpectedVersion: failedCreated.Execution.Version, Result: failedResult, ResultHash: mustResultHash(t, failedCreated.Execution, failedResult)})
	if err != nil || failed.Status != durable.ExecutionStatusFailed {
		t.Fatalf("FinishExecution(failed) = %#v, %v", failed, err)
	}
}

func testTypedErrors(t *testing.T, factory BackendFactory) {
	backend, _, cleanup := openBackend(t, factory)
	defer cleanup()
	contractFacts(t, "LoadExecution", "missing", durable.ClaimID{}, 0, 0, 0)
	_, err := backend.LoadExecution(context.Background(), "missing")
	var executionErr *durable.ExecutionError
	if !errors.Is(err, durable.ErrNotFound) || !errors.As(err, &executionErr) || executionErr.ExecutionID != "missing" {
		t.Fatalf("LoadExecution() error = %#v, want typed not-found execution error", err)
	}
	created := mustStart(t, backend, "typed-attempt", testSpec("typed"), claimID(1), time.Second)
	contractFacts(t, "StartAttempt", created.Execution.ID, created.Execution.Lease.ClaimID, created.Execution.Lease.Token, 0, 0)
	_, err = backend.StartAttempt(context.Background(), durable.StartAttemptRequest{ExecutionID: "typed-attempt", Lease: reference(created.Execution), OperationID: "", Kind: durable.AttemptKindTool})
	var attemptErr *durable.AttemptError
	if !errors.Is(err, durable.ErrInvalidArgument) || !errors.As(err, &attemptErr) || attemptErr.ExecutionID != "typed-attempt" {
		t.Fatalf("StartAttempt() error = %#v, want typed invalid attempt error", err)
	}
}

func openBackend(t *testing.T, factory BackendFactory) (durable.Backend, func(*testing.T) durable.Backend, func()) {
	t.Helper()
	if factory == nil {
		t.Fatal("nil BackendFactory")
	}
	backend, reopen, cleanup := factory(t)
	if backend == nil || reopen == nil || cleanup == nil {
		t.Fatal("BackendFactory returned nil component")
	}
	return backend, reopen, cleanup
}

func testSpec(prompt string) durable.ExecutionSpec {
	return durable.ExecutionSpec{Request: agent.Request{Prompt: prompt}}
}

func startRequest(t *testing.T, executionID durable.ExecutionID, spec durable.ExecutionSpec, claim durable.ClaimID, ttl time.Duration) durable.StartExecutionRequest {
	t.Helper()
	return durable.StartExecutionRequest{
		ExecutionID: executionID,
		OwnerID:     "owner",
		ClaimID:     claim,
		LeaseTTL:    ttl,
		Spec:        spec,
		SpecHash:    mustSpecHash(t, spec),
	}
}

func mustStart(t *testing.T, backend durable.Backend, executionID durable.ExecutionID, spec durable.ExecutionSpec, claim durable.ClaimID, ttl time.Duration) durable.StartResult {
	t.Helper()
	result, err := backend.StartExecution(context.Background(), startRequest(t, executionID, spec, claim, ttl))
	if err != nil {
		t.Fatalf("StartExecution(%q) error = %v", executionID, err)
	}
	return result
}

func resumeAfterExpiry(t *testing.T, backend durable.Backend, executionID durable.ExecutionID, claim durable.ClaimID, ttl time.Duration) durable.ResumeResult {
	t.Helper()
	request := durable.ResumeExecutionRequest{ExecutionID: executionID, OwnerID: "new-owner", ClaimID: claim, LeaseTTL: ttl}
	deadline := time.Now().Add(3 * time.Second)
	for {
		result, err := backend.ResumeExecution(context.Background(), request)
		if err == nil {
			return result
		}
		if !errors.Is(err, durable.ErrBusy) || time.Now().After(deadline) {
			t.Fatalf("ResumeExecution() after lease expiry error = %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func reference(execution durable.Execution) durable.LeaseRef {
	if execution.Lease == nil {
		panic("execution has no lease")
	}
	return durable.LeaseRef{OwnerID: execution.Lease.OwnerID, Token: execution.Lease.Token}
}

func testCheckpoint(t *testing.T, execution durable.Execution, sequence uint64, prompt string) durable.Checkpoint {
	t.Helper()
	continuation := agent.Continuation{
		SchemaVersion:     agent.ContinuationSchemaVersion,
		Request:           agent.Request{Prompt: prompt},
		Messages:          []message.Message{message.NewText(message.RoleUser, prompt)},
		NextOperationTurn: 0,
		Phase:             agent.ContinuationReady,
	}
	hash, err := durable.HashContinuation(execution.ID, execution.SpecHash, sequence, continuation)
	if err != nil {
		t.Fatalf("HashContinuation() error = %v", err)
	}
	return durable.Checkpoint{Sequence: sequence, Continuation: continuation, ContinuationHash: hash}
}

func startAttempt(t *testing.T, backend durable.Backend, executionID durable.ExecutionID, lease durable.LeaseRef, operationID string, kind durable.AttemptKind) durable.AttemptStart {
	t.Helper()
	inputHash := sha256.Sum256([]byte(operationID))
	started, err := backend.StartAttempt(context.Background(), durable.StartAttemptRequest{ExecutionID: executionID, Lease: lease, OperationID: operationID, Kind: kind, InputHash: inputHash})
	if err != nil || started.Decision != durable.AttemptDecisionExecute {
		t.Fatalf("StartAttempt(%q) = %#v, %v", operationID, started, err)
	}
	return started
}

func mustSpecHash(t *testing.T, spec durable.ExecutionSpec) [32]byte {
	t.Helper()
	hash, err := durable.HashExecutionSpec(spec)
	if err != nil {
		t.Fatalf("HashExecutionSpec() error = %v", err)
	}
	return hash
}

func mustResultHash(t *testing.T, execution durable.Execution, result agent.Result) [32]byte {
	t.Helper()
	hash, err := durable.HashResult(execution.ID, execution.SpecHash, execution.Version+1, result)
	if err != nil {
		t.Fatalf("HashResult() error = %v", err)
	}
	return hash
}

func claimID(seed byte) durable.ClaimID {
	var claim durable.ClaimID
	claim[len(claim)-1] = seed
	return claim
}
