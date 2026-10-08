package contract

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/durable"
)

func testClaimResponseLossReopen(t *testing.T, factory BackendFactory) {
	t.Run("StartExecution", func(t *testing.T) {
		backend, reopen, cleanup := openBackend(t, factory)
		defer cleanup()
		request := startRequest(t, "claim-start-loss", testSpec("start"), claimID(21), time.Second)
		committed, err := backend.StartExecution(context.Background(), request)
		if err != nil {
			contractFacts(t, "StartExecution", request.ExecutionID, request.ClaimID, 0, 0, 0)
			t.Fatalf("commit error = %v", err)
		}
		contractFacts(t, "StartExecution", request.ExecutionID, request.ClaimID, committed.Execution.Lease.Token, 0, committed.Execution.Version)
		replayed, err := reopen(t).StartExecution(context.Background(), request)
		if err != nil || !reflect.DeepEqual(replayed, committed) {
			t.Fatalf("response-loss replay = %#v, %v, want %#v", replayed, err, committed)
		}
	})

	t.Run("ResumeExecution", func(t *testing.T) {
		backend, reopen, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "claim-resume-loss", testSpec("resume"), claimID(22), time.Second)
		lease := reference(created.Execution)
		if _, err := backend.ReleaseExecution(context.Background(), durable.ReleaseExecutionRequest{ExecutionID: created.Execution.ID, Lease: lease}); err != nil {
			contractFacts(t, "ReleaseExecution", created.Execution.ID, created.Execution.Lease.ClaimID, lease.Token, created.Execution.Version, created.Execution.Version)
			t.Fatalf("prepare release error = %v", err)
		}
		request := durable.ResumeExecutionRequest{
			ExecutionID: created.Execution.ID,
			OwnerID:     "resume-owner",
			ClaimID:     claimID(23),
			LeaseTTL:    time.Second,
		}
		committed, err := backend.ResumeExecution(context.Background(), request)
		if err != nil {
			contractFacts(t, "ResumeExecution", request.ExecutionID, request.ClaimID, 0, created.Execution.Version, 0)
			t.Fatalf("commit error = %v", err)
		}
		contractFacts(t, "ResumeExecution", request.ExecutionID, request.ClaimID, committed.Execution.Lease.Token, created.Execution.Version, committed.Execution.Version)
		replayed, err := reopen(t).ResumeExecution(context.Background(), request)
		if err != nil || !reflect.DeepEqual(replayed, committed) {
			t.Fatalf("response-loss replay = %#v, %v, want %#v", replayed, err, committed)
		}
	})
}

func testLeaseMutationResponseLossReopen(t *testing.T, factory BackendFactory) {
	t.Run("RenewExecution", func(t *testing.T) {
		backend, reopen, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "renew-loss", testSpec("renew"), claimID(24), time.Second)
		request := durable.RenewExecutionRequest{ExecutionID: created.Execution.ID, Lease: reference(created.Execution), LeaseTTL: 2 * time.Second}
		first, err := backend.RenewExecution(context.Background(), request)
		if err != nil {
			contractFacts(t, "RenewExecution", request.ExecutionID, created.Execution.Lease.ClaimID, request.Lease.Token, created.Execution.Version, 0)
			t.Fatalf("commit error = %v", err)
		}
		secondBackend := reopen(t)
		second, err := secondBackend.RenewExecution(context.Background(), request)
		loaded, loadErr := secondBackend.LoadExecution(context.Background(), request.ExecutionID)
		contractFacts(t, "RenewExecution", request.ExecutionID, created.Execution.Lease.ClaimID, request.Lease.Token, created.Execution.Version, loaded.Version)
		if err != nil || loadErr != nil {
			t.Fatalf("response-loss replay error = %v, load error = %v", err, loadErr)
		}
		if first.Token != second.Token || second.Token != request.Lease.Token || second.ExpiresAt.Before(first.ExpiresAt) || loaded.Version != created.Execution.Version {
			t.Fatalf("first lease = %#v, replay lease = %#v, execution = %#v", first, second, loaded)
		}
	})

	t.Run("ReleaseExecution", func(t *testing.T) {
		backend, reopen, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "release-loss", testSpec("release"), claimID(25), time.Second)
		lease := reference(created.Execution)
		startAttempt(t, backend, created.Execution.ID, lease, "turn:0:model", durable.AttemptKindModel)
		request := durable.ReleaseExecutionRequest{ExecutionID: created.Execution.ID, Lease: lease}
		committed, err := backend.ReleaseExecution(context.Background(), request)
		if err != nil {
			contractFacts(t, "ReleaseExecution", request.ExecutionID, created.Execution.Lease.ClaimID, lease.Token, created.Execution.Version, 0)
			t.Fatalf("commit error = %v", err)
		}
		contractFacts(t, "ReleaseExecution", request.ExecutionID, created.Execution.Lease.ClaimID, lease.Token, created.Execution.Version, committed.Execution.Version)
		replayed, err := reopen(t).ReleaseExecution(context.Background(), request)
		if err != nil || !reflect.DeepEqual(replayed, committed) {
			t.Fatalf("response-loss replay = %#v, %v, want %#v", replayed, err, committed)
		}
		if len(replayed.Reconcile) != 1 || replayed.Reconcile[0].Status != durable.AttemptStatusUnknown {
			t.Fatalf("reconcile = %#v, want one unknown attempt", replayed.Reconcile)
		}
	})

	t.Run("SuspendExecution", func(t *testing.T) {
		backend, reopen, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "suspend-loss", testSpec("suspend"), claimID(26), time.Second)
		lease := reference(created.Execution)
		operationID := "turn:0:model"
		started := startAttempt(t, backend, created.Execution.ID, lease, operationID, durable.AttemptKindModel)
		request := durable.SuspendExecutionRequest{ExecutionID: created.Execution.ID, Lease: lease, ExpectedVersion: created.Execution.Version}
		committed, err := backend.SuspendExecution(context.Background(), request)
		if err != nil {
			contractFacts(t, "SuspendExecution", request.ExecutionID, created.Execution.Lease.ClaimID, lease.Token, request.ExpectedVersion, 0)
			t.Fatalf("commit error = %v", err)
		}
		secondBackend := reopen(t)
		replayed, err := secondBackend.SuspendExecution(context.Background(), request)
		contractFacts(t, "SuspendExecution", request.ExecutionID, created.Execution.Lease.ClaimID, lease.Token, request.ExpectedVersion, replayed.Version)
		if err != nil || !reflect.DeepEqual(replayed, committed) {
			t.Fatalf("response-loss replay = %#v, %v, want %#v", replayed, err, committed)
		}
		resumed, err := secondBackend.ResumeExecution(context.Background(), durable.ResumeExecutionRequest{
			ExecutionID: request.ExecutionID, OwnerID: "after-suspend", ClaimID: claimID(27), LeaseTTL: time.Second,
		})
		if err != nil {
			t.Fatalf("resume after suspension error = %v", err)
		}
		decision, err := secondBackend.StartAttempt(context.Background(), durable.StartAttemptRequest{
			ExecutionID: request.ExecutionID,
			Lease:       reference(resumed.Execution),
			OperationID: operationID,
			Kind:        started.Attempt.Kind,
			InputHash:   started.Attempt.InputHash,
		})
		if err != nil || decision.Decision != durable.AttemptDecisionReconcile || decision.Attempt.Status != durable.AttemptStatusUnknown {
			t.Fatalf("converged StartAttempt() = %#v, %v", decision, err)
		}
	})
}

func testCheckpointResponseLossReopen(t *testing.T, factory BackendFactory) {
	backend, reopen, cleanup := openBackend(t, factory)
	defer cleanup()
	created := mustStart(t, backend, "checkpoint-loss", testSpec("checkpoint"), claimID(28), time.Second)
	request := durable.SaveCheckpointRequest{
		ExecutionID:     created.Execution.ID,
		Lease:           reference(created.Execution),
		ExpectedVersion: created.Execution.Version,
		Checkpoint:      testCheckpoint(t, created.Execution, 1, "checkpoint-loss"),
	}
	committed, err := backend.SaveCheckpoint(context.Background(), request)
	if err != nil {
		contractFacts(t, "SaveCheckpoint", request.ExecutionID, created.Execution.Lease.ClaimID, request.Lease.Token, request.ExpectedVersion, 0)
		t.Fatalf("commit error = %v", err)
	}
	contractFacts(t, "SaveCheckpoint", request.ExecutionID, created.Execution.Lease.ClaimID, request.Lease.Token, request.ExpectedVersion, committed.Version)
	replayed, err := reopen(t).SaveCheckpoint(context.Background(), request)
	if err != nil || !reflect.DeepEqual(replayed, committed) {
		t.Fatalf("response-loss replay = %#v, %v, want %#v", replayed, err, committed)
	}
}

func testAttemptResponseLossReopen(t *testing.T, factory BackendFactory) {
	t.Run("StartAttempt", func(t *testing.T) {
		backend, reopen, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "attempt-start-loss", testSpec("attempt"), claimID(29), time.Second)
		request := durable.StartAttemptRequest{
			ExecutionID: created.Execution.ID,
			Lease:       reference(created.Execution),
			OperationID: "turn:0:model",
			Kind:        durable.AttemptKindModel,
			InputHash:   sha256.Sum256([]byte("attempt-start-loss")),
		}
		committed, err := backend.StartAttempt(context.Background(), request)
		if err != nil {
			contractFacts(t, "StartAttempt", request.ExecutionID, created.Execution.Lease.ClaimID, request.Lease.Token, 0, 0)
			t.Fatalf("commit error = %v", err)
		}
		contractFacts(t, "StartAttempt", request.ExecutionID, created.Execution.Lease.ClaimID, request.Lease.Token, 0, committed.Attempt.Version)
		replayed, err := reopen(t).StartAttempt(context.Background(), request)
		if err != nil || !reflect.DeepEqual(replayed, committed) {
			t.Fatalf("response-loss replay = %#v, %v, want %#v", replayed, err, committed)
		}
	})

	t.Run("FinishAttempt", func(t *testing.T) {
		backend, reopen, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "attempt-finish-loss", testSpec("attempt"), claimID(30), time.Second)
		lease := reference(created.Execution)
		started := startAttempt(t, backend, created.Execution.ID, lease, "turn:0:call:0", durable.AttemptKindTool)
		request := durable.FinishAttemptRequest{
			ExecutionID: created.Execution.ID, Lease: lease, OperationID: started.Attempt.OperationID,
			AttemptNumber: started.Attempt.Number, ExpectedAttemptVersion: started.Attempt.Version, Payload: []byte("settled"),
		}
		committed, err := backend.FinishAttempt(context.Background(), request)
		if err != nil {
			contractFacts(t, "FinishAttempt", request.ExecutionID, created.Execution.Lease.ClaimID, lease.Token, request.ExpectedAttemptVersion, 0)
			t.Fatalf("commit error = %v", err)
		}
		contractFacts(t, "FinishAttempt", request.ExecutionID, created.Execution.Lease.ClaimID, lease.Token, request.ExpectedAttemptVersion, committed.Version)
		replayed, err := reopen(t).FinishAttempt(context.Background(), request)
		if err != nil || !reflect.DeepEqual(replayed, committed) {
			t.Fatalf("response-loss replay = %#v, %v, want %#v", replayed, err, committed)
		}
	})

	t.Run("MarkAttemptUnknown", func(t *testing.T) {
		backend, reopen, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "attempt-unknown-loss", testSpec("attempt"), claimID(31), time.Second)
		lease := reference(created.Execution)
		started := startAttempt(t, backend, created.Execution.ID, lease, "turn:0:model", durable.AttemptKindModel)
		request := durable.MarkAttemptUnknownRequest{
			ExecutionID: created.Execution.ID, Lease: lease, OperationID: started.Attempt.OperationID,
			AttemptNumber: started.Attempt.Number, ExpectedAttemptVersion: started.Attempt.Version,
			Payload: []byte("partial"), Failure: &durable.FailureRecord{Code: "transport", Message: "unknown"},
		}
		committed, err := backend.MarkAttemptUnknown(context.Background(), request)
		if err != nil {
			contractFacts(t, "MarkAttemptUnknown", request.ExecutionID, created.Execution.Lease.ClaimID, lease.Token, request.ExpectedAttemptVersion, 0)
			t.Fatalf("commit error = %v", err)
		}
		contractFacts(t, "MarkAttemptUnknown", request.ExecutionID, created.Execution.Lease.ClaimID, lease.Token, request.ExpectedAttemptVersion, committed.Version)
		reopened := reopen(t)
		replayed, err := reopened.MarkAttemptUnknown(context.Background(), request)
		if err != nil || !reflect.DeepEqual(replayed, committed) {
			t.Fatalf("response-loss replay = %#v, %v, want %#v", replayed, err, committed)
		}
		if _, err := reopened.ReleaseExecution(context.Background(), durable.ReleaseExecutionRequest{ExecutionID: created.Execution.ID, Lease: lease}); err != nil {
			t.Fatalf("ReleaseExecution() error = %v", err)
		}
		resumed, err := reopened.ResumeExecution(context.Background(), durable.ResumeExecutionRequest{
			ExecutionID: created.Execution.ID,
			OwnerID:     "unknown-reader",
			ClaimID:     claimID(41),
			LeaseTTL:    time.Second,
		})
		if err != nil || len(resumed.Reconcile) != 1 || !reflect.DeepEqual(resumed.Reconcile[0], committed) {
			t.Fatalf("ResumeExecution() unresolved attempts = %#v, %v, want %#v", resumed.Reconcile, err, committed)
		}
	})

	t.Run("ReconcileAttempt", func(t *testing.T) {
		backend, reopen, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "attempt-reconcile-loss", testSpec("attempt"), claimID(32), time.Second)
		lease := reference(created.Execution)
		started := startAttempt(t, backend, created.Execution.ID, lease, "turn:0:model", durable.AttemptKindModel)
		unknown, err := backend.MarkAttemptUnknown(context.Background(), durable.MarkAttemptUnknownRequest{
			ExecutionID: created.Execution.ID, Lease: lease, OperationID: started.Attempt.OperationID,
			AttemptNumber: started.Attempt.Number, ExpectedAttemptVersion: started.Attempt.Version,
			Failure: &durable.FailureRecord{Code: "transport", Message: "unknown"},
		})
		if err != nil {
			t.Fatalf("prepare unknown attempt error = %v", err)
		}
		if _, err := backend.ReleaseExecution(context.Background(), durable.ReleaseExecutionRequest{ExecutionID: created.Execution.ID, Lease: lease}); err != nil {
			t.Fatalf("prepare release error = %v", err)
		}
		request := durable.ReconcileAttemptRequest{
			ExecutionID: created.Execution.ID, OperationID: unknown.OperationID,
			AttemptNumber: unknown.Number, ExpectedAttemptVersion: unknown.Version,
			Resolution: durable.ReconcileResolutionSucceed, Payload: []byte("confirmed"),
		}
		committed, err := backend.ReconcileAttempt(context.Background(), request)
		if err != nil {
			contractFacts(t, "ReconcileAttempt", request.ExecutionID, durable.ClaimID{}, lease.Token, request.ExpectedAttemptVersion, 0)
			t.Fatalf("commit error = %v", err)
		}
		contractFacts(t, "ReconcileAttempt", request.ExecutionID, durable.ClaimID{}, lease.Token, request.ExpectedAttemptVersion, committed.Version)
		replayed, err := reopen(t).ReconcileAttempt(context.Background(), request)
		if err != nil || !reflect.DeepEqual(replayed, committed) {
			t.Fatalf("response-loss replay = %#v, %v, want %#v", replayed, err, committed)
		}
	})
}

func testAttemptStaleMutationPreservesState(t *testing.T, factory BackendFactory) {
	backend, _, cleanup := openBackend(t, factory)
	defer cleanup()
	created := mustStart(t, backend, "attempt-stale", testSpec("attempt"), claimID(33), time.Second)
	lease := reference(created.Execution)

	finishedStart := startAttempt(t, backend, created.Execution.ID, lease, "turn:0:call:0", durable.AttemptKindTool)
	staleLease := lease
	staleLease.Token++
	staleFinish := durable.FinishAttemptRequest{
		ExecutionID: created.Execution.ID, Lease: staleLease, OperationID: finishedStart.Attempt.OperationID,
		AttemptNumber: finishedStart.Attempt.Number, ExpectedAttemptVersion: finishedStart.Attempt.Version, Payload: []byte("done"),
	}
	if _, err := backend.FinishAttempt(context.Background(), staleFinish); !errors.Is(err, durable.ErrLeaseLost) {
		contractFacts(t, "FinishAttempt", staleFinish.ExecutionID, created.Execution.Lease.ClaimID, staleLease.Token, staleFinish.ExpectedAttemptVersion, 0)
		t.Fatalf("stale lease error = %v, want ErrLeaseLost", err)
	}
	staleFinish.Lease = lease
	finished, err := backend.FinishAttempt(context.Background(), staleFinish)
	if err != nil || finished.Version != finishedStart.Attempt.Version+1 {
		t.Fatalf("valid FinishAttempt() after rejection = %#v, %v", finished, err)
	}

	unknownStart := startAttempt(t, backend, created.Execution.ID, lease, "turn:1:model", durable.AttemptKindModel)
	unknownRequest := durable.MarkAttemptUnknownRequest{
		ExecutionID: created.Execution.ID, Lease: lease, OperationID: unknownStart.Attempt.OperationID,
		AttemptNumber: unknownStart.Attempt.Number, ExpectedAttemptVersion: unknownStart.Attempt.Version + 1,
		Failure: &durable.FailureRecord{Code: "transport", Message: "unknown"},
	}
	if _, err := backend.MarkAttemptUnknown(context.Background(), unknownRequest); !errors.Is(err, durable.ErrConflict) {
		contractFacts(t, "MarkAttemptUnknown", unknownRequest.ExecutionID, created.Execution.Lease.ClaimID, lease.Token, unknownRequest.ExpectedAttemptVersion, unknownStart.Attempt.Version)
		t.Fatalf("stale attempt version error = %v, want ErrConflict", err)
	}
	unknownRequest.ExpectedAttemptVersion = unknownStart.Attempt.Version
	unknown, err := backend.MarkAttemptUnknown(context.Background(), unknownRequest)
	if err != nil || unknown.Version != unknownStart.Attempt.Version+1 {
		t.Fatalf("valid MarkAttemptUnknown() after rejection = %#v, %v", unknown, err)
	}
	if _, err := backend.ReleaseExecution(context.Background(), durable.ReleaseExecutionRequest{ExecutionID: created.Execution.ID, Lease: lease}); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
	staleReconcile := durable.ReconcileAttemptRequest{
		ExecutionID: created.Execution.ID, OperationID: unknown.OperationID, AttemptNumber: unknown.Number,
		ExpectedAttemptVersion: unknown.Version + 1, Resolution: durable.ReconcileResolutionRetry,
	}
	if _, err := backend.ReconcileAttempt(context.Background(), staleReconcile); !errors.Is(err, durable.ErrConflict) {
		contractFacts(t, "ReconcileAttempt", staleReconcile.ExecutionID, durable.ClaimID{}, lease.Token, staleReconcile.ExpectedAttemptVersion, unknown.Version)
		t.Fatalf("stale reconciliation version error = %v, want ErrConflict", err)
	}
	staleReconcile.ExpectedAttemptVersion = unknown.Version
	reconciled, err := backend.ReconcileAttempt(context.Background(), staleReconcile)
	if err != nil || reconciled.Version != unknown.Version+1 {
		t.Fatalf("valid ReconcileAttempt() after rejection = %#v, %v", reconciled, err)
	}
	loaded, err := backend.LoadExecution(context.Background(), created.Execution.ID)
	if err != nil || loaded.Version != created.Execution.Version {
		t.Fatalf("attempt mutations changed execution = %#v, %v", loaded, err)
	}
}

func testFinishResponseLossReopen(t *testing.T, factory BackendFactory) {
	backend, reopen, cleanup := openBackend(t, factory)
	defer cleanup()
	created := mustStart(t, backend, "finish-loss", testSpec("finish"), claimID(34), time.Second)
	result := agent.Result{Text: "done", Valid: true}
	request := durable.FinishExecutionRequest{
		ExecutionID:     created.Execution.ID,
		Lease:           reference(created.Execution),
		ExpectedVersion: created.Execution.Version,
		Result:          result,
		ResultHash:      mustResultHash(t, created.Execution, result),
	}
	committed, err := backend.FinishExecution(context.Background(), request)
	if err != nil {
		contractFacts(t, "FinishExecution", request.ExecutionID, created.Execution.Lease.ClaimID, request.Lease.Token, request.ExpectedVersion, 0)
		t.Fatalf("commit error = %v", err)
	}
	contractFacts(t, "FinishExecution", request.ExecutionID, created.Execution.Lease.ClaimID, request.Lease.Token, request.ExpectedVersion, committed.Version)
	replayed, err := reopen(t).FinishExecution(context.Background(), request)
	if err != nil || !reflect.DeepEqual(replayed, committed) {
		t.Fatalf("response-loss replay = %#v, %v, want %#v", replayed, err, committed)
	}
}

func testRejectedMutationPreservesVersions(t *testing.T, factory BackendFactory) {
	t.Run("checkpoint codec stale version and lease", func(t *testing.T) {
		backend, _, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "rejected-checkpoint", testSpec("checkpoint"), claimID(35), time.Second)
		lease := reference(created.Execution)
		valid := durable.SaveCheckpointRequest{
			ExecutionID: created.Execution.ID, Lease: lease, ExpectedVersion: created.Execution.Version,
			Checkpoint: testCheckpoint(t, created.Execution, 1, "first"),
		}
		saved, err := backend.SaveCheckpoint(context.Background(), valid)
		if err != nil {
			t.Fatalf("SaveCheckpoint() error = %v", err)
		}
		corrupt := valid
		corrupt.ExpectedVersion = saved.Version
		corrupt.Checkpoint = testCheckpoint(t, created.Execution, 2, "corrupt")
		corrupt.Checkpoint.ContinuationHash[0] ^= 0xff
		if _, err := backend.SaveCheckpoint(context.Background(), corrupt); !errors.Is(err, durable.ErrCorruptCheckpoint) {
			contractFacts(t, "SaveCheckpoint", corrupt.ExecutionID, created.Execution.Lease.ClaimID, lease.Token, corrupt.ExpectedVersion, saved.Version)
			t.Fatalf("corrupt checkpoint error = %v, want ErrCorruptCheckpoint", err)
		}
		stale := valid
		stale.Checkpoint = testCheckpoint(t, created.Execution, 2, "stale")
		if _, err := backend.SaveCheckpoint(context.Background(), stale); !errors.Is(err, durable.ErrConflict) {
			t.Fatalf("stale version error = %v, want ErrConflict", err)
		}
		staleLease := valid
		staleLease.ExpectedVersion = saved.Version
		staleLease.Checkpoint = testCheckpoint(t, created.Execution, 2, "stale-lease")
		staleLease.Lease.Token++
		if _, err := backend.SaveCheckpoint(context.Background(), staleLease); !errors.Is(err, durable.ErrLeaseLost) {
			t.Fatalf("stale lease error = %v, want ErrLeaseLost", err)
		}
		loaded, err := backend.LoadExecution(context.Background(), created.Execution.ID)
		if err != nil || !reflect.DeepEqual(loaded, saved) {
			t.Fatalf("rejected checkpoints mutated state = %#v, %v, want %#v", loaded, err, saved)
		}
	})

	t.Run("suspend stale version", func(t *testing.T) {
		backend, _, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "rejected-suspend", testSpec("suspend"), claimID(36), time.Second)
		request := durable.SuspendExecutionRequest{
			ExecutionID: created.Execution.ID, Lease: reference(created.Execution), ExpectedVersion: created.Execution.Version + 1,
		}
		if _, err := backend.SuspendExecution(context.Background(), request); !errors.Is(err, durable.ErrConflict) {
			contractFacts(t, "SuspendExecution", request.ExecutionID, created.Execution.Lease.ClaimID, request.Lease.Token, request.ExpectedVersion, created.Execution.Version)
			t.Fatalf("stale version error = %v, want ErrConflict", err)
		}
		loaded, err := backend.LoadExecution(context.Background(), created.Execution.ID)
		if err != nil || !reflect.DeepEqual(loaded, created.Execution) {
			t.Fatalf("rejected suspension mutated state = %#v, %v", loaded, err)
		}
	})

	t.Run("finish unsettled attempt", func(t *testing.T) {
		backend, _, cleanup := openBackend(t, factory)
		defer cleanup()
		created := mustStart(t, backend, "rejected-finish", testSpec("finish"), claimID(37), time.Second)
		lease := reference(created.Execution)
		startAttempt(t, backend, created.Execution.ID, lease, "turn:0:model", durable.AttemptKindModel)
		result := agent.Result{Text: "unsafe", Valid: true}
		request := durable.FinishExecutionRequest{
			ExecutionID: created.Execution.ID, Lease: lease, ExpectedVersion: created.Execution.Version,
			Result: result, ResultHash: mustResultHash(t, created.Execution, result),
		}
		if _, err := backend.FinishExecution(context.Background(), request); !errors.Is(err, durable.ErrReconcileRequired) {
			contractFacts(t, "FinishExecution", request.ExecutionID, created.Execution.Lease.ClaimID, lease.Token, request.ExpectedVersion, created.Execution.Version)
			t.Fatalf("unsettled attempt error = %v, want ErrReconcileRequired", err)
		}
		loaded, err := backend.LoadExecution(context.Background(), created.Execution.ID)
		if err != nil || loaded.Version != created.Execution.Version || loaded.Status != durable.ExecutionStatusRunning {
			t.Fatalf("rejected finish mutated execution = %#v, %v", loaded, err)
		}
	})
}

func contractFacts(t *testing.T, command string, executionID durable.ExecutionID, claim durable.ClaimID, token, expectedVersion, actualVersion uint64) {
	t.Helper()
	t.Logf(
		"command=%s executionID=%q claim=%x token=%d expectedVersion=%d actualVersion=%d",
		command,
		executionID,
		claim,
		token,
		expectedVersion,
		actualVersion,
	)
}
