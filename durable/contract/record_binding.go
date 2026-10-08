package contract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/durable"
)

func testRecordBinding(t *testing.T, factory BackendFactory) {
	backend, reopen, cleanup := openBackend(t, factory)
	defer cleanup()
	origin := mustStart(t, backend, "origin", testSpec("same"), claimID(1), time.Second)
	target := mustStart(t, backend, "target", testSpec("same"), claimID(2), time.Second)
	checkpoint := testCheckpoint(t, origin.Execution, 1, "same")
	request := durable.SaveCheckpointRequest{ExecutionID: target.Execution.ID, Lease: reference(target.Execution), ExpectedVersion: target.Execution.Version, Checkpoint: checkpoint}
	if _, err := backend.SaveCheckpoint(context.Background(), request); !errors.Is(err, durable.ErrCorruptCheckpoint) {
		t.Fatalf("cross-execution checkpoint = %v", err)
	}
	request.ExecutionID, request.Lease = origin.Execution.ID, reference(origin.Execution)
	request.Checkpoint.Sequence = 2
	if _, err := backend.SaveCheckpoint(context.Background(), request); !errors.Is(err, durable.ErrCorruptCheckpoint) {
		t.Fatalf("relabeled checkpoint = %v", err)
	}
	result := agent.Result{Text: "done", Valid: true}
	finish := durable.FinishExecutionRequest{ExecutionID: target.Execution.ID, Lease: reference(target.Execution), ExpectedVersion: target.Execution.Version, Result: result, ResultHash: mustResultHash(t, origin.Execution, result)}
	if _, err := backend.FinishExecution(context.Background(), finish); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("cross-execution result = %v", err)
	}
	for _, before := range []durable.Execution{origin.Execution, target.Execution} {
		after, err := reopen(t).LoadExecution(context.Background(), before.ID)
		if err != nil || after.Version != before.Version || after.Checkpoint != nil || after.Result != nil {
			t.Fatalf("rejected transplant changed record: %+v error=%v", after, err)
		}
	}
}
