package durable

import (
	"context"

	"github.com/Viking602/venat/agent"
)

type boundaryObserver struct {
	active *activeExecution
}

func (observer boundaryObserver) ObserveBoundary(ctx context.Context, continuation agent.Continuation) error {
	if cause := observer.active.stopCauseValue(); cause != nil {
		return cause
	}
	execution := observer.active.snapshot()
	if execution.Lease == nil {
		return executionRuntimeError(observer.active.id, ErrLeaseLost)
	}
	sequence := uint64(1)
	if execution.Checkpoint != nil {
		sequence = execution.Checkpoint.Sequence + 1
		if sequence == 0 {
			return executionRuntimeError(observer.active.id, ErrConflict)
		}
	}
	hash, err := HashContinuation(execution.ID, execution.SpecHash, sequence, continuation)
	if err != nil {
		return runtimeOperationError("hash continuation", err)
	}
	saved, err := observer.active.runtime.backend.SaveCheckpoint(ctx, SaveCheckpointRequest{
		ExecutionID:     observer.active.id,
		Lease:           leaseReference(*execution.Lease),
		ExpectedVersion: execution.Version,
		Checkpoint: Checkpoint{
			Sequence:         sequence,
			Continuation:     continuation,
			ContinuationHash: hash,
		},
	})
	if err != nil {
		return backendExecutionOperationError("save checkpoint", observer.active.id, err)
	}
	observer.active.setExecution(saved)
	return nil
}
