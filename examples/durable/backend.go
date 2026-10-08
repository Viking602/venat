package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/Viking602/venat/durable"
)

type exampleBackend struct {
	state *backendState
}

type backendState struct {
	mu        sync.Mutex
	execution *durable.Execution
	attempts  map[string][]durable.Attempt
	nextToken uint64
}

func newBackend() *exampleBackend {
	return &exampleBackend{state: &backendState{attempts: make(map[string][]durable.Attempt)}}
}

func (backend *exampleBackend) reopen() *exampleBackend {
	return &exampleBackend{state: backend.state}
}

func (backend *exampleBackend) StartExecution(ctx context.Context, request durable.StartExecutionRequest) (durable.StartResult, error) {
	if err := ctx.Err(); err != nil {
		return durable.StartResult{}, err
	}
	hash, err := durable.HashExecutionSpec(request.Spec)
	if request.ExecutionID == "" || request.OwnerID == "" || request.ClaimID == (durable.ClaimID{}) || request.LeaseTTL <= 0 || err != nil {
		return durable.StartResult{}, executionError(request.ExecutionID, durable.ErrInvalidArgument)
	}
	if hash != request.SpecHash {
		return durable.StartResult{}, executionError(request.ExecutionID, durable.ErrConflict)
	}

	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if backend.state.execution == nil {
		backend.state.nextToken = 1
		backend.state.execution = &durable.Execution{
			ID:       request.ExecutionID,
			Spec:     copyValue(request.Spec),
			SpecHash: request.SpecHash,
			Status:   durable.ExecutionStatusRunning,
			Version:  1,
			Lease: &durable.Lease{
				OwnerID: request.OwnerID, ClaimID: request.ClaimID, Token: 1, ExpiresAt: time.Now().Add(request.LeaseTTL),
			},
		}
		return durable.StartResult{Execution: copyValue(*backend.state.execution), Created: true}, nil
	}
	execution := backend.state.execution
	if execution.ID != request.ExecutionID {
		return durable.StartResult{}, executionError(request.ExecutionID, durable.ErrNotFound)
	}
	if execution.SpecHash != request.SpecHash {
		return durable.StartResult{}, executionError(request.ExecutionID, durable.ErrConflict)
	}
	if terminal(execution.Status) {
		return durable.StartResult{Execution: copyValue(*execution)}, nil
	}
	reconcile, err := backend.claim(request.ExecutionID, request.OwnerID, request.ClaimID, request.LeaseTTL)
	if err != nil {
		return durable.StartResult{}, err
	}
	return durable.StartResult{Execution: copyValue(*execution), Reconcile: reconcile}, nil
}

func (backend *exampleBackend) ResumeExecution(ctx context.Context, request durable.ResumeExecutionRequest) (durable.ResumeResult, error) {
	if err := ctx.Err(); err != nil {
		return durable.ResumeResult{}, err
	}
	if request.ExecutionID == "" || request.OwnerID == "" || request.ClaimID == (durable.ClaimID{}) || request.LeaseTTL <= 0 {
		return durable.ResumeResult{}, executionError(request.ExecutionID, durable.ErrInvalidArgument)
	}
	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if backend.state.execution == nil || backend.state.execution.ID != request.ExecutionID {
		return durable.ResumeResult{}, executionError(request.ExecutionID, durable.ErrNotFound)
	}
	if terminal(backend.state.execution.Status) {
		return durable.ResumeResult{Execution: copyValue(*backend.state.execution)}, nil
	}
	reconcile, err := backend.claim(request.ExecutionID, request.OwnerID, request.ClaimID, request.LeaseTTL)
	if err != nil {
		return durable.ResumeResult{}, err
	}
	return durable.ResumeResult{Execution: copyValue(*backend.state.execution), Reconcile: reconcile}, nil
}

func (backend *exampleBackend) claim(executionID durable.ExecutionID, ownerID string, claimID durable.ClaimID, ttl time.Duration) ([]durable.Attempt, error) {
	execution := backend.state.execution
	if leaseActive(execution.Lease) {
		if execution.Lease.OwnerID == ownerID && execution.Lease.ClaimID == claimID {
			return nil, nil
		}
		return nil, executionError(executionID, durable.ErrBusy)
	}
	reconcile := backend.reconcileRunning(0)
	backend.state.nextToken++
	execution.Status = durable.ExecutionStatusRunning
	execution.Lease = &durable.Lease{
		OwnerID: ownerID, ClaimID: claimID, Token: backend.state.nextToken, ExpiresAt: time.Now().Add(ttl),
	}
	return reconcile, nil
}

func (backend *exampleBackend) RenewExecution(ctx context.Context, request durable.RenewExecutionRequest) (durable.Lease, error) {
	if err := ctx.Err(); err != nil {
		return durable.Lease{}, err
	}
	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if err := backend.requireLease(request.ExecutionID, request.Lease); err != nil {
		return durable.Lease{}, err
	}
	if request.LeaseTTL <= 0 {
		return durable.Lease{}, executionError(request.ExecutionID, durable.ErrInvalidArgument)
	}
	backend.state.execution.Lease.ExpiresAt = time.Now().Add(request.LeaseTTL)
	return *backend.state.execution.Lease, nil
}

func (backend *exampleBackend) SaveCheckpoint(ctx context.Context, request durable.SaveCheckpointRequest) (durable.Execution, error) {
	if err := ctx.Err(); err != nil {
		return durable.Execution{}, err
	}
	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if err := backend.requireLease(request.ExecutionID, request.Lease); err != nil {
		return durable.Execution{}, err
	}
	execution := backend.state.execution
	if err := durable.ValidateCheckpoint(execution.ID, execution.SpecHash, request.Checkpoint); err != nil {
		return durable.Execution{}, executionError(request.ExecutionID, err)
	}
	if execution.Checkpoint != nil && execution.Checkpoint.Sequence == request.Checkpoint.Sequence {
		if execution.Checkpoint.ContinuationHash == request.Checkpoint.ContinuationHash {
			return copyValue(*execution), nil
		}
		return durable.Execution{}, executionError(request.ExecutionID, durable.ErrConflict)
	}
	if execution.Version != request.ExpectedVersion || (execution.Checkpoint != nil && request.Checkpoint.Sequence <= execution.Checkpoint.Sequence) {
		return durable.Execution{}, executionError(request.ExecutionID, durable.ErrConflict)
	}
	checkpoint := copyValue(request.Checkpoint)
	execution.Checkpoint = &checkpoint
	execution.Version++
	return copyValue(*execution), nil
}

func (backend *exampleBackend) SuspendExecution(ctx context.Context, request durable.SuspendExecutionRequest) (durable.Execution, error) {
	if err := ctx.Err(); err != nil {
		return durable.Execution{}, err
	}
	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if err := backend.requireLease(request.ExecutionID, request.Lease); err != nil {
		return durable.Execution{}, err
	}
	if backend.state.execution.Version != request.ExpectedVersion {
		return durable.Execution{}, executionError(request.ExecutionID, durable.ErrConflict)
	}
	backend.reconcileRunning(request.Lease.Token)
	backend.state.execution.Status = durable.ExecutionStatusSuspended
	backend.state.execution.Lease = nil
	backend.state.execution.Version++
	return copyValue(*backend.state.execution), nil
}

func (backend *exampleBackend) FinishExecution(ctx context.Context, request durable.FinishExecutionRequest) (durable.Execution, error) {
	if err := ctx.Err(); err != nil {
		return durable.Execution{}, err
	}
	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if err := backend.requireLease(request.ExecutionID, request.Lease); err != nil {
		return durable.Execution{}, err
	}
	hash, err := durable.HashResult(request.ExecutionID, backend.state.execution.SpecHash, request.ExpectedVersion+1, request.Result)
	if err != nil || hash != request.ResultHash {
		return durable.Execution{}, executionError(request.ExecutionID, durable.ErrConflict)
	}
	if backend.state.execution.Version != request.ExpectedVersion {
		return durable.Execution{}, executionError(request.ExecutionID, durable.ErrConflict)
	}
	for _, attempts := range backend.state.attempts {
		for _, attempt := range attempts {
			if attempt.Status == durable.AttemptStatusRunning || attempt.Status == durable.AttemptStatusUnknown {
				return durable.Execution{}, executionError(request.ExecutionID, durable.ErrReconcileRequired)
			}
		}
	}
	result := copyValue(request.Result)
	backend.state.execution.Result = &result
	backend.state.execution.ResultHash = request.ResultHash
	backend.state.execution.Status = durable.ExecutionStatusCompleted
	if result.Failure != nil {
		backend.state.execution.Status = durable.ExecutionStatusFailed
	}
	backend.state.execution.Lease = nil
	backend.state.execution.Version++
	return copyValue(*backend.state.execution), nil
}

func (backend *exampleBackend) ReleaseExecution(ctx context.Context, request durable.ReleaseExecutionRequest) (durable.ReleaseResult, error) {
	if err := ctx.Err(); err != nil {
		return durable.ReleaseResult{}, err
	}
	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if err := backend.requireLease(request.ExecutionID, request.Lease); err != nil {
		return durable.ReleaseResult{}, err
	}
	reconcile := backend.reconcileRunning(request.Lease.Token)
	backend.state.execution.Lease = nil
	return durable.ReleaseResult{Execution: copyValue(*backend.state.execution), Reconcile: reconcile}, nil
}

func (backend *exampleBackend) StartAttempt(ctx context.Context, request durable.StartAttemptRequest) (durable.AttemptStart, error) {
	if err := ctx.Err(); err != nil {
		return durable.AttemptStart{}, err
	}
	if request.OperationID == "" || (request.Kind != durable.AttemptKindModel && request.Kind != durable.AttemptKindTool) {
		return durable.AttemptStart{}, attemptError(request.ExecutionID, request.OperationID, 0, durable.ErrInvalidArgument)
	}
	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if err := backend.requireLease(request.ExecutionID, request.Lease); err != nil {
		return durable.AttemptStart{}, err
	}
	attempts := backend.state.attempts[request.OperationID]
	if len(attempts) > 0 {
		latest := attempts[len(attempts)-1]
		if latest.Kind != request.Kind || latest.InputHash != request.InputHash {
			return durable.AttemptStart{}, attemptError(request.ExecutionID, request.OperationID, latest.Number, durable.ErrConflict)
		}
		switch latest.Status {
		case durable.AttemptStatusSucceeded, durable.AttemptStatusFailed:
			return durable.AttemptStart{Attempt: copyValue(latest), Decision: durable.AttemptDecisionReplay}, nil
		case durable.AttemptStatusUnknown:
			return durable.AttemptStart{Attempt: copyValue(latest), Decision: durable.AttemptDecisionReconcile}, nil
		case durable.AttemptStatusRunning:
			return durable.AttemptStart{}, attemptError(request.ExecutionID, request.OperationID, latest.Number, durable.ErrBusy)
		case durable.AttemptStatusAbandoned:
		}
	}
	number := len(attempts) + 1
	attempt := durable.Attempt{
		ExecutionID: request.ExecutionID,
		OperationID: request.OperationID,
		Kind:        request.Kind,
		Number:      number,
		InputHash:   request.InputHash,
		Status:      durable.AttemptStatusRunning,
		Lease:       &durable.LeaseRef{OwnerID: request.Lease.OwnerID, Token: request.Lease.Token},
		Version:     1,
	}
	backend.state.attempts[request.OperationID] = append(attempts, attempt)
	return durable.AttemptStart{Attempt: copyValue(attempt), Decision: durable.AttemptDecisionExecute}, nil
}

func (backend *exampleBackend) FinishAttempt(ctx context.Context, request durable.FinishAttemptRequest) (durable.Attempt, error) {
	return backend.settleAttempt(ctx, request.ExecutionID, request.Lease, request.OperationID, request.AttemptNumber, request.ExpectedAttemptVersion, request.Payload, request.Failure, false)
}

func (backend *exampleBackend) MarkAttemptUnknown(ctx context.Context, request durable.MarkAttemptUnknownRequest) (durable.Attempt, error) {
	return backend.settleAttempt(ctx, request.ExecutionID, request.Lease, request.OperationID, request.AttemptNumber, request.ExpectedAttemptVersion, request.Payload, request.Failure, true)
}

func (backend *exampleBackend) settleAttempt(ctx context.Context, executionID durable.ExecutionID, lease durable.LeaseRef, operationID string, number int, version uint64, payload []byte, failure *durable.FailureRecord, unknown bool) (durable.Attempt, error) {
	if err := ctx.Err(); err != nil {
		return durable.Attempt{}, err
	}
	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if err := backend.requireLease(executionID, lease); err != nil {
		return durable.Attempt{}, err
	}
	attempt, err := backend.attempt(operationID, number)
	if err != nil {
		return durable.Attempt{}, err
	}
	if attempt.Status != durable.AttemptStatusRunning || attempt.Version != version || attempt.Lease == nil || attempt.Lease.Token != lease.Token || attempt.Lease.OwnerID != lease.OwnerID {
		return durable.Attempt{}, attemptError(executionID, operationID, number, durable.ErrConflict)
	}
	attempt.Payload = append([]byte(nil), payload...)
	attempt.Failure = copyFailure(failure)
	attempt.Lease = nil
	attempt.Version++
	attempt.Status = durable.AttemptStatusSucceeded
	if failure != nil {
		attempt.Status = durable.AttemptStatusFailed
	}
	if unknown {
		attempt.Status = durable.AttemptStatusUnknown
	}
	return copyValue(*attempt), nil
}

func (backend *exampleBackend) ReconcileAttempt(ctx context.Context, request durable.ReconcileAttemptRequest) (durable.Attempt, error) {
	if err := ctx.Err(); err != nil {
		return durable.Attempt{}, err
	}
	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if backend.state.execution == nil || backend.state.execution.ID != request.ExecutionID {
		return durable.Attempt{}, attemptError(request.ExecutionID, request.OperationID, request.AttemptNumber, durable.ErrNotFound)
	}
	if leaseActive(backend.state.execution.Lease) {
		return durable.Attempt{}, attemptError(request.ExecutionID, request.OperationID, request.AttemptNumber, durable.ErrBusy)
	}
	attempt, err := backend.attempt(request.OperationID, request.AttemptNumber)
	if err != nil {
		return durable.Attempt{}, err
	}
	if attempt.Status != durable.AttemptStatusUnknown || attempt.Version != request.ExpectedAttemptVersion {
		return durable.Attempt{}, attemptError(request.ExecutionID, request.OperationID, request.AttemptNumber, durable.ErrConflict)
	}
	attempt.Payload = append([]byte(nil), request.Payload...)
	attempt.Failure = copyFailure(request.Failure)
	attempt.Lease = nil
	attempt.Version++
	switch request.Resolution {
	case durable.ReconcileResolutionSucceed:
		attempt.Status = durable.AttemptStatusSucceeded
	case durable.ReconcileResolutionFail:
		attempt.Status = durable.AttemptStatusFailed
	case durable.ReconcileResolutionRetry:
		attempt.Status = durable.AttemptStatusAbandoned
	default:
		return durable.Attempt{}, attemptError(request.ExecutionID, request.OperationID, request.AttemptNumber, durable.ErrInvalidArgument)
	}
	return copyValue(*attempt), nil
}

func (backend *exampleBackend) LoadExecution(ctx context.Context, executionID durable.ExecutionID) (durable.Execution, error) {
	if err := ctx.Err(); err != nil {
		return durable.Execution{}, err
	}
	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if backend.state.execution == nil || backend.state.execution.ID != executionID {
		return durable.Execution{}, executionError(executionID, durable.ErrNotFound)
	}
	if backend.state.execution.Checkpoint != nil {
		if err := durable.ValidateCheckpoint(backend.state.execution.ID, backend.state.execution.SpecHash, *backend.state.execution.Checkpoint); err != nil {
			return durable.Execution{}, executionError(executionID, err)
		}
	}
	return copyValue(*backend.state.execution), nil
}

func (backend *exampleBackend) requireLease(executionID durable.ExecutionID, reference durable.LeaseRef) error {
	if backend.state.execution == nil || backend.state.execution.ID != executionID {
		return executionError(executionID, durable.ErrNotFound)
	}
	lease := backend.state.execution.Lease
	if !leaseActive(lease) || lease.OwnerID != reference.OwnerID || lease.Token != reference.Token {
		return executionError(executionID, durable.ErrLeaseLost)
	}
	return nil
}

func (backend *exampleBackend) attempt(operationID string, number int) (*durable.Attempt, error) {
	attempts := backend.state.attempts[operationID]
	if number < 1 || number > len(attempts) {
		return nil, attemptError(backend.state.execution.ID, operationID, number, durable.ErrNotFound)
	}
	return &attempts[number-1], nil
}

func (backend *exampleBackend) reconcileRunning(token uint64) []durable.Attempt {
	var reconciled []durable.Attempt
	for operationID, attempts := range backend.state.attempts {
		for index := range attempts {
			attempt := &attempts[index]
			if attempt.Status != durable.AttemptStatusRunning || attempt.Lease == nil || (token != 0 && attempt.Lease.Token != token) {
				continue
			}
			attempt.Status = durable.AttemptStatusUnknown
			attempt.Lease = nil
			attempt.Version++
			reconciled = append(reconciled, copyValue(*attempt))
		}
		backend.state.attempts[operationID] = attempts
	}
	return reconciled
}

func leaseActive(lease *durable.Lease) bool {
	return lease != nil && lease.ExpiresAt.After(time.Now())
}

func terminal(status durable.ExecutionStatus) bool {
	return status == durable.ExecutionStatusCompleted || status == durable.ExecutionStatusFailed
}

func executionError(executionID durable.ExecutionID, err error) error {
	return &durable.ExecutionError{ExecutionID: executionID, Err: err}
}

func attemptError(executionID durable.ExecutionID, operationID string, number int, err error) error {
	return &durable.AttemptError{ExecutionID: executionID, OperationID: operationID, AttemptNumber: number, Err: err}
}

func copyFailure(failure *durable.FailureRecord) *durable.FailureRecord {
	if failure == nil {
		return nil
	}
	copied := *failure
	return &copied
}

func copyValue[T any](value T) T {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var copied T
	if err := json.Unmarshal(encoded, &copied); err != nil {
		panic(err)
	}
	return copied
}
