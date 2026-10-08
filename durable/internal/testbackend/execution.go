package testbackend

import (
	"context"
	"fmt"
	"time"

	"github.com/Viking602/venat/durable"
)

func (backend *Backend) StartExecution(ctx context.Context, request durable.StartExecutionRequest) (durable.StartResult, error) {
	if err := ctx.Err(); err != nil {
		return durable.StartResult{}, err
	}
	if !validExecutionID(request.ExecutionID) || !validLeaseInput(request.OwnerID, request.ClaimID, request.LeaseTTL) {
		return durable.StartResult{}, executionError(request.ExecutionID, durable.ErrInvalidArgument)
	}
	hash, err := durable.HashExecutionSpec(request.Spec)
	if err != nil {
		return durable.StartResult{}, executionError(request.ExecutionID, fmt.Errorf("%w: hash execution spec: %v", durable.ErrInvalidArgument, err))
	}
	if hash != request.SpecHash {
		return durable.StartResult{}, executionError(request.ExecutionID, durable.ErrConflict)
	}

	backend.store.mu.Lock()
	defer backend.store.mu.Unlock()
	record, exists := backend.store.executions[request.ExecutionID]
	if exists {
		if record.execution.SpecHash != request.SpecHash {
			return durable.StartResult{}, executionError(request.ExecutionID, durable.ErrConflict)
		}
		if terminal(record.execution.Status) {
			return durable.StartResult{Execution: cloneExecution(record.execution)}, nil
		}
		return claimForStart(record, request, backend.store.trustedNow())
	}

	record = &executionRecord{
		execution: durable.Execution{
			ID:       request.ExecutionID,
			Spec:     cloneJSON(request.Spec),
			SpecHash: request.SpecHash,
			Status:   durable.ExecutionStatusRunning,
			Version:  1,
		},
		attempts: make(map[string][]durable.Attempt),
		claims:   make(map[durable.ClaimID]claimRecord),
		releases: make(map[leaseKey]durable.ReleaseResult),
		suspends: make(map[suspendKey]durable.Execution),
		finishes: make(map[finishKey]durable.Execution),
	}
	record.nextToken = 1
	record.execution.Lease = &durable.Lease{
		OwnerID:   request.OwnerID,
		ClaimID:   request.ClaimID,
		Token:     record.nextToken,
		ExpiresAt: backend.store.trustedNow().Add(request.LeaseTTL),
	}
	result := durable.StartResult{Execution: cloneExecution(record.execution), Created: true}
	record.claims[request.ClaimID] = claimRecord{
		mode:     "start",
		ownerID:  request.OwnerID,
		leaseTTL: request.LeaseTTL,
		specHash: request.SpecHash,
		token:    record.nextToken,
		start:    pointerTo(cloneStartResult(result)),
	}
	backend.store.executions[request.ExecutionID] = record
	return cloneStartResult(result), nil
}

func claimForStart(record *executionRecord, request durable.StartExecutionRequest, now time.Time) (durable.StartResult, error) {
	if prior, ok := record.claims[request.ClaimID]; ok {
		if prior.mode != "start" || prior.ownerID != request.OwnerID || prior.leaseTTL != request.LeaseTTL || prior.specHash != request.SpecHash {
			return durable.StartResult{}, executionError(request.ExecutionID, durable.ErrConflict)
		}
		if record.nextToken > prior.token {
			return durable.StartResult{}, executionError(request.ExecutionID, durable.ErrLeaseLost)
		}
		if leaseActive(record.execution.Lease, now) && record.execution.Lease.ClaimID == request.ClaimID && prior.start != nil {
			return cloneStartResult(*prior.start), nil
		}
	}
	if leaseActive(record.execution.Lease, now) {
		return durable.StartResult{}, executionError(request.ExecutionID, durable.ErrBusy)
	}
	reconcile := reconcileForClaim(record)
	record.nextToken++
	record.execution.Status = durable.ExecutionStatusRunning
	record.execution.Lease = &durable.Lease{
		OwnerID:   request.OwnerID,
		ClaimID:   request.ClaimID,
		Token:     record.nextToken,
		ExpiresAt: now.Add(request.LeaseTTL),
	}
	result := durable.StartResult{Execution: cloneExecution(record.execution), Reconcile: reconcile}
	record.claims[request.ClaimID] = claimRecord{
		mode:     "start",
		ownerID:  request.OwnerID,
		leaseTTL: request.LeaseTTL,
		specHash: request.SpecHash,
		token:    record.nextToken,
		start:    pointerTo(cloneStartResult(result)),
	}
	return cloneStartResult(result), nil
}

func (backend *Backend) ResumeExecution(ctx context.Context, request durable.ResumeExecutionRequest) (durable.ResumeResult, error) {
	if !validExecutionID(request.ExecutionID) || !validLeaseInput(request.OwnerID, request.ClaimID, request.LeaseTTL) {
		return durable.ResumeResult{}, contextOrValidation(ctx, executionError(request.ExecutionID, durable.ErrInvalidArgument))
	}
	var result durable.ResumeResult
	err := backend.withRecord(ctx, request.ExecutionID, "", func(record *executionRecord, now time.Time) error {
		if terminal(record.execution.Status) {
			result = durable.ResumeResult{Execution: cloneExecution(record.execution)}
			return nil
		}
		if prior, ok := record.claims[request.ClaimID]; ok {
			if prior.mode != "resume" || prior.ownerID != request.OwnerID || prior.leaseTTL != request.LeaseTTL {
				return executionError(request.ExecutionID, durable.ErrConflict)
			}
			if record.nextToken > prior.token {
				return executionError(request.ExecutionID, durable.ErrLeaseLost)
			}
			if leaseActive(record.execution.Lease, now) && record.execution.Lease.ClaimID == request.ClaimID && prior.resume != nil {
				result = cloneResumeResult(*prior.resume)
				return nil
			}
		}
		if leaseActive(record.execution.Lease, now) {
			return executionError(request.ExecutionID, durable.ErrBusy)
		}
		reconcile := reconcileForClaim(record)
		record.nextToken++
		record.execution.Status = durable.ExecutionStatusRunning
		record.execution.Lease = &durable.Lease{
			OwnerID:   request.OwnerID,
			ClaimID:   request.ClaimID,
			Token:     record.nextToken,
			ExpiresAt: now.Add(request.LeaseTTL),
		}
		result = durable.ResumeResult{Execution: cloneExecution(record.execution), Reconcile: reconcile}
		record.claims[request.ClaimID] = claimRecord{
			mode:     "resume",
			ownerID:  request.OwnerID,
			leaseTTL: request.LeaseTTL,
			token:    record.nextToken,
			resume:   pointerTo(cloneResumeResult(result)),
		}
		return nil
	})
	return cloneResumeResult(result), err
}

func (backend *Backend) LoadExecution(ctx context.Context, executionID durable.ExecutionID) (durable.Execution, error) {
	if !validExecutionID(executionID) {
		return durable.Execution{}, contextOrValidation(ctx, executionError(executionID, durable.ErrInvalidArgument))
	}
	var execution durable.Execution
	err := backend.withRecord(ctx, executionID, "", func(record *executionRecord, _ time.Time) error {
		if record.execution.Checkpoint != nil {
			if err := durable.ValidateCheckpoint(record.execution.ID, record.execution.SpecHash, *record.execution.Checkpoint); err != nil {
				return executionError(executionID, err)
			}
		}
		execution = cloneExecution(record.execution)
		return nil
	})
	return execution, err
}

func (backend *Backend) RenewExecution(ctx context.Context, request durable.RenewExecutionRequest) (durable.Lease, error) {
	if !validExecutionID(request.ExecutionID) || request.Lease.OwnerID == "" || request.Lease.Token == 0 || request.LeaseTTL <= 0 {
		return durable.Lease{}, contextOrValidation(ctx, executionError(request.ExecutionID, durable.ErrInvalidArgument))
	}
	var lease durable.Lease
	err := backend.withRecord(ctx, request.ExecutionID, "", func(record *executionRecord, now time.Time) error {
		if err := requireActiveLease(record, request.ExecutionID, request.Lease, now); err != nil {
			return err
		}
		record.execution.Lease.ExpiresAt = now.Add(request.LeaseTTL)
		lease = *record.execution.Lease
		return nil
	})
	return lease, err
}

func (backend *Backend) SaveCheckpoint(ctx context.Context, request durable.SaveCheckpointRequest) (durable.Execution, error) {
	if !validExecutionID(request.ExecutionID) || request.Lease.OwnerID == "" || request.Lease.Token == 0 {
		return durable.Execution{}, contextOrValidation(ctx, executionError(request.ExecutionID, durable.ErrInvalidArgument))
	}
	var execution durable.Execution
	err := backend.withRecord(ctx, request.ExecutionID, "", func(record *executionRecord, now time.Time) error {
		if err := requireActiveLease(record, request.ExecutionID, request.Lease, now); err != nil {
			return err
		}
		if err := durable.ValidateCheckpoint(record.execution.ID, record.execution.SpecHash, request.Checkpoint); err != nil {
			return executionError(request.ExecutionID, err)
		}
		if current := record.execution.Checkpoint; current != nil {
			if current.Sequence == request.Checkpoint.Sequence {
				if current.ContinuationHash != request.Checkpoint.ContinuationHash {
					return executionError(request.ExecutionID, durable.ErrConflict)
				}
				execution = cloneExecution(record.execution)
				return nil
			}
			if request.Checkpoint.Sequence < current.Sequence {
				return executionError(request.ExecutionID, durable.ErrConflict)
			}
		}
		if record.execution.Version != request.ExpectedVersion {
			return executionError(request.ExecutionID, durable.ErrConflict)
		}
		checkpoint := cloneJSON(request.Checkpoint)
		record.execution.Checkpoint = &checkpoint
		record.execution.Version++
		execution = cloneExecution(record.execution)
		return nil
	})
	return execution, err
}

func (backend *Backend) SuspendExecution(ctx context.Context, request durable.SuspendExecutionRequest) (durable.Execution, error) {
	if !validExecutionID(request.ExecutionID) || request.Lease.OwnerID == "" || request.Lease.Token == 0 {
		return durable.Execution{}, contextOrValidation(ctx, executionError(request.ExecutionID, durable.ErrInvalidArgument))
	}
	var execution durable.Execution
	err := backend.withRecord(ctx, request.ExecutionID, "", func(record *executionRecord, now time.Time) error {
		if record.nextToken > request.Lease.Token {
			return executionError(request.ExecutionID, durable.ErrLeaseLost)
		}
		key := suspendKey{lease: leaseKey{ownerID: request.Lease.OwnerID, token: request.Lease.Token}, expectedVersion: request.ExpectedVersion}
		if prior, ok := record.suspends[key]; ok {
			execution = cloneExecution(prior)
			return nil
		}
		if err := requireActiveLease(record, request.ExecutionID, request.Lease, now); err != nil {
			return err
		}
		if record.execution.Version != request.ExpectedVersion {
			return executionError(request.ExecutionID, durable.ErrConflict)
		}
		reconcileRunning(record, request.Lease.Token)
		record.execution.Status = durable.ExecutionStatusSuspended
		record.execution.Lease = nil
		record.execution.Version++
		execution = cloneExecution(record.execution)
		record.suspends[key] = cloneExecution(execution)
		return nil
	})
	return execution, err
}

func (backend *Backend) FinishExecution(ctx context.Context, request durable.FinishExecutionRequest) (durable.Execution, error) {
	if !validExecutionID(request.ExecutionID) || request.Lease.OwnerID == "" || request.Lease.Token == 0 {
		return durable.Execution{}, contextOrValidation(ctx, executionError(request.ExecutionID, durable.ErrInvalidArgument))
	}
	var execution durable.Execution
	err := backend.withRecord(ctx, request.ExecutionID, "", func(record *executionRecord, now time.Time) error {
		hash, hashErr := durable.HashResult(record.execution.ID, record.execution.SpecHash, request.ExpectedVersion+1, request.Result)
		if hashErr != nil || hash != request.ResultHash {
			return executionError(request.ExecutionID, durable.ErrConflict)
		}
		if record.nextToken > request.Lease.Token {
			return executionError(request.ExecutionID, durable.ErrLeaseLost)
		}
		key := finishKey{
			lease:           leaseKey{ownerID: request.Lease.OwnerID, token: request.Lease.Token},
			expectedVersion: request.ExpectedVersion,
			resultHash:      request.ResultHash,
		}
		if prior, ok := record.finishes[key]; ok {
			execution = cloneExecution(prior)
			return nil
		}
		if terminal(record.execution.Status) {
			return executionError(request.ExecutionID, durable.ErrConflict)
		}
		if err := requireActiveLease(record, request.ExecutionID, request.Lease, now); err != nil {
			return err
		}
		if record.execution.Version != request.ExpectedVersion {
			return executionError(request.ExecutionID, durable.ErrConflict)
		}
		if hasUncertainAttempts(record) {
			return executionError(request.ExecutionID, durable.ErrReconcileRequired)
		}
		result := cloneJSON(request.Result)
		record.execution.Result = &result
		record.execution.ResultHash = request.ResultHash
		record.execution.Status = durable.ExecutionStatusCompleted
		if request.Result.Failure != nil {
			record.execution.Status = durable.ExecutionStatusFailed
		}
		record.execution.Lease = nil
		record.execution.Version++
		execution = cloneExecution(record.execution)
		record.finishes[key] = cloneExecution(execution)
		return nil
	})
	return execution, err
}

func (backend *Backend) ReleaseExecution(ctx context.Context, request durable.ReleaseExecutionRequest) (durable.ReleaseResult, error) {
	if !validExecutionID(request.ExecutionID) || request.Lease.OwnerID == "" || request.Lease.Token == 0 {
		return durable.ReleaseResult{}, contextOrValidation(ctx, executionError(request.ExecutionID, durable.ErrInvalidArgument))
	}
	var result durable.ReleaseResult
	err := backend.withRecord(ctx, request.ExecutionID, "", func(record *executionRecord, _ time.Time) error {
		if record.nextToken > request.Lease.Token {
			return executionError(request.ExecutionID, durable.ErrLeaseLost)
		}
		key := leaseKey{ownerID: request.Lease.OwnerID, token: request.Lease.Token}
		if prior, ok := record.releases[key]; ok {
			result = cloneReleaseResult(prior)
			return nil
		}
		if !leaseMatches(record.execution.Lease, request.Lease) {
			return executionError(request.ExecutionID, durable.ErrLeaseLost)
		}
		reconcile := reconcileRunning(record, request.Lease.Token)
		record.execution.Lease = nil
		result = durable.ReleaseResult{Execution: cloneExecution(record.execution), Reconcile: reconcile}
		record.releases[key] = cloneReleaseResult(result)
		return nil
	})
	return cloneReleaseResult(result), err
}

func hasUncertainAttempts(record *executionRecord) bool {
	for _, attempts := range record.attempts {
		for _, attempt := range attempts {
			if attempt.Status == durable.AttemptStatusRunning || attempt.Status == durable.AttemptStatusUnknown {
				return true
			}
		}
	}
	return false
}

func pointerTo[T any](value T) *T { return &value }
