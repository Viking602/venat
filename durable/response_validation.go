package durable

import "fmt"

func validateAttemptStart(request StartAttemptRequest, started AttemptStart) error {
	attempt := started.Attempt
	if attempt.ExecutionID != request.ExecutionID || attempt.OperationID != request.OperationID ||
		attempt.Kind != request.Kind || attempt.InputHash != request.InputHash || attempt.Number <= 0 || attempt.Version == 0 {
		return attemptRuntimeError(request.ExecutionID, request.OperationID, attempt.Number, fmt.Errorf("%w: attempt response does not match request identity", ErrConflict))
	}
	if !validAttemptDecision(started, request.Lease) {
		return attemptRuntimeError(request.ExecutionID, request.OperationID, attempt.Number, fmt.Errorf("%w: invalid attempt decision or state", ErrConflict))
	}
	return nil
}

func validAttemptDecision(started AttemptStart, lease LeaseRef) bool {
	attempt := started.Attempt
	switch started.Decision {
	case AttemptDecisionExecute:
		return attempt.Status == AttemptStatusRunning && attempt.Lease != nil && *attempt.Lease == lease && len(attempt.Payload) == 0 && attempt.Failure == nil
	case AttemptDecisionReplay:
		return attempt.Lease == nil && ((attempt.Status == AttemptStatusSucceeded && attempt.Failure == nil) || (attempt.Status == AttemptStatusFailed && attempt.Failure != nil))
	case AttemptDecisionReconcile:
		return attempt.Status == AttemptStatusUnknown && attempt.Lease == nil
	default:
		return false
	}
}

func validateFinishedExecution(before, finished Execution, expectedHash [32]byte) error {
	if before.Version == ^uint64(0) || finished.ID != before.ID || finished.SpecHash != before.SpecHash ||
		finished.Version != before.Version+1 || finished.Lease != nil || finished.Result == nil || finished.ResultHash != expectedHash {
		return claimedExecutionConflict(before.ID, "invalid finish execution response")
	}
	specHash, err := HashExecutionSpec(finished.Spec)
	if err != nil || specHash != before.SpecHash {
		return claimedExecutionConflict(before.ID, "finished execution spec mismatch")
	}
	if err := validateClaimedExecutionStatus(finished, before.ID, ""); err != nil {
		return err
	}
	return validateExecutionCheckpoint(finished, before.ID)
}
