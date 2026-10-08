package durable

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/Viking602/venat/agent"
)

// HashExecutionSpec returns the canonical JSON SHA-256 of spec.
func HashExecutionSpec(spec ExecutionSpec) ([32]byte, error) {
	return canonicalJSONHash(spec)
}

// HashContinuation binds canonical continuation bytes to their execution,
// immutable spec and checkpoint sequence in a domain-separated envelope.
func HashContinuation(executionID ExecutionID, specHash [32]byte, sequence uint64, continuation agent.Continuation) ([32]byte, error) {
	if executionID == "" || specHash == ([32]byte{}) || sequence == 0 {
		return [32]byte{}, ErrInvalidArgument
	}
	encoded, err := agent.EncodeContinuation(continuation)
	if err != nil {
		return [32]byte{}, fmt.Errorf("encode continuation: %w", err)
	}
	return canonicalJSONHash(struct {
		Domain       string          `json:"domain"`
		ExecutionID  ExecutionID     `json:"executionId"`
		SpecHash     [32]byte        `json:"specHash"`
		Sequence     uint64          `json:"sequence"`
		Continuation json.RawMessage `json:"continuation"`
	}{"venat.durable.checkpoint.v2", executionID, specHash, sequence, encoded})
}

// ValidateCheckpoint verifies sequence, continuation integrity, schema version,
// and canonical hash bound to the expected execution and immutable spec.
func ValidateCheckpoint(executionID ExecutionID, specHash [32]byte, checkpoint Checkpoint) error {
	if checkpoint.Sequence == 0 {
		return fmt.Errorf("%w: checkpoint sequence is zero", ErrCorruptCheckpoint)
	}
	hash, err := HashContinuation(executionID, specHash, checkpoint.Sequence, checkpoint.Continuation)
	if err != nil {
		return errors.Join(ErrCorruptCheckpoint, err)
	}
	if hash != checkpoint.ContinuationHash {
		return fmt.Errorf("%w: continuation hash mismatch", ErrCorruptCheckpoint)
	}
	return nil
}

// HashResult binds a result to its execution, immutable spec, terminal status
// and committed execution version. Use ExpectedVersion+1 when finishing.
func HashResult(executionID ExecutionID, specHash [32]byte, version uint64, result agent.Result) ([32]byte, error) {
	if executionID == "" || specHash == ([32]byte{}) || version == 0 {
		return [32]byte{}, ErrInvalidArgument
	}
	return canonicalJSONHash(struct {
		Domain      string          `json:"domain"`
		ExecutionID ExecutionID     `json:"executionId"`
		SpecHash    [32]byte        `json:"specHash"`
		Status      ExecutionStatus `json:"status"`
		Version     uint64          `json:"version"`
		Result      agent.Result    `json:"result"`
	}{"venat.durable.result.v2", executionID, specHash, resultStatus(result), version, result})
}

func resultStatus(result agent.Result) ExecutionStatus {
	if result.Failure != nil {
		return ExecutionStatusFailed
	}
	return ExecutionStatusCompleted
}

func canonicalJSONHash(value any) ([32]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return [32]byte{}, fmt.Errorf("canonical JSON encode: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return [32]byte{}, fmt.Errorf("canonical JSON decode: %w", err)
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return [32]byte{}, err
	}
	canonical, err := json.Marshal(document)
	if err != nil {
		return [32]byte{}, fmt.Errorf("canonical JSON normalize: %w", err)
	}
	return sha256.Sum256(canonical), nil
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("canonical JSON contains trailing data")
		}
		return fmt.Errorf("canonical JSON trailing data: %w", err)
	}
	return nil
}
