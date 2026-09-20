// Package orchestration provides policy-free scheduling and bounded mechanical
// execution for Agent dispatches.
package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Viking602/venat/agent"
)

var (
	// ErrInvalidArgument reports an invalid orchestration contract value.
	ErrInvalidArgument = errors.New("orchestration: invalid argument")
	// ErrMaxTicks reports that a scheduler produced work beyond this Drive call's
	// tick budget.
	ErrMaxTicks = errors.New("orchestration: maximum ticks reached")
	// ErrSchedulerPanic reports a panic contained at the Scheduler boundary.
	ErrSchedulerPanic = errors.New("orchestration: scheduler panic")
	// ErrExecutorPanic reports a panic contained at the Executor boundary.
	ErrExecutorPanic = errors.New("orchestration: executor panic")
	// ErrMaxWallClock reports that one Drive invocation exceeded its wall-clock
	// envelope.
	ErrMaxWallClock = errors.New("orchestration: maximum wall clock reached")
	// ErrDispatchTimeout reports one dispatch that exceeded its local deadline.
	ErrDispatchTimeout = errors.New("orchestration: dispatch timeout")
)

// Scheduler is a pure scheduling function over the supplied State snapshot.
type Scheduler interface {
	Next(context.Context, State) ([]Dispatch, error)
}

// SchedulerFunc adapts a function to Scheduler.
type SchedulerFunc func(context.Context, State) ([]Dispatch, error)

// Next delegates to f.
func (f SchedulerFunc) Next(ctx context.Context, state State) ([]Dispatch, error) {
	return f(ctx, state)
}

// Handoff carries application-owned routing context. From and To are opaque.
type Handoff struct {
	From    string          `json:"from,omitempty"`
	To      string          `json:"to,omitempty"`
	Reason  string          `json:"reason,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Dispatch is one stable, opaque-routed Agent execution request.
type Dispatch struct {
	ID           string             `json:"id"`
	Route        string             `json:"route"`
	Request      agent.Request      `json:"request"`
	OutputPolicy agent.OutputPolicy `json:"outputPolicy"`
	Handoff      *Handoff           `json:"handoff,omitempty"`
	Metadata     map[string]string  `json:"metadata,omitempty"`
}

// ValidateDispatch validates the mechanical dispatch contract without
// interpreting Route, Handoff, or Metadata.
func ValidateDispatch(dispatch Dispatch) error {
	if err := dispatch.Request.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	if err := agent.ValidateOutputPolicy(dispatch.OutputPolicy); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	if strings.TrimSpace(dispatch.ID) == "" {
		return fmt.Errorf("%w: dispatch ID is empty", ErrInvalidArgument)
	}
	if strings.TrimSpace(dispatch.Route) == "" {
		return fmt.Errorf("%w: dispatch %q route is empty", ErrInvalidArgument, dispatch.ID)
	}
	if len(dispatch.OutputPolicy.Schema) > 0 && !json.Valid(dispatch.OutputPolicy.Schema) {
		return fmt.Errorf("%w: dispatch %q output schema is invalid JSON", ErrInvalidArgument, dispatch.ID)
	}
	if dispatch.Handoff == nil {
		return nil
	}
	if len(dispatch.Handoff.Payload) > 0 && !json.Valid(dispatch.Handoff.Payload) {
		return fmt.Errorf("%w: dispatch %q handoff payload is invalid JSON", ErrInvalidArgument, dispatch.ID)
	}
	if dispatch.Handoff.To != "" && dispatch.Handoff.To != dispatch.Route {
		return fmt.Errorf("%w: dispatch %q handoff target %q conflicts with route %q", ErrInvalidArgument, dispatch.ID, dispatch.Handoff.To, dispatch.Route)
	}
	return nil
}

// Outcome is the complete mechanical fact folded after one Dispatch.
type Outcome struct {
	Tick     int          `json:"tick"`
	Dispatch Dispatch     `json:"dispatch"`
	Result   agent.Result `json:"result"`
}

// State is the deterministic Scheduler input and Drive output.
type State struct {
	Tick     int       `json:"tick"`
	Outcomes []Outcome `json:"outcomes,omitempty"`
}

// Executor executes one Dispatch. Agent failures remain Result data; error is
// reserved for infrastructure failure.
type Executor interface {
	Execute(context.Context, Dispatch, agent.Sink) (agent.Result, error)
}

// ExecutorFunc adapts a function to Executor.
type ExecutorFunc func(context.Context, Dispatch, agent.Sink) (agent.Result, error)

// Execute delegates to f.
func (f ExecutorFunc) Execute(ctx context.Context, dispatch Dispatch, sink agent.Sink) (agent.Result, error) {
	return f(ctx, dispatch, sink)
}

// SchedulerError attributes a Scheduler error or panic to the State tick it
// observed.
type SchedulerError struct {
	Tick int
	Err  error
}

func (failure *SchedulerError) Error() string {
	if failure == nil {
		return "orchestration: scheduler failed"
	}
	return fmt.Sprintf("orchestration: scheduler failed at tick %d: %v", failure.Tick, failure.Err)
}

func (failure *SchedulerError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.Err
}

// DispatchError preserves the Dispatch and partial Result associated with one
// infrastructure failure.
type DispatchError struct {
	Dispatch Dispatch
	Result   agent.Result
	Err      error
}

func (failure *DispatchError) Error() string {
	if failure == nil {
		return "orchestration: dispatch failed"
	}
	return fmt.Sprintf("orchestration: dispatch %q failed: %v", failure.Dispatch.ID, failure.Err)
}

func (failure *DispatchError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.Err
}
