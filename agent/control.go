package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/Viking602/venat/message"
)

var (
	errControlClosed = errors.New("agent control is not accepting input")
	errControlUsed   = errors.New("agent control already belongs to an execution")
	// Fixed limits bound queued user input; applications own admission policy.
	errControlLimit = errors.New("agent input queue limit exceeded")
)

type queuedInput struct {
	message message.Message
	ack     chan error
	size    int
}

// Control belongs to exactly one Engine execution. Its zero value is ready to
// attach to Engine.Control or LoopInput.Control. Never reuse a completed handle.
// Queue contents are process-local; only acknowledged input has crossed the
// Engine's boundary observers and entered its execution transcript.
type Control struct {
	mu        sync.Mutex
	started   bool
	accepting bool
	cancel    context.CancelFunc
	ctx       context.Context
	pending   []queuedInput
	inflight  []queuedInput
	bytes     int
}

// Send queues authorized user input for the next safe model boundary. A nil
// immediate error means queued, not consumed. The returned buffered channel
// yields exactly one result: nil after boundary observers succeed, or an error
// if the execution cannot consume it. A failed persistence acknowledgement can
// be ambiguous; inspect durable state before resending. Send never accepts a
// budget change, tool output, peer-agent message, or system instruction.
func (control *Control) Send(input Request) (<-chan error, error) {
	if control == nil {
		return nil, errControlClosed
	}
	if input.Budget != nil || (strings.TrimSpace(input.Prompt) == "" && len(input.Content) == 0) {
		return nil, errors.New("agent input requires user content and cannot change the budget")
	}
	current, err := requestMessage(input)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(current)
	if err != nil {
		return nil, err
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if !control.accepting || control.ctx.Err() != nil {
		return nil, errControlClosed
	}
	if len(control.pending)+len(control.inflight) >= 64 || len(encoded) > 1<<20 || control.bytes+len(encoded) > 4<<20 {
		return nil, errControlLimit
	}
	ack := make(chan error, 1)
	control.pending = append(control.pending, queuedInput{message: current, ack: ack, size: len(encoded)})
	control.bytes += len(encoded)
	return ack, nil
}

// Cancel interrupts the execution's context, including its current model/tool
// call. Cancellation does not prove that an external effect never started.
func (control *Control) Cancel() {
	if control == nil {
		return
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	control.accepting = false
	if control.cancel != nil {
		control.cancel()
	}
}

func (control *Control) start(ctx context.Context) (context.Context, func(), error) {
	if control == nil {
		return ctx, func() {}, nil
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.started {
		return ctx, func() {}, errControlUsed
	}
	control.started = true
	control.accepting = true
	control.ctx, control.cancel = context.WithCancel(ctx)
	return control.ctx, control.finish, nil
}

func (control *Control) finish() {
	control.mu.Lock()
	defer control.mu.Unlock()
	control.accepting = false
	control.cancel()
	for _, pending := range append(control.inflight, control.pending...) {
		pending.ack <- errControlClosed
		close(pending.ack)
	}
	control.pending, control.inflight = nil, nil
	control.bytes = 0
}

func (control *Control) take() []message.Message {
	if control == nil {
		return nil
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	control.inflight = append(control.inflight, control.pending...)
	inputs := make([]message.Message, 0, len(control.pending))
	for _, input := range control.pending {
		inputs = append(inputs, message.Clone(input.message))
	}
	control.pending = nil
	return inputs
}

func (control *Control) acknowledge(err error) {
	if control == nil {
		return
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	for _, input := range control.inflight {
		control.bytes -= input.size
		if err != nil {
			input.ack <- fmt.Errorf("agent input boundary: %w", err)
		} else {
			input.ack <- nil
		}
		close(input.ack)
	}
	control.inflight = nil
}

func validatePendingInput(history, pending []message.Message) error {
	if len(pending) == 0 {
		return nil
	}
	if len(history) < len(pending) || !reflect.DeepEqual(history[len(history)-len(pending):], pending) {
		return errors.New("agent compaction removed or changed pending user input")
	}
	return nil
}

// Terminal admission and Send use the same lock. Inputs accepted before this
// decision force another model step; later sends are rejected immediately.
func (control *Control) continueOrSeal() bool {
	if control == nil {
		return false
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if len(control.pending) > 0 {
		return true
	}
	control.accepting = false
	return false
}
