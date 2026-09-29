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
	// errControlSteer is only the cause of a turn-local cancellation. It must
	// never cancel the execution context or permanently close Control.
	errControlSteer = errors.New("agent model turn interrupted by steering")
)

const maxControlQueue = 64

type queuedInput struct {
	message message.Message
	ack     chan error
	size    int
	seq     uint64
}

type controlTurnState struct {
	cancel  context.CancelCauseFunc
	started func() bool
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
	steers    []queuedInput
	followups []queuedInput
	inflight  []queuedInput
	bytes     int
	nextSeq   uint64
	turn      controlTurnState
}

// Send queues authorized user input for the next safe model boundary. A nil
// immediate error means queued, not consumed. The returned buffered channel
// yields exactly one result: nil after boundary observers succeed, or an error
// if the execution cannot consume it. A failed persistence acknowledgement can
// be ambiguous; inspect durable state before resending. Send never accepts a
// budget change, raw tool-result block, or system instruction. Applications
// forwarding worker evidence must label it as untrusted data; the collaboration
// completion hook supplies that labeling and tracks consumption receipts.
func (control *Control) Send(input Request) (<-chan error, error) {
	if control == nil {
		return nil, errControlClosed
	}
	return control.enqueue(input, &control.pending)
}

// Steer queues an authorized direction correction and promptly interrupts a
// safe active model turn. Send and Steer retain admission order when consumed
// together, so an older user message is never reordered after a newer
// correction. If a model stream is active and no streaming tool has begun, the
// stream is interrupted with a turn-local cancellation; the execution remains
// alive and consumes the correction on its next safe boundary. If early tool
// overlap has already started, interruption is deferred so its effect/result
// pairing remains factual; unstarted calls are skipped before dispatch.
func (control *Control) Steer(input Request) (<-chan error, error) {
	if control == nil {
		return nil, errControlClosed
	}
	ack, err := control.enqueue(input, &control.steers)
	if err != nil {
		return nil, err
	}
	control.mu.Lock()
	control.interruptTurnLocked()
	control.mu.Unlock()
	return ack, nil
}

// FollowUp queues authorized work for a subsequent model turn. Unlike Steer it
// never interrupts the current model or tool loop; it is consumed only after
// that work reaches a natural model boundary.
func (control *Control) FollowUp(input Request) (<-chan error, error) {
	if control == nil {
		return nil, errControlClosed
	}
	return control.enqueue(input, &control.followups)
}

func (control *Control) enqueue(input Request, queue *[]queuedInput) (<-chan error, error) {
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
	if !control.accepting || control.ctx == nil || control.ctx.Err() != nil {
		return nil, errControlClosed
	}
	if control.queueLenLocked() >= maxControlQueue || len(encoded) > 1<<20 || control.bytes+len(encoded) > 4<<20 {
		return nil, errControlLimit
	}
	ack := make(chan error, 1)
	control.nextSeq++
	*queue = append(*queue, queuedInput{message: current, ack: ack, size: len(encoded), seq: control.nextSeq})
	control.bytes += len(encoded)
	return ack, nil
}

func (control *Control) queueLenLocked() int {
	return len(control.pending) + len(control.steers) + len(control.followups) + len(control.inflight)
}

func (control *Control) interruptTurnLocked() {
	if control.turn.cancel == nil || control.turn.started != nil && control.turn.started() {
		return
	}
	control.turn.cancel(errControlSteer)
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
	if control.turn.cancel != nil {
		control.turn.cancel(context.Canceled)
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
	if control.cancel != nil {
		control.cancel()
	}
	if control.turn.cancel != nil {
		control.turn.cancel(context.Canceled)
		control.turn = controlTurnState{}
	}
	all := make([]queuedInput, 0, control.queueLenLocked())
	all = append(all, control.inflight...)
	all = append(all, control.steers...)
	all = append(all, control.pending...)
	all = append(all, control.followups...)
	for _, pending := range all {
		pending.ack <- errControlClosed
		close(pending.ack)
	}
	control.pending, control.steers, control.followups, control.inflight = nil, nil, nil, nil
	control.bytes = 0
}

// beginTurn installs a child context for one model stream. Steering cancels
// this child only while no early tool has started. The returned cleanup clears
// the registration even when provider collection fails.
func (control *Control) beginTurn(ctx context.Context, started func() bool) (context.Context, func()) {
	if control == nil {
		return ctx, func() {}
	}
	turnCtx, cancel := context.WithCancelCause(ctx)
	control.mu.Lock()
	control.turn = controlTurnState{cancel: cancel, started: started}
	if len(control.steers) > 0 {
		control.interruptTurnLocked()
	}
	control.mu.Unlock()
	return turnCtx, func() {
		control.mu.Lock()
		control.turn = controlTurnState{}
		control.mu.Unlock()
		cancel(nil)
	}
}

func (control *Control) take() []message.Message {
	if control == nil {
		return nil
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	merged := mergeQueued(control.steers, control.pending)
	count := len(merged)
	control.inflight = append(control.inflight, merged...)
	inputs := make([]message.Message, 0, count)
	for _, input := range merged {
		inputs = append(inputs, message.Clone(input.message))
	}
	control.steers, control.pending = nil, nil
	return inputs
}

func mergeQueued(first, second []queuedInput) []queuedInput {
	merged := make([]queuedInput, 0, len(first)+len(second))
	for left, right := 0, 0; left < len(first) || right < len(second); {
		switch {
		case right == len(second) || left < len(first) && first[left].seq < second[right].seq:
			merged = append(merged, first[left])
			left++
		default:
			merged = append(merged, second[right])
			right++
		}
	}
	return merged
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

// hasSteer reports a correction still waiting to be admitted at a model
// boundary. Once taken, it is already part of the next request and must not
// cause that request's tool calls to be skipped.
func (control *Control) hasSteer() bool {
	if control == nil {
		return false
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	return len(control.steers) > 0
}

type controlDispatchContextKey struct{}

func withControlDispatch(ctx context.Context, control *Control) context.Context {
	if control == nil {
		return ctx
	}
	return context.WithValue(ctx, controlDispatchContextKey{}, control)
}

func controlFromDispatch(ctx context.Context) *Control {
	if ctx == nil {
		return nil
	}
	control, _ := ctx.Value(controlDispatchContextKey{}).(*Control)
	return control
}

func (control *Control) shouldSkipTool() bool {
	if control == nil {
		return false
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	return len(control.steers) > 0
}

// Terminal admission and Send use the same lock. Inputs accepted before this
// decision force another model step; later sends are rejected immediately.
func (control *Control) continueOrSeal() bool {
	if control == nil {
		return false
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if len(control.pending)+len(control.steers) > 0 {
		return true
	}
	if len(control.followups) > 0 {
		control.pending = append(control.pending, control.followups...)
		control.followups = nil
		return true
	}
	control.accepting = false
	return false
}

// Recovery may replace the overlap executor. Publish its stable predicate under
// the same lock as Steer, rather than closing over a concurrently reassigned pointer.
func (control *Control) updateTurnStarted(started func() bool) {
	if control == nil {
		return
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.turn.cancel != nil {
		control.turn.started = started
		if len(control.steers) > 0 {
			control.interruptTurnLocked()
		}
	}
}
