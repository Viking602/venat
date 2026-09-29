package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/message"
)

var (
	ErrRuntimeClosed         = errors.New("orchestration: runtime closed")
	ErrTaskNotFound          = errors.New("orchestration: task not found")
	ErrQueueFull             = errors.New("orchestration: task queue is full")
	ErrTaskBusy              = errors.New("orchestration: task is already queued")
	ErrTaskComplete          = errors.New("orchestration: task is complete")
	ErrCompletionCursorStale = errors.New("orchestration: completion cursor is stale")
)

// CompletionCursorError reports that a completion journal cursor predates the
// retained journal window. Callers must resynchronize explicitly; DrainCompletions
// never silently skips discarded completions.
type CompletionCursorError struct {
	Cursor   uint64
	Earliest uint64
}

func (err *CompletionCursorError) Error() string {
	return fmt.Sprintf("%v: cursor=%d earliest=%d", ErrCompletionCursorStale, err.Cursor, err.Earliest)
}

func (err *CompletionCursorError) Is(target error) bool {
	return target == ErrCompletionCursorStale
}

// TaskStatus describes the independently scheduled lifetime of one worker.
type TaskStatus string

const (
	TaskQueued   TaskStatus = "queued"
	TaskRunning  TaskStatus = "running"
	TaskIdle     TaskStatus = "idle"
	TaskCanceled TaskStatus = "canceled"
)

// TaskRequest configures one worker turn. Engine is used when Factory is nil.
type TaskRequest struct {
	ID           string
	Request      agent.Request
	OutputPolicy agent.OutputPolicy
	Engine       agent.Engine
}

// EngineFactory creates an independent worker engine for a task. The runtime
// installs a fresh Control before each turn, so a factory may safely return a
// configured engine value shared by no execution.
type EngineFactory func(TaskRequest) (agent.Engine, error)

// RuntimeOptions bounds the in-memory scheduler. QueueSize bounds accepted
// queued turns in addition to workers currently executing.
type RuntimeOptions struct {
	MaxConcurrency int
	QueueSize      int
	Factory        EngineFactory
}

// ProgressSnapshot is the latest transient frame for a task. Runtime retains
// only this bounded snapshot; full output remains in TaskResult.
type ProgressSnapshot struct {
	Sequence uint64
	Kind     agent.FrameKind
	Text     string
	ToolName string
	Done     bool
	Updated  time.Time
}

// TaskResult is one typed worker-turn completion. Agent failures remain in
// Result; Err is reserved for runtime/worker infrastructure and cancellation.
type TaskResult struct {
	ID        string
	Turn      uint64
	Sequence  uint64
	Result    agent.Result
	Err       error
	Started   time.Time
	Completed time.Time
}

// TaskCompletion is delivered once per completed turn on Runtime.Completions.
type TaskCompletion = TaskResult

// TaskSnapshot is a point-in-time, ownership-independent view of a worker.
type TaskSnapshot struct {
	ID        string
	Turn      uint64
	Status    TaskStatus
	Progress  ProgressSnapshot
	Result    TaskResult
	HasResult bool
	Created   time.Time
	Updated   time.Time
}

type taskState struct {
	runtime *Runtime
	id      string
	spec    TaskRequest

	mu           sync.Mutex
	status       TaskStatus
	turn         uint64
	turnDone     chan struct{}
	turnResult   *TaskResult
	result       TaskResult
	hasResult    bool
	resultTurn   uint64
	created      time.Time
	updated      time.Time
	progress     ProgressSnapshot
	control      *agent.Control
	continuation *agent.Continuation
	pending      *agent.Request
	cancel       context.CancelFunc
}

// TaskHandle is a stable reference to one runtime task.
type TaskHandle struct {
	runtime *Runtime
	id      string
}

func (h TaskHandle) ID() string { return h.id }

func (h TaskHandle) Snapshot() (TaskSnapshot, error) {
	if h.runtime == nil {
		return TaskSnapshot{}, ErrTaskNotFound
	}
	return h.runtime.Inspect(h.id)
}

// Await waits for the currently scheduled turn. If a follow-up is scheduled,
// a later Await observes that new turn rather than the already delivered one.
func (h TaskHandle) Await(ctx context.Context) (TaskResult, error) {
	if h.runtime == nil {
		return TaskResult{}, ErrTaskNotFound
	}
	return h.runtime.await(ctx, h.id)
}

func (h TaskHandle) Cancel() error {
	if h.runtime == nil {
		return ErrTaskNotFound
	}
	return h.runtime.Cancel(h.id)
}

func (h TaskHandle) Send(ctx context.Context, request agent.Request) error {
	if h.runtime == nil {
		return ErrTaskNotFound
	}
	return h.runtime.SendFollowup(ctx, h.id, request)
}

// Runtime owns all worker goroutines and their lifetime. It has no durable
// backend; callers must retain handles or drain completions before Close.
type Runtime struct {
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan *taskState

	mu     sync.RWMutex
	tasks  map[string]*taskState
	closed bool

	options  RuntimeOptions
	wg       sync.WaitGroup
	once     sync.Once
	complete chan TaskCompletion

	inboxMu       sync.Mutex
	inbox         []TaskCompletion
	completionSeq uint64
	inboxCapacity int

	stateGeneration chan struct{}
}

func NewRuntime(ctx context.Context, options RuntimeOptions) (*Runtime, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if options.MaxConcurrency < 0 || options.QueueSize < 0 {
		return nil, fmt.Errorf("%w: negative runtime capacity", ErrInvalidArgument)
	}
	if options.MaxConcurrency == 0 {
		options.MaxConcurrency = 4
	}
	if options.QueueSize == 0 {
		options.QueueSize = options.MaxConcurrency * 2
	}
	runtimeCtx, cancel := context.WithCancel(ctx)
	inboxCapacity := options.QueueSize + options.MaxConcurrency
	if inboxCapacity < 64 {
		inboxCapacity = 64
	}
	runtime := &Runtime{
		ctx: runtimeCtx, cancel: cancel, queue: make(chan *taskState, options.QueueSize),
		tasks: make(map[string]*taskState), options: options,
		complete:        make(chan TaskCompletion, options.QueueSize+options.MaxConcurrency),
		inboxCapacity:   inboxCapacity,
		stateGeneration: make(chan struct{}),
	}
	runtime.inbox = make([]TaskCompletion, 0, runtime.inboxCapacity)
	for index := 0; index < options.MaxConcurrency; index++ {
		runtime.wg.Add(1)
		go runtime.worker()
	}
	return runtime, nil
}

// Completions returns bounded advisory typed notifications. Notifications may
// be dropped when full; DrainCompletions is the authoritative cursor API.
func (runtime *Runtime) Completions() <-chan TaskCompletion {
	if runtime == nil {
		return nil
	}
	return runtime.complete
}

// DrainCompletions returns every retained completion after cursor and the
// newest cursor. The journal retains max(64, QueueSize+MaxConcurrency)
// completions. A cursor older than that window returns CompletionCursorError;
// callers must explicitly resynchronize rather than silently losing entries.
// Each consumer owns its cursor; draining does not consume another consumer's
// entries.
func (runtime *Runtime) DrainCompletions(cursor uint64) ([]TaskCompletion, uint64, error) {
	if runtime == nil {
		return nil, cursor, ErrRuntimeClosed
	}
	runtime.inboxMu.Lock()
	defer runtime.inboxMu.Unlock()
	if len(runtime.inbox) == 0 {
		return nil, cursor, nil
	}
	earliest := runtime.inbox[0].Sequence
	if cursor < earliest-1 {
		return nil, cursor, &CompletionCursorError{Cursor: cursor, Earliest: earliest}
	}
	completions := make([]TaskCompletion, 0)
	next := cursor
	for _, completion := range runtime.inbox {
		if completion.Sequence <= cursor {
			continue
		}
		completions = append(completions, cloneTaskResult(completion))
		if completion.Sequence > next {
			next = completion.Sequence
		}
	}
	return completions, next, nil
}

func (runtime *Runtime) taskIDs() []string {
	runtime.mu.RLock()
	ids := make([]string, 0, len(runtime.tasks))
	for id := range runtime.tasks {
		ids = append(ids, id)
	}
	runtime.mu.RUnlock()
	return ids
}

func (runtime *Runtime) Spawn(request TaskRequest) (TaskHandle, error) {
	if runtime == nil {
		return TaskHandle{}, ErrRuntimeClosed
	}
	if strings.TrimSpace(request.ID) == "" {
		return TaskHandle{}, fmt.Errorf("%w: task ID is empty", ErrInvalidArgument)
	}
	if err := request.Request.Validate(); err != nil {
		return TaskHandle{}, fmt.Errorf("%w: task %q request: %v", ErrInvalidArgument, request.ID, err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return TaskHandle{}, ErrRuntimeClosed
	}
	if _, exists := runtime.tasks[request.ID]; exists {
		return TaskHandle{}, fmt.Errorf("%w: duplicate task %q", ErrInvalidArgument, request.ID)
	}
	state := &taskState{runtime: runtime, id: request.ID, spec: cloneTaskRequest(request), status: TaskQueued, turn: 1, turnDone: make(chan struct{}), turnResult: new(TaskResult), created: time.Now()}
	select {
	case runtime.queue <- state:
		runtime.tasks[state.id] = state
		return TaskHandle{runtime: runtime, id: state.id}, nil
	default:
		return TaskHandle{}, ErrQueueFull
	}
}

func (runtime *Runtime) Inspect(id string) (TaskSnapshot, error) {
	if runtime == nil {
		return TaskSnapshot{}, ErrRuntimeClosed
	}
	runtime.mu.RLock()
	state := runtime.tasks[id]
	runtime.mu.RUnlock()
	if state == nil {
		return TaskSnapshot{}, ErrTaskNotFound
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	snapshot := TaskSnapshot{ID: state.id, Turn: state.turn, Status: state.status, Progress: state.progress, Created: state.created, Updated: state.updated, HasResult: state.hasResult}
	if state.hasResult {
		snapshot.Result = cloneTaskResult(state.result)
	}
	return snapshot, nil
}

func (runtime *Runtime) await(ctx context.Context, id string) (TaskResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	runtime.mu.RLock()
	state := runtime.tasks[id]
	runtime.mu.RUnlock()
	if state == nil {
		return TaskResult{}, ErrTaskNotFound
	}
	state.mu.Lock()
	done := state.turnDone
	result := state.turnResult
	state.mu.Unlock()
	select {
	case <-done:
		return cloneTaskResult(*result), result.Err
	case <-ctx.Done():
		return TaskResult{}, ctx.Err()
	}
}

// Await waits for the selected task turns in caller order. It has no whole
// runtime barrier: a completed worker can be consumed while others run.
func (runtime *Runtime) Await(ctx context.Context, ids ...string) ([]TaskResult, error) {
	results := make([]TaskResult, 0, len(ids))
	for _, id := range ids {
		result, err := runtime.await(ctx, id)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

// WaitForTasks blocks until every selected task reaches idle or canceled.
// It is intended for guardrails to wait for a state change before requesting
// the parent's next model boundary.
func (runtime *Runtime) WaitForTasks(ctx context.Context, ids ...string) error {
	if runtime == nil {
		return ErrRuntimeClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		// Snapshot the generation before checking states. A concurrent state
		// transition then closes this exact channel, so no wakeup is lost.
		runtime.mu.RLock()
		generation := runtime.stateGeneration
		states := make([]*taskState, len(ids))
		for index, id := range ids {
			states[index] = runtime.tasks[id]
		}
		runtime.mu.RUnlock()
		for index, state := range states {
			if state == nil {
				return fmt.Errorf("%w: task %q", ErrTaskNotFound, ids[index])
			}
			state.mu.Lock()
			pending := state.status == TaskQueued || state.status == TaskRunning
			state.mu.Unlock()
			if pending {
				select {
				case <-generation:
				case <-ctx.Done():
					return ctx.Err()
				}
				break
			}
			if index == len(states)-1 {
				return nil
			}
		}
		if len(states) == 0 {
			return nil
		}
	}
}

func (runtime *Runtime) SendFollowup(ctx context.Context, id string, request agent.Request) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := request.Validate(); err != nil {
		return fmt.Errorf("%w: follow-up: %v", ErrInvalidArgument, err)
	}
	runtime.mu.RLock()
	state := runtime.tasks[id]
	runtime.mu.RUnlock()
	if state == nil {
		return ErrTaskNotFound
	}
	state.mu.Lock()
	if state.status == TaskCanceled {
		state.mu.Unlock()
		return context.Canceled
	}
	if state.status == TaskRunning && state.control != nil {
		ack, err := state.control.Send(request)
		state.mu.Unlock()
		if err != nil {
			return err
		}
		select {
		case err := <-ack:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if state.status != TaskIdle {
		state.mu.Unlock()
		return ErrTaskBusy
	}
	previousStatus := state.status
	previousTurn := state.turn
	previousDone := state.turnDone
	previousResult := state.turnResult
	previousPending := state.pending
	previousUpdated := state.updated
	state.status = TaskQueued
	state.turn++
	freshDone := make(chan struct{})
	state.turnDone = freshDone
	state.turnResult = new(TaskResult)
	copyRequest := cloneRequest(request)
	state.pending = &copyRequest
	state.updated = time.Now()
	// Keep the state lock through queue admission. Cancel and Close therefore
	// cannot overwrite an accepted turn or a rollback.
	if err := runtime.enqueue(state); err != nil {
		state.pending = previousPending
		state.status = previousStatus
		state.turn = previousTurn
		state.turnDone = previousDone
		state.turnResult = previousResult
		state.updated = previousUpdated
		state.mu.Unlock()
		close(freshDone)
		runtime.signalStateChange()
		return err
	}
	state.mu.Unlock()
	runtime.signalStateChange()
	return nil
}

func (runtime *Runtime) enqueue(state *taskState) error {
	runtime.mu.RLock()
	closed := runtime.closed
	runtime.mu.RUnlock()
	if closed {
		return ErrRuntimeClosed
	}
	select {
	case runtime.queue <- state:
		return nil
	default:
		return ErrQueueFull
	}
}

func (runtime *Runtime) signalStateChange() {
	runtime.mu.Lock()
	generation := runtime.stateGeneration
	if generation != nil {
		close(generation)
		runtime.stateGeneration = make(chan struct{})
	}
	runtime.mu.Unlock()
}

func (runtime *Runtime) Cancel(id string) error {
	if runtime == nil {
		return ErrRuntimeClosed
	}
	runtime.mu.RLock()
	state := runtime.tasks[id]
	runtime.mu.RUnlock()
	if state == nil {
		return ErrTaskNotFound
	}
	state.mu.Lock()
	if state.status == TaskCanceled {
		state.mu.Unlock()
		return nil
	}
	if state.status == TaskIdle {
		state.status = TaskCanceled
		state.updated = time.Now()
		state.mu.Unlock()
		runtime.signalStateChange()
		return nil
	}
	if state.status == TaskQueued {
		state.status = TaskCanceled
		state.updated = time.Now()
		completion := TaskResult{ID: state.id, Turn: state.turn, Err: context.Canceled, Completed: state.updated}
		completion = runtime.recordCompletion(completion)
		state.finishLocked(completion)
		state.mu.Unlock()
		runtime.signalStateChange()
		return nil
	}
	state.status = TaskCanceled
	state.updated = time.Now()
	if state.cancel != nil {
		state.cancel()
	}
	if state.control != nil {
		state.control.Cancel()
	}
	state.mu.Unlock()
	runtime.signalStateChange()
	return nil
}

func (runtime *Runtime) worker() {
	defer runtime.wg.Done()
	for {
		select {
		case <-runtime.ctx.Done():
			return
		case state := <-runtime.queue:
			if state != nil {
				runtime.execute(state)
			}
		}
	}
}

func (runtime *Runtime) execute(state *taskState) {
	state.mu.Lock()
	if state.status == TaskCanceled {
		if state.resultTurn != state.turn {
			completion := TaskResult{ID: state.id, Turn: state.turn, Err: context.Canceled, Completed: time.Now()}
			completion = runtime.recordCompletion(completion)
			state.finishLocked(completion)
			state.mu.Unlock()
			runtime.signalStateChange()
			return
		}
		state.mu.Unlock()
		return
	}
	request := state.spec.Request
	if state.pending != nil {
		request = *state.pending
		state.pending = nil
	}
	state.status = TaskRunning
	state.updated = time.Now()
	workerCtx, cancel := context.WithCancel(runtime.ctx)
	state.cancel = cancel
	control := &agent.Control{}
	state.control = control
	state.mu.Unlock()

	engine, err := runtime.engineFor(state, request)
	var result agent.Result
	started := time.Now()
	capture := &continuationCapture{}
	if err == nil {
		engine.Control = control
		priorBoundary := engine.Boundaries
		engine.Boundaries = agent.BoundaryObserverFunc(func(ctx context.Context, continuation agent.Continuation) error {
			if priorBoundary != nil {
				if err := priorBoundary.ObserveBoundary(ctx, continuation); err != nil {
					return err
				}
			}
			return capture.ObserveBoundary(ctx, continuation)
		})
		if stateContinuation := runtime.continuation(state); stateContinuation != nil {
			continuation := *stateContinuation
			continuation.Messages = append(continuation.Messages, requestMessage(request))
			continuation.Phase = agent.ContinuationReady
			continuation.Request = cloneRequest(state.spec.Request)
			continuation.OutputPolicy = cloneOutputPolicy(state.spec.OutputPolicy)
			if !engine.LoopPolicy.UnlimitedIterations {
				perTurnLimit := engine.LoopPolicy.MaxIterations
				if perTurnLimit <= 0 {
					perTurnLimit = 12
				}
				engine.LoopPolicy.MaxIterations = perTurnLimit + len(continuation.Steps)
			}
			if validateErr := agent.ValidateContinuation(continuation); validateErr != nil {
				result = agent.Result{Failure: (&agent.AgentFailure{Kind: agent.FailureKindEngineError, Reason: validateErr.Error()}).WithCause(validateErr)}
			} else {
				result = engine.ResumeStream(workerCtx, continuation, agent.SinkFunc(func(ctx context.Context, frame agent.Frame) error { return runtime.observeFrame(ctx, state, frame) }))
			}
		} else {
			result = engine.RunStream(workerCtx, request, state.spec.OutputPolicy, agent.SinkFunc(func(ctx context.Context, frame agent.Frame) error { return runtime.observeFrame(ctx, state, frame) }))
		}
	}
	cancel()
	state.mu.Lock()
	state.control = nil
	state.cancel = nil
	completion := TaskResult{ID: state.id, Turn: state.turn, Result: cloneAgentResult(result), Err: err, Started: started, Completed: time.Now()}
	if completion.Err == nil && state.status == TaskCanceled {
		completion.Err = context.Canceled
	}
	if completion.Err == nil && workerCtx.Err() != nil && state.status == TaskCanceled {
		completion.Err = context.Canceled
	}
	if completion.Err == nil && completion.Result.Failure == nil && len(completion.Result.Messages) > 0 {
		next := capture.continuation(state.spec.Request, state.spec.OutputPolicy, completion.Result)
		if next != nil {
			state.continuation = next
		}
	}
	completion = runtime.recordCompletion(completion)
	state.finishLocked(completion)
	state.mu.Unlock()
	runtime.signalStateChange()
}

func (runtime *Runtime) recordCompletion(completion TaskCompletion) TaskCompletion {
	runtime.inboxMu.Lock()
	runtime.completionSeq++
	completion.Sequence = runtime.completionSeq
	runtime.inbox = append(runtime.inbox, cloneTaskResult(completion))
	if len(runtime.inbox) > runtime.inboxCapacity {
		runtime.inbox = runtime.inbox[1:]
	}
	runtime.inboxMu.Unlock()
	select {
	case runtime.complete <- cloneTaskResult(completion):
	default:
		// DrainCompletions remains authoritative when the advisory channel is full.
	}
	return completion
}

type continuationCapture struct {
	mu    sync.Mutex
	value *agent.Continuation
}

func (capture *continuationCapture) ObserveBoundary(_ context.Context, value agent.Continuation) error {
	capture.mu.Lock()
	snapshot := cloneContinuation(value)
	capture.value = &snapshot
	capture.mu.Unlock()
	return nil
}

func (capture *continuationCapture) continuation(request agent.Request, policy agent.OutputPolicy, result agent.Result) *agent.Continuation {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.value == nil {
		return nil
	}
	value := cloneContinuation(*capture.value)
	value.SchemaVersion = agent.ContinuationSchemaVersion
	value.Request = cloneRequest(request)
	value.OutputPolicy = cloneOutputPolicy(policy)
	value.Messages = message.CloneMessages(capture.value.Messages)
	if len(result.Messages) > len(value.Messages) {
		value.Messages = message.CloneMessages(result.Messages)
	}
	value.Usage = result.Usage
	value.Steps = cloneSteps(result.Steps)
	if len(value.Steps) > 0 {
		value.Steps[len(value.Steps)-1].Decision = agent.StepDecisionContinue
	}
	value.ToolCallsUsed = result.ToolCallsUsed
	value.RepairCount = result.RepairCount
	value.Phase = agent.ContinuationReady
	if value.NextOperationTurn < len(value.Steps) {
		value.NextOperationTurn = len(value.Steps)
	}
	if err := agent.ValidateContinuation(value); err != nil {
		return nil
	}
	return &value
}

func (runtime *Runtime) engineFor(state *taskState, request agent.Request) (agent.Engine, error) {
	spec := state.spec
	spec.Request = request
	var engine agent.Engine
	var err error
	if runtime.options.Factory != nil {
		engine, err = runtime.options.Factory(spec)
	} else {
		engine = spec.Engine
	}
	if err != nil {
		return agent.Engine{}, err
	}
	return engine, nil
}

func (runtime *Runtime) observeFrame(ctx context.Context, state *taskState, frame agent.Frame) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state.mu.Lock()
	state.progress.Sequence++
	state.progress.Kind = frame.Kind
	state.progress.Text = frame.Text
	state.progress.Done = frame.Kind == agent.FrameDone || frame.Kind == agent.FrameError
	state.progress.Updated = time.Now()
	if frame.ToolCall != nil {
		state.progress.ToolName = frame.ToolCall.Name
	} else if frame.ToolResult != nil {
		state.progress.ToolName = frame.ToolResult.Name
	}
	state.updated = state.progress.Updated
	state.mu.Unlock()
	return nil
}

func (runtime *Runtime) continuation(state *taskState) *agent.Continuation {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.continuation == nil {
		return nil
	}
	snapshot := cloneContinuation(*state.continuation)
	return &snapshot
}

func (state *taskState) finishLocked(result TaskResult) {
	wasCanceled := state.status == TaskCanceled
	state.result = result
	*state.turnResult = result
	state.hasResult = true
	state.resultTurn = result.Turn
	state.status = TaskIdle
	if wasCanceled || errors.Is(result.Err, context.Canceled) {
		state.status = TaskCanceled
	}
	state.updated = result.Completed
	close(state.turnDone)
}

func (runtime *Runtime) Close() error {
	if runtime == nil {
		return nil
	}
	runtime.once.Do(func() {
		runtime.mu.Lock()
		runtime.closed = true
		states := make([]*taskState, 0, len(runtime.tasks))
		for _, state := range runtime.tasks {
			states = append(states, state)
		}
		runtime.mu.Unlock()
		for _, state := range states {
			state.mu.Lock()
			switch state.status {
			case TaskQueued:
				state.status = TaskCanceled
				completion := TaskResult{ID: state.id, Turn: state.turn, Err: context.Canceled, Completed: time.Now()}
				completion = runtime.recordCompletion(completion)
				state.finishLocked(completion)
			case TaskRunning:
				state.status = TaskCanceled
				state.updated = time.Now()
			}
			if state.cancel != nil {
				state.cancel()
			}
			if state.control != nil {
				state.control.Cancel()
			}
			state.mu.Unlock()
		}
		runtime.signalStateChange()
		runtime.cancel()
		runtime.wg.Wait()
		close(runtime.complete)
	})
	return nil
}

func requestMessage(request agent.Request) message.Message {
	parts := message.CloneContent(request.Content)
	if strings.TrimSpace(request.Prompt) != "" {
		parts = append([]message.ContentPart{message.TextPart(request.Prompt)}, parts...)
	}
	if len(parts) == 0 {
		parts = []message.ContentPart{message.TextPart("Continue the assigned task and return the result.")}
	}
	result := message.Message{Role: message.RoleUser, Kind: message.KindStandard, Content: parts}
	result.SyncLegacyContent()
	return result
}

func cloneRequest(request agent.Request) agent.Request {
	request.Content = message.CloneContent(request.Content)
	if request.Budget != nil {
		budget := *request.Budget
		request.Budget = &budget
	}
	if request.SessionBudget != nil {
		budget := *request.SessionBudget
		request.SessionBudget = &budget
	}
	if request.ModelTimeouts != nil {
		timeouts := *request.ModelTimeouts
		request.ModelTimeouts = &timeouts
	}
	return request
}

func cloneTaskRequest(request TaskRequest) TaskRequest {
	request.Request = cloneRequest(request.Request)
	request.OutputPolicy = cloneOutputPolicy(request.OutputPolicy)
	return request
}

func cloneOutputPolicy(policy agent.OutputPolicy) agent.OutputPolicy {
	policy.Schema = append(json.RawMessage(nil), policy.Schema...)
	return policy
}

func cloneContinuation(value agent.Continuation) agent.Continuation {
	value.Request = cloneRequest(value.Request)
	value.OutputPolicy = cloneOutputPolicy(value.OutputPolicy)
	value.Messages = message.CloneMessages(value.Messages)
	value.Steps = cloneSteps(value.Steps)
	return value
}

func cloneSteps(steps []agent.Step) []agent.Step {
	if steps == nil {
		return nil
	}
	result := make([]agent.Step, len(steps))
	for index, step := range steps {
		result[index] = step
		if step.ModelCall != nil {
			modelCall := *step.ModelCall
			result[index].ModelCall = &modelCall
		}
		result[index].ToolCalls = append([]agent.ToolCallTrace(nil), step.ToolCalls...)
		for callIndex := range result[index].ToolCalls {
			result[index].ToolCalls[callIndex].Arguments = append(json.RawMessage(nil), step.ToolCalls[callIndex].Arguments...)
			result[index].ToolCalls[callIndex].Output = append(json.RawMessage(nil), step.ToolCalls[callIndex].Output...)
		}
		result[index].Observations = append([]agent.Observation(nil), step.Observations...)
	}
	return result
}

func cloneAgentResult(result agent.Result) agent.Result {
	result.Structured = append(json.RawMessage(nil), result.Structured...)
	result.Messages = message.CloneMessages(result.Messages)
	result.Steps = cloneSteps(result.Steps)
	return result
}

func cloneTaskResult(result TaskResult) TaskResult {
	result.Result = cloneAgentResult(result.Result)
	return result
}
