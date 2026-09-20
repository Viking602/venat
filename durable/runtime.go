package durable

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Viking602/venat/agent"
	"github.com/Viking602/venat/provider"
	"github.com/Viking602/venat/tool"
)

const (
	defaultLeaseTTL          = 30 * time.Second
	defaultSettlementTimeout = 5 * time.Second
)

var (
	errBackendOperation = errors.New("durable: backend operation failed")
	errRuntimeOperation = errors.New("durable: runtime operation failed")
)

// Options configures Runtime-owned lease and settlement behavior.
type Options struct {
	OwnerID           string
	LeaseTTL          time.Duration
	SettlementTimeout time.Duration
}

// Runtime adds optional continuation and effect durability to agent.Engine.
type Runtime struct {
	backend           Backend
	ownerID           string
	leaseTTL          time.Duration
	settlementTimeout time.Duration

	mu            sync.Mutex
	active        map[ExecutionID]*activeExecution
	pendingClaims map[ExecutionID]pendingClaim
	closed        bool
	closeErrors   []error
}

type pendingClaim struct {
	mode     string
	specHash [32]byte
	claimID  ClaimID
	expires  time.Time
}

// New constructs a Runtime without taking ownership of backend.
func New(backend Backend, options Options) (*Runtime, error) {
	if nilInterface(backend) {
		return nil, fmt.Errorf("%w: nil backend", ErrInvalidArgument)
	}
	if options.LeaseTTL < 0 || options.SettlementTimeout < 0 {
		return nil, fmt.Errorf("%w: negative duration", ErrInvalidArgument)
	}
	if options.OwnerID != "" && strings.TrimSpace(options.OwnerID) == "" {
		return nil, fmt.Errorf("%w: owner ID is blank", ErrInvalidArgument)
	}
	ownerID := options.OwnerID
	if ownerID == "" {
		generated, err := randomOwnerID()
		if err != nil {
			return nil, fmt.Errorf("generate owner ID: %w", err)
		}
		ownerID = generated
	}
	leaseTTL := options.LeaseTTL
	if leaseTTL == 0 {
		leaseTTL = defaultLeaseTTL
	}
	settlementTimeout := options.SettlementTimeout
	if settlementTimeout == 0 {
		settlementTimeout = defaultSettlementTimeout
	}
	return &Runtime{
		backend:           backend,
		ownerID:           ownerID,
		leaseTTL:          leaseTTL,
		settlementTimeout: settlementTimeout,
		active:            make(map[ExecutionID]*activeExecution),
		pendingClaims:     make(map[ExecutionID]pendingClaim),
	}, nil
}

// Start creates or resumes executionID using the persisted ExecutionSpec.
func (runtime *Runtime) Start(ctx context.Context, executionID ExecutionID, engine agent.Engine, request agent.Request, policy agent.OutputPolicy) (agent.Result, error) {
	return runtime.start(ctx, executionID, engine, request, policy, nil)
}

// StartStream is Start with a transient live output Sink.
func (runtime *Runtime) StartStream(ctx context.Context, executionID ExecutionID, engine agent.Engine, request agent.Request, policy agent.OutputPolicy, sink agent.Sink) (agent.Result, error) {
	return runtime.start(ctx, executionID, engine, request, policy, sink)
}

func (runtime *Runtime) start(ctx context.Context, executionID ExecutionID, engine agent.Engine, request agent.Request, policy agent.OutputPolicy, sink agent.Sink) (agent.Result, error) {
	if err := validateRuntimeCall(ctx, executionID); err != nil {
		return agent.Result{}, err
	}
	spec := ExecutionSpec{Request: cloneRequest(request), OutputPolicy: cloneOutputPolicy(policy)}
	if err := validateExecutionSpec(spec); err != nil {
		return agent.Result{}, executionRuntimeError(executionID, err)
	}
	specHash, err := HashExecutionSpec(spec)
	if err != nil {
		return agent.Result{}, executionRuntimeError(executionID, fmt.Errorf("%w: hash execution spec: %v", ErrInvalidArgument, err))
	}
	active, err := runtime.enter(ctx, executionID)
	if err != nil {
		return agent.Result{}, err
	}
	var result agent.Result
	var runErr error
	var controlErr error
	defer func() {
		active.finish(controlErr)
		runtime.leave(active, controlErr)
	}()

	claimID, err := runtime.claimID(executionID, "start", specHash)
	if err != nil {
		return agent.Result{}, err
	}
	claimed, claimErr := runtime.backend.StartExecution(active.ctx, StartExecutionRequest{
		ExecutionID: executionID,
		OwnerID:     runtime.ownerID,
		ClaimID:     claimID,
		LeaseTTL:    runtime.leaseTTL,
		Spec:        spec,
		SpecHash:    specHash,
	})
	runtime.resolveClaim(executionID, claimErr)
	if claimErr != nil {
		return agent.Result{}, backendExecutionOperationError("start execution", executionID, claimErr)
	}
	if validationErr := validateClaimedExecution(claimed.Execution, executionID, runtime.ownerID); validationErr != nil {
		return agent.Result{}, runtime.rejectInvalidClaim(active, claimed.Execution, validationErr)
	}
	active.setExecution(claimed.Execution)
	result, runErr, controlErr = runtime.runClaimed(active, engine, sink, claimed.Execution, claimed.Reconcile, ResumeTarget{})
	return result, runErr
}

// Resume claims and resumes an existing execution without target assertions.
func (runtime *Runtime) Resume(ctx context.Context, executionID ExecutionID, engine agent.Engine) (agent.Result, error) {
	return runtime.ResumeWithOptions(ctx, executionID, engine, ResumeOptions{})
}

// ResumeWithOptions claims and resumes an existing execution only when its
// persisted checkpoint matches every non-zero target fact.
func (runtime *Runtime) ResumeWithOptions(ctx context.Context, executionID ExecutionID, engine agent.Engine, options ResumeOptions) (agent.Result, error) {
	return runtime.resume(ctx, executionID, engine, nil, options)
}

// ResumeStream is Resume with a transient live output Sink.
func (runtime *Runtime) ResumeStream(ctx context.Context, executionID ExecutionID, engine agent.Engine, sink agent.Sink) (agent.Result, error) {
	return runtime.ResumeStreamWithOptions(ctx, executionID, engine, sink, ResumeOptions{})
}

// ResumeStreamWithOptions is ResumeWithOptions with a transient live output
// Sink.
func (runtime *Runtime) ResumeStreamWithOptions(ctx context.Context, executionID ExecutionID, engine agent.Engine, sink agent.Sink, options ResumeOptions) (agent.Result, error) {
	return runtime.resume(ctx, executionID, engine, sink, options)
}

func (runtime *Runtime) resume(ctx context.Context, executionID ExecutionID, engine agent.Engine, sink agent.Sink, options ResumeOptions) (agent.Result, error) {
	if err := validateRuntimeCall(ctx, executionID); err != nil {
		return agent.Result{}, err
	}
	active, err := runtime.enter(ctx, executionID)
	if err != nil {
		return agent.Result{}, err
	}
	var result agent.Result
	var runErr error
	var controlErr error
	defer func() {
		active.finish(controlErr)
		runtime.leave(active, controlErr)
	}()

	claimID, err := runtime.claimID(executionID, "resume", [32]byte{})
	if err != nil {
		return agent.Result{}, err
	}
	claimed, claimErr := runtime.backend.ResumeExecution(active.ctx, ResumeExecutionRequest{
		ExecutionID: executionID,
		OwnerID:     runtime.ownerID,
		ClaimID:     claimID,
		LeaseTTL:    runtime.leaseTTL,
	})
	runtime.resolveClaim(executionID, claimErr)
	if claimErr != nil {
		return agent.Result{}, backendExecutionOperationError("resume execution", executionID, claimErr)
	}
	if validationErr := validateClaimedExecution(claimed.Execution, executionID, runtime.ownerID); validationErr != nil {
		return agent.Result{}, runtime.rejectInvalidClaim(active, claimed.Execution, validationErr)
	}
	active.setExecution(claimed.Execution)
	result, runErr, controlErr = runtime.runClaimed(active, engine, sink, claimed.Execution, claimed.Reconcile, options.Target)
	return result, runErr
}

func (runtime *Runtime) runClaimed(active *activeExecution, engine agent.Engine, sink agent.Sink, execution Execution, reconcile []Attempt, target ResumeTarget) (agent.Result, error, error) {
	if terminalExecution(execution.Status) {
		if execution.Result == nil {
			err := executionRuntimeError(execution.ID, ErrConflict)
			return agent.Result{}, err, err
		}
		result := cloneAgentResult(*execution.Result)
		hash, err := HashResult(result)
		if err != nil || hash != execution.ResultHash {
			failure := executionRuntimeError(execution.ID, ErrConflict)
			return agent.Result{}, failure, failure
		}
		if targetErr := resumeTargetMismatch(execution, target); targetErr != nil {
			return agent.Result{}, targetErr, targetErr
		}
		return result, nil, nil
	}
	if execution.Lease == nil {
		err := executionRuntimeError(execution.ID, ErrLeaseLost)
		return agent.Result{}, err, err
	}
	if len(reconcile) > 0 {
		reconcileErr := &ReconcileRequiredError{Execution: cloneExecution(execution), Attempts: cloneAttempts(reconcile)}
		cleanupErr := runtime.release(active)
		return agent.Result{}, errors.Join(reconcileErr, cleanupErr), cleanupErr
	}
	if targetErr := resumeTargetMismatch(execution, target); targetErr != nil {
		cleanupErr := runtime.release(active)
		return agent.Result{}, errors.Join(targetErr, cleanupErr), cleanupErr
	}

	stopHeartbeat := active.heartbeat()
	configured := runtime.configureEngine(active, engine)
	var result agent.Result
	if execution.Checkpoint != nil {
		if sink == nil {
			result = configured.Resume(active.ctx, execution.Checkpoint.Continuation)
		} else {
			result = configured.ResumeStream(active.ctx, execution.Checkpoint.Continuation, sink)
		}
	} else if sink == nil {
		result = configured.Run(active.ctx, execution.Spec.Request, execution.Spec.OutputPolicy)
	} else {
		result = configured.RunStream(active.ctx, execution.Spec.Request, execution.Spec.OutputPolicy, sink)
	}
	stopHeartbeat()
	return runtime.complete(active, result)
}

func (runtime *Runtime) configureEngine(active *activeExecution, engine agent.Engine) agent.Engine {
	engine.ModelInterceptor = provider.ChainStreamInterceptors(modelAttemptInterceptor{active: active}, engine.ModelInterceptor)
	engine.ToolInterceptor = tool.ChainInterceptors(toolAttemptInterceptor{active: active}, engine.ToolInterceptor)
	engine.Boundaries = agent.JoinBoundaryObservers(engine.Boundaries, boundaryObserver{active: active})
	return engine
}

func (runtime *Runtime) complete(active *activeExecution, result agent.Result) (agent.Result, error, error) {
	active.stopNewEffects()
	stopCause, controlContext, heartbeatErr := active.stopState()
	if stopCause != nil {
		switch {
		case errors.Is(stopCause, ErrSuspended):
			cleanupErr := runtime.suspend(controlContext, active)
			return result, errors.Join(ErrSuspended, cleanupErr), cleanupErr
		case errors.Is(stopCause, ErrClosed):
			cleanupErr := runtime.release(active)
			return result, errors.Join(ErrClosed, cleanupErr), cleanupErr
		default:
			cleanupErr := runtime.release(active)
			return result, errors.Join(stopCause, cleanupErr), cleanupErr
		}
	}
	if heartbeatErr != nil {
		cleanupErr := runtime.release(active)
		return result, errors.Join(heartbeatErr, cleanupErr), cleanupErr
	}
	if callerErr := active.callerCtx.Err(); callerErr != nil {
		cleanupErr := runtime.release(active)
		return result, errors.Join(callerErr, cleanupErr), cleanupErr
	}
	if result.Failure != nil && isRuntimeInfrastructure(result.Failure) {
		cleanupErr := runtime.release(active)
		return result, errors.Join(result.Failure, cleanupErr), cleanupErr
	}

	settlementCtx, cancel := active.settlementContext()
	defer cancel()
	if err := active.waitEffects(settlementCtx); err != nil {
		cleanupErr := runtime.release(active)
		operationErr := fmt.Errorf("wait for effect settlement: %w", err)
		return result, errors.Join(operationErr, cleanupErr), cleanupErr
	}
	execution := active.snapshot()
	if execution.Lease == nil {
		err := executionRuntimeError(active.id, ErrLeaseLost)
		return result, err, err
	}
	resultHash, err := HashResult(result)
	if err != nil {
		cleanupErr := runtime.release(active)
		operationErr := fmt.Errorf("hash terminal result: %w", err)
		return result, errors.Join(operationErr, cleanupErr), cleanupErr
	}
	finished, err := runtime.backend.FinishExecution(active.callerCtx, FinishExecutionRequest{
		ExecutionID:     active.id,
		Lease:           leaseReference(*execution.Lease),
		ExpectedVersion: execution.Version,
		Result:          result,
		ResultHash:      resultHash,
	})
	if err != nil {
		cleanupErr := runtime.release(active)
		operationErr := backendOperationError("finish execution", err)
		return result, errors.Join(operationErr, cleanupErr), cleanupErr
	}
	active.setExecution(finished)
	return result, nil, nil
}

func (runtime *Runtime) suspend(ctx context.Context, active *activeExecution) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := active.waitEffects(ctx); err != nil {
		return errors.Join(err, runtime.release(active))
	}
	execution := active.snapshot()
	if execution.Lease == nil {
		return executionRuntimeError(active.id, ErrLeaseLost)
	}
	suspended, err := runtime.backend.SuspendExecution(ctx, SuspendExecutionRequest{
		ExecutionID:     active.id,
		Lease:           leaseReference(*execution.Lease),
		ExpectedVersion: execution.Version,
	})
	if err != nil {
		return errors.Join(backendOperationError("suspend execution", err), runtime.release(active))
	}
	active.setExecution(suspended)
	return nil
}

func (runtime *Runtime) release(active *activeExecution) error {
	active.stopNewEffects()
	ctx, cancel := active.settlementContext()
	defer cancel()
	if err := active.waitEffects(ctx); err != nil {
		return fmt.Errorf("wait for effect settlement: %w", err)
	}
	execution := active.snapshot()
	if execution.Lease == nil {
		return nil
	}
	released, err := runtime.backend.ReleaseExecution(ctx, ReleaseExecutionRequest{
		ExecutionID: active.id,
		Lease:       leaseReference(*execution.Lease),
	})
	if err != nil {
		return backendOperationError("release execution", err)
	}
	active.setExecution(released.Execution)
	if len(released.Reconcile) > 0 {
		return &ReconcileRequiredError{Execution: cloneExecution(released.Execution), Attempts: cloneAttempts(released.Reconcile)}
	}
	return nil
}

// Suspend requests durable suspension of a locally active execution.
func (runtime *Runtime) Suspend(ctx context.Context, executionID ExecutionID) error {
	if err := validateRuntimeCall(ctx, executionID); err != nil {
		return err
	}
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return executionRuntimeError(executionID, ErrClosed)
	}
	active := runtime.active[executionID]
	runtime.mu.Unlock()
	if active == nil {
		return executionRuntimeError(executionID, ErrNotActive)
	}
	if err := active.requestSuspend(ctx); err != nil {
		return err
	}
	select {
	case <-active.done:
		return active.finalControlError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close idempotently stops new calls, cancels active executions, and releases
// their leases without closing the caller-owned Backend.
func (runtime *Runtime) Close(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	runtime.mu.Lock()
	if !runtime.closed {
		runtime.closed = true
		for _, active := range runtime.active {
			active.requestStop(ctx, ErrClosed)
		}
	}
	active := make([]*activeExecution, 0, len(runtime.active))
	for _, execution := range runtime.active {
		active = append(active, execution)
	}
	prior := append([]error(nil), runtime.closeErrors...)
	runtime.mu.Unlock()

	joined := prior
	for _, execution := range active {
		select {
		case <-execution.done:
			if err := execution.finalControlError(); err != nil {
				joined = append(joined, err)
			}
		case <-ctx.Done():
			joined = append(joined, ctx.Err())
			return errors.Join(joined...)
		}
	}
	return errors.Join(joined...)
}

func (runtime *Runtime) enter(ctx context.Context, executionID ExecutionID) (*activeExecution, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return nil, executionRuntimeError(executionID, ErrClosed)
	}
	if _, exists := runtime.active[executionID]; exists {
		return nil, executionRuntimeError(executionID, ErrBusy)
	}
	active := newActiveExecution(ctx, runtime, executionID)
	runtime.active[executionID] = active
	return active, nil
}

func (runtime *Runtime) leave(active *activeExecution, controlErr error) {
	runtime.mu.Lock()
	delete(runtime.active, active.id)
	if errors.Is(active.stopCauseValue(), ErrClosed) && controlErr != nil {
		runtime.closeErrors = append(runtime.closeErrors, controlErr)
	}
	runtime.mu.Unlock()
}

func (runtime *Runtime) claimID(executionID ExecutionID, mode string, specHash [32]byte) (ClaimID, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if prior, ok := runtime.pendingClaims[executionID]; ok {
		if time.Now().Before(prior.expires) && prior.mode == mode && prior.specHash == specHash {
			return prior.claimID, nil
		}
		delete(runtime.pendingClaims, executionID)
	}
	claimID, err := randomClaimID()
	if err != nil {
		return ClaimID{}, fmt.Errorf("generate claim ID: %w", err)
	}
	runtime.pendingClaims[executionID] = pendingClaim{
		mode:     mode,
		specHash: specHash,
		claimID:  claimID,
		expires:  time.Now().Add(runtime.leaseTTL),
	}
	return claimID, nil
}

func (runtime *Runtime) resolveClaim(executionID ExecutionID, err error) {
	if err != nil && !definitiveClaimError(err) {
		return
	}
	runtime.mu.Lock()
	delete(runtime.pendingClaims, executionID)
	runtime.mu.Unlock()
}

func definitiveClaimError(err error) bool {
	return errors.Is(err, ErrInvalidArgument) ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrConflict) ||
		errors.Is(err, ErrBusy) ||
		errors.Is(err, ErrLeaseLost) ||
		errors.Is(err, ErrCorruptCheckpoint) ||
		errors.Is(err, ErrReconcileRequired)
}

func validateRuntimeCall(ctx context.Context, executionID ExecutionID) error {
	if ctx == nil || strings.TrimSpace(string(executionID)) == "" {
		return executionRuntimeError(executionID, ErrInvalidArgument)
	}
	return nil
}

func validateClaimedExecution(execution Execution, executionID ExecutionID, ownerID string) error {
	if execution.ID != executionID {
		return claimedExecutionConflict(executionID, "backend returned a different execution ID")
	}
	if execution.Version == 0 {
		return claimedExecutionConflict(executionID, "backend returned a zero execution version")
	}
	if err := validateExecutionSpec(execution.Spec); err != nil {
		return claimedExecutionConflict(executionID, "backend returned an invalid execution spec")
	}
	specHash, err := HashExecutionSpec(execution.Spec)
	if err != nil || specHash != execution.SpecHash {
		return claimedExecutionConflict(executionID, "execution spec hash mismatch")
	}
	if err := validateClaimedExecutionStatus(execution, executionID, ownerID); err != nil {
		return err
	}
	return validateExecutionCheckpoint(execution, executionID)
}

func validateClaimedExecutionStatus(execution Execution, executionID ExecutionID, ownerID string) error {
	switch execution.Status {
	case ExecutionStatusRunning:
		if execution.Lease == nil || execution.Lease.OwnerID != ownerID || execution.Lease.Token == 0 || execution.Lease.ClaimID == (ClaimID{}) {
			return claimedExecutionConflict(executionID, "backend returned an invalid claimed lease")
		}
		if execution.Result != nil {
			return claimedExecutionConflict(executionID, "running execution contains a terminal result")
		}
	case ExecutionStatusCompleted, ExecutionStatusFailed:
		if execution.Lease != nil || execution.Result == nil {
			return claimedExecutionConflict(executionID, "terminal execution has an invalid lease or result")
		}
		resultHash, err := HashResult(*execution.Result)
		if err != nil || resultHash != execution.ResultHash {
			return claimedExecutionConflict(executionID, "terminal result hash mismatch")
		}
		if (execution.Status == ExecutionStatusCompleted) != (execution.Result.Failure == nil) {
			return claimedExecutionConflict(executionID, "terminal status disagrees with Agent result")
		}
	default:
		return claimedExecutionConflict(executionID, fmt.Sprintf("backend returned invalid claimed status %q", execution.Status))
	}
	return nil
}

func validateExecutionCheckpoint(execution Execution, executionID ExecutionID) error {
	if execution.Checkpoint == nil {
		return nil
	}
	if err := ValidateCheckpoint(*execution.Checkpoint); err != nil {
		return executionRuntimeError(executionID, err)
	}
	return nil
}

func claimedExecutionConflict(executionID ExecutionID, reason string) error {
	return executionRuntimeError(executionID, fmt.Errorf("%w: %s", ErrConflict, reason))
}

func (runtime *Runtime) rejectInvalidClaim(active *activeExecution, execution Execution, validationErr error) error {
	if execution.Lease == nil {
		return validationErr
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(active.callerCtx), runtime.settlementTimeout)
	defer cancel()
	_, err := runtime.backend.ReleaseExecution(ctx, ReleaseExecutionRequest{
		ExecutionID: execution.ID,
		Lease:       leaseReference(*execution.Lease),
	})
	return errors.Join(validationErr, backendOperationError("release invalid claim", err))
}

func validateExecutionSpec(spec ExecutionSpec) error {
	if err := spec.Request.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	if err := agent.ValidateOutputPolicy(spec.OutputPolicy); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}
	if len(spec.OutputPolicy.Schema) > 0 && !json.Valid(spec.OutputPolicy.Schema) {
		return ErrInvalidArgument
	}
	return nil
}

func terminalExecution(status ExecutionStatus) bool {
	return status == ExecutionStatusCompleted || status == ExecutionStatusFailed
}

func randomOwnerID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func randomClaimID() (ClaimID, error) {
	var value ClaimID
	_, err := rand.Read(value[:])
	return value, err
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

type backendCallError struct {
	op  string
	err error
}

func backendOperationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &backendCallError{op: operation, err: err}
}

func backendExecutionOperationError(operation string, executionID ExecutionID, err error) error {
	wrapped := backendOperationError(operation, err)
	if wrapped != nil && errors.Is(err, ErrCorruptCheckpoint) {
		return executionRuntimeError(executionID, wrapped)
	}
	return wrapped
}

func (failure *backendCallError) Error() string {
	return fmt.Sprintf("durable: %s: %v", failure.op, failure.err)
}

func (failure *backendCallError) Unwrap() []error {
	return []error{errBackendOperation, failure.err}
}

type runtimeCallError struct {
	op  string
	err error
}

func runtimeOperationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &runtimeCallError{op: operation, err: err}
}

func (failure *runtimeCallError) Error() string {
	return fmt.Sprintf("durable: %s: %v", failure.op, failure.err)
}

func (failure *runtimeCallError) Unwrap() []error {
	return []error{errRuntimeOperation, failure.err}
}

func isRuntimeInfrastructure(err error) bool {
	return errors.Is(err, errBackendOperation) ||
		errors.Is(err, errRuntimeOperation) ||
		errors.Is(err, ErrInvalidArgument) ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrConflict) ||
		errors.Is(err, ErrBusy) ||
		errors.Is(err, ErrLeaseLost) ||
		errors.Is(err, ErrCorruptCheckpoint) ||
		errors.Is(err, ErrReconcileRequired) ||
		errors.Is(err, ErrResumeTargetMismatch) ||
		errors.Is(err, ErrNotActive) ||
		errors.Is(err, ErrSuspended) ||
		errors.Is(err, ErrClosed)
}
