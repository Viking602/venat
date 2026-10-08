# Backend and extension development

Venat extension points are narrow Go interfaces. Implement the lowest seam that owns the behavior; keep application policy and deployment concerns outside the SDK module.

## Model providers

Implement `provider.Driver`:

```go
type Driver interface {
    Metadata() Metadata
    Stream(context.Context, Request) (Stream, error)
}
```

Normalize provider output into ordered `provider.Event` values. Emit exactly one terminal event. Preserve usage, tool-call identity, provider state, and response metadata. Run the provider conformance suite and adapter-specific transport tests.

Provider streams are pull-based. After any valid event, a failure is partial and must not trigger automatic reopen; use `OpenRetryingStream` only for policy-approved open or pre-first-event failures. See [ADR-030](adr/ADR-030-stream-lifecycle-semantics.md).

A provider interceptor may call its supplied driver zero or one time. It must not mutate the request or retain mutable request members. Return `provider.ErrNotStarted` only when there is proof that no remote effect began.

## Tools

Implement `tool.Driver` directly or use `tool/kit` for ordinary Go functions, HTTP calls, and local processes. Definitions require stable names and valid JSON Schema. Return `tool.ErrNotExecuted` only when the underlying operation provably did not begin.

A tool interceptor follows the same zero-or-one-call and immutable-input contract. Authorization, approvals, quotas, retries, and business routing belong in application-owned interceptors or drivers.

`UpdateSink` is the single real-time tool-output seam. Emit `UpdateProgress` with message/data or `UpdateOutput` with non-empty content parts, stop on callback error, and return a terminal result whose parts match streamed output. The Bus supplies trusted call identity and sequence, backpressure, cloning, parallel sink serialization, and fixed count/byte limits. A driver that emits an update has started and must not return `ErrNotExecuted`.

Function tools can accept `UpdateSink`. `ProcessTool` publishes output as its stdout/stderr pipes are drained. Drivers that emit nothing remain valid one-shot implementations.

## Skills

Use `skill.Skill` for reusable instruction text and declared resources. Registration validates names and duplicate ownership. Skill activation changes context; it must not silently install tools or launch work.

## Agent extensions

Choose one explicit seam:

- `agent.Hook` for ordered lifecycle observation or deterministic context changes
- `provider.StreamInterceptor` and `tool.Interceptor` around effects
- `agent.StepDecider` for factual continue/finish/fail overrides
- `agent.StepObserver` for completed-step observation
- `agent.OutputGuardrail` for terminal output allow/replace/retry/block decisions
- `agent.BoundaryObserver` for safe continuation snapshots
- `agent.Sink` for transient output delivery
- context managers for deterministic context preparation

Observer, hook, guardrail, and sink side effects are not covered by durable effect settlement. Make them deterministic, idempotent, or independently durable.

## Durable backends

Implement all 12 methods of `durable.Backend`. The application owns schema, migrations, transactions, connection pools, replication, monitoring, and backup. Venat does not provide an additional storage abstraction, unit-of-work layer, capability probe, base backend, public memory backend, or production schema.

### Global invariants

- Use trusted backend time for lease expiry. Never accept caller wall-clock time.
- Allocate fencing tokens monotonically per execution. A token is never reused, including after release, suspension, or expiry.
- Treat `ClaimID` as the stable 128-bit command identity for `StartExecution` and `ResumeExecution`.
- Lock the execution record before reading lease state or mutating any child attempt. Lock affected attempt rows in the same transaction.
- Keep execution-version CAS independent from attempt-version CAS. Attempt mutations never advance `Execution.Version`.
- Validate checkpoint codec, schema version, and hash before storage and after loading.
- Compute record-bound checkpoint/result hashes using the locked execution's
  immutable SpecHash. See [hash migration](security-hardening.md#backend-migration).
- Return the exact committed value for a recognized response-loss retry. Never apply the transition twice.
- Atomically convert applicable `running` attempts to `unknown` when a lease is released, suspended, expires into a later claim, or is superseded by a later claim.
- Reject terminal execution completion while any attempt is `running` or `unknown`.
- Clone mutable slices, maps, messages, results, checkpoints, and attempts before retaining or returning them.

### Method-by-method state transitions

`E.v` means `Execution.Version`; `A.v` means `Attempt.Version`. “Exact replay” means the backend recognizes the same committed command and returns its original result without a second transition.

| Method | Required input state and transaction reads | Atomic writes and version movement | Fencing, response-loss replay, and typed errors |
| --- | --- | --- | --- |
| `StartExecution` | Lock execution by `ExecutionID`; if present, read immutable `SpecHash`, terminal state, lease, claim receipt, and all unresolved attempts. | Missing execution: insert immutable spec, `running`, `E.v=1`, first lease token, and claim receipt. Existing non-terminal without an active competing lease: allocate a higher token and atomically converge prior running attempts to `unknown`; `E.v` does not move. Return every unresolved `unknown` attempt, including those that were already unknown before this claim. | Exact `(ExecutionID, ClaimID, owner, TTL, spec hash)` returns the recorded `StartResult`. Same claim with different input or different spec returns `ErrConflict`; active competing lease returns `ErrBusy`; an overtaken old claim returns `ErrLeaseLost`; malformed input returns `ErrInvalidArgument`. A terminal execution returns its terminal value without a new lease. |
| `ResumeExecution` | Lock an existing execution; read terminal state, lease, claim receipt, and all unresolved attempts. | For a claimable non-terminal execution, allocate a higher token, set status to `running`, and converge prior running attempts to `unknown` in the same transaction. Return all existing and newly converged `unknown` attempts in `ResumeResult.Reconcile`; `E.v` does not move. | Exact `(ExecutionID, ClaimID, owner, TTL)` returns the recorded `ResumeResult`. Missing execution returns `ErrNotFound`; active competing lease returns `ErrBusy`; changed command input returns `ErrConflict`; overtaken claim returns `ErrLeaseLost`. Terminal state is returned without a lease. |
| `LoadExecution` | Read a consistent execution snapshot and its checkpoint; no lease is required. | No writes; no version movement. | Missing execution returns `ErrNotFound`. Invalid stored continuation bytes, schema version, or hash returns `ErrCorruptCheckpoint`. Return an ownership-safe copy. |
| `RenewExecution` | Lock execution and verify an unexpired lease with exact owner and token using backend time. | Replace only `Lease.ExpiresAt`; neither `E.v` nor `A.v` moves. | Stale, expired, released, or mismatched lease returns `ErrLeaseLost`. Retrying after response loss is safe and may extend expiry again; it must not change token or any version. |
| `SaveCheckpoint` | Validate the checkpoint first. Lock execution; verify the active lease; read current sequence/hash and `E.v`. | A strictly newer sequence fully replaces the prior checkpoint and increments `E.v` once. | Same sequence and hash is exact replay and returns the committed execution even when the original expected version is now stale. Same sequence with different hash, lower sequence, or stale expected version returns `ErrConflict`. Invalid codec/hash returns `ErrCorruptCheckpoint`; stale lease returns `ErrLeaseLost`. |
| `SuspendExecution` | Lock execution and all running attempts; verify active lease and expected `E.v`. | Converge running attempts owned by the lease to `unknown`, set `suspended`, clear lease, and increment `E.v` once in one transaction. | The receipt identity is `(lease owner, token, expected E.v)`. Exact replay returns the committed execution. Stale lease returns `ErrLeaseLost`; stale version returns `ErrConflict`. |
| `FinishExecution` | Validate `ResultHash`; lock execution and relevant attempts; verify active lease, expected `E.v`, and absence of `running` or `unknown` attempts. | Store the full result, set `completed` or `failed`, clear lease, and increment `E.v` once. | The receipt identity includes lease, expected version, and result hash. Exact replay returns the committed terminal execution. Changed result or stale version returns `ErrConflict`; stale lease returns `ErrLeaseLost`; unsettled attempts return `ErrReconcileRequired`. |
| `ReleaseExecution` | Lock execution and running attempts; verify lease owner and token. | Converge running attempts owned by that token to `unknown` and clear lease atomically. Status and `E.v` do not move. | The lease owner/token is the command identity. Exact replay returns the original `ReleaseResult`, including its reconciliation list. A later token makes the old command return `ErrLeaseLost`. |
| `StartAttempt` | Lock execution, verify active lease, then lock attempts for `(ExecutionID, OperationID)`. Compare kind and input hash with the latest attempt. | No prior attempt: insert number 1, `running`, `A.v=1`. Latest `abandoned`: insert the next number. A running attempt from an older lease converges to `unknown` and increments only `A.v`. `E.v` never moves. | A running attempt with the same lease/kind/input is exact replay and returns its original `execute` decision. Known `succeeded`/`failed` returns `replay`; `unknown` returns `reconcile`. Changed kind/input returns `ErrConflict`; stale lease returns `ErrLeaseLost`. The Runtime must have only one loop per fenced lease because the exact `execute` replay is for response-loss recovery, not concurrent effect dispatch. |
| `FinishAttempt` | Lock execution and the exact attempt; verify active lease, attempt number, expected `A.v`, and that the attempt is `running` under that lease. | Store payload/failure, set `succeeded` or `failed`, clear attempt lease, and increment `A.v` once. `E.v` does not move. | Exact `(attempt number, expected A.v, payload, failure)` replay returns the committed attempt. Changed payload/failure or stale attempt version returns `ErrConflict`; stale execution lease returns `ErrLeaseLost`; missing attempt returns `ErrNotFound`. |
| `MarkAttemptUnknown` | Same locks and fencing as `FinishAttempt`. | Store available payload/failure, set `unknown`, clear attempt lease, and increment `A.v` once. `E.v` does not move. | Exact replay returns the committed unknown attempt. Changed outcome or stale attempt version returns `ErrConflict`; stale lease returns `ErrLeaseLost`; missing attempt returns `ErrNotFound`. |
| `ReconcileAttempt` | Lock execution and exact attempt. The execution must have no active lease. Verify `unknown`, attempt number, and expected `A.v`. | Resolve to `succeeded`, `failed`, or `abandoned`; store the resolution payload/failure; increment `A.v` once. `E.v` does not move. | Exact `(attempt number, expected A.v, resolution, payload, failure)` replay returns the committed attempt. Active lease returns `ErrBusy`; changed resolution or stale attempt version returns `ErrConflict`; missing attempt returns `ErrNotFound`. |

All backend errors must retain `ExecutionID`. Attempt errors must also retain `OperationID` and attempt number through `durable.ExecutionError` or `durable.AttemptError`. Use the exported sentinel chain: `ErrInvalidArgument`, `ErrNotFound`, `ErrBusy`, `ErrConflict`, `ErrLeaseLost`, `ErrCorruptCheckpoint`, and `ErrReconcileRequired`.

### Relational pseudotransactions

These sketches describe ordering, not an official schema.

Claim:

```text
BEGIN
  execution = lock execution by ExecutionID
  prior = lock claim receipt by (ExecutionID, ClaimID)
  if prior matches the full command: return prior response
  reject immutable-spec, terminal, active-lease, or superseded-claim conflicts
  running = lock running attempts for ExecutionID
  update running -> unknown where owned by the superseded token
  token = allocate monotonically above every prior token
  write lease and claim receipt, including the complete response
COMMIT
```

Execution CAS mutation:

```text
validate codec and bounded JSON inputs before transaction
BEGIN
  execution = lock execution by ExecutionID
  validate record-bound hash using execution's immutable SpecHash
  receipt = lock mutation receipt by command identity
  if receipt matches: return its complete response
  verify active lease owner/token using backend time
  verify expected E.v
  lock and validate affected attempts
  apply one transition; increment E.v exactly once when specified
  write receipt and response in the same commit
COMMIT
```

Attempt CAS mutation:

```text
BEGIN
  execution = lock execution by ExecutionID
  verify active lease, or verify no active lease for reconciliation
  attempt = lock attempt by (ExecutionID, OperationID, AttemptNumber)
  if committed state exactly matches this command: return attempt
  verify expected A.v and source status
  apply one transition and increment A.v once; never increment E.v
COMMIT
```

Do not acknowledge success before the transaction containing both state and replay identity commits. A lost response must be recoverable through a new backend process handle.

### Keys and indexes

Choose names and physical layout for the application database. The contract normally needs:

- a unique execution key on `ExecutionID`
- claim receipt uniqueness on `(ExecutionID, ClaimID)`
- attempt uniqueness on `(ExecutionID, OperationID, AttemptNumber)`
- an index for the latest attempt by `(ExecutionID, OperationID, AttemptNumber descending)`
- an index for unsettled attempts by `(ExecutionID, Status)`
- lease-expiry lookup by `ExpiresAt` when the application runs a sweeper
- durable mutation receipts, or equivalent state-derived replay keys, for release, suspend, finish, and attempt settlements

### Conformance

Every adapter must run:

```go
func TestBackendContract(t *testing.T) {
    durablecontract.RunBackendContractTests(t, factory)
}
```

The factory returns a backend, a process-reopen function over the same persistent state, and cleanup. Passing only unit tests for individual storage methods is insufficient. The suite exercises concurrent claims, fencing, stale CAS rejection, response-loss replay through a reopened process handle, unknown convergence, checkpoint codec/hash validation, terminal rules, ownership safety, and typed diagnostics.

Venat intentionally ships no reusable in-process or production backend. The backend in `examples/durable` exists only to make the recovery flow executable.

## External protocol adapters

Protocol clients, schedulers, webhooks, sandboxes, hosted services, and domain bundles live in application repositories or separate modules. Adapt them to `provider.Driver`, `tool.Driver`, `skill` resources, `orchestration.Executor`, or `durable.Backend` without adding a second composition layer inside Venat.
