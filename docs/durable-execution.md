# Durable execution

`durable` adds optional crash recovery to one `agent.Engine` execution. It does not persist application orchestration, identity, policy, or deployment state.

## Ownership model

Venat owns the execution verbs and conformance tests. The application owns:

- the backend implementation and schema
- execution ID namespace
- database transactions and operational guarantees
- real-world investigation of ambiguous effects
- reconciliation policy
- compatible Engine construction on resume

A backend is injected into `durable.New`; the runtime never closes it.

## Immutable execution input

`Start` persists an `ExecutionSpec` containing `agent.Request` and `agent.OutputPolicy` plus `HashExecutionSpec`. A repeated Start for the same ID must use the same hash. Persisted input is authoritative: if an execution already exists, the runtime ignores the newly supplied copy after the backend validates equality.

Functions and external drivers cannot be hashed reliably. A resumed call must supply an Engine with compatible model, tools, hooks, skills, guardrails, and context behavior. Applications that need revision fencing should include their revision in the execution ID or backend namespace.

## Lease and fencing

A claim has a random 128-bit `ClaimID`, an owner ID, a backend-timed expiry, and a monotonically increasing fencing token.

- A live lease rejects every different claim with `ErrBusy`, including the same owner.
- An exact claim retry replays its original claim result.
- A later claim receives a higher token.
- Mutations from an older token fail with `ErrLeaseLost`.
- Renewal extends expiry without changing `Execution.Version`.

The runtime renews at `LeaseTTL/3`. Lease loss prevents new effects and cancels the Agent loop.

## Continuation checkpoints

The Agent emits a complete `agent.Continuation` at safe phases:

1. `ready` — context is ready for the next model call.
2. `model_complete` — a complete assistant tool-call turn is pending.
3. `tools_complete` — the corresponding tool-result turn is complete.
4. `validating_output` — terminal assistant output is ready for output processing.

A checkpoint has a strictly increasing sequence and a canonical content hash. Saving it fully replaces the prior continuation. A backend and runtime both reject malformed or hash-mismatched state with `ErrCorruptCheckpoint`; neither merges or repairs it.

`Resume` calls `Engine.Resume` when a checkpoint exists and `Engine.Run` otherwise.

### Operation-targeted resume

`ResumeWithOptions` and `ResumeStreamWithOptions` accept a `ResumeTarget`. Non-zero `CheckpointSequence`, `Phase`, and `OperationID` fields are independent assertions; `Resume` and `ResumeStream` delegate with zero options.

The runtime claims and validates the execution first, then rejects unresolved attempts before comparing the target. It compares before starting the Engine, invoking hooks, delivering sink frames, or opening provider/tool effects. `ready` exposes the next `turn:N:model` slot. `model_complete` exposes each checkpointed pending `turn:N:call:M` slot. `tools_complete` and `validating_output` have no immediately pending model/tool slot, so an `OperationID` assertion cannot match them.

A mismatch returns `ErrResumeTargetMismatch` and a `ResumeTargetError` containing expected and actual sequence/phase facts plus available operation IDs. The runtime best-effort releases the lease. A target is only a factual precondition: it never edits continuation state, injects payload, selects a component, or bypasses reconciliation.

### Application-owned human approval

`examples/durable` composes approval without adding approval state or policy to `agent` or `durable`:

1. An application store creates one pending record keyed by `ExecutionID + OperationID` from `Hook.BeforeToolCall`.
2. The pending hook waits for cancellation while an application controller calls `Runtime.Suspend`. The already-saved `model_complete` checkpoint remains the recovery source and no tool effect begins.
3. The controller persists an approved or denied audit fact, loads the current checkpoint sequence, and calls `ResumeWithOptions` with that sequence, `model_complete`, and the pending operation ID.
4. An application tool wrapper delegates approved calls. It converts denied calls into an ordinary `ToolResult{IsError:true}` without invoking the underlying driver.

Request creation and decisions are idempotent in the application store. Replayed approved calls are still protected by durable attempt replay. Multiple pending calls may require multiple suspend/decision cycles because every hook runs before a tool batch dispatches.

### Wire version and rollout

`agent.ContinuationSchemaVersion` is currently `2`. Version 2 adds typed request
content and the native-output opt-in. The strict decoder accepts versions 1 and
2, rejects unknown/missing/duplicate fields and future versions, and rejects v2
fields even when smuggled into a v1 document. There is no v0 translator.

Version 1 documents retain their original version and canonical bytes on decode
and re-encode, so existing SHA-256 checkpoint hashes remain valid. The immutable
`agent/testdata/continuation-v1.json` fixture covers this guarantee. Resume first
validates the stored checkpoint and hash, then performs a pure private in-memory
upgrade. The next safe boundary writes the v2 continuation and matching hash
atomically through `Backend.SaveCheckpoint`; it never rewrites storage on read.

This version writes v2 immediately; it has no reader-only deployment mode.
Pause dispatch, drain or suspend active executions, upgrade every worker, then
resume dispatch so no v1-only worker can claim newly written v2 checkpoints.
Older binaries reject v2. Once a v2 checkpoint is written, rollback requires draining
those executions or restoring an application-owned backup; never downgrade a
payload or edit its version/hash manually. Subsequent versions must retain closed
per-version decoding, golden fixtures, explicit conversion, atomic hash updates,
and a documented rollback path.

Live input receipts succeed only after the runtime's checkpoint observer succeeds.
The process-local pending queue is not durable. A failed receipt after a response
loss can coexist with a committed checkpoint; inspect recorded Messages before
resending. Unknown model/tool effects still require reconciliation. See
[execution control](agent-execution.md#input-during-execution).

Completed model responses with malformed raw JSON arguments use model-attempt
envelope v2, storing the original argument text separately from JSON fields.
Replay restores the exact event kind and text; it must not reinterpret a full
call as a delta. Ordinary model/tool attempts still use envelope v1; readers
accept both model envelope versions. Older workers reject v2, so upgrade all
workers sharing an execution queue before writing these attempts. Continuation
schema remains v2 and its v1 canonical compatibility is unchanged.

Optional Jev context scores use ordinary tool attempts and require no new backend
record kind or schema. Settled selections replay without contacting Jev. Their
API key lives only in runtime dependencies, never in tool arguments, scores or
continuations. Auxiliary model usage is carried by the structured tool result.

## Effect attempts

Each model or tool effect uses a stable logical operation ID:

```text
turn:<n>:model
turn:<n>:call:<i>
```

`StartAttempt` compares the operation kind and canonical input hash and returns one decision:

- `execute` — this attempt may invoke the effect.
- `replay` — a succeeded or failed outcome already exists.
- `reconcile` — the outcome is unknown.

Attempts have their own numbers and versions so parallel tool slots do not contend on execution checkpoint versions.

### Known outcome

The provider wrapper persists the complete event sequence after receiving the first legal terminal event and before returning that terminal event to the Agent. A tool outcome is persisted before its result returns through the bus.

Only `provider.ErrNotStarted` and `tool.ErrNotExecuted` prove that an operation did not begin. Such failures may be stored as known failed outcomes.

### Unknown outcome

An effect is unknown when it may have started but no terminal outcome can be proven. Examples:

- a model stream fails before a terminal event
- a model stream is closed early
- a tool returns an ordinary error after it may have executed, including after a transient update
- settlement itself cannot be confirmed

The runtime records `unknown` when possible. Release and later claims also convert stranded running attempts to unknown. An execution with a running or unknown attempt cannot finish.

## Reconciliation

Unknown effects require an application decision:

```go
err := runtime.Reconcile(ctx, executionID, operationID, durable.Reconciliation{
    AttemptNumber:  attempt.Number,
    AttemptVersion: attempt.Version,
    Resolution:     durable.ReconcileResolutionRetry,
})
```

Resolution meanings:

- `succeed` — persist caller-supplied terminal model events or tool output.
- `fail` — persist a failure and an optional valid partial outcome.
- `retry` — abandon the unknown attempt; the next resume creates exactly one higher attempt number.

Model success requires a complete conformant terminal event sequence. Model failure may contain a conformant prefix. Tool result name and call ID must match the checkpointed call; empty identity fields are filled by the runtime, conflicting values are rejected.

Reconciliation never runs the Engine and never claims the execution. The application calls `Resume` explicitly afterward.

## Completion and errors

The runtime commits terminal status only when the Engine result contains no runtime infrastructure failure and every effect is settled. Ordinary `agent.AgentFailure` remains result data and produces a failed terminal execution.

Infrastructure paths return a Go error and the partial `agent.Result` available at that point. Callers must treat a result as a terminal durable outcome only when the error is nil.

## Cancellation, suspension, and close

Caller cancellation is not implicit completion or suspension. The runtime uses a cancellation-independent, bounded settlement context to converge in-flight attempts, then releases the execution as resumable running state.

`Suspend` acts only on an execution active in the same runtime. It prevents new effects, waits for settlement within the caller deadline, atomically persists suspended status, and releases the lease. The stopped Start or Resume returns `ErrSuspended`.

`Close(ctx)` is idempotent. It rejects new work with `ErrClosed`, stops active loops, settles effects within bounds, and releases claims. Unreleased claims remain protected by expiry and fencing.

## Streaming

Streaming output is a transient side channel. A provider attempt persists its complete terminal event sequence, so replay for an unfinished execution may emit provider-derived frames again.

A tool attempt persists only its normalized final result or failure. `FrameToolUpdate` is never written to an attempt, continuation, or checkpoint. Tool settlement completes before the final `FrameToolResult` is delivered. If a settled tool attempt replays after process reopen, the Agent emits that final result without reconstructing historical updates. Replaying an already terminal execution emits no frames.

Applications that require durable progress must persist a separate idempotent projection and define their own acknowledgement and retention semantics.

## Backend verification

Run `durable/contract.RunBackendContractTests` against every backend and every reopen mechanism. See [Backend and extension development](plugin-development.md) and [`examples/durable`](../examples/durable).
