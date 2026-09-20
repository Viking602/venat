# Extension seams

Venat favors explicit, narrow extension points. An extension should preserve input ownership, cancellation, deterministic ordering, and the error semantics of its seam.

## Provider interceptors

`provider.StreamInterceptor` wraps one model request. It may invoke `next` zero or one time. The chain rejects multiple calls, mutation of the request, and panics. A zero-call error gains `provider.ErrNotStarted` proof.

Use provider interceptors for transport-neutral concerns that belong exactly around a model effect, such as metrics, request signing, or an application idempotency header.

`provider.OpenRetryingStream` is a transport helper rather than a logical Agent retry. A custom `ShouldRetry` policy may approve only open and pre-first-event failures. Once output exists, `PartialStreamError` makes replay an explicit reconciliation concern.

## Tool interceptors

`tool.Interceptor` wraps one tool call with the same immutable-input and zero-or-one-call contract. A zero-call error gains `tool.ErrNotExecuted` proof.

Use tool interceptors for application authorization, audit, or effect-specific controls. The core does not assign policy meaning to tool metadata.

## Tool update sinks

`tool.UpdateSink` is a synchronous push callback inside one `Driver.Execute`. Emit `UpdateProgress` for message/data state and `UpdateOutput` for non-empty content parts. Return callback errors immediately; the Bus also records the first failure, cancels the driver child context, and returns it if the driver ignores it.

The Bus owns update identity and sequence, clones mutable fields, enforces 65,536-update/64-MiB limits, and serializes a shared sink in parallel batches. Streamed output must agree with terminal `Result.Parts`. `ErrToolUpdateProtocol` and `ErrToolUpdateLimit` occur after execution starts and do not prove `ErrNotExecuted`.

## Hooks

`agent.HookChain` runs hooks in caller order. Hooks can observe or deterministically transform supported lifecycle values. Each hook receives an ownership-independent value; panic becomes a typed Agent failure.

Do not assume hook side effects are covered by `durable.Runtime`. Persist them independently or make them idempotent.

Application-owned approval can compose a `BeforeToolCall` hook, `durable.Runtime.Suspend`, a separately persisted decision keyed by execution and operation ID, and a tool driver wrapper. Resume with checkpoint/operation assertions; do not encode approval state in Agent continuation.

## Step decisions and observation

`agent.StepDecider` receives an immutable loop snapshot at a continue boundary and may choose only continue, finish, or fail. It does not route another Agent.

`agent.StepObserver` receives each finalized step before the loop advances. Observer failure aborts the loop and remains visible in the Agent failure chain.

## Output guardrails

Output guardrails receive terminal output and return allow, replace, retry, or block. Configure strict budgets for repair loops. The observer receives every non-allow decision.

Guardrails validate or transform output; authorization and business approval remain outside the Agent loop.

## Continuation boundaries

`agent.BoundaryObserver` synchronously receives complete continuations at safe phases. Observers are joined in order and stop on the first error. Durable execution appends its observer after caller observers.

A boundary observer must not edit the continuation or persist a partial merge. Use a complete replacement record with a content hash.

## Output sinks

`agent.Sink` receives transient frames. `agent.Broadcast` serially fans out to multiple sinks; `agent.Accumulator` folds provider-derived frames and final tool results while deliberately ignoring `FrameToolUpdate`.

Sink calls apply synchronous backpressure. Errors abort the current execution path. Frames, including tool updates, are not a durable event log; persist an application projection separately when durable progress is required.

## Context managers

Context managers prepare message history before model calls. A deterministic manager keeps uninterrupted and resumed execution equivalent. External model calls or writes inside context preparation require application-owned idempotency because the durable effect ledger covers only provider and tool boundaries.

With the built-in context builders and a positive `ContextTokenTarget`, the
Agent fits only the provider-facing view and retains the full transcript.
Custom managers retain their existing `Compact`/`CompactTo` behavior; they must
preserve pending user input, complete exchanges, and skill/cache context. Use a
model-aware `CompactTo` for media. See [Agent execution](agent-execution.md).

## Schedulers and executors

`orchestration.Scheduler` is the extension point for application routing policy. It receives a cloned state and returns opaque dispatches. `orchestration.Executor` maps each route to concrete execution.

Schedulers should be deterministic for a persisted state. Executors return Agent failures as `agent.Result` data and infrastructure failures as Go errors.

## Durable backends

`durable.Backend` is the only persistence extension in the core. Implementations must pass the published conformance suite. The runtime owns attempt codecs; backends persist payload bytes without interpreting provider or tool data.
