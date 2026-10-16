# ADR-031 Core Agent SDK Consolidation — Removing `orchestration` and `durable`

## Status

Accepted — 2026-10-16.

Supersedes the live guidance in [ADR-029](ADR-029-agent-sdk-and-optional-durable-runtime.md)
and [ADR-030](ADR-030-stream-lifecycle-semantics.md) where they prescribe the
top-level `orchestration/` and `durable/` packages (including `durable/contract`
and `durable/internal/testbackend`). Historical ADRs remain unchanged as
decision history.

## Context

ADR-029 reduced Venat from a platform runtime to seven package families:
`message`, `provider`, `tool`, `skill`, `agent`, `orchestration`, and `durable`.

Real production integration in downstream systems (such as Elpis) demonstrated
that the five core Agent packages (`message`, `provider`, `tool`, `skill`, and
`agent`) are the only SDK packages consumed in practice:

1. **Production hosts already own durability and domain state.** Real
   multi-tenant applications maintain their own database transactions, run/task
   state machines, lease epochs, Cedar/RBAC authorization, S3/object-store
   artifacts, and audit outboxes. Adopting `durable.Runtime` and implementing the
   12-method `durable.Backend` contract creates a competing state machine and a
   second source of recovery truth.
2. **Production hosts already own task delegation and scheduling.** Real
   subagent delegation requires tenant-scoped persistence, fencing, and
   artifact-backed inputs/outputs rather than `orchestration.Runtime`'s
   in-memory goroutine pool or `orchestration.Drive`'s batch loop.
3. **`agent` already owns the necessary recovery and interception primitives.**
   `agent.Continuation` (strict v1/v2 canonical codec and `ValidateContinuation`),
   `agent.BoundaryObserver`, deterministic `OperationID` assignment
   (`turn:%d:model`, `turn:%d:call:%d`), `provider.StreamInterceptor`
   (`provider.ErrNotStarted`), `tool.Interceptor` (`tool.ErrNotExecuted`),
   `agent.Control`, and `agent.NewAgentTool` all live in the five core packages
   without depending on `orchestration` or `durable`.
4. **Maintaining `orchestration` and `durable` in the core module adds defect
   surface and migration noise.** Reconciling and replaying effects outside
   `agent`, or reverse-engineering `agent.Continuation` from `agent.Result` in
   `orchestration`, coupled SDK releases to unused state-machine protocols.

## Decision

### 1. Exhaustive five-package capability graph

Venat's production capability graph is consolidated to five top-level package
families:

- `message/` — provider-neutral messages, content parts, tool calls, tool results, and JSON resource bounds
- `provider/` — streaming model driver contract, interceptors, conformance normalization, retry/failover helpers, and provider adapters
- `tool/` — typed tool drivers, bus, validation, update sinks, output store, interceptors, and `tool/kit` helpers
- `skill/` — reusable instruction bundles, discovery, and confined resource loading
- `agent/` — bounded Agent loop, working memory, hooks, guardrails, streaming frames, live control, synchronous `NewAgentTool`, continuation codec, and resume

The dependency graph remains strictly one-way:

```text
message      skill
   │           │
   ├── provider│
   └── tool    │
        \      /
          agent
```

### 2. Removal of `orchestration` and `durable`

- Delete `orchestration/`, `durable/`, `examples/orchestration/`,
  `examples/durable/`, `examples/async-agents/`, and `docs/durable-execution.md`.
- No compatibility shims, aliases, or stub packages remain.
- Applications own multi-agent scheduling, asynchronous worker pools, lease
  fencing, effect-attempt persistence, and unknown-effect reconciliation above
  the `agent`, `provider`, and `tool` seams.

### 3. Architecture gate updates

- `scripts/publicany/main.go`, `scripts/check-business-words.sh`, and
  `scripts/check-import-boundaries.sh` enforce the five required scopes
  (`message`, `provider`, `tool`, `skill`, `agent`) fail-closed.
- `scripts/check-legacy-absence.sh` treats `orchestration` and `durable` as
  removed directories and rejects their import paths in production code and
  current documentation.

## Consequences

- Downstream applications that import `message`, `provider`, `tool`, `skill`,
  and `agent` require zero code changes.
- Single-Agent continuation persistence (`agent.Continuation`,
  `EncodeContinuation`, `DecodeContinuation`, `ValidateContinuation`,
  `BoundaryObserver`) and effect interception (`provider.StreamInterceptor`,
  `tool.Interceptor`, `OperationID`, `ErrNotStarted`, `ErrNotExecuted`) remain
  first-class contracts in the core SDK.
- Multi-agent orchestration and durable execution state machines are owned by
  consuming applications or separate ecosystem modules where they can bind
  directly to real storage and tenancy models.

## Verification

- `make verify` and `make ci-local` pass with the five-scope architecture gates.
- All remaining executable examples under `examples/` pass.
