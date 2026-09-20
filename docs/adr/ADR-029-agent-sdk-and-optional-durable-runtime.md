# ADR-029 Agent SDK and Optional Durable Runtime

## Status

Accepted — 2026-08-28.

Amended — 2026-08-29 to add the synchronous Agent-as-tool seam. This ADR
remains the live architecture authority for the breaking cutover from the
platform-shaped v0.16 candidate to a small Agent SDK with optional
orchestration and durability.

Amended — 2026-09-20 to add typed user content, native structured-output hints,
default provider-context fitting, and execution-scoped input/cancellation.
These remain Agent mechanisms; application integration is a separate phase.

This decision supersedes the live recommendations from ADR-017, ADR-020,
ADR-024, ADR-025, ADR-027, and ADR-028 where they prescribe the root Runner,
the five-layer platform, Harness/session persistence, or platform storage
contracts. It also supersedes the identity and collaboration placement from
ADR-026. Those ADRs remain unchanged as historical records.

## Context

Venat's root `Runner` delegates through a platform runtime, while `api` storage
contracts, worker integration, identity, policy, collaboration, and recovery
concepts reach into the Agent loop. The result is not a minimal Agent SDK:
applications must adopt framework-owned business concepts to call a model and
a tool, and backends must implement table-shaped stores rather than execution
semantics.

The Agent loop, multi-agent mechanics, and crash recovery are useful as three
independent capabilities. Identity, approval, policy, workflow, routing,
resource management, registries, shared workspaces, and business state are
application decisions. The package graph must make that separation executable
instead of documenting exceptions to it.

## Decision

### Product boundary

Venat consists of three independently usable parts:

1. `agent` executes one Agent request: context construction, hooks, provider
   calls, tool calls, loop control, output validation, streaming, budgets, and
   stable effect operation IDs.
2. `orchestration` provides policy-free scheduling protocols and a mechanical,
   bounded executor. Applications supply routing and supervision policy.
3. `durable` resumes one Agent execution after process failure through an
   injected execution-semantic backend. Durability is optional.

Applications own why work executes: identity, authorization, approval,
supervision, routing, quotas, resources, workflow, agent registries, business
messages, and shared workspaces.

### Exhaustive public package graph

The normative top-level capability directories are:

- `agent/`
- `provider/`
- `tool/`
- `orchestration/`
- `durable/`
- `message/`
- `skill/`
- `examples/`

Repository support directories such as `docs/` and `scripts/`, and the nested
`durable/contract` conformance package, are not additional product layers.

Allowed production dependencies are:

```text
application ───────→ agent
      ├────────────→ orchestration ─→ agent
      └────────────→ durable ───────→ agent/provider/tool/message
external adapter ─→ durable.Backend
agent ─────────────→ provider/tool/skill/message
provider/tool ─────→ message
```

`message` imports no other Venat package. `provider` and `tool` may import only
`message`. `skill` remains a leaf capability. `agent` may import `message`,
`provider`, `tool`, and `skill`, but not `orchestration` or `durable`.
`orchestration` may import `agent` and `message`, but not `durable`. `durable`
may import `agent`, `provider`, `tool`, and `message`, but not
`orchestration`. Only `examples` may compose all three product parts inside
this repository.

### Symbol cutover inventory

This is a clean cutover. Every caller moves in the same change; old import
paths, aliases, deprecated shims, and public development backends do not
remain.

| Old surface | New surface or disposition |
| --- | --- |
| Root `venat.Runner` and domain façades | Delete; applications import and compose the target packages directly |
| `api.Task` | `agent.Request` with `Prompt`, typed user `Content`, and optional `Budget` |
| `api.TaskBudget` | `agent.Budget` |
| Platform task/run IDs, status, identity, and agent definitions | Delete from the SDK; application-owned |
| `multiagent` | Replace with policy-free `orchestration` protocols |
| `hook.Handler`, `hook.Chain`, `hook.NewChain` | `agent.Hook`, `agent.HookChain`, `agent.NewHookChain` |
| `stream.Frame`, `Sink`, `SinkFunc`, `Broadcast`, `Accumulator` | Same concepts in `agent` |
| `api.StoreProvider`, `api.UnitOfWork`, and per-table stores | Replace with execution-semantic `durable.Backend` |
| `agent.Harness`, `OpenHarness`, `Operation`, `RunState`, `session.Storage` | Delete; use the single `agent.Engine` loop plus `agent.Continuation` and optional `durable.Runtime` |
| `TurnCheckpoint`, `CheckpointRecorder` | `agent.Continuation`, `agent.BoundaryObserver` |
| `StepPolicy.Next` and handoff decisions | `StepDecider.Decide`; continue, finish, or fail only |
| `StepRecorder` | `StepObserver.ObserveStep` |
| `OutputGuardrailRecorder` | `OutputGuardrailObserver.ObserveOutputGuardrailDecision` |
| `AgentFailure.Retryable`, `Escalatable` | Delete; applications decide from kind, reason, and error chains |
| `FailureKindUnsafeAction` | Factual `FailureKindOutputBlocked` |
| `AsTool`, `SubagentScheduler`, `tool.CallerInfo`, and subagent identity or governance surfaces | Delete; `agent.NewAgentTool` is a new synchronous adapter over a materialized Engine and does not restore the old names or policy |
| Tool approval, permissions, policy, risk, effect, idempotency, retry, origin, and tags | Delete; applications enforce policy before the effect |
| `provider.NativeToolHost` | Delete; provider tool calls return to the Agent and execute through `tool.Bus` |
| Root, `api`, `worker`, `policy`, `session`, `transport`, `packs`, `coding`, `eval`, `cmd/venat`, old `contract`, root `internal`, `multiagent`, `hook`, `stream`, and `_examples` | Delete after retained callers and tests move |

Symbols for admission, quota, resource claims, workflow, blackboard, mailbox,
outbox, approval, registries, pricing, tracing, triggers, MCP adapters, and
business manifests are deleted rather than renamed into the target packages.

### Agent continuation and effect seams

`agent.Engine` has one loop for ordinary and durable calls. `Request` and
`Budget` replace platform tasks. `Continuation` is a complete, JSON-encodable
snapshot validated before `Resume`; invalid durable snapshots fail as corrupt
rather than being guessed or repaired. Synchronous boundary observation occurs
before each externally significant transition and fails closed before the next
effect.

`agent.Control` belongs to one execution and accepts only authorized user input.
It has a bounded in-memory queue, acknowledges consumption after safe boundary
observers succeed, and cancels the execution context. It is not a registry or
mailbox, cannot inject peer/tool output, and does not promise exactly-once input
delivery. Applications own authorization and routing to the right handle.
Default context fitting retains the full recovery transcript and trims only the
provider-facing view. See [Agent execution](../agent-execution.md) and the
[continuation wire contract](../durable-execution.md) for compatibility details.

Each logical provider request and tool call carries a stable `OperationID`.
Provider and tool interceptors may call their next effect zero or one time and
may not mutate the operation ID or effect input. They report only whether an
effect is known not to have started; every ambiguous post-dispatch failure is
unknown. The optional durable runtime is the first consumer of these seams and
wraps caller interceptors outermost.

### Synchronous Agent-as-tool seam

`agent.NewAgentTool` wraps an already configured child `Engine` as one
non-terminal `tool.Driver`. The default input is a strict `{task: string}`
object. A custom input schema passes the complete JSON arguments to the child
as its prompt. The adapter does not copy the parent's messages, thinking,
steps, tool results, or output policy into the child.

The child returns one ToolResult to the parent. Typed Agent failures remain
error ToolResult data, while cancellation and update-sink failures return Go
errors after child usage is settled. Child frames become progress-only tool
updates: thinking, partial arguments, child tool output, and child result
payloads do not cross the boundary. Nesting defaults to four levels and honors
the strictest ancestor limit. Recursive provider usage is added to the nearest
parent loop once. A parent token ceiling serializes AgentTool children against
one shared remaining-token pool; without that ceiling, standard Tool
Definition concurrency applies.

Applications still own Agent registries, identity, routing, parallel or chained
workflows, background tasks, and process isolation. Inside a `durable.Runtime`,
an AgentTool call is one ordinary parent tool attempt. A crash that leaves the
outer attempt ambiguous becomes `unknown` and requires application
reconciliation; the SDK does not replay the child silently. It provides no
child checkpoint, background/query/kill API, or independent `ExecutionID`.
`agent` still does not import `durable`.

### Orchestration seam

`orchestration.Scheduler` is a pure function of cloned `State` and returns
opaque routed `Dispatch` values. `Executor` executes one dispatch. `Drive`
provides validation, bounded ticks, bounded concurrency, cancellation,
deterministic folding, panic containment, and partial-state infrastructure
errors. Agent failures remain result data. The package supplies no supervisor,
router, team model, workflow policy, blackboard, persistence, or reference
scheduler.

### Durable seam

`durable.Backend` exposes execution verbs only: claim and renew leases, save
or resume checkpoints, suspend and finish executions, and start, settle, mark
unknown, or reconcile provider/tool attempts. It exposes no transactions,
tables, sub-stores, schemas, connections, capability probes, or ownership
lifecycle.

The backend contract requires canonical hashes, exact-retry idempotency,
monotonic fencing and versions, backend-trusted lease time, checkpoint
replacement rather than merging, and per-attempt compare-and-swap. A finished
execution cannot contain running or unknown attempts. Ambiguous effects require
explicit application reconciliation; the runtime never hides business review
or retry policy in a background scanner.

The repository publishes backend conformance tests and a private test backend,
not a public in-memory implementation. Production Postgres, Temporal, Redis,
or application-specific adapters live outside this module and import the
contract.

### Documentation and enforcement

Historical ADRs, versioned product specifications, and release notes remain
unaltered. Live documentation points only to this architecture; the migration
guide may name deleted symbols solely to explain their replacement.

Import-boundary, business-word, public-API, and absence gates fail closed when a
required package is missing or empty. CI compiles external temporary modules
that consume the direct Agent SDK and an injected durable backend, so deleting
old directories cannot create a false-green architecture check.

## Anti-patterns rejected by this ADR

| Anti-pattern | Why it is wrong |
| --- | --- |
| Keeping a thin root Runner | It recreates a privileged composition layer and hides the actual package graph |
| Renaming every platform store into `durable` | CRUD tables do not express execution, fencing, or ambiguous-effect semantics |
| A second durable Agent loop | Ordinary and durable execution would diverge in hooks, tools, budgets, and output behavior |
| Built-in supervisor or router policy | Routing values are opaque application data, not SDK identity |
| Treating transport errors as proof of no effect | A request may have reached the provider or tool before the error arrived |
| Public memory backend | It becomes an accidental reference schema and weakens the Position D boundary |
| Compatibility aliases and deprecated import paths | They preserve the old graph and make the breaking boundary unenforceable |
| Empty package checks passing | Deleted scopes must fail the gate, not silently escape it |

## Impact

A basic application imports only `agent`, `provider`, `tool`, `message`, and
optionally `skill`. Applications add `orchestration` only for mechanical
multi-Agent dispatch and `durable` only for resumable execution. Backends can
implement a stable execution contract without adopting application tables.

The change intentionally breaks every root Runner, `api`, worker, Harness,
session, and multiagent consumer. Migration requires direct composition. The
smaller API and enforced package graph remove compatibility weight and prevent
platform policy from returning through implementation convenience.

## References

- ADR-008 framework versus business boundary
- ADR-012 storage contract and Position D
- ADR-015 strong bounded Agent loop
- ADR-016 explicit multi-Agent scheduler
- ADR-017 durable Runner boundary (superseded live guidance)
- ADR-020 v0.15 architecture program (superseded live guidance)
- ADR-024 five-layer architecture (superseded)
- ADR-025 Runner façade slimming (superseded)
- ADR-026 identity and collaboration types (superseded placement)
- ADR-027 package map and ArtifactStore (superseded)
- ADR-028 Agent Harness and session store (superseded)
- `docs/architecture-boundaries.md`
- `docs/migration.md`
