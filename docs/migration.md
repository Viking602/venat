# Migration to the direct Agent SDK

ADR-031 and ADR-029 define the direct Agent SDK boundary. The former platform façade, storage providers, worker runtime, and the top-level `orchestration` and `durable` packages were removed. No compatibility aliases or deprecated shims remain.

This document may name deleted APIs for migration purposes. Current API guidance lives in [Public API](public-api.md).

## Security boundary changes

HTTPTool and the `openai`/`anthropic` provider drivers confine redirects even
with a custom client; `kit.Tool` rejects recursive input types; provider,
continuation, and skill parsing enforce fixed resource bounds. See
[Security hardening](security-hardening.md) for exact limits.
## Execution-completeness changes

- Opt into provider JSON Schema output with `OutputPolicy.Native`; existing local
  validation/repair policies keep their default behavior. Invalid enabled schemas
  now fail before effects, and repair checkpoints record a continue decision.
- ProcessTool confirmed nonzero exits are completed error results with logs and
  exit status. Code that expected every nonzero exit to abort must inspect
  `Result.IsError` or supply its own wrapper. Confirmed signals with an active
  context and ordinary missing/permission-denied launch failures are feedback;
  cancellation and infrastructure errors remain errors. Oversized logs are
  truncated and drained, with a notice, instead of killing the process.
- `kit.Tool` preserves explicit `tool.Result` outputs and recognizes a confirmed
  `exec.ExitError` (including single-chain wrapping) as feedback. Joined errors
  and uncertain failures remain fatal. Go argument conversion errors are feedback
  before the function runs.
- `Bus.Execute` returns an error result for unknown names instead of
  `ErrToolNotFound`; inspect `Result.IsError`. Unknown names and other rejected
  calls count toward the Agent call budget. No unregistered driver is executed.
- Terminal tools end an execution only on successful results. A failed terminal
  call now returns control to the model; explicit step policies and budgets still
  apply. See [error boundaries](agent-execution.md#recoverable-feedback-and-fatal-boundaries).
- A positive context target with built-in builders now fits the provider view;
  irreducible or media context needs an explicit model-aware compactor. The full
  transcript remains available for results and recovery.
- `Request.Content` adds typed user media; old Request composites using positional
  fields should be changed to named fields. Mutable media is cloned at boundaries.
- Use one fresh Control per execution. Queued input and persistent acknowledgement
  have different guarantees; see [Agent execution](agent-execution.md).
- New continuations use v2; legacy v1 canonical bytes stay readable. Follow the
  continuation wire rollout procedure in [Agent execution](agent-execution.md#continuation-wire-version-and-rollout).
- `Request.SessionBudget` can carry cumulative token and active wall-clock limits
  through continuation and resume. `Request.ModelTimeouts` adds Codex-style
  connection and stream-idle defaults; set `DisableDefaults` when an application
  needs only explicit deadlines.

## Package migration map

| Removed surface | Current composition |
| --- | --- |
| root `venat.Runner` and `NewDevelopment` | import `agent`, `provider`, `tool`, `message`, and `skill` directly |
| `api.Task` | `agent.Request` |
| `api.TaskBudget` | `agent.Budget` |
| `hook.Handler` / `hook.Chain` | `agent.Hook` / `agent.HookChain` |
| `stream.Frame` / `stream.Sink` | `agent.Frame` / `agent.Sink` |
| `multiagent` and `orchestration` scheduling types | application-owned schedulers and worker pools |
| `api.StoreProvider`, `api.UnitOfWork`, and `durable` | application-owned persistence using `agent.Continuation`, `agent.BoundaryObserver`, `provider.StreamInterceptor`, and `tool.Interceptor` |
| `agent.Harness` and `session.Storage` | one `agent.Engine` loop with `agent.Continuation` and `Engine.Resume` |
| turn checkpoint callbacks | `agent.BoundaryObserver` and `agent.Continuation` |
| step routing that included handoff | `agent.StepDecider` for continue/finish/fail; put peer routing in an application scheduler |
| built-in Agent-as-tool helpers | use `agent.NewAgentTool` for synchronous in-process delegation; use an application `tool.Driver` for registries, workflows, process isolation, or persistent children |
| MCP, cron, webhook, sandbox, bundle, and evaluation integrations | application or ecosystem adapters over current interfaces |

Platform concepts such as application identity, approvals, quotas, resource claims, mailboxes, registries, pricing, tracing, triggers, deployment, and domain manifests no longer have core replacement symbols. Keep those models in the application that owns their policy and persistence.

## Before: platform-wide construction

Earlier applications created a root object, supplied a broad store provider, persisted task records, and invoked lifecycle commands through that façade. This forced even a single model/tool loop to adopt unrelated platform records.

## After: direct Agent execution

```go
engine := agent.Engine{
    Provider: modelDriver,
    Tools:    tool.NewBus(toolDrivers...),
    Model:    modelName,
    ToolMode: tool.ModeSequential,
}

result := engine.Run(ctx, agent.Request{
    Prompt: prompt,
    Budget: &agent.Budget{
        MaxTokens:    tokenLimit,
        MaxToolCalls: toolLimit,
        MaxSteps:     stepLimit,
    },
}, agent.OutputPolicy{})
```

Update behavior at the same time:

- inspect `result.Failure` as terminal Agent data
- use the Go error boundary of the host or durable runtime for infrastructure failure
- replace shared stream imports with `agent.Sink` and `agent.Frame`
- handle `agent.FrameToolUpdate` as transient progress/content and keep only the final tool result in Agent history
- replace ad hoc string update kinds with `tool.UpdateProgress` or `tool.UpdateOutput`; output updates require content parts
- move approval and retry decisions outside `agent.Engine`
- do not recreate deleted task or run records merely to call an Agent

## Persistence and orchestration ownership

Multi-agent scheduling (`orchestration`) and execution persistence state machines (`durable`) live in the consuming application or separate adapter modules, where they can integrate directly with the host's database transactions, tenancy, leases, and artifact stores.

Use `agent.BoundaryObserver`, `agent.EncodeContinuation` / `agent.DecodeContinuation`, `provider.StreamInterceptor` (`provider.ErrNotStarted`), and `tool.Interceptor` (`tool.ErrNotExecuted`) as the SDK seams for persistence and replay.

## Source cleanup checklist

- [ ] Replace root-module imports with direct package imports (`agent`, `provider`, `tool`, `message`, `skill`).
- [ ] Remove platform record conversion code and broad storage adapters.
- [ ] Replace old hook and stream package imports with Agent equivalents.
- [ ] Move routing, identity, approval, quota, orchestration, and persistence state machines into the application.
- [ ] Run `go mod tidy` to remove obsolete dependencies.
- [ ] Run `go test ./...` and race tests.

## Semantic differences to preserve

1. A non-nil `agent.Result.Failure` is terminal data, not an infrastructure error.
2. Streaming is transient and not an exactly-once event log.
3. Only `provider.ErrNotStarted` and `tool.ErrNotExecuted` prove that an effect did not begin.
## Bounded response recovery and explicit Jev capability

- Completed invalid tool JSON, empty final output and output-length cutoff now
  request correction (at most three retries, within existing budgets). Invalid
  response batches execute no tools. Repeated incompleteness is `repair_failed`.
  Missing/conflicting call IDs and broken/interrupted streams remain fatal.
- Confirmed `ProcessTool` local deadlines become error results on Darwin/Linux;
  parent deadlines, cancellation and unknown effects keep their prior semantics.
  Oversized logs retain head and tail; HTTP oversize becomes bounded error feedback
  with its status and no Structured payload. Neither behavior proves no effects ran.
- `ContextBuilderFunc` now participates in default fitting when a target is set.
  Canonical contents replace duplicate legacy mirrors in estimates. Oversized plain
  tool output may be shortened only in the model view; transcript bytes remain.
- Jev is disabled by default. Explicit `BuildDeps.ContextSelection` plus a named
  `select_context` tool exposes advisory Noul scores, not automatic compaction.
  Set protocol, BaseURL, APIKey and Model; no environment/default endpoint is used.
- Existing continuation canonical encodings (v1 and v2) are unchanged.
