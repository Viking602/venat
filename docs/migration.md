# Migration to the direct Agent SDK

ADR-029 is a breaking clean cutover. The former platform façade and platform-owned storage, worker, collaboration, policy, session, transport, bundle, sandbox, evaluation, and command packages were removed. No compatibility aliases or deprecated shims remain.

This document may name deleted APIs for migration purposes. Current API guidance lives in [Public API](public-api.md).

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
- New checkpoints use v2; legacy v1 hashes stay readable. Follow the coordinated
  reader/writer and rollback procedure in [Durable execution](durable-execution.md#wire-version-and-rollout).
- `Request.SessionBudget` can carry cumulative token and active wall-clock limits
  through continuation and durable resume. `Request.ModelTimeouts` adds Codex-style
  connection and stream-idle defaults; set `DisableDefaults` when an application
  needs only explicit deadlines. `orchestration.DriveOptions` accepts whole-drive
  and per-dispatch wall-clock limits.

## Package migration map

| Removed surface | Current composition |
| --- | --- |
| root `venat.Runner` and `NewDevelopment` | import `agent`, `orchestration`, and optional `durable` directly |
| `api.Task` | `agent.Request` |
| `api.TaskBudget` | `agent.Budget` |
| `hook.Handler` / `hook.Chain` | `agent.Hook` / `agent.HookChain` |
| `stream.Frame` / `stream.Sink` | `agent.Frame` / `agent.Sink` |
| `multiagent` scheduling types | policy-free `orchestration.Scheduler`, `Dispatch`, `State`, `Executor`, and `Drive` |
| `api.StoreProvider`, `api.UnitOfWork`, and table stores | execution-semantic `durable.Backend` when single-Agent durability is required |
| `agent.Harness` and `session.Storage` | one `agent.Engine` loop with `agent.Continuation`; optionally wrap it in `durable.Runtime` |
| turn checkpoint callbacks | `agent.BoundaryObserver` and `agent.Continuation` |
| step routing that included handoff | `agent.StepDecider` for continue/finish/fail; put peer routing in an application scheduler |
| built-in Agent-as-tool helpers | use `agent.NewAgentTool` for synchronous in-process delegation; use an application `tool.Driver`, `orchestration.Executor`, or coordinator for registries, workflows, process isolation, or independently durable children |
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

## Orchestration migration

Define application routing data as opaque strings or JSON and implement:

```go
type Scheduler interface {
    Next(context.Context, orchestration.State) ([]orchestration.Dispatch, error)
}

type Executor interface {
    Execute(context.Context, orchestration.Dispatch, agent.Sink) (agent.Result, error)
}
```

`orchestration.Drive` supplies bounded mechanical execution, not identity or strategy. Persist `orchestration.State` in application storage if scheduling must survive a restart. Use globally stable dispatch IDs; the SDK rejects reuse across ticks.

## Durability migration

Do not map every former table into `durable`. Implement only the twelve execution-semantic backend operations in `durable.Backend` and run `durable/contract.RunBackendContractTests`.

The durable data model contains:

- immutable execution spec and canonical hash
- status, version, fenced lease, checkpoint, and terminal Agent result
- model/tool attempts with input hash, independent version, opaque payload, and failure fact

Typical flow:

```go
runtime, err := durable.New(backend, durable.Options{OwnerID: processID})
if err != nil {
    return err
}
result, err := runtime.Start(ctx, executionID, engine, request, outputPolicy)
```

On process recovery, construct a new runtime over a reopened backend handle and call `Resume`. A typed `durable.ReconcileRequiredError` means a provider or tool may have executed without a proven result. Investigate externally, call `Reconcile` with `succeed`, `fail`, or `retry`, then explicitly call `Resume` again.

Controllers that previously resumed through application tokens should persist their own decision record, load the latest checkpoint, and call `ResumeWithOptions` with sequence, phase, and operation assertions. The SDK does not store approval policy or decision payloads in continuation state.

Do not automatically retry an unknown effect. Only `provider.ErrNotStarted` and `tool.ErrNotExecuted` prove that no effect began.

Version `1` is the first supported continuation wire format. Before enabling a v1 writer, drain pre-contract candidate executions, restart them under new execution IDs, or convert them offline with application knowledge; the SDK does not guess a v0 representation. Do not mix v1 writers with binaries that do not understand v1. After the first v1 checkpoint, rollback requires restoring application storage or a pre-write rollback point.

## Backend cutover checklist

- [ ] Choose an application execution ID namespace and revision strategy.
- [ ] Implement trusted-time lease expiry and monotonic fencing tokens.
- [ ] Implement exact claim and mutation replay.
- [ ] Keep execution-version and attempt-version CAS independent.
- [ ] Validate every save, load, and reopen checkpoint with `durable.ValidateCheckpoint`.
- [ ] Convert stranded running attempts to unknown on release, expiry, suspend, and later claim.
- [ ] Reject finish while an attempt is running or unknown.
- [ ] Implement all three explicit reconciliation resolutions.
- [ ] Return ownership-independent values.
- [ ] Pass the complete backend conformance suite, including reopen and concurrency cases.

## Source cleanup checklist

- [ ] Replace root-module imports with direct package imports.
- [ ] Remove platform record conversion code and broad storage adapters.
- [ ] Replace old hook and stream package imports with Agent equivalents.
- [ ] Move routing, identity, approval, quota, and deployment policy into the application.
- [ ] Replace former recipe or bundle dependencies with application modules.
- [ ] Run `go mod tidy` to remove obsolete protocol and scheduler dependencies.
- [ ] Run `go test ./...`, race tests, and backend conformance tests.

## Semantic differences to preserve

1. A non-nil `agent.Result.Failure` is terminal data, not an infrastructure error.
2. `durable.Runtime` returns partial results on infrastructure errors; a result is terminal only when the Go error is nil.
3. Streaming is transient and not an exactly-once event log.
4. Durable execution covers provider and tool effects only. Hook, observer, guardrail, context-manager, and sink side effects need their own idempotency.
5. Durable state covers one Agent execution. Application orchestration state remains separate.

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
- Malformed full-call model attempts write envelope v2. Upgrade shared workers
  before use; older readers reject this envelope. Ordinary attempts and existing
  continuation canonical encodings are unchanged. See the durable rollout notes.
