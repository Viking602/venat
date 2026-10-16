# Active plan

## Core 5-package SDK consolidation (ADR-031)

- Consolidated the production capability graph to `message`, `provider`, `tool`,
  `skill`, and `agent`.
- Removed `orchestration/` and `durable/` (and their dedicated examples) so
  multi-agent scheduling and execution-persistence state machines remain owned
  by consuming applications.
- Retained `agent.Continuation` (v1/v2 canonical codec and validation),
  `agent.BoundaryObserver`, `OperationID`, `provider.StreamInterceptor`,
  `tool.Interceptor`, `agent.Control`, and `agent.NewAgentTool`.

## Post-consolidation correctness and gate hardening

Status: implemented and locally verified, 2026-10-16. Unreleased.

- OpenAI Responses delivers the complete tool call at `output_item.done` when
  safe streaming tools are enabled; previously it was dropped when no argument
  deltas preceded it.
- Provider drivers confine redirects of caller-supplied HTTP clients to the
  original origin, matching the existing HTTPTool and default-client policy.
- Structured-output repair and `Engine.Resume` carry cumulative context usage,
  and failed/recovered steps snapshot context usage before `StepObserver` runs,
  so repaired and resumed runs with working memory and boundary observers
  produce valid continuations.
- Usage from a working-memory summary is reported when compaction then fails.
- When tools are registered and a completed turn emits a plain-text tool call
  (protocol tags such as `<tool_call>`/`<function=...>` or a JSON tool-call
  envelope naming a registered tool) instead of structured `ToolCalls`,
  response recovery instructs the model to invoke the structured tool API
  rather than terminating the loop early.
- `ValidateCompleteTurns` no longer lets a duplicate result for a known call ID
  stand in for an empty-ID call. OpenAI Chat emits the resolved index for
  index-less deltas; OpenAI Responses binds delayed `call_id`/`name` at
  `output_item.done`; Anthropic keeps one provider-state block when a stop event
  precedes its start and ignores unstarted stop events; complete tool calls with
  arguments ignore trailing deltas; scripted replays deep-clone mutable event
  fields; rejected oversized tool names truncate on a UTF-8 boundary.
- The public-any gate now detects `interface{}` spellings and accepts only a
  standalone tag comment; import/package gates fail on `go list` errors;
  `tidy-check` restores `go.mod`/`go.sum` when tidy fails; dead lint excludes
  were removed. The pinned toolchain moves to Go 1.26.9, which clears ten
  reachable standard-library vulnerabilities; the module still declares Go 1.25.
- Intended behavior kept: an empty-ID tool call may still match an unknown
  result ID, a streamed tool output still supplies legacy text-only results,
  and small `OutputStore` preview thresholds still store bodies.

## Nonterminal pauses and live direction changes

- Explicit provider pauses retain assistant state and continue automatically;
  repeated non-progress and exhausted loop allowance report typed failures.
- Control separates safe-boundary input, active steering, and deferred follow-up.
- Already started writes retain real results; obsolete pending calls are skipped.
- Local examples: `examples/pause-continuation` and `examples/live-steering`.

## Practical agent runtime

Implemented capabilities:

- Local output artifacts with previews, range reads, and search.
- Model-backed working memory, recent complete tool groups, summary usage, and
  recognized context-overflow recovery.
- Ordered parallel tool groups and concurrent child token reservations.
- Executable acceptance checks through existing output guardrails.
- Explicitly opted-in complete-call streaming for safe read tools.

Current setup and ownership contracts are documented in `docs/agent-execution.md`.

## Codex-style execution envelopes

Status: implemented, 2026-09-20.

- [x] Persist a session budget through requests and continuations for cumulative
  token and active wall-clock limits.
- [x] Enforce Codex-compatible model connection and stream-idle defaults, with
  optional whole-request timeout and typed timeout causes.
- [x] Preserve deterministic concurrency, partial-result, and cancellation behavior.

The defaults are intentionally local SDK policy. Account quotas, provider rate
limits, application session identity, and durable backend retention remain
application-owned.

## Agent recovery and explicit context selection

Status: complete, 2026-09-20. The user approved execution of the
Codex/OMP audit findings and optional Jev integration. Preserve all earlier
local changes. Baseline `make ci-local` passed before this pass.

### Delivery sequence

1. **Bounded recovery and completed tool failures**
   - [x] Let the Agent correct malformed JSON from a complete model response
     without executing that call; keep missing/conflicting identities and
     interrupted streams fatal.
   - [x] Distinguish empty output and length cutoff from completed output,
     with bounded continuation and truthful final failure semantics.
   - [x] Return confirmed local process timeout as feedback while preserving
     parent cancellation, ambiguous execution, I/O, and sink failures.
   - [x] Preserve bounded head/tail output and HTTP status for oversized bodies;
     do not turn incomplete transport failures into successful tool execution.
2. **Context preparation**
   - [x] Bound oversized tool output in the provider view without changing the
     execution transcript, tool/result pairing, or protected instructions.
   - [x] Avoid duplicate legacy-field token charges; document conservative
     estimates and caller-owned output/schema reserves.
   - [x] Make ContextBuilderFunc use default fitting when a target is set.
   - [x] Persist remote decisions as ordinary tool results; keep deterministic
     fitting reproducible and existing continuation encodings unchanged.
3. **Explicit, optional Jev protocol**
   - [x] Reuse llmux's typed evaluation transport. Require protocol, BaseURL,
     APIKey, and a model version from the caller; no default host, environment
     discovery, or network activity when disabled.
   - [x] Supply advisory typed context-candidate retention scores through Noul;
     honor explicit protected entries and preserve order and source hashes.
   - [x] Settle selection effects through the existing durable effect boundary,
     replay settled results, and stop on unresolved infrastructure effects.
   - [x] Record model/usage and settle scores before the following model request;
     never store credentials in continuations or effect payloads.
4. **Acceptance and documentation**
   - [x] Regression tests for recovery caps, cancellation, malformed protocol,
     local HTTP capture, deterministic resume, and durable reopen.
   - [x] Document setup, disabled/misconfigured behavior, migration, and scope.
   - [x] Run examples and `make ci-local`; report authenticated Jev quality and
     latency as unverified until caller-supplied credentials are available.

This pass supplies SDK capabilities. It does not select a production backend,
run a sandbox service, own background process registries, or claim semantic
compression quality from mock tests. Generative summaries and interruptible
process sessions remain separately measurable follow-ups; the initial Jev
capability scores original material and does not generate summaries.

Implementation decision: use an explicitly selected tool instead of an automatic
remote compactor. Existing tool settlement freezes nondeterministic Jev decisions;
no additional prepared-context state or continuation schema is needed. The SDK
returns probabilities; retention thresholds and automatic deletion are caller
policy. Auxiliary usage is reported separately, not folded into main-model spend.

Acceptance met: recoverable failures reach another model turn without replaying
completed side effects; unrecoverable failures remain explicit; enabled context
selection uses only the supplied endpoint/key; restart reuses settled choices;
the default SDK path makes no Jev request.

## Recoverable tool failures

Status: implementation and local verification complete, 2026-09-20.
Follow-up requested by the user: audit early
Agent exits and let the model correct ordinary tool failures within the loop.

- [x] Return unavailable tool names and typed argument conversion failures as
  error results without invoking a driver/function; retain bounded call budgets.
- [x] Finish on a terminal tool only when that terminal call succeeds.
- [x] Preserve explicit `tool.Result` returned by `kit.Tool`, including IsError;
  recognize confirmed process exits from wrapped Go functions without masking
  cancellation, joined failures, or infrastructure errors.
- [x] Handle ordinary process launch rejection and confirmed signal termination;
  truncate and drain excess output instead of killing the process for log size.
- [x] Keep HTTP rejection status visible, and verify sequential/parallel batches,
  model correction, result/stream agreement, and durable replay without rerunning
  settled failures. Document the remaining fatal-error boundary.
- [x] Run focused suites, examples, and the full CI gate. Baseline `make ci-local`
  passed before this follow-up, including the previous SDK changes.

Verification: final `make ci-local` passed with normal/race suites, vet,
staticcheck, lint, vulnerability reachability, and architecture checks. All five
executable examples passed. Real subprocess tests covered exit codes 1, 2, 7,
126, 127, 128, 137, and 255, signal termination, wrapped Go functions, and a child
writing a completion marker after producing oversized logs. Mixed rejected and
successful calls continued in sequential and parallel modes. Durable reopen
replayed a settled rejected terminal call without executing it again, then
completed a corrected call. Cancellation, joined infrastructure errors, sink
failure, and bounded repeated rejection retained their stop behavior.

The [error matrix](../agent-execution.md#recoverable-feedback-and-fatal-boundaries)
records which errors still stop execution. The [migration notes](../migration.md)
cover changed Bus return semantics, rejected-call accounting, terminal tools,
and function result handling. Changes remain local and unreleased.

## Agent execution completeness

Status: SDK implementation and local verification complete, 2026-09-20.
The user approved implementation of the
Codex Core comparison findings and explicitly scoped this delivery to Venat;
application integrations are a separate phase.

The implementation keeps the seven package families and the ownership rules
in ADR-029. No new scheduler, persistence backend, sandbox service, or agent
registry is introduced. New execution controls must have explicit ownership,
bounded input, cancellation, and checkpoint semantics.

### Implementation sequence and acceptance

1. **Tool feedback and structured output**
   - [x] Return confirmed process exit failures as model-visible tool results,
     retaining logs and exit status; keep cancellation and infrastructure
     failures as Go errors and mark proven pre-start failure correctly.
   - [x] Reject invalid output policies before context hooks, model calls, or
     tools; preserve this behavior on resume and child agents.
   - [x] Forward the full supported output schema through existing provider
     response-format plumbing, including repair and resume, without dropping
     non-string enum values or mutable-value ownership.
   - Acceptance: an Agent observes a failed command and makes a successful
     correction; invalid schemas cause zero effects; HTTP request tests prove
     the schema reaches supported provider adapters.
2. **Default context preparation and rich input**
   - [x] Supply deterministic bounded history preparation through the existing
     token-target extension, retaining system/skill/cache context, task input,
     and complete tool exchanges. Reject an irreducible over-limit history.
   - [x] Keep cumulative spend, context estimates, and durable transcript/step
     accounting distinct. Preserve resumability after context reduction.
   - [x] Carry typed multimodal request content through build, clone, dispatch,
     continuation, and durable execution specifications.
   - Acceptance: first-turn and mid-run fitting, protected-prefix overflow,
     tool-group integrity, ownership, multimodal forwarding, and checkpoint
     round trips have regression coverage. No model-generated summary is
     silently recomputed during replay.
3. **Execution input and interruption**
   - [x] Implement a concrete bounded, execution-scoped control mechanism for
     queued input and cancellation, with explicit input-source restrictions.
   - [x] Consume input only at safe loop boundaries, including a pending input
     arriving before terminal completion. Reject closed/stale handles.
   - [x] Distinguish queued input from checkpointed input; preserve cancellation
     and unknown-effect behavior under durable execution.
   - Acceptance: concurrent submission, finish races, queue limits, cancellation,
     checkpoint failure, and resume are tested with deterministic synchronization
     and the race detector.
4. **Integration proof and compatibility**
   - [x] Exercise the completed mechanisms together in a runnable Agent example
     and recovery tests using the existing private test backend.
   - [x] Document how existing orchestration and synchronous child agents compose
     with controls; keep application-owned asynchronous registries and routing
     outside the SDK.
   - [x] Update API, migration, persistence compatibility, and package-boundary
     documentation. Explicitly record remaining application-stage work.
   - [x] Run focused tests, runnable examples, `make verify`, and `make ci-local`.

### Completed verification and compatibility

- `make verify` and `make ci-local` passed, including race tests, staticcheck,
  lint, public-any/import/vocabulary/removed-surface gates, and Sentrux.
  Govulncheck found no reachable vulnerabilities; it reported one vulnerability
  in a required module that this code does not call.
- All five executable examples passed: `agent`, `orchestration`, `durable`,
  `subagent`, and the new `interactive` scenario. The interactive scenario used
  two real child-process calls, three deterministic model calls, and eight
  validated in-memory boundaries, ending with `{"status":"ok"}`.
- HTTP capture tests verify native schema forwarding through OpenAI Chat and
  Responses, including exact large integer enum values. Native is explicit
  opt-in; existing local validation/repair remains available without it.
- Tests cover concurrent live input and finish races, cancellation, bounded
  admission, failed checkpoint acknowledgements, committed-checkpoint response
  loss, reopen/resume without input duplication, settled model replay, and
  unknown started tools blocking replay pending reconciliation.
- Default context fitting retains task/system/skill/cache context and complete
  exchanges without deleting recovery evidence. It uses a conservative byte
  estimate; media needs a model-aware compactor. Custom managers keep their
  existing compaction behavior.
- New continuations use v2. The immutable v1 fixture retains its canonical bytes.
  Usage and guarantees are in [Agent execution](../agent-execution.md).
