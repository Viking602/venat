# Agent execution

The SDK supplies execution mechanisms. Applications still own sessions, user
authorization, routing, process/sandbox resources, and persistent backends.

## Codex-style execution envelopes

The SDK keeps four limits separate:

- `Request.Budget` bounds one `Engine` execution with token, step, tool-call,
  and wall-clock dimensions.
- `Request.SessionBudget` is carried through a `Continuation` and bounds the
  cumulative logical session. It can cap total tokens and active wall-clock
  time without adding application identity to the SDK.
- `Request.ModelTimeouts` bounds the provider turn. Unless
  `DisableDefaults` is set, model connection uses a 15-second deadline and a
  streamed response may remain idle for at most five minutes. An optional
  `RequestTimeout` caps the whole model request; otherwise the execution or
  caller context remains the outer deadline. A timed-out stream is closed and
  returns a typed provider error.
- `orchestration.DriveOptions` keeps the existing default of four concurrent
  dispatches and 64 ticks, and now accepts `MaxWallClock` for the whole drive
  and `DispatchTimeout` for each executor call. Zero leaves the caller context
  as the outer bound.

These controls do not retry a partially delivered model stream or infer that a
timed-out external effect did not happen. They only stop the owning boundary;
the existing durable unknown-effect and reconciliation rules still apply.

## Local executable proof

```bash
go run ./examples/interactive
```

The example starts real child processes and uses a deterministic local provider.
It checks this complete path: failed command → model-visible logs and exit code
→ live user correction → successful command → locally validated native-schema
output. It needs no API key. Its boundary observer validates snapshots in memory;
it is not a production persistence or real-model evaluation.

## Command failure feedback

`tool/kit.ProcessTool` returns `IsError=true` and nil Go error after a confirmed
nonzero exit or signal termination while its context is active. Captured
stdout/stderr and exit status reach the model. Missing executables, missing
working directories, and permission-denied startup failures also become error
results; no process was started in those cases.

The helper retains at most 1 MiB of stdout/stderr, drains excess bytes until the
child exits, then appends a truncation notice. Log size alone no longer kills the
child. The bounded log contains the first and last 512 KiB, plus notices, so
final diagnostics survive truncation. The stable head streams immediately; the
rolling tail streams on completion. Streamed output and the final result agree. The helper remains an unsandboxed one-shot local process.

On Darwin and Linux, a `kit.Timeout` (or the built-in process timeout) kills the
managed process group. Once the child is reaped, it returns error feedback with
exit status and a partial-side-effects warning. An application deadline or user
cancellation remains a Go error. Other platforms retain fatal timeout behavior;
arbitrary Go functions and HTTP requests cannot prove completed execution from
a deadline alone.

`kit.Tool` also recognizes a confirmed `*exec.ExitError` from a Go function,
including ordinary `%w` wrappers. It retains the returned output and captured
stderr. Joined errors, cancelled contexts, I/O failures, and stream-sink failures
remain Go errors. No generic error-string matching or automatic command retry is
used. The model chooses the next action within the existing budget.

## Recoverable feedback and fatal boundaries

| Condition | Engine behavior |
| --- | --- |
| Unknown tool name, including a tool call with no configured bus | Error result; no unregistered driver executes; model can choose another action |
| Schema rejection or Go argument conversion failure, such as integer overflow | Error result; target function is not invoked |
| Explicit `tool.Result{IsError: true}` from a driver or `kit.Tool` | Completed error result returned to the model |
| Failed terminal tool, including rejected arguments | Continue; a terminal call must succeed to finish the execution |
| HTTP response status 400 or higher | Error result retaining status and body; complete JSON bodies remain in Structured |
| HTTP body larger than 1 MiB | Bounded error result retaining status and prefix; Structured is cleared; the request may already have taken effect |
| Confirmed local process timeout on Darwin/Linux | Error result with logs, exit status and a partial-side-effects warning |
| Completed response with invalid tool JSON, empty final text or length cutoff | Up to three corrective model turns; no tool from the rejected response executes |
| Process exit, ordinary launch rejection, or oversized logs | Behavior described above |
| Parent cancellation/deadline, execution budget, explicit step-policy stop | Stop according to the existing execution contract |
| Recognized provider context-length rejection | Shrink configured context and retry a bounded number of times; retain the original error if recovery fails |
| Other provider open/stream errors, missing/conflicting identities or terminal protocol violations | Preserve the Go error; no blind model/effect retry |
| HTTP transport/read failure, process I/O/resource failure | Preserve the Go error and unknown-effect handling |
| Hook/interceptor/sink/checkpoint failure, panic, invalid static configuration | Stop; do not conceal infrastructure or extension failures as business feedback |
| Irreducible context or structured-output repair exhaustion | Preserve the existing typed Agent failure |

Response corrections count toward model usage and step/iteration budgets. Their
count lives in checkpointed step observations, so compaction or resume cannot
reset it. Exhausting the bounded correction limit while awaiting usable output returns
`FailureKindRepairFailed`; the existing MaxIterations soft ceiling remains a
mechanical stop for tool loops and preserves its prior contract.
Caller output guardrails still run first and may explicitly replace the output.

Rejected calls count toward `ToolCallsUsed` and `MaxToolCalls`, including unknown
names. Sequential and parallel batches return completed rejections in call order
alongside successful siblings. Fatal errors retain existing partial-result and
unknown-effect semantics. A successful terminal call still ends a mixed batch;
ordinary sibling errors do not undo its successful completion.

Custom functions should distinguish completed domain failures explicitly:

```go
driver, err := kit.Tool("lookup", func(ctx context.Context, in Input) (tool.Result, error) {
    if in.Path == "" {
        return tool.Result{Content: "Provide a nonempty path.", IsError: true}, nil
    }
    // Infrastructure or uncertain effects still return a Go error.
    return lookup(ctx, in.Path)
})
```

`kit.Tool` preserves the returned Result's content, structured data, and IsError
and binds its identity to the actual call. Arbitrary Go errors are not assumed
to be domain failures. In durable execution, nil Go error with IsError means a
settled, replayable observation; it does not mean the requested task succeeded.
Recovery tests verify that checkpoint failure replays the rejection without
reexecuting the settled tool, then allows correction. A cancelled process may
already have performed effects and still requires reconciliation when uncertain.

## User content and structured output

`agent.Request.Content` carries ordered `message.ContentPart` values after the
optional `Prompt`. Supported user kinds are text, image, audio, and file.
Provider reasoning, signatures, opaque provider blocks, and evidence metadata
are rejected as user input. Provider modality support remains adapter-specific.
Requests and content bytes are cloned through execution, dispatch, and durability.

```go
result := engine.Run(ctx, agent.Request{
    Prompt: "Describe the image",
    Content: []message.ContentPart{{
        Kind: message.ContentImage,
        MediaType: "image/png",
        Data: pngBytes,
    }},
}, agent.OutputPolicy{
    Schema: json.RawMessage(`{"type":"object","properties":{"description":{"type":"string"}},"required":["description"]}`),
    Native: true,
    Validate: true,
})
```

`Native` is opt-in; existing policies remain local-only. Native output forwards
the supported JSON Schema subset through `provider.ResponseFormat.RawSchema`.
The OpenAI Chat and Responses adapters preserve its exact JSON values, including
large integer enums. Native output is requested in non-strict mode without
rewriting optional fields; enable `Validate` for authoritative local validation.
Other adapters may ignore the native hint, and models may reject unsupported
formats. Choose a compatible model or leave `Native` disabled.

Invalid enabled schemas and negative repair limits fail before effects. Native
mode requires a schema. Repair and resume retain the same schema. A step whose
output requires repair transitions to `continue` before the next checkpoint.

## Default context fitting

Set `LoopPolicy.ContextTokenTarget` (or `LoopInput.ContextTokenTarget` for
`RunMessages`) to the allowance reserved for **message history**, after reserving
model output, tool schemas, format, and provider overhead. It does not replace
the cumulative token budget.

Without an explicit compactor, the SDK fits the provider request after hooks:

- protect every system and user message, skill context, explicit cache prefix,
  failed tool exchanges, and the most recent complete message/tool group;
- remove older unprotected groups without splitting tool calls from results;
- if still over budget, shorten oversized plain tool results to a bounded
  head/tail view; preserve identities and IsError, mark omitted content, and keep
  skill bodies, signed/provider content, media and cache prefixes intact;
- keep the full transcript, step accounting, and operation IDs in execution
  results and checkpoints;
- fail before contacting the provider if protected context cannot fit.

The estimate charges one token per canonical serialized message byte plus 16
tokens of framing. Legacy Text/Content and identical Structured mirrors count
once; distinct structured values remain charged. This is intentionally conservative, not a model tokenizer or a promise
about a provider's actual context usage. Media requires a model-aware `CompactTo`
because URI length does not determine media token cost. Zero target leaves
context fitting disabled. `ContextBuilderFunc` uses this default fitting because
its Compact method is a no-op. Existing custom `Compact`/`CompactTo` semantics remain;
they must preserve complete exchanges, protected context, and newly queued input.

The default fitter does not produce a model-generated summary. Enable working
memory below to retain semantic task state instead of only dropping old groups.

## Model-backed working memory

```go
memory, err := agent.NewWorkingMemory(agent.WorkingMemoryConfig{
    Provider: summaryProvider,
    Model: summaryModel,
    SystemInstructions: "Complete the requested work and retain its evidence.",
    RecentTurns: 2,
    SummaryMaxTokens: 4096,
})
if err != nil {
    return err
}
engine.WorkingMemory = memory
engine.LoopPolicy.ContextTokenTarget = 64000
```

WorkingMemory summarizes goals, constraints, evidence, file/artifact references,
failed attempts, and remaining work. It preserves the explicit cache prefix,
system instructions, the latest user correction, and recent complete message/tool
groups. A long single-user tool loop can be summarized; prior summaries can
themselves be summarized rather than accumulating forever.

Provider and model are explicit. A configured `ContextBuilder` takes precedence
over `Engine.WorkingMemory`; WorkingMemory also implements ContextManager.
A fitting history makes no summary request. `WorkingMemoryConfig.Observer`
reports progress and auxiliary usage. Engine's invocation-scoped accounting
includes summary usage in total usage, including failed summary work where the
provider reports usage.

Compaction separates transcript evidence from model context. Old complete
exchanges are marked `ContextArchived`, not deleted from `Result.Messages` or
continuations. `message.ContextView` and supported provider adapters omit those
entries from model requests. This preserves step/tool identity validation while
the model sees only the summary and retained context. Fresh queued inputs and
active skill instructions remain visible before they can be summarized.

Summary calls are auxiliary provider work, not automatically durable model
attempts. Checkpoints retain the generated summary and its usage; interruption
before that checkpoint can cause another summary request on resume. No
exactly-once summary billing guarantee is implied.

The fallback estimate is a text heuristic, not an exact tokenizer. Supply a
`WorkingMemoryEstimator` for model-specific accounting and media. Reserve output
and tool-schema space in `ContextTokenTarget`. Summary output is marked as
untrusted context evidence, not promoted into system instructions. Empty,
truncated, tool-calling, or failed summaries do not silently replace history.
`examples/working-memory` exercises summary and main-model calls with combined
usage; it is not a real-model summary-quality benchmark.

## Retrievable large tool output

Create an `OutputStore` with `tool.NewOutputStore(privateDirectory)` and set
`Engine.OutputStore`. Engine registers `tool_output_read` and `tool_output_search`
automatically, rejects conflicting registrations, and processes final output
after tool hooks. Large text/structured bodies become previews with stable
SHA-256 `artifact://` references. Identity, `IsError`, and media are preserved.

The read tool accepts `reference`, byte `offset`, and `limit`. Search scans large
artifacts with bounded returned evidence; its offsets support subsequent reads.
Retrieval results are not recursively spilled. `WithPreviewThreshold`,
`WithReadLimit`, and `WithSearchLimit` customize limits.

The caller owns the private rooted local store and must call `Close`; Engine
does not close it. Identical output reuses a stable reference. No measured
provider cache-hit improvement is claimed. Only received output is saved: the
process/HTTP adapters' existing capture limits still apply before the store.
Bytes already discarded upstream cannot be recovered. Storage errors never
produce references to nonexistent output. Run `examples/tool-output`.

## Ordered parallelism and early reads

`ToolMode: tool.ModeParallel` runs consecutive parallel-capable calls in groups
separated by `ConcurrencySequential` barriers: two reads overlap, then an edit
runs, then the next reads overlap. A failed group does not cross the next barrier.
Per-tool/exclusive limits still apply. AgentTools share finite parent token
capacity through short-held reservations rather than a lock covering whole child
executions; known batches receive predetermined shares.

`Engine.SafeStreamingTools` explicitly lists trusted read-only tools that may
start before the model stream finishes; `LoopInput` has the same option.
Adapters emit complete calls on the internal `provider.Request.CompleteToolCalls`
path. Argument fragments are never executed. Sequential/unsafe barriers and
hook-rewritten names constrain early work. Started calls are consumed once, not
redispatched after streaming. Model and tool progress share a serialized sink.

Enable this only for tools whose early execution is acceptable even if a later
stream event fails. It is not permission to speculate on writes. Run
`examples/parallel-tools` and `examples/streaming-tools` for channel-controlled
proof of overlap.

## Executable task acceptance

`agent.NewToolVerifier(name, checkBus, calls)` uses the existing output guardrail:

```go
verify, err := agent.NewToolVerifier("acceptance", checks, []tool.Call{
    {Name: "run_tests", Arguments: json.RawMessage(`{}`)},
})
if err != nil {
    return err
}
engine.OutputGuardrails = append(engine.OutputGuardrails, verify)
```

Checks are application-selected repeatable tools, separate from model actions.
A completed failed check returns bounded head/tail diagnostics for correction.
Infrastructure errors and cancellation stop verification. Checks run again on
the next proposed final assistant answer. Use read-only checks and wrap drivers
for additional authorization or effect recording. Their work is not charged as
model-requested tool calls.

`examples/task-verification` runs a real process against a temporary configuration
file: premature completion fails, diagnostics reach the model, a repair changes
the file, and the check passes.

`Engine.ResponseContinuation` (or its LoopInput counterpart) customizes
output-length recovery. The default preserves requested detail instead of always
demanding a shorter answer.


## Explicit Jev context scoring

Jev is a typed evaluator, not a text-summary generator. Venat reuses llmux v0.4.0's
TypeSafe System One transport and exposes one optional tool, `select_context`:

```go
engine, err := agent.Build(agent.Spec{
    Model: "your-main-model",
    Tools: []string{"select_context"},
}, agent.BuildDeps{
    Providers: provider.Single(mainDriver),
    ContextSelection: &kit.ContextSelectionConfig{
        Protocol: "typesafe-system-one",
        BaseURL: callerBaseURL, // includes the version prefix, for example /v1
        APIKey: callerAPIKey,
        Model: callerJevModel, // pin a version for reproducible evaluations
    },
})
```

Both tool selection and configuration are required for Build to expose it.
Without configuration no Jev client or request is created. SDK code never loads
an environment key or fills in a default endpoint/model. Selected but incomplete
configuration fails during Build. Applications constructing Engine directly can
register `kit.ContextSelectionTool(name, config)` with their existing tool Bus.
The existing registry is cloned before registration; conflicting names fail.

The tool accepts `task` and ordered `candidates`, each with `id`, `text` and
`protected`. It sends Noul questions for unprotected candidates and returns:

- original candidate order, IDs and SHA-256 hashes;
- `mustKeep` for protected entries, with no evaluation for those entries;
- `retainProbability` without rounding it into a Boolean or imposing a threshold;
- protocol, requested/resolved model, input/output/total token usage.

Maximum input is 32 candidates and 16 KiB for the encoded System One wire
request. Invalid/oversized input returns error feedback without contacting Jev.
Requests have a 30-second
ceiling, no automatic retries and no redirects. Remote/transport/protocol errors
remain Go errors. Error reporting excludes upstream bodies and credential-bearing
transport details; APIKey is omitted from JSON configuration serialization.

The tool scores caller/model-supplied candidates. It does not automatically inspect
or filter the full transcript, infer which content is protected, generate a
summary, or install a nondeterministic Compact hook. The caller must protect
instructions, user constraints, unresolved errors and complete tool exchanges.
The deterministic default fitter above remains responsible for the context limit.
This keeps scoring available without introducing an SDK retention policy.

Scores and provenance are ordinary structured tool results, persisted and replayed
by durable's existing tool effect protocol. A settled selection is reused after
checkpoint failure/reopen, even with the endpoint offline. Ambiguous remote calls
remain unknown and require the existing reconciliation flow. Auxiliary Jev tokens
are reported separately in the result; `Result.Usage`/`Budget.MaxTokens` still
measure the main model. Each evaluation consumes the normal tool-call budget.

Local HTTP capture, invalid responses, protected entries and durable reopen are
tested. Compression quality, actual token savings, latency and live provider
compatibility require caller credentials and a representative evaluation set;
no authenticated Jev quality result is claimed here.

## Explicit nonterminal pauses

`provider.StopReasonPause` means the provider ended a response but has not finished
the task. Anthropic `pause_turn` maps to this signal. A completed response
containing commentary is not guessed to be a pause.

Engine retains the paused assistant state and usage, records a continue step, and
samples again without fabricating a user "continue" instruction. Final-answer
guardrails and output validation do not run on the paused response. Tool progress
and newly consumed input reset the consecutive-pause streak; after eight allowed
consecutive continuations, another pause returns `ErrPauseContinuationLimit`.
Hitting the loop ceiling while still paused is not reported as successful
completion. Cancellation and explicit execution limits remain authoritative.

Run `go run ./examples/pause-continuation` for two explicit pauses followed by a
final answer without manual prompting.

## Input during execution

Attach a fresh `&agent.Control{}` to `Engine.Control`, or to `LoopInput.Control`
for the low-level entry point. Start the execution normally. Once running, a
separate application goroutine can submit authorized user input:

Choose the input mode deliberately:

| API | Behavior |
| --- | --- |
| `Control.Steer` | Correct the current direction; interrupt active model sampling where safe, then continue the same execution |
| `Control.Send` | Add information at the next safe model boundary without interrupting sampling |
| `Control.FollowUp` | Queue a subsequent task until current work naturally finishes |
| `Control.Cancel` | End execution; reject unconsumed receipts |

For example, `control.Steer(agent.Request{Prompt: "Do not edit yet. Locate the failing path first."})`
returns an admission result and a consumption receipt immediately. Await the
receipt from the application goroutine, not from a tool/hook callback.

Send and Steer preserve admission order so an older instruction cannot be moved
after its correction. Steering does not kill or pretend to roll back started
tool effects. Real completed results are retained; obsolete calls that have not
started are skipped. If a safe streaming tool has already started, interruption
waits for a safe boundary instead of breaking its call/result pairing.

Run `go run ./examples/live-steering` for an active model stream redirected into
a second request, without canceling the whole run.


```go
ack, err := control.Send(agent.Request{Prompt: "Also check cancellation"})
if err != nil {
    // Not admitted: inactive/closed handle, invalid input, or full queue.
    return err
}
select {
case err := <-ack:
    // nil: appended to the transcript and all boundary observers succeeded.
    // error: not confirmed; inspect durable state before resending.
case <-ctx.Done():
    // Cancels this wait only. It does not retract an already queued input.
}
```

Admission and consumption are different. Each buffered receipt yields exactly
one result, then closes. A successful receipt does not mean the model has already
answered the input. With `durable.Runtime`, it means the checkpoint observer also
succeeded; without a persistent observer it means in-memory consumption only.
Do not synchronously wait on the receipt inside a hook or output sink: the loop
must return from that callback before it can consume the input.

The queue permits 64 outstanding inputs, 1 MiB of serialized message data per
input, and 4 MiB total. It preserves admission order and clones input. Input is
consumed before a model request, never in the middle of a tool exchange. Pending
input forces a natural final answer or successful terminal tool to continue.
Budget stops, explicit step-policy stops, and errors still end execution and
reject unconsumed receipts. Terminal admission and input submission are synchronized. Inputs after
the terminal decision are rejected even if output validation is still finishing.

`control.Cancel()` cancels the active model/tool context. Handles are single-use.
Use Send for authorized input. For worker results,
`Runtime.CompletionHook(parentControl)` handles provenance labeling and
safe-boundary retention; never present raw worker output as trusted instructions.

Queued input is process-local. Acknowledged input lives in checkpoint Messages.
A crash before acknowledgement, or a checkpoint response loss, may leave the
caller uncertain; inspect the checkpoint rather than blindly replaying input.
Reopen with a fresh control. No exactly-once input delivery is claimed.

## Live multi-agent collaboration

`orchestration.Drive` retains batch/tick behavior. Use
`orchestration.NewRuntime` for independently progressing workers:

- `Runtime.Tools()` exposes `spawn`, `inspect`, `await`, `send`, `steer`, and `cancel`.
- Spawn returns a handle immediately; the runtime owns a worker pool and queue.
- Send adds input at a safe boundary or continues idle-worker history.
- `Runtime.Steer` / `TaskHandle.Steer` redirect a running worker's sampling while
  preserving its identity and history; idle workers receive a new follow-up turn.
  Running workers acknowledge consumption; idle workers acknowledge admission.
- `CompletionHook(parentControl)` exposes evidence at the parent's model boundary
  and queues it into the transcript. Without a control it is provider-view only.
- `DrainCompletions(cursor)` reads a bounded retained journal; consumers own their
  cursors. `ErrCompletionCursorStale` explicitly reports a cursor that fell behind
  retention, rather than returning an incomplete success. Latest task results
  remain available through Inspect/Await. The `Completions` channel is advisory.
- `RequiredTasksGuardrail(runtime)` tracks dynamically spawned tasks; explicit
  IDs restrict the set. It waits instead of burning retries, and reports failed
  or canceled required work as an output block.
- Close cancels and joins owned work. Its lifetime is separate from a parent's
  current turn.

The runtime is in-memory, not a persistent scheduler or workspace sandbox.
`examples/async-agents` runs an actual parent Engine with task tools, fast-result
consumption before a slow worker completes, retained follow-up context,
independent cancellation, and shutdown checks.

CubeSandbox/MCP adapters, interactive process sessions, and a production durable
backend remain external. Summaries and tool-output retrieval are implemented
above.
