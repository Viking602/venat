# Public API

The public surface is the set of directly importable package families below. The module root intentionally declares no Go package.

## `message`

Provider-neutral values shared by model and tool boundaries:

- `Message`, `ContentPart`, and role/kind constants
- `ToolDefinition`, `ToolCall`, and `ToolResult`
- history validation and ownership-safe clone helpers

Messages preserve structured content, provider state, response metadata, tool identities, and legacy text synchronization without embedding application policy.

## `provider`

`provider.Driver` exposes `Metadata` and streaming `Stream`. A `provider.Stream` emits normalized events until a terminal `EventDone` or `EventError` and then EOF.

Important contracts:

- `Request.OperationID` identifies one logical model slot.
- `StreamInterceptor` invokes `next` zero or one time and receives immutable input.
- `ErrNotStarted` is the only proof that a failed call did not start.
- Conformance helpers validate event order, usage, terminal behavior, and adapter normalization.
- `OpenRetryingStream` uses `StreamRetryOptions.ShouldRetry` only for open or pre-first-event failures; context cancellation, deadlines, and emitted output are hard stops.
- A receive or terminal failure after valid output is a non-retryable `PartialStreamError` that preserves the cause.
- `WithStreamIdleTimeout` closes an idle stream and returns `ErrStreamIdleTimeout`;
  Agent model policies also expose typed connection and total-request timeout
  errors.

Provider-specific adapters remain under `provider/<adapter>`.

## `tool`

`tool.Driver` exposes a definition and executes one typed call. `tool.Bus` validates calls and dispatches sequentially or in bounded parallel mode.

Important contracts:

- `Call.OperationID` identifies one logical tool slot.
- `Interceptor` invokes `next` zero or one time and receives immutable input.
- `ErrNotExecuted` is the only proof that an effect did not start.
- Parallel results retain call identity; partial results and per-call failures remain observable.
- `UpdateProgress` carries `Message`/`Data`; `UpdateOutput` requires non-empty `Parts`. `UpdateSink` is the only live-output seam.
- The Bus deep-clones updates, overwrites call identity, assigns per-call `Sequence`, serializes a shared parallel sink, and applies synchronous backpressure.
- One call is limited to 65,536 updates and 64 MiB of decoded update data. Invalid lifecycle/content returns `ErrToolUpdateProtocol`; overflow returns `ErrToolUpdateLimit`.
- Streamed output parts become the final result when `Result.Parts` is absent and must match when it is present. `Result.IsError` remains a completed business result.
- Unknown names and invalid arguments are error results; rejected calls still
  consume the Agent call budget. Terminal tools finish only after a successful
  result. `kit.Tool` preserves explicit Result outputs and confirmed process exit
  feedback; ordinary Go errors retain their infrastructure semantics. See
  [error boundaries](agent-execution.md#recoverable-feedback-and-fatal-boundaries).

`tool/kit` contains application-neutral function, HTTP, process, adapter, and bundle helpers. `ContextSelectionTool` and `ContextSelectionConfig` compose llmux's external TypeSafe transport into an optional typed scoring tool. `BuildDeps.ContextSelection` exposes it only when `select_context` is named in `Spec.Tools`; see [explicit Jev setup](agent-execution.md#explicit-jev-context-scoring). External wire-protocol implementations remain in their adapter modules.

Function tools may accept an `UpdateSink`. Process tools emit `UpdateOutput` as stdout/stderr is read. Drivers that never invoke the sink retain one-shot behavior.

## `skill`

`skill.Skill`, registry, discovery, and resource APIs load reusable instructions. Skills contribute context only. They do not grant tools, create identities, schedule peers, or start another execution loop.

## `agent`

`agent.Engine` owns one bounded Agent loop.

Construct an Engine directly for explicit wiring, or use `Spec` with `Build` and
`BuildDeps`. `Build` resolves the provider and optional fallback model, selects
the named tool subset, resolves active and available skills, installs the hook
chain and context manager, and snapshots mutable loop, stop-sequence, and
provider-extension defaults. Missing providers, models, tools, or skills fail
during construction rather than on the first model turn.

Primary inputs and outputs:

```go
type Request struct {
    Prompt        string
    Budget        *Budget
    SessionBudget *SessionBudget
    ModelTimeouts *ModelTimeoutPolicy
    Content       []message.ContentPart
}

type Result struct {
    Text          string
    Structured    json.RawMessage
    Valid         bool
    Failure       *AgentFailure
    Steps         []Step
    Usage         provider.Usage
    ToolCallsUsed int
    StopReason    provider.StopReason
    Messages      []message.Message
}
```

Primary entry points:

- `Engine.Run` / `Engine.RunStream`
- `Engine.Resume` / `Engine.ResumeStream`
- low-level `Engine.RunMessages`

`Request.Validate` and `ValidateOutputPolicy` preflight inputs without performing
effects; orchestration and durable admission use the same checks. Typed user
content supports text, images, audio, and files, subject to provider support.

`SessionBudget` is persisted with a continuation and bounds cumulative session
tokens and active wall-clock time. `Budget` remains the per-Engine execution
budget. `ModelTimeouts` follows the Codex provider shape: 15 seconds for stream
connection, 5 minutes of stream-idle time, and an optional total model-request
deadline. Set `DisableDefaults` to use only explicitly supplied durations.

`OutputPolicy.Native` explicitly opts into native JSON Schema forwarding;
`Validate` and `Repair` retain their local semantics. `ResponseFormat.RawSchema`
preserves values such as numeric enums and takes precedence over its legacy
typed `Schema`. The OpenAI Chat and Responses adapters forward it.

A positive `ContextTokenTarget` with no custom compactor now enables conservative
text-only request fitting. The full execution transcript is retained. `Control`
adds bounded live user input and cancellation to a single execution. See
[execution behavior and examples](agent-execution.md) for acknowledgement,
context, compatibility, and recovery limits.

`AgentToolConfig` and `NewAgentTool` expose an already configured child
`Engine` as a non-terminal `tool.Driver`.

```go
type AgentToolConfig struct {
    Definition   tool.Definition
    Budget       *Budget
    OutputPolicy OutputPolicy
    MaxDepth     int
}

func NewAgentTool(child Engine, config AgentToolConfig) (tool.Driver, error)
```

- A zero input schema becomes a strict required `{task: string}` object. A
  custom schema passes the complete JSON arguments as the child prompt.
- The child receives no parent transcript or output policy. `MaxDepth` defaults
  to four and cannot loosen an ancestor limit.
- Child Agent failures return as error ToolResult data. Cancellation and
  update-sink failures remain Go errors after usage settlement.
- Child frames emit progress-only updates without thinking, tool arguments, or
  child result payloads.
- Recursive usage enters the parent budget once. A bounded parent's AgentTool
  calls serialize against one token pool; unbounded parents retain Tool
  Definition concurrency.

Extension seams:

- `Hook` and ordered `HookChain`
- `StepDecider` and `StepObserver`
- `OutputGuardrail` and output observer
- `BoundaryObserver` for safe `Continuation` snapshots
- provider/tool interceptors
- `Sink` for transient output
- context managers and skills

`FrameToolUpdate` carries a deep-copied tool update. Normal turn order is provider tool-call/done frames, zero or more tool-update frames, the final tool-result frame, then the next model turn. The accumulator, messages, steps, and continuations ignore tool updates; only the final result enters history.

A `Continuation` records the complete provider-neutral state at one of four phases: `ready`, `model_complete`, `tools_complete`, or `validating_output`. `ValidateContinuation` rejects missing, contradictory, reordered, or malformed state rather than repairing it.

## `orchestration`

`Scheduler.Next(context.Context, State)` is a policy-free scheduling boundary. It returns stable `Dispatch` values with an opaque route, Agent request, output policy, optional handoff payload, and metadata.

`Executor.Execute` performs one dispatch. `Drive` provides only mechanical behavior:

- validation and globally unique dispatch IDs
- bounded ticks, concurrency, Drive wall-clock, and per-dispatch deadlines
- context cancellation and panic containment
- deterministic success folding and multi-error ordering
- source-labeled serialized output frames

Agent failures remain `Result` data. The `error` return is reserved for infrastructure failure.

## `durable`

`durable.Backend` is an execution-semantic persistence interface with exactly these operations:

- start, resume, renew, checkpoint, suspend, finish, release, and load execution
- start, finish, mark unknown, and reconcile an effect attempt

`durable.Runtime` uses that interface through:

- `Start` / `StartStream`
- `Resume` / `ResumeStream` and target-asserting `ResumeWithOptions` / `ResumeStreamWithOptions`
- `Suspend`
- `Reconcile`
- `Close`

Runtime errors preserve typed execution or attempt attribution and support `errors.Is` against the durable sentinels. Any Agent result available when infrastructure fails is returned as partial data. Callers may treat the result as a terminal execution outcome only when the Go error is nil.

`ResumeTarget` can assert the checkpoint sequence, continuation phase, and pending operation slot after checkpoint/reconciliation validation but before Engine, hook, sink, provider, or tool work. A mismatch returns `ErrResumeTargetMismatch` with `ResumeTargetError` facts and best-effort releases the lease; it never changes the continuation.

Durable provider attempts retain complete terminal event sequences. Tool attempts retain only the normalized final result or failure: transient tool updates are never stored in attempts or checkpoints, and reopen replay emits no historical updates. Tool settlement completes before the final tool-result frame can be delivered.

See [Durable execution](durable-execution.md) for fencing and ambiguity rules.

## Public-shape rules

Exported functions in all public package families must not return `[]any`. Exported fields must not contain loose `any` unless the immediately preceding line carries `// godoc-allow-any`. A genuinely open function result requires `//venat:allow-public-any` immediately above its declaration.

The AST-based architecture gate scans `message`, `provider`, `tool`, `skill`, `agent`, `orchestration`, and `durable`; missing or empty scopes fail the gate.
