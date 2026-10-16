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

Observer, hook, guardrail, and sink side effects are not covered by automatic effect settlement. Make them deterministic, idempotent, or independently durable in application storage.

## External protocol adapters

Protocol clients, schedulers, webhooks, sandboxes, hosted services, persistence backends, and domain bundles live in application repositories or separate modules. Adapt them to `provider.Driver`, `tool.Driver`, `skill` resources, `agent.Hook`, `agent.BoundaryObserver`, `provider.StreamInterceptor`, or `tool.Interceptor` without adding a second composition layer inside Venat.
