# Quickstart

This guide runs one bounded Agent directly. The complete executable version is in [`examples/agent`](../examples/agent).

## 1. Install

```bash
go get github.com/Viking602/venat@latest
```

## 2. Provide a model driver

Implement `provider.Driver` or use one of the provider adapter packages. A driver returns a stream of provider-neutral events.

```go
type Driver struct{}

func (Driver) Metadata() provider.Metadata {
    return provider.Metadata{Name: "application-model"}
}

func (Driver) Stream(ctx context.Context, request provider.Request) (provider.Stream, error) {
    // Translate request to the provider and normalize its response.
}
```

Provider calls are issued with stable model operation IDs such as `turn:0:model`. Interceptors must invoke their supplied `next` driver zero or one time and must not mutate the request.

## 3. Add typed tools

`tool/kit` builds a JSON-schema-backed driver from a Go function:

```go
lookup, err := kit.Tool("lookup", func(ctx context.Context, input struct {
    Query string `json:"query"`
}, updates tool.UpdateSink) (string, error) {
    if err := updates(tool.Update{
        Kind:    tool.UpdateProgress,
        Message: "searching",
    }); err != nil {
        return "", err
    }
    result, err := search(ctx, input.Query)
    if err != nil {
        return "", err
    }
    if err := updates(tool.Update{
        Kind:  tool.UpdateOutput,
        Parts: []message.ContentPart{message.TextPart(result)},
    }); err != nil {
        return "", err
    }
    return result, nil
})
if err != nil {
    return err
}
```

A tool call receives a stable operation ID such as `turn:0:call:0`. The Bus overwrites update identity, assigns sequence numbers, deep-clones mutable payloads, and applies synchronous sink backpressure. Sequential mode preserves dispatch order; parallel mode serializes the shared sink, retains result identity, and returns deterministic error aggregation.

## 4. Run the Agent

```go
engine := agent.Engine{
    Provider: Driver{},
    Tools:    tool.NewBus(lookup),
    Model:    "application-model",
    ToolMode: tool.ModeSequential,
}

result := engine.Run(ctx, agent.Request{
    Prompt: "Find the requested record",
    Budget: &agent.Budget{
        MaxTokens:    8_000,
        MaxToolCalls: 8,
        MaxSteps:     6,
        MaxWallClock: 30 * time.Second,
    },
}, agent.OutputPolicy{})

if result.Failure != nil {
    log.Printf("agent failed: kind=%s reason=%s", result.Failure.Kind, result.Failure.Reason)
}
```

`agent.Result` always retains the available transcript, steps, usage, tool count, stop reason, and partial output. Agent failures are data, not orchestration policy.

## 5. Stream transient output

```go
result := engine.RunStream(ctx, request, policy, agent.SinkFunc(
    func(ctx context.Context, frame agent.Frame) error {
        if frame.Kind == agent.FrameToolUpdate {
            fmt.Printf("%s #%d\n", frame.ToolUpdate.Kind, frame.ToolUpdate.Sequence)
            return nil
        }
        fmt.Printf("%s: %s\n", frame.Kind, frame.Text)
        return nil
    },
))
```

A sink may receive text, thinking, tool calls, tool updates, tool results, completion, or error frames. Tool updates are ordered per operation ID and ignored by Agent history and accumulators. Delivery is transient. Persist a terminal `agent.Result` or an independently idempotent application projection rather than treating frames as exactly-once records. See [ADR-030](adr/ADR-030-stream-lifecycle-semantics.md).

## 6. Observe continuations and intercept effects when needed

Attach an `agent.BoundaryObserver` to persist canonical `agent.Continuation` snapshots at safe boundaries (`ready`, `model_complete`, `tools_complete`, `validating_output`), and use `Engine.Resume` / `Engine.ResumeStream` to continue from a validated snapshot. Wrap provider or tool execution with `provider.StreamInterceptor` and `tool.Interceptor` when integrating with application-owned persistence, leases, or audit logs.
