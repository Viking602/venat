# Venat

[![Go Reference](https://pkg.go.dev/badge/github.com/Viking602/venat/agent.svg)](https://pkg.go.dev/github.com/Viking602/venat/agent)
[![CI](https://github.com/Viking602/venat/actions/workflows/ci.yml/badge.svg)](https://github.com/Viking602/venat/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Viking602/venat?sort=semver)](https://github.com/Viking602/venat/releases)

Venat is a typed Go SDK for bounded Agent loops, application-defined multi-Agent orchestration, and optional crash-safe execution. Applications import the packages they need and retain ownership of identity, routing policy, storage schema, deployment, and operations.

For command-error correction, typed input, native JSON output, context fitting,
and live user input, see [Agent execution](docs/agent-execution.md) and run
`go run ./examples/interactive` for a credential-free local smoke scenario.

> **Status:** The latest published release is [v0.17.0](https://github.com/Viking602/venat/releases/tag/v0.17.0). Public APIs may change before v1.0.

## Packages

| Package | Responsibility |
| --- | --- |
| [`agent`](https://pkg.go.dev/github.com/Viking602/venat/agent) | One bounded model/tool loop, a synchronous AgentTool adapter, hooks, output validation, transient output, continuation, and resume |
| [`message`](https://pkg.go.dev/github.com/Viking602/venat/message) | Provider-neutral messages, content, tool calls, and tool results |
| [`provider`](https://pkg.go.dev/github.com/Viking602/venat/provider) | Streaming model driver contract, interceptors, conformance helpers, and provider adapters |
| [`tool`](https://pkg.go.dev/github.com/Viking602/venat/tool) | Typed tool drivers, validation, execution modes, real-time updates, and interceptors |
| [`skill`](https://pkg.go.dev/github.com/Viking602/venat/skill) | Reusable instruction resources and discovery |
| [`orchestration`](https://pkg.go.dev/github.com/Viking602/venat/orchestration) | Pure scheduling protocol plus bounded mechanical dispatch execution |
| [`durable`](https://pkg.go.dev/github.com/Viking602/venat/durable) | Optional execution persistence, leases, checkpoints, effect settlement, replay, and reconciliation |
| [`durable/contract`](https://pkg.go.dev/github.com/Viking602/venat/durable/contract) | Conformance suite for application-supplied durable backends |

There is no privileged root façade. Composition remains visible in application code.

## Install

```bash
go get github.com/Viking602/venat@latest
```

Venat requires Go 1.25 or newer.

## Direct Agent execution

An `agent.Engine` receives a provider, an optional tool bus, and explicit loop configuration. `agent.Result.Failure` is terminal execution data; infrastructure setup and transport concerns remain at the caller boundary.

```go
engine := agent.Engine{
    Provider: providerDriver,
    Tools:    tool.NewBus(toolDrivers...),
    Model:    "your-model",
    ToolMode: tool.ModeSequential,
}

result := engine.Run(ctx, agent.Request{
    Prompt: "Summarize the repository",
    Budget: &agent.Budget{MaxSteps: 8, MaxToolCalls: 12},
}, agent.OutputPolicy{})
if result.Failure != nil {
    // Inspect the factual Agent failure and partial trace.
}
```

Use `RunStream` with an `agent.Sink` for transient text, thinking, tool-call, tool-update, tool-result, done, and error frames. A tool function may accept `tool.UpdateSink` and emit typed progress or ordered content parts; the Bus supplies trusted call identity and per-call sequence. Sink delivery is synchronous and not durable output.

## Delegating to a child Agent

`agent.NewAgentTool` exposes an already configured child Engine as a
non-terminal tool. The child keeps its own provider, model, tools, hooks,
skills, loop policy, and context manager.

```go
child := agent.Engine{
    Provider: childProvider,
    Model:    childModel,
}

researcher, err := agent.NewAgentTool(child, agent.AgentToolConfig{
    Definition: tool.Definition{
        Name:             "researcher",
        Description:      "Delegate a research task.",
        ConcurrencyGroup: "subagents",
        MaxConcurrency:   4,
    },
})
if err != nil {
    return err
}

parent := agent.Engine{
    Provider: parentProvider,
    Tools:    tool.NewBus(researcher),
    Model:    parentModel,
}
```

The default input is `{"task":"..."}`. Applications keep ownership of Agent
registries, identity, routing, workflows, process isolation, background work,
and independently durable child lifecycles.

## Application-defined orchestration

`orchestration.Scheduler` is a pure function over an immutable state snapshot. It returns stable dispatch IDs and opaque routes. `orchestration.Drive` executes each tick with bounded concurrency and folds successful outcomes deterministically.

```go
state, err := orchestration.Drive(ctx, scheduler, executor, orchestration.DriveOptions{
    MaxTicks:       16,
    MaxConcurrency: 4,
})
```

Agent identity, teams, routing strategy, shared state, approvals, and deployment are application concerns. Persist `orchestration.State` in application storage when orchestration itself must survive a restart.

## Optional durable execution

`durable.Runtime` wraps the same `agent.Engine` loop. It persists immutable execution input, safe continuations, fenced leases, and provider/tool attempt outcomes through an injected `durable.Backend`.

```go
runtime, err := durable.New(applicationBackend, durable.Options{
    OwnerID: "worker-7",
})
if err != nil {
    return err
}

result, err := runtime.Start(
    ctx,
    durable.ExecutionID("job-42"),
    engine,
    agent.Request{Prompt: "Perform the task"},
    agent.OutputPolicy{},
)
```

A provider or tool effect whose outcome cannot be proven becomes `unknown`. The runtime returns a typed `durable.ReconcileRequiredError`; the application records its real-world finding and explicitly chooses `succeed`, `fail`, or `retry` with `Runtime.Reconcile`, then calls `Resume`.

Applications own backend schema and operations. Venat ships no production backend. Every backend must run `durable/contract.RunBackendContractTests`.

Use `ResumeWithOptions` when an application controller must assert the current checkpoint sequence, phase, or pending operation before any Engine or effect work. `examples/durable` also demonstrates application-owned human approval through `Hook`, `Suspend`, a separate approval store, a tool wrapper, and targeted resume; no approval policy is built into the SDK.

## Executable examples

```bash
go run ./examples/agent
go run ./examples/subagent
go run ./examples/orchestration
go run ./examples/durable
```

The durable example keeps its illustrative backend inside the example directory; it is not a reusable storage adapter.

## Architecture

The dependency graph is intentionally one-way:

```text
message      skill
   │           │
   ├── provider│
   └── tool    │
        \      /
          agent
         /     \
orchestration  durable
```

`agent` and `orchestration` never depend on `durable`. The optional durability layer depends inward on Agent contracts. Executable architecture gates validate production, internal-test, and external-test imports and fail when a required package scope disappears.

See:

- [Quickstart](docs/quickstart.md)
- [Public API](docs/public-api.md)
- [Durable execution](docs/durable-execution.md)
- [Backend and extension development](docs/plugin-development.md)
- [Architecture boundaries](docs/architecture-boundaries.md)
- [Breaking migration](docs/migration.md)
- [ADR-029](docs/adr/ADR-029-agent-sdk-and-optional-durable-runtime.md)
- [ADR-030](docs/adr/ADR-030-stream-lifecycle-semantics.md)

## Development

```bash
make verify
make ci-local
```

`make verify` runs formatting, vet, dependency tidiness, lint, tests, and architecture checks. `make ci-local` adds static analysis, vulnerability scanning, and race tests.

Contributions must preserve the direct package graph, avoid speculative public interfaces, and include behavior-level verification for changed contracts. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
