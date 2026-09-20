# Venat documentation

Venat is a direct-import Agent SDK with an optional durable execution layer. Start with the package matching the behavior you need; no application-wide framework bootstrap is required.

## Start here

1. [Quickstart](quickstart.md) — construct and run one Agent.
2. [Public API](public-api.md) — package contracts and error semantics.
3. [Architecture boundaries](architecture-boundaries.md) — permitted dependency graph.
4. [Durable execution](durable-execution.md) — backend, lease, checkpoint, attempt, and reconciliation semantics.
5. [Backend and extension development](plugin-development.md) — implement providers, tools, skills, and durable backends.

## Composition

- [Extensions](extensions.md) — hooks, interceptors, observers, sinks, and context managers.
- [Agent execution](agent-execution.md) — command feedback, rich input, native
  structured output, default context fitting, live input, and recovery receipts.
- [Ecosystem split](ecosystem-split.md) — SDK responsibilities versus application and adapter responsibilities.
- [Breaking migration](migration.md) — move from the former platform surface to direct package composition.

## Project records

- [ADR index](adr/README.md) — ADR-029 defines the package graph; ADR-030 defines provider/tool stream lifecycle and replay semantics.
- [Product-spec archive](product-spec/README.md) — versioned design snapshots, not current API guidance.
- [Release notes](release-notes/) — behavior shipped by each historical release.
- [Semantic versioning](semver.md)
- [Active plan](plans/active-plan.md)
- [Future backlog](plans/future-backlog.md)

Current package documentation remains authoritative for exact Go signatures:

- [`agent`](https://pkg.go.dev/github.com/Viking602/venat/agent)
- [`orchestration`](https://pkg.go.dev/github.com/Viking602/venat/orchestration)
- [`durable`](https://pkg.go.dev/github.com/Viking602/venat/durable)
