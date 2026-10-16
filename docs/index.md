# Venat documentation

Venat is a direct-import Agent SDK. Start with the package matching the behavior you need; no application-wide framework bootstrap is required.

## Start here

1. [Quickstart](quickstart.md) — construct and run one Agent.
2. [Public API](public-api.md) — package contracts and error semantics.
3. [Architecture boundaries](architecture-boundaries.md) — permitted dependency graph.
4. [Backend and extension development](plugin-development.md) — implement providers, tools, skills, and Agent hooks/interceptors.

## Composition

- [Extensions](extensions.md) — hooks, interceptors, observers, sinks, and context managers.
- [Agent execution](agent-execution.md) — command feedback, rich input, native
  structured output, default context fitting, live input, and recovery receipts.
- [Ecosystem split](ecosystem-split.md) — SDK responsibilities versus application and adapter responsibilities.
- [Breaking migration](migration.md) — move from the former platform surface to direct package composition.
- [Security hardening](security-hardening.md) — scan fixes and resource bounds.

## Project records

- [ADR index](adr/README.md) — ADR-031 and ADR-029 define the package graph; ADR-030 defines provider/tool stream lifecycle semantics.
- [Product-spec archive](product-spec/README.md) — versioned design snapshots, not current API guidance.
- [Release notes](release-notes/) — behavior shipped by each historical release.
- [Semantic versioning](semver.md)
- [Active plan](plans/active-plan.md)
- [Future backlog](plans/future-backlog.md)

Current package documentation remains authoritative for exact Go signatures:

- [`agent`](https://pkg.go.dev/github.com/Viking602/venat/agent)
- [`provider`](https://pkg.go.dev/github.com/Viking602/venat/provider)
- [`tool`](https://pkg.go.dev/github.com/Viking602/venat/tool)
- [`message`](https://pkg.go.dev/github.com/Viking602/venat/message)
- [`skill`](https://pkg.go.dev/github.com/Viking602/venat/skill)
