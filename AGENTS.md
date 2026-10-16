# Repository guidance

`AGENTS.md` is the single source of repository guidance for coding agents.

## Project overview

Venat is a typed Agent SDK for Go (`github.com/Viking602/venat`, Go 1.25). The production capability graph is exhaustive:

- `message/` — provider-neutral messages and tool values
- `provider/` — model driver contract, interceptors, conformance, and adapters
- `tool/` — typed tool drivers, bus, validation, interceptors, and application-neutral helpers
- `skill/` — reusable instruction resources and discovery
- `agent/` — one bounded Agent loop, hooks, outputs, continuation, and resume

Supporting surfaces are `examples/`, `scripts/`, and `docs/`. Applications are the composition root and own identity, routing policy, orchestration, persistence schema, deployment, and operations. See `docs/architecture-boundaries.md` and ADR-031.

## Build, test, and development commands

- `make verify` — fast local gate: formatting, vet, tidy check, lint, tests, and architecture checks.
- `make ci-local` — full CI-parity gate: adds staticcheck, vulnerability scanning, and race tests.
- `make fmt` / `make fmt-check` — format or verify Go formatting.
- `make test` / `make test-race` — normal or race-enabled package tests.
- `make architecture-check` — Sentrux plus vocabulary, public-shape, import-boundary, and removed-surface gates.
- `go run ./examples/agent`
- `go run ./examples/subagent`
- `go run ./examples/interactive`

Run `make verify` before routine changes and `make ci-local` before substantial changes.

## Public any-field contract

Exported functions in `message/`, `provider/`, `tool/`, `skill/`, and `agent/` must not return `[]any`; exported fields must not contain loose `any`.

Genuine provider bodies, host payloads, or JSON Schema objects require an escape-hatch comment on the immediately preceding line:

- `//venat:allow-public-any` above a function signature
- `// godoc-allow-any` above a struct field

The AST gate in `scripts/publicany` enforces this contract and fails if a required scope is missing or empty. Test files are exempt.

## Coding style and naming

Use `gofmt` as the source of truth; goimports uses local prefix `github.com/Viking602/venat`. Go files and the Makefile use tabs. Markdown uses two-space continuation, LF, and final newlines.

Prefer package-context names that read naturally at call sites. Avoid package-name stutter, generic files (`types.go`, `helpers.go`, `utils.go`), package or directory renames, exported-symbol renames, and new linting stacks unless explicitly approved.

Demand-driven API surface: introduce an interface only with its second implementation, and export a symbol only with its first consumer outside the package. Tests and examples do not count as that consumer. Speculative fields, interfaces, and helpers are rejected.

Clone mutable values before retaining or returning them. Keep cancellation, deterministic ordering, and ownership explicit. Do not add application identity, routing values, approval policy, quota policy, deployment records, or backend schema to the SDK.

## Testing

Use the standard `testing` package. Keep tests beside the package under test with `_test.go` suffixes and names such as `TestThing_Behavior`. Mark helpers with `t.Helper()`. Prefer table-driven cases and assert typed errors with `errors.Is` / `errors.As`.

Focused suites:

```bash
go test ./agent ./message ./provider/... ./tool/... ./skill/...
```

## Commit and pull request conventions

Use Conventional Commits with a focused scope, for example `feat(agent): ...`, `fix(provider): ...`, `test(tool): ...`, or `docs(migration): ...`. Explain why public behavior changes, list validation commands, link issues, and identify public API impact.

Never include AI-generated or AI co-author attribution in commit messages or pull request bodies.

## Gotchas

- `agent.Result.Failure` is terminal Agent data; infrastructure failure remains outside the Agent result contract.
- Only `provider.ErrNotStarted` and `tool.ErrNotExecuted` prove that an effect did not begin.
- Streaming output is transient, not an exactly-once event log.
- Multi-agent orchestration and durable execution backends are application-owned concerns outside the core SDK.
- Architecture gates are fail-closed. Missing required package scopes must fail rather than pass with an empty scan.
