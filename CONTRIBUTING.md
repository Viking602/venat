# Contributing to Venat

Venat is a small, typed Agent SDK. Changes must preserve the direct package graph and observable contracts.

## Setup

Requirements:

- Go 1.25 or newer
- `goimports`
- `golangci-lint`
- `staticcheck`
- `govulncheck`
- `sentrux` with the Go plugin for architecture checks

Run:

```bash
go mod download
make verify
```

Before a substantial change, run:

```bash
make ci-local
```

## Package boundaries

Production capability families are exhaustive:

- `message`
- `provider`
- `tool`
- `skill`
- `agent`

Examples, documentation, and scripts are supporting surfaces rather than new capability families. Applications are the composition root.

Read [docs/architecture-boundaries.md](docs/architecture-boundaries.md) before adding imports or directories. Do not bypass a boundary with aliases, bridge packages, generated code, or test-only reverse imports.

## API design

- Prefer package-context names that read naturally at call sites.
- Add an interface only with its second implementation.
- Export a symbol only with its first non-test consumer outside the package.
- Avoid generic files such as `types.go`, `helpers.go`, or `utils.go`.
- Use typed result values instead of exported `[]any`.
- Add `// godoc-allow-any` only for a genuinely open public field.
- Make ownership explicit: clone mutable inputs before retaining them and return ownership-independent values.
- Preserve context cancellation and deterministic ordering.
- Keep policy, identity, schema, deployment, and domain vocabulary in applications.

Breaking clean cutovers update every in-repository caller and remove obsolete code. Do not leave compatibility aliases or deprecated paths unless an accepted decision explicitly requires them.

## Testing

Use the standard `testing` package. Keep tests beside the package and name them `TestThing_Behavior`. Mark helpers with `t.Helper()` and use `errors.Is` / `errors.As` for typed error contracts.

Tests should defend observable behavior, boundaries, invariants, transitions, and real failure modes. Avoid tests of source text or incidental implementation details.

Relevant focused commands:

```bash
go test ./agent ./message ./provider/... ./tool/... ./skill/...
```

## Formatting and verification

Go files and the Makefile use tabs. Markdown uses two-space list continuation where needed, LF endings, and final newlines.

```bash
make fmt
make verify
make ci-local
```

Architecture checks are fail-closed. A missing required package scope is a failure, not an empty success.

## Commits and pull requests

Use Conventional Commits with a focused scope, for example:

```text
feat(agent): add continuation boundary
fix(provider): bound stream idle timeout
test(tool): cover parallel batch cancellation
docs(migration): explain direct package cutover
```

A pull request should:

- explain the behavioral need and chosen boundary
- identify public API or backend-contract impact
- list exact validation commands run
- include migration guidance for breaking changes
- link the relevant issue or architecture decision

Do not include generated-by or AI co-author attribution in commit messages or pull request bodies.
