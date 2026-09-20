# ADR-024 Five-Layer Architecture and Worker Integration

## Status

Accepted — 2026-08-15. Effective from v0.15.0. Amends the four-layer
wording in ADR-015 / ADR-016 / ADR-017 documentation. Does not rename
the `worker` package.

## Context

Docs described four layers: Packs → Multi-Agent → Agent Loop → Durable
Runner. `worker/` is a real integration layer: it polls envelopes, takes
leases, drives `agent.Engine` and `multiagent` teams, and calls the root
`Runner`. Leaving it unnamed made import rules incomplete.
`.sentrux/rules.toml` still refuses `layer_direction` because Sentrux
0.5.7 treats any cross-layer import as illegal, which would block the
façade → `internal/core` composition root.

`scripts/check-import-boundaries.sh` already locked `api`, `agent`,
`multiagent`, and the root façade. It did not lock `worker`, `coding`,
or `packs`.

## Decision

### Five layers

```
Packs / Workflow / Examples     domain configuration; host-mounted
        ↓ host wiring
Worker integration (worker/)    poll, lease, execute, team drive
        ↓
Multi-Agent (multiagent/)       schedule, dispatch, handoff, vote
        ↓
Agent Loop (agent/)             one-task bounded loop
        ↓
Durable Runner (root + internal) persist, recover, govern
```

`coding/` is a domain runtime used by hosts and examples. It is not a
sixth kernel layer. Packs name coding tools; they must not import
`coding/`.

### Import seams (machine-checked)

Existing:

- `api/` imports no Venat package
- `agent/` never imports `multiagent/`
- `multiagent/` never imports the root module, `worker/`, or `internal/`
- root façade never imports `multiagent/`

Added:

- `worker/` never imports `packs/` or `coding/`
- `packs/` never imports `coding/`, `worker/`, or the root module
- `coding/` never imports `worker/`, `packs/`, or the root module

`worker/` **may** import the root `venat` package: that is the
integration seam.

### Sentrux

Keep `layer_direction` off. Document the five layers and the script
rules in `.sentrux/rules.toml`. Sentrux continues to enforce cycles,
coupling, and god files.

## Anti-patterns rejected by this ADR

| Anti-pattern | Why it is wrong |
| ------------ | --------------- |
| A pack importing `coding` to bind tools | Packs are manifests; hosts bind drivers |
| Worker importing a pack to "help" wiring | Reintroduces domain vocabulary into the integration layer |
| Enabling Sentrux `layer_direction` without a finer rule | Blocks façade → internal composition |

## Impact

`make architecture-check` fails new imports that cross the added seams.
Docs and `AGENTS.md` say five layers.

## References

- ADR-008, ADR-015, ADR-016, ADR-017
- `scripts/check-import-boundaries.sh`
- `docs/architecture-boundaries.md`
