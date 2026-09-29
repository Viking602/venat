# Architecture boundaries

The production package graph remains one-way. The practical-runtime extension adds model-backed memory, local output artifacts, and in-memory worker collaboration within the existing families; historical SDK-only exclusions in ADR-029 do not exclude these implemented capabilities.

## Package graph

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

Allowed project imports by package scope:

| Scope | Allowed project package families |
| --- | --- |
| `message` | `message` |
| `provider` | `provider`, `message` |
| `tool` | `tool`, `message` |
| `skill` | `skill` |
| `agent` | `agent`, `message`, `provider`, `tool`, `skill` |
| `orchestration` | `orchestration`, `agent`, `message`, `provider`, `tool` |
| `durable` | `durable`, `agent`, `message`, `provider`, `tool` |

Subpackages may import their own package family. Tests follow the same graph; an external test package may import the package it tests.

## Consequences

- `message`, `provider`, `tool`, and `skill` remain leaf contracts.
- `agent` owns the model/tool loop, optional working memory, output artifacts, and completion checks; it does not own a multi-agent scheduler or persistence backend.
- `orchestration` depends inward on Agent, provider, and tool values for live workers and task tools, never on durability.
- `durable` depends inward on Agent and effect contracts and never on orchestration.
- Applications are the composition root. They map routes to engines, persist orchestration state when needed, and inject a durable backend when needed.
- Protocol adapters and domain integrations stay in application or ecosystem modules unless they implement an approved package-family extension.
- Provider pull streams and tool push updates remain domain-specific under ADR-030; do not introduce a generic stream package or competing tool driver protocol.
- Local OutputStore persistence is tool-output retrieval, not an application execution database. In-memory Runtime tasks are independently addressable but do not claim crash recovery.
- Summary and streaming features must perform useful work through existing model/tool contracts; an extension interface alone is not a completed feature.

## Executable enforcement

`make architecture-check` runs five fail-closed gates:

1. `sentrux check .` rejects cycles and excessive file coupling.
2. `scripts/check-business-words.sh` scans every required production scope for domain vocabulary leakage.
3. `scripts/check-public-any.sh` runs an AST command over every required public package scope.
4. `scripts/check-import-boundaries.sh` checks production, internal-test, and external-test imports against the table above.
5. `scripts/check-legacy-absence.sh` rejects removed package imports and symbols in production code and current documentation.

Each package scope must exist and contain production Go source. Deleting a directory cannot turn a gate green by producing an empty package list.

## Public API shape

Exported functions must not return `[]any`. Exported fields containing `any` require `// godoc-allow-any` immediately above the field. A genuinely open function result requires `//venat:allow-public-any` immediately above the declaration. The exception must identify a real provider, host payload, or JSON Schema boundary; convenience is not sufficient.

## Demand-driven extension

Add an interface only with a second implementation. Export a symbol only with its first non-test consumer outside the package. Prefer an application adapter over a new core package when the behavior contains routing values, identity, approval, quota, storage schema, deployment, or domain policy.
