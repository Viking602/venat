# Architecture decision records

ADRs are immutable decision history. A later ADR may supersede their live guidance without rewriting the original record.

## Current architecture

- [ADR-031 — Core Agent SDK consolidation](ADR-031-core-agent-sdk-consolidation.md) consolidates the production capability graph to `message`, `provider`, `tool`, `skill`, and `agent`, superseding ADR-029's inclusion of `orchestration` and `durable`.
- [ADR-029 — Agent SDK and optional durable runtime](ADR-029-agent-sdk-and-optional-durable-runtime.md) defines the direct-import Agent SDK cutover from the earlier platform surface.
- [ADR-030 — Stream lifecycle semantics](ADR-030-stream-lifecycle-semantics.md) keeps provider pull streams and tool push updates domain-specific while fixing their shared ordering, backpressure, bounds, terminal, and retry rules.
- [ADR-019 — Project identity: Venat](ADR-019-project-identity-venat.md) records the module identity.
- [ADR-009 — Capability public API](ADR-009-capability-public-api.md) remains the origin of the public `any` discipline where ADR-029 retains it.

For exact current signatures, use package documentation and [Public API](../public-api.md). For package boundaries, use [Architecture boundaries](../architecture-boundaries.md).

## Historical archive

The records below remain unchanged to explain prior releases. They are not current usage guidance when they conflict with ADR-029 or ADR-030.

- [ADR-001 — Profile and Agent instance separation](ADR-001-profile-agent-instance-separation.md)
- [ADR-002 — Task DAG failure policy](ADR-002-task-dag-failure-policy.md)
- [ADR-003 — Plugin registry lifecycle](ADR-003-plugin-registry-lifecycle.md)
- [ADR-004 — Middleware order and short circuit](ADR-004-middleware-order-and-short-circuit.md)
- [ADR-005 — Capability invoker governance layer](ADR-005-capability-invoker-governance-layer.md)
- [ADR-006 — Blackboard evidence model](ADR-006-blackboard-evidence-model.md)
- [ADR-007 — Event-store replay semantics](ADR-007-eventstore-replay-semantics.md)
- [ADR-008 — Framework versus business boundary](ADR-008-framework-vs-business.md)
- [ADR-010 — Usage and budget policy composition](ADR-010-usage-budget-policy-composition.md)
- [ADR-011 — Four-layer context model](ADR-011-context-four-layer-model.md)
- [ADR-012 — Storage contract position](ADR-012-storage-contract-position-d.md)
- [ADR-013 — Memory kernel versus pipeline](ADR-013-memory-kernel-vs-pipeline.md)
- [ADR-014 — Agent ontology stance](ADR-014-agent-ontology-stance.md)
- [ADR-015 — Strong bounded Agent loop](ADR-015-strong-bounded-agent-loop.md)
- [ADR-016 — Explicit multi-Agent scheduler](ADR-016-explicit-multi-agent-scheduler.md)
- [ADR-017 — Durable execution boundary](ADR-017-durable-runner-boundary.md)
- [ADR-018 — Self-sufficient Agent layer](ADR-018-self-sufficient-agent-layer.md)
- [ADR-020 — v0.15 architecture program](ADR-020-v015-architecture-program.md)
- [ADR-021 — Memory surface unification](ADR-021-memory-surface-unification.md)
- [ADR-022 — Unit-of-work capability split](ADR-022-unit-of-work-capability-split.md)
- [ADR-023 — Dual-model convergence](ADR-023-dual-model-convergence.md)
- [ADR-024 — Five-layer architecture](ADR-024-five-layer-architecture.md)
- [ADR-025 — Root façade slimming](ADR-025-runner-facade-slim.md)
- [ADR-026 — Identity and collaboration types](ADR-026-identity-and-collaboration-types.md)
- [ADR-027 — Package map and artifact store](ADR-027-package-map-and-artifact-store.md)
- [ADR-028 — Agent harness and session](ADR-028-agent-harness-and-session.md)

## Adding a decision

Use the next numeric identifier. Include status, context, decision, invariants, alternatives, consequences, migration impact, and verification. A replacement decision must identify the exact guidance it supersedes.
