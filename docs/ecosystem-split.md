# Ecosystem split

Venat keeps only application-neutral execution contracts in the core module.

## Core module

The core module owns:

- provider-neutral messages and model streaming
- typed tools and validation
- reusable instruction resources
- one bounded Agent loop, its continuation format, and effect interception seams

These contracts are useful across products without assuming identity, organization, deployment, business state, or storage schema.

## Application ownership

Applications own:

- Agent identity, roles, teams, and routing strategy
- authorization, approvals, quotas, risk, and retry policy
- domain records and state machines
- orchestration-state persistence
- durable backend schema and operations
- credentials, tenancy, observability, and deployment
- reconciliation investigations and decisions

The application is the composition root. It selects concrete providers, tools, schedulers, executors, and backends.

## Adapter modules

Protocol-specific or operational integrations should ship in separate modules when they do not belong to a model provider adapter already maintained here. Examples include external tool protocols, timers, webhooks, hosted queues, databases, workflow services, and sandboxes.

An adapter should target one narrow current interface:

- `provider.Driver` and `provider.StreamInterceptor`
- `tool.Driver` and `tool.Interceptor`
- `skill` resource loading
- `agent.BoundaryObserver` and `agent.Hook`

## Acceptance rule

A capability belongs in the core only when its vocabulary and invariants remain useful without application policy. A second real implementation is required before adding a core interface. Otherwise, build the adapter in the consuming application or ecosystem module.
