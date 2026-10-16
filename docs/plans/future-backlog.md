# Future backlog

This backlog is non-normative. Items require demonstrated consumers and a decision-complete design before implementation.

## Provider and tool quality

- Expand adapter conformance fixtures for provider-specific streaming edge cases.
- Add measured performance baselines for long streams and large tool schemas.
- Publish separate ecosystem adapters only when ownership and maintenance are clear.

## Agent execution

- Improve context preparation benchmarks without adding hidden model calls.
- Expand continuation compatibility fixtures for future schema evolution.
- Add more output-policy schema coverage when real consumers need additional keywords.

## Ecosystem and application integration

- Document application patterns for integrating `agent.Continuation` and
  effect interceptors with application-owned databases and object stores.
- Publish separate ecosystem adapters only when at least two production
  consumers share the same contract.
## Explicit non-goals

The backlog does not reserve core types for application identity, routing strategy, approvals, quotas, deployment, domain records, or backend schema. Those remain application or ecosystem responsibilities.
