# Security scan hardening

The 2026-10-04 repository scan identified thirteen findings in the scanned
revision. The changes below enforce SDK protocol and resource boundaries while
keeping deployment, authorization and backend storage policy application-owned.

| Finding | Enforcement | Regression coverage |
| --- | --- | --- |
| Unbound durable attempt responses | Compare execution, operation, kind, input hash, number, version, lease and decision-specific state before effects or replay. | `TestRuntime_RejectsMismatchedAttemptResponsesBeforeEffects` |
| HTTPTool redirect credential forwarding | Confine every redirect to the original scheme and host, including caller-supplied clients; preserve their stricter policy. | `TestHTTPTool_RejectsCrossOriginRedirectBeforeSendingHeaders` |
| Unbounded attempt payload decoding | Check byte, depth, value and collection budgets before unmarshalling either envelope. | `TestAttemptDecoders_RejectExcessivePayloadBeforeMaterialization` |
| Unbound durable claims | Match the issued ClaimID and Start's requested SpecHash before invoking the Engine. Never release an unrelated returned claim. | `TestRuntime_BindsClaimAndRequestedSpecBeforeEngine` |
| Anthropic silent state accumulation | Count every wire frame and byte; cap blocks and citations before retention. | `TestAnthropicStream_BoundsSilentBlocksAndCitations` |
| Oversized tool-name suggestion work | Reject names before lookup/ranking; bound diagnostic text and compute each candidate's score once. | `TestBus_RejectsOversizedNamesBeforeHintsOrExecution` |
| Unbounded continuation decoding | Check resource budgets before strict decoding and canonicalization. | `TestDecodeContinuation_RejectsResourceExhaustionBeforeSemanticDecode` |
| Transplantable checkpoint/result hashes | Bind domain, execution and spec, plus checkpoint sequence or terminal status/version. | `TestRecordHashes_RejectTransplantAndSequenceRelabeling`; backend contract record-binding cases |
| OpenAI Chat array amplification | Bound JSON collections before unmarshalling; cap total tool items, retained indexes and argument bytes. | `TestChatStream_RejectsDenseArraysAndOversizedNames` |
| OpenAI Responses silent state accumulation | Count all frames/bytes and cap retained indexes and terminal arrays. | `TestResponsesStream_BoundsSilentStateAndTerminalArrays` |
| Unbounded skill traversal | Read bounded directory batches and count all entries, including empty directories; enforce depth, path and a cooperative deadline. | `TestResource_BoundsEmptyDirectoriesAndDepth` |
| Invalid successful FinishExecution responses | Validate identity, immutable spec, exact version transition, terminal status, cleared lease and exact result hash before returning nil. | `TestRuntime_RejectsInvalidFinishResponseWithPartialResult` |
| Recursive typed input validation bypass | Reject recursive input type graphs during tool construction. | `TestTool_RejectsRecursiveInputsAtConstruction` |

## Fixed resource boundaries

- Tool names: 256 UTF-8 bytes, enforced at registration, dispatch and built-in
  provider normalization. Oversized dispatch names produce bounded error feedback.
- Provider streams: 65,536 wire frames and 64 MiB cumulative wire bytes,
  including ignored SSE fields, silent events and heartbeats. Before unmarshalling one
  frame, enforce 16 MiB, 128 levels, 65,536 JSON values and 1,024 elements or
  members per collection. Retained tool/output/block indexes and Anthropic
  citations are capped at 1,024; Chat arguments are capped at 1 MiB per call.
- Continuations and attempt envelopes: 64 MiB encoded JSON, 128 levels,
  524,288 values and 65,536 elements or members per collection. Both encode and
  decode enforce the document bounds, so a newly persisted envelope is readable.
- Skill manifests: 256 regular resources, 1,024 total entries, 32 directory
  levels, 4,096 path bytes and a five-second cooperative traversal deadline.
  The existing 8 MiB per-resource limit remains in force.

Protocol or replay validation failures remain infrastructure errors. A request
rejected after being sent, an oversized tool outcome, or an interrupted stream
does not prove an effect was never executed; durable unknown-effect semantics
still apply. These checks do not make an application backend a trusted authority
against its own malicious writes. Storage authenticity, if required, uses an
application-owned MAC or signature.

## Backend migration

Durable hash helpers now require record context:

```go
checkpoint.ContinuationHash, err = durable.HashContinuation(
    execution.ID, execution.SpecHash, checkpoint.Sequence, checkpoint.Continuation,
)
err = durable.ValidateCheckpoint(execution.ID, execution.SpecHash, checkpoint)
resultHash, err = durable.HashResult(
    execution.ID, execution.SpecHash, expectedVersion+1, result,
)
```

`HashResult` derives terminal status from `result.Failure`. On read, use the
committed `Execution.Version`; on FinishExecution, use `ExpectedVersion+1`.
Backends must compare against their locked record's immutable SpecHash, rather
than accepting a context supplied by a different record.

This changes the Go helper signatures and the stored hash contract. Content-only
legacy digests are rejected; there is no automatic read fallback. Pause dispatch,
drain or suspend active executions, back up storage, validate original payload
digests and record association, and atomically recompute checkpoint and terminal
digests using trusted record metadata. Migrate stored mutation/claim response
receipts as well as current records. Upgrade backend and all workers together
before resuming dispatch. Preserve the original backup for coordinated rollback.
Continuation schema versions 1 and 2 still retain their canonical wire format;
the change is the durable hash envelope around that format.

Every external backend must rerun `durable/contract.RunBackendContractTests`,
including its process-reopen implementation. The contract now rejects copied
checkpoint/result hashes and relabeled checkpoint sequences without mutation.

Local validation: `make verify` and `make ci-local`. Historical cloud scan
findings describe their frozen revision; a new cloud scan is a separate run.
