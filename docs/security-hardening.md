# Security scan hardening

The 2026-10-04 repository scan identified thirteen findings in the scanned
revision. The changes below enforce SDK protocol and resource boundaries while
keeping deployment, authorization and backend storage policy application-owned.

| Finding | Enforcement | Regression coverage |
| --- | --- | --- |
| HTTPTool redirect credential forwarding | Confine every redirect to the original scheme and host, including caller-supplied clients; preserve their stricter policy. | `TestHTTPTool_RejectsCrossOriginRedirectBeforeSendingHeaders` |
| Provider caller-client redirect credential forwarding | `openai` and `anthropic` drivers wrap a caller-supplied `http.Client` in a copy that confines redirects to the original scheme and host before and after its own `CheckRedirect`; the caller's client is not mutated. | `TestClientOrDefault_ConfinesCallerClientRedirects` |
| Anthropic silent state accumulation | Count every wire frame and byte; cap blocks and citations before retention. | `TestAnthropicStream_BoundsSilentBlocksAndCitations` |
| Oversized tool-name suggestion work | Reject names before lookup/ranking; bound diagnostic text and compute each candidate's score once. | `TestBus_RejectsOversizedNamesBeforeHintsOrExecution` |
| Unbounded continuation decoding | Check resource budgets before strict decoding and canonicalization. | `TestDecodeContinuation_RejectsResourceExhaustionBeforeSemanticDecode` |
| OpenAI Chat array amplification | Bound JSON collections before unmarshalling; cap total tool items, retained indexes and argument bytes. | `TestChatStream_RejectsDenseArraysAndOversizedNames` |
| OpenAI Responses silent state accumulation | Count all frames/bytes and cap retained indexes and terminal arrays. | `TestResponsesStream_BoundsSilentStateAndTerminalArrays` |
| Unbounded skill traversal | Read bounded directory batches and count all entries, including empty directories; enforce depth, path and a cooperative deadline. | `TestResource_BoundsEmptyDirectoriesAndDepth` |
| Recursive typed input validation bypass | Reject recursive input type graphs during tool construction. | `TestTool_RejectsRecursiveInputsAtConstruction` |

## Fixed resource boundaries

- Tool names: 256 UTF-8 bytes, enforced at registration, dispatch and built-in
  provider normalization. Oversized dispatch names produce bounded error feedback.
- Provider streams: 65,536 wire frames and 64 MiB cumulative wire bytes,
  including ignored SSE fields, silent events and heartbeats. Before unmarshalling one
  frame, enforce 16 MiB, 128 levels, 65,536 JSON values and 1,024 elements or
  members per collection. Retained tool/output/block indexes and Anthropic
  citations are capped at 1,024; Chat arguments are capped at 1 MiB per call.
- Continuations: 64 MiB encoded JSON, 128 levels, 524,288 values and 65,536
  elements or members per collection. Both encode and decode enforce the
  document bounds, so a newly persisted continuation is readable.
- Skill manifests: 256 regular resources, 1,024 total entries, 32 directory
  levels, 4,096 path bytes and a five-second cooperative traversal deadline.
  The existing 8 MiB per-resource limit remains in force.

Protocol or replay validation failures remain infrastructure errors. A request
rejected after being sent, an oversized tool outcome, or an interrupted stream
does not prove an effect was never executed; only `provider.ErrNotStarted` and
`tool.ErrNotExecuted` prove that an effect did not begin.

Local validation: `make verify` and `make ci-local`. Historical cloud scan
findings describe their frozen revision; a new cloud scan is a separate run.
