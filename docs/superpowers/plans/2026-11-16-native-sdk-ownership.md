# Native SDK ownership implementation plan

> **For agentic workers:** Use superpowers:executing-plans inline under the existing autonomous authorization.

**Goal:** Bind all native A2A task operations to the verified AHP scope, grant and conversation before upstream reads or updates its taskstore.

**Architecture:** Add `sdk.NewA2AHandler(executor, options...)`, wrapping the pinned upstream handler with a mandatory principal interceptor. Derive upstream authenticated owner identity from verified scope/task/context; never from JSON metadata. Native SendMessage context must match the task token before history is appended.

**Spec:** User PDF rev3.2 custom-agent A2A/task-token contract. This is an implementation repair, not an additional source of product requirements.

**Tech stack:** Go1.26.11, a2a-go/v2@v2.7.0, existing transport.Principal and State SDK.

- [x] Reproduce cross-subject/tenant/deployment/grant/context GetTask, CancelTask, SendMessage, SSE subscribe/send and ListTasks against the bare upstream composition.
- [x] Add `pkg/sdk/a2a_handler.go`: require a coherent task principal, set stable owner identity and reject mismatched message context before native taskstore operations.
- [x] Attach the wrapper in existing SDK HTTP/Registry/HITL integration entrypoints; retain signed-JWT/mTLS/real-State checks.
- [x] Verify foreign requests cannot change history or cancel/execute; the owner can still read/list/continue.
- [x] Document ingress mTLS/JWT verification prerequisites and custom taskstore ownership contract.
- [x] Run package/real-State regression and the whole control on the fixed candidate.

Constraints: preserve original run input, lease fencing and SDK HITL. A per-task ownership guard is not a durable taskstore and does not prove process recovery. Keep physical microVM mock and Cloud IAM/KMS synthetic profiles as authorized. No remote publication.

Verified evidence: `native-a2a-owner-boundary-red` reproduced private reads and cancellation under a foreign principal; `native-a2a-owner-boundary-green` passed 94 test records including seven real-State cases. `native-a2a-owner-signed-realstate-green` passed the three native HTTP/Registry/HITL cases with real signed JWTs and mTLS. The default store remains in-memory; custom durable-store filtering is not established by this proof.

Whole control: `native-a2a-owner-canonical-whole`, source `11399a7de18ab9230da49fad88a71872996b675a`, 4359 PASS / 0 FAIL / 50 SKIP, 645.216 s, tracked host/Linux sources unchanged. Go1.26.11, feature tags and Python venv retained. The running stand remains `cededfb0`; this is not deployment acceptance of the new SDK.
