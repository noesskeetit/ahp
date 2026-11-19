# Native SDK HITL continuation implementation plan

> **For agentic workers:** Use superpowers:executing-plans inline. User authorized autonomous implementation; no unverified gateway delegation.

**Goal:** Resume the same native A2A task and AHP State run after input-required, preserving original input and prior checkpoint.

**Architecture:** The native server owns its task history; SDK resolves the original message from the stored task and replays the original CreateRun binding. A continuation is an idempotent State inbox event. The runtime graph owns checkpoint restore and consumption/acknowledgment of that event. State payload immutability, FIFO and fencing remain enforced.

**Tech stack:** Go1.26.11, pinned a2a-go/v2@v2.7.0, existing State API, PostgreSQL, actual ingress/State mTLS.

**Spec:** User PDF rev3.2, Figure 18 and custom-agent lifecycle. This plan implements that contract; it adds no product requirements. Source PDF remains private under Downloads.

## Constraints and decisions

- Keep AHP grant ID distinct from opaque native task ID; only verified principal grants execution.
- Keep run payload immutable. Do not weaken State key-reuse checks.
- Native continuation must name an existing input-required task, its conversation and a stable message ID.
- Preserve the runtime's wait kind in the server-produced question metadata so repeated delivery retains the same inbox kind after State wakes the run.
- Inbox event records run/grant/native task/message/question IDs and the new input. Same key with another body is refused by State.
- Do not emit another submitted-task event for an existing native task.
- The graph reads/acks its inbox under its current fence after persisting its own checkpoint. This change does not add an atomic checkpoint+inbox-ack API.
- A new executor object is a process-memory reset test, not an OS-process restart. Default upstream taskstore is in-memory; durable taskstore and full crash recovery remain a separate required platform task.

## Task 1: reproduce on real State

Create `tests/space/sdk_native_hitl_realstate_test.go`.

- [x] Write `TestSDKNativeA2AHITLResumesSameRunRealState`: actual A2A HTTP server; first graph call performs one labelled HTTP fixture effect, checkpoints, interrupts; second input completes from the same checkpoint.
- [x] Assert one run, unchanged original payload, increasing epoch, one effect, two graph entries, one durable continuation event, and its acknowledgment after the graph's next checkpoint.
- [x] Run it with `-tags=ahp_exec,ahp_data,state_integration` and capture functional RED before product edits.

## Task 2: preserve input and deliver the answer

Modify `pkg/sdk/a2a_executor.go`; use a focused helper file if needed.

- [x] Resolve original input from `ExecutorContext.StoredTask.History`, checking task/context and message identity before dispatch.
- [x] Reuse CreateRun's original payload, verify State's grant/thread binding, enqueue continuation by stable message ID.
- [x] Skip submitted-task creation for continuation; retain ordinary submitted behavior for first invocation.
- [x] Put the wait kind in question metadata produced by the SDK; keep the State lease lifecycle unchanged.
- [x] Verify functional GREEN and existing SDK grant/claim/cancel negative cases.

## Task 3: challenge the recovery boundary

- [x] Run a continuation after replacing the executor object while retaining the actual upstream store; label the boundary accurately.
- [ ] Reject malformed continuation history and another question/message binding before graph execution.
- [x] Check the original-input key-reuse and stale-lease tests still run and pass.
- [ ] Write scoped evidence and remaining native taskstore/replay requirements; do not mark the whole platform accepted.

## Review focus

- Old unacknowledged answers must not silently answer another question.
- Answer arrival before Suspend must not lose the wakeup.
- An unconfirmed side effect must not become permission to repeat it.
- Client metadata must not replace task or conversation authority.
- Process-local A2A storage must not be described as crash recovery.

## Execution rulings

- Native HTTP RED confirmed that CreateRun was receiving continuation input.
- After preserving original input, the graph resumed (two entries, effect once) but the real State API refused Go SDK AckInbox: top-level `run_id` is unknown; the server requires `fence`.
- Repair `sdk/go/state/inbox.go` as a prerequisite, with independent real-State regressions for the direct Enqueue receipt and fenced AckInbox. Keep the State server contract unchanged.

- Verified source `c257cb60`: 86 PASS / 0 FAIL / 0 SKIP across complete Go SDK/state-client packages plus 7 selected real-State integration cases. Evidence label `native-sdk-hitl-reset-authority-green`.
- Enqueue receipt decoding, AckInbox fence shape and bounded error reason parsing were repaired against independent real-State RED/GREEN cases.
- Request-authority mismatch now returns an A2A permission error before SDK execution events; invalid stored-task/message bindings have 12 negative cases. This does not replace host/taskstore ownership authorization.
- Remaining challenge: replay of an already consumed message while another human question is active, fully durable native taskstore, and OS-process restart. These are not accepted by the executor-object replacement proof.
