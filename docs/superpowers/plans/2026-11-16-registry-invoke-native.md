# Registry one-shot invocation: native A2A and original grant

> Execute inline under the user's standing autonomous authorization. PDF rev3.2 is the normative source; no new product requirement is inferred from ADR drafts.

**Goal:** One-shot Registry Invoke/GetInvokedTask must reach the same immutable native A2A runtime using the original target grant, even when the runtime task ID is different and the version is draining.

**Design:** Reuse production `agentRPC` (A2A1.0, correlated JSON-RPC, bounded responses, no redirects). Build a native user message with a stable operation-derived ID and the target context that runtimeAuthority already issues. Preserve the target Identity grant on the aliased task pin in the same PostgreSQL transaction. Machine observation resolves that grant only for the pin's tenant/installation/subject; runtime ID remains a separate internal field. No extra credentials, queue or external service.

**Files:** `internal/registry/invokeapi.go`, `runtime.go`, `store.go`; new focused native invocation tests; `cmd/ahp/trigger_dispatcher.go` uses subject-owned pins so the same invocation mapping is usable by triggers. Existing native SDK composition test gets a one-shot variant. Default native taskstore is still in memory; this change does not claim workflow crash recovery.

## Tasks

- [x] RED: actual pinned upstream A2A server + real Registry PG; old Invoke returned no native task. The failed receipt is retained.
- [x] Native A2A1.0 shared RPC transport, transactional original-grant alias and schema revision7; wrong owner/installation, alias replay/conflict, correct grant on a fresh dispatcher, draining reads and exact JSON-RPC correlation covered. Human historical reads use the original grant while retaining the public runtime task ID.
- [x] Actual Go SDK/State API/mTLS/PG checkpoint composition passed for conversation and one-shot transports. A separate leg proved the actual reference receiver accepts exact text and persists a pending run. Its worker/model are not started; admission/Identity/readiness are named fixtures. The legacy text-kind hint remains compatible with both tested receivers.
- [x] A deterministic real-PG test exposed alias row locking before the subject erasure gate (SQLSTATE55P03). The alias now takes the subject gate first and revalidates its owner when locking the reservation. Final focused series:28/0/0.
- [x] Canonical check on source `ac35f643`:4382/0/51,794.259s, preserved feature tags/Python venv, Go1.26.11, unchanged sources. The first full run4380/2/50 exposed old trigger/admission fixtures; those failures remain recorded. The extra conditional auth skip had a no-effect random mutation; supplement5/0/1 is separate and does not rewrite the whole result.
- [x] Built/deployed five exact-source binaries `ac35f643`, keeping backend configuration and the synchronous State pair unchanged. Registry schema7 and retained original DB read-only verified. Deployed smoke and five live Dot/memory/model regressions passed. New checkpoint/matrix under Downloads; prior manifests remain immutable.

## Review focus

Opaque native task IDs must never replace Identity grant authority. No caller-selected endpoint/version/context. Wrong subject/installation receives no mapping. Deleting the source pin and inserting its alias must be atomic. A draining readiness gate applies to new invocation, not observation. Unknown reply is not proof that invocation did not happen. Keep native process/taskstore recovery explicitly open.

Evidence root: `Downloads/AHP-PDF-Closure-20261115/`. New checkpoint `checkpoints/registry-invoke-20261116/`, service matrix `PLATFORM-MATRIX-20261116-REGISTRY-INVOKE.md`. Separate signed native OCI image remains source `cededfb0`; no image was silently replaced. Native SDK taskstore/process-crash recovery, real Python LangGraph composition, incremental stream/reconnect, revoke-first cancel/SLO, busy rollout/drain, JWT renewal and final profiling remain open. Whole platform is not accepted.
