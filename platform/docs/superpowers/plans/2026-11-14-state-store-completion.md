# State Store working integration Implementation Plan

> **For agentic workers:** Execute inline with superpowers:executing-plans. User authorized autonomous direct work; no Qwen/delegation. Preserve sealed research and all other worktrees.

**Goal:** Integrate the proven common State candidate into the actual platform branch, finish safe retention and large shared data, and reduce append-path history work with measured results.

**Architecture:** Use the existing Go State owner/HTTP/mTLS and Go/Python SDK. Records, leases, receipts and lifecycle remain State-owned; native execution belongs to harness adapters. Reviewed common snapshots are complete logical recovery documents. Generic storage never infers native reference/lifetime semantics from opaque JSON.

**Tech Stack:** Go 1.26, pgx/PostgreSQL, existing Redis/Artifact APIs, Python ADK SDK adapter. No new external storage dependency.

**Spec:** User-selected PDF rev3.2 SHA256 044e045b73b7c34b642bd3c9eb5701b93eed470873a1e0a44e8b1d024821a7e2; user request to finish four remaining items; sealed REV47/43/48/49 evidence is a baseline, not product acceptance.

## Global Constraints

Mac only. One heavy suite at a time using the existing global resource lock; pressure<4, free disk>=8GiB, bounded Go GOMAXPROCS=1/GOMEMLIMIT=128MiB/-p=1. Own fresh PostgreSQL databases, own Redis/MinIO if needed, Compose label ahp-handbook. Secrets only by ignored/external reference; never printed. No mutation of sealed evidence or foreign worktrees/containers. Keep native exact recovery, source epoch fencing, CAS/idempotence/atomicity and explicit references. Fixture model is not real LLM acceptance.

## Review Focus

- Lost ACK and same key with changed bytes must not republish after pruning, migration or lease recovery.
- Snapshot pins/hold/active writers and opaque native cross-references must not silently become disposable.
- User/app shared data and large chunks must outlive the writing session and keep correct scope; failed publication must not expose mixed state.
- An append fast path must reject invalid targets/positions/markers and detect read corruption without replaying the entire prior document for each delta.
- Mixed legacy/new deployments and old readers/exporters must fail explicitly rather than drop state; root-aware retention needs a versioned transfer format.

## Task 1: Real platform and SDK integration

**Files:** reviewed REV47 differences under internal/state, stateapi, identity, identityapi, artifacts; cmd/ahp/state_common.go, state_checkpoint_history.go, liveness_state.go; sdk/go/state/common.go; sdk/python/ahp_state/common.py; docs/state-common.md.
**Interfaces:** consumes CommitRecords/ReadRecords/ReadRecordsBundle from REV47; produces opt-in AHP_STATE_COMMON_MODE=interval mounting and native-safe common SDK methods.

- [ ] Check fresh branch baseline State tests on own PostgreSQL and record any pre-existing failure.
- [ ] Port only the 76 changed/new reviewed backend files; keep old defaults for legacy callers.
- [ ] Add failing mounting/SDK HTTP behavior tests, implement mode validation, common Artifact reference scan, DTOs and methods.
- [ ] Verify real-PG State/StateAPI/Identity/Artifact regressions plus cmd build and SDK transports.
- [ ] Commit only task-owned files and record exact evidence/commands.

## Task 2: Safe common history retention

**Files:** internal/state/common_retention.go and tests, stateapi common admin, SDK methods, deployment transfer validator/profile as required.
**Interfaces:** consumes complete common snapshots, interval roots/anchors and lease/hold barriers. Produces explicit PruneCommonHistory with conservative protected prefix/root semantics, durable operation replay and live payload/reference reclamation.

- [ ] Define and test pin/hold/active-writer refusal, root retention, duplicate requests and stale expected head; generic unknown native retention stays refused.
- [ ] Write real-PG RED tests: preserved historic root + current head; reclaim unreachable closed versions/payloads; keep idempotency receipts after cuts leave readable window.
- [ ] Implement transactional bounded prefix pruning with explicit native roots/retention floor and audit/replay.
- [ ] Extend versioned transfer to preserve floors, retained roots and receipts; prior formats must explicitly refuse the new schema.
- [ ] Verify cold native resume, pinned historical read, GC/Artifact roots, migration and late rollback, then commit.

## Task 3: Versioned large shared data and ADK adapter

**Files:** sdk/python/ahp_state shared payload helper and ADK adapter package; matching generic Go helper; native integration tests.
**Interfaces:** consumes owner-scoped KV and atomic common+KV publication. Produces immutable content-addressed chunk values with a small versioned per-key owner manifest, hydrated from a coherent manifest snapshot.

- [ ] Test real native ADK user/app aggregate>1MiB and a large single value, cross-user read rules, source-session deletion, failed publication and cold process.
- [ ] Implement bounded immutable chunks staged before atomic manifest+event publication; verify content hashes; never store shared data as session-owned Artifact.
- [ ] Keep explicit limits/catalog rules; test duplicate chunks, concurrent owner CAS, empty/null/Unicode values and orphan-stage denial/cleanup policy.
- [ ] Verify old owner profile migration/refusal explicitly and Go/Python SDK parity, then commit.

## Task 4: Incremental append hot path

**Files:** internal/state/common append/version storage/integrity and tests; no native runtime replacement.
**Interfaces:** consumes validated delta, unchanged records/ordered roles, current integrity root and metadata stats; produces a bounded incremental append mode with exact full historical reads.

- [ ] Profile current same ADK history trace SQL/CPU and write tests for malformed append/target/ordinal and deleted relations/opaque bytes.
- [ ] Choose the smallest incremental representation with verifiable integrity (reviewed metadata counters/segment digests or immutable shared tree); record version/upgrade semantics before changing writes.
- [ ] RED->GREEN append, mixed full/delta, stale writers, lost ACK, corruption and rollback cases.
- [ ] Compare identical baseline/new native trace and bounded concurrent smoke; report CPU/SQL/latency and storage separately, then commit.

## Task 5: Whole platform verification and handoff

- [ ] Run npm run check with own PostgreSQL and Redis, relevant deployment cmd builds, SDK checks and native eight-adapter suite on final source pins.
- [ ] Run accessible composed platform smoke and real protocol checks; fixture and real LLM results distinguished.
- [ ] Independent final review only if compatible with user's gateway restriction; otherwise explicitly recorded self-review with RED->GREEN fixes.
- [ ] Publish readable Downloads report, exact status matrix, source hashes and evidence; finish the goal only when required implementations and their checks are complete.
