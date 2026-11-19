# State Store: synchronous replication and primary-loss acceptance

> Execute inline under existing autonomous authorization with executing-plans/TDD. Only the user PDF rev3.2 is normative.

**Goal:** Confirm acknowledged State data survives loss of its PostgreSQL process/node profile and connect the working Mac prototype to a synchronous pair. §2.5 guarantee6 requires a synchronous replica; restarting the same single PG is insufficient.

**Architecture:** Two owned Docker PG17.11 containers on the existing isolated AHP network, separate labelled volumes, exact existing image digest, bounded memory. `remote_apply`, named synchronous standby, dedicated replication role. Start with a small isolated State/API drill, then move the quiescent live DB only; retain the original live DB read-only and leave the test DB on its original server. pgx multi-host `target_session_attrs=read-write` discovers the promoted writer. Manual promotion requires the old writer confirmed stopped; restore a synchronous standby before new mutation ACKs are allowed.

**Scope:** Same Mac/Docker host only. Process/container failure is exercised; physical host/disk failure and an automatic HA controller remain unverified. Do not claim independent failure domains or 99.9% availability.

## Files / interfaces

- `scripts/local-pg-sync.mjs`: owner-aware setup/status/pause/resume/crash/promote/rejoin control; private control path, pinned image and exact network/volume labels, no public ports or credential output.
- `tests/space/state_sync_replication_realstate_test.go`: actual State API+mTLS/PG write/ACK before failover, no ACK while standby is unavailable, old snapshot/readback and new fenced write after promotion/rejoin. Operator requests use a bounded private filesystem handshake, not an HTTP control server.
- Downloads acceptance operator: orchestration/migration/evidence only, outside product; no raw config in Git.

## Tasks

- [x] RED audit: current live PG `replicas=0`, `synchronous_standby_names=''`; PDF guarantee6 not proven. Existing PG17 supports replica WAL but contains ~1GB test DB, so avoid cloning it as the new target.
- [x] Build a guarded two-PG profile and prove `remote_apply` + `sync` on separate volumes.
- [x] Run real State API failover test; preserve timeout/unknown outcomes and retry only the same idempotent operation. Confirm old writer fenced before promotion, then restore replication.
- [x] Freeze current writers, snapshot original live DB, restore into pair, update only private DSNs, start five verified candidate binaries and prove user-visible history/new work. Retain original DB read-only and the private snapshot. Logical fingerprints match for 2082 tables / 165442 rows; fixture test DB remains on its original server.
- [x] Verify a live acknowledged cut through primary loss, reconnect and replica rejoin. Actual Chromium reload preserved the conversation; independent SQL confirmed the new turn, two checkpoints and six stream events on the promoted writer. All five application PIDs/binary hashes stayed unchanged across the database fault. This is a controlled observation, not a recovery SLO.
- [x] Appropriate regressions/network smoke: five deployed regressions, post-fault real-model turn and canonical whole check 4381/0/50 on source `fcfd9ffd`; final smoke passed. Updated service matrix/checkpoint under Downloads records exact source/runtime, limits, manifests and public secret scan. Whole platform remains unaccepted.

Review focus: no split brain/restart policy; loss of standby must not yield durable ACK; unknown acknowledgement is not rollback; old primary cannot rejoin as writer; node failure is not a physical host failure; source backup remains available; no unbounded WAL/volume/cache growth; stop on resource guard rather than touching foreign containers.

Evidence: `state-sync-current-stand-red` fails the actual synchronous-replica requirement. First pair preserved checkpoint/stream after kill but its rewind failed because boot flags were not persisted for PostgreSQL single-user crash recovery; that failed result and the controlled original-WAL repair remain separate evidence. File settings now match boot flags, including WAL retention. `state-sync-pair-complete-green` passed the original full drill. A new pair created from the fixed profile passed `state-sync-fresh-receipt-replay` (source `f69257fc`): real State API/mTLS/PG, no ACK for new write or receipt replay during paused standby, exact confirmed checkpoint/stream/KV after kill, manual promotion after fencing, normal rewind/rejoin, new epoch and stale-writer rejection. Promotion 500ms; synchronous rejoin 1617ms. Go verdict 1/0/0, operator phases 4/4. This isolated drill has no LLM, independent host/disk or automatic-controller claim.

Live activation: `state-sync-live-migration` moved only `ahp_hive_closure_06c3da1663` to the fresh pair and deployed five source-`fcfd9ffd` Go1.26.11 binaries. Snapshot SHA `8798bd9512d80567fbf0f88172117c9890066cbc798713995bed2662cefc9217` remains private. The original server now holds a read-only historical copy of this live DB and the unchanged writable fixture DB. The active pair is resolved through `Downloads/AHP-PDF-Closure-20261115/private/active-database.json` and `private/pg-sync-final/control.json`; do not treat the original server as the live writer. Browser fault drill observed promotion 511ms, replica rejoin 7729ms and 9249ms for the browser's complete operator-recovery wait. No application process restart was needed. The separate signed native OCI agent remains independently pinned to source `cededfb0` / image `sha256:828793a96a2f65a9a4f518ee43b768036ab3ebf46cd0b239db3bfe8e099760d6`.

Acceptance package: `Downloads/AHP-PDF-Closure-20261115/checkpoints/state-sync-20261116/`; service matrix `PLATFORM-MATRIX-20261116-STATE-SYNC.md`. Physical microVM remains the permitted mock; Cloud IAM/KMS is synthetic. Remaining native workflow/taskstore/stream/cancel, rollout, token-renewal and profiling requirements are still open.
