# Durable native SDK hosting: recover the platform run, not only messages

> Continue inline under the user's standing autonomous authorization. PDF rev3.2 §2.3/§2.5/§4.2 and figure6 are normative. This is an open implementation block, not accepted delivery.

**Required outcome:** A2A task submission, graph execution, a crash, replacement worker, effect-result replay, interrupt and human continuation use the same durable State run. No process-local task/question map is required after restart. Native Go and actual Python LangGraph paths must be checked independently, then together through the runtime/gateway boundary. Existing signed runtime and State synchronous pair stay available until a checked replacement exists.

## Observed integration constraints

- `pkg/sdk/a2a_executor.go` creates the State run before submitted, claims later, and calls Suspend/Release before emitting native input-required/completed events. State checkpoints alone omit the native initial message, question identity and protocol history needed by `nativeInvocationInput`.
- Pinned a2a-go/v2 v2.7.0 uses a separate event producer/consumer and an eventpipe with a default256-item memory buffer. Returning from executor `yield` confirms enqueue, not TaskStore.Create/Update durability. Moving a checkpoint next to yield cannot close the crash window.
- Upstream `factory.loadExecutionContext` appends a user answer via TaskStore.Update before invoking the executor. A State mutation requiring a run lease cannot simply be inserted there: the run is waiting and has not been claimed yet. Inbox Enqueue is the authorized durable ingress primitive; its dedup/CAS semantics and the exact native history/version must agree.
- Current State supports opaque common records plus an atomic bundle of KV, inbox ACK, stream and run status. Terminal protocol snapshot/status must commit while the current epoch still owns the run. This implementation retains the selected State-service driver. Do not bypass it with an unfenced SQL taskstore; the PDF's separate per-deployment SQL-driver option is not ruled out by this choice.
- Fresh RED also exposed that both atomic layouts refused `waiting`, while Go SDK dropped its `wait_for`/`deadline` fields. A native question and run suspension therefore could not be published in one transaction. Source `9c6baaf7` fixes this primitive and passed focused real-PG/wire tests (15 PASS, zero failures/skips); this is not yet a durable native handler. Additional concurrent ingress and broader regression checks follow.

### Atomic suspend checkpoint, 16 November

Candidate `367c1cfc270d172f732b8dcf350cffb7ffd0ba94` passes **660 PASS / 0 FAIL / 0 SKIP** across State, State HTTP API, Go State SDK and SDK host with `-race`, Go1.26.11 and unchanged sources. Receipt: `raw/linux-native-state-atomic-suspend-final-regressions.json` under `Downloads/AHP-PDF-Closure-20261115`.

The same thread lock now covers checkpoint/common snapshot, stream, waiting target/deadline and lease removal. Common commits include inbox ACK before evaluating readiness; already unread matching input leaves the run pending. Checked: early, concurrent and unrelated input, blob/record readback, replacement epoch, stale writer, invalid input and transaction rollback after stream conflict. Go SDK fields survive both transports. A separate actual SDK/mTLS/JWT/HTTP/PG leg also passed5/0/0; its issuer is explicitly a context-aligned fixture, not production Identity or LLM E2E.

No schema or table changes. Python common methods already forward the raw run-status mapping; no new Python-host recovery result is claimed. The candidate is **not deployed**. A read-only `/proc` executable/hash check confirmed five existing deployment processes still run `ac35f643`.

Unknown commit outcome is checked for **CommonCommit**: the same key/body returns its receipt after lease removal; changed wait target is KEY_REUSE_MISMATCH. Legacy PutBatch still checks the lease before any global receipt lookup. Its replay-after-release guarantee therefore remains an open separate requirement; do not advertise universal batch ACK recovery or remove fencing to accommodate it. The owned native host direction uses common receipt semantics, and the legacy gap remains in the full platform matrix.

The original cold-handler RED remains open. An atomic waiting primitive alone does not preserve upstream native task/question state, recover an orphan, or prove a real OS/Python LangGraph restart.
- A runtime task ID may differ from its Identity grant. Preserve both and derive scope from verified JWT/mTLS. An unknown reply is not permission to create a new run, message key or tool effect.

### SDK atomic lease end, 16 November

Candidate `c57b57a3b26c82eb2ac65e6f1ec0d2027e3dbce6` adds
`ActiveLease.CommitAndEnd(ctx, CommonCommit)`. It binds the claim fence, stops
heartbeat and execution, then publishes the common state/ACK/stream/run transition
in one State transaction. A waiting or terminal transition is required; a foreign
fence and structurally invalid requests are rejected before stopping the graph.
Use a bounded cleanup context independent of `lease.Context()`.

Actual Go host SDK -> mTLS/JWT -> State HTTP -> PostgreSQL tests deliberately lose
the first successful HTTP reply for waiting/done/failed/canceled. The durable run
and snapshot exist; the identical retry returns the same revision/cut; one stream
event remains; heartbeat does not resume; a new key from the old epoch is refused.
Identity is a fixture, not a production IAM claim. First GREEN attempt exposed a
test mismatch: atomic `done` requires a close reason whereas standalone Release
permits omission. The test now records `graph_completed`; State policy was not
weakened. Known-stale and invalid-input host guards are tested separately.

Receipt `raw/linux-native-host-atomic-end-regressions.json`: **675 PASS / 0 FAIL /
0 SKIP**, State/State API/Go State SDK/host SDK with Go1.26.11 and `-race`, unchanged
sources. This is not the canonical whole-platform check. Read-only executable
hash observation still finds the five deployed `ac35f643` processes.

**Integration remains open:** the existing LangGraphExecutor still uses separate
Suspend/Release and upstream memory taskstore. Adding this primitive does not
change that fact. The next implementation must actually use it in the State-owned
native host; persist native input/question/history, then close the cold-handler
RED, orphan recovery and actual Python LangGraph/process-kill requirements.

## Acceptance sequence

- [x] RED: entire handler/taskstore discarded after input-required. Source `7625529f`, receipt `raw/linux-native-sdk-cold-handler-red.json`: executor-only replacement passes, fresh handler fails with native `TASK_NOT_FOUND` before graph execution. The initial checkpoint Put ACK and waiting run were observed before replacement; the failed leg does not prove checkpoint restoration or an OS-process restart.
- [ ] Choose and implement an owned native protocol-state path using State primitives. Preserve initial input, native task/context/question IDs, history, version and exact pending invocation. Persist final/suspended state before releasing its epoch; include inbox ACK only with the corresponding durable state.
- [ ] Verify ownership, CAS/replay/mismatched input, lease loss, duplicate answers, unknown commit ACK and failure between checkpoint and protocol publication. No buffered event is advertised durable before its State commit.
- [ ] Kill real host/worker processes with empty local homes. GetTask/history/HITL survive; a replacement claims a higher epoch; the old writer fails. A non-idempotent fixture behind the actual Tool Gateway is executed once after lost ACK/recovery.
- [ ] Actual Python LangGraph: preserved super-step/pending writes, Command(resume), original run and effect key through native A2A. A Go GraphFunc or history reconstruction cannot substitute for this leg.
- [ ] Expose the supported protocol capabilities precisely; verify stream/reconnect and cancellation separately. Run canonical regressions, deploy the checked candidate, then refresh matrix and immutable evidence.

### State-owned Go host checkpoint, 16 November

Candidate `73a481148c8f298f044037a7f4b2dbfafdd42680` implements
`NewDurableA2AHandler` and `RunDurableNativeWorker`. This is a new explicit host
constructor, not an alteration of the legacy upstream-memory constructor.
Admission persists the original native message in the State run under an
idempotent message key. The public native task ID is the run ID; the Identity
grant task stays separate. Per-run common snapshots retain native history,
question and pending input IDs. Terminal/waiting snapshot, input ACK, stream and
run transition use `ActiveLease.CommitAndEnd`. A filtered worker obtains fresh
State-issued credentials and recovers only the native payload profile.

Verified receipt `raw/linux-native-state-owned-final-regressions.json`: **682
PASS / 0 FAIL / 0 SKIP**, Go1.26.11/race, immutable sources. This comprises four
State/SDK packages and three selected native State integration tests; it is not
the canonical whole-platform suite.

- Existing cold-handler RED now passes on the new constructor: initial send,
  question, entirely fresh handler, same-run answer, terminal replay after a
  second fresh handler. Original question ID and input survive.
- Actual process test uses three separate native HTTP hosts with fresh empty
  working directories. First SIGKILL occurs while waiting; second while the
  accepted answer is executing. The third host's background worker continues
  without a caller resend; the caller only polls GetTask until completion.
- Native ingress verifies gateway mTLS and signed task JWT; State uses mTLS and
  PostgreSQL. Identity authorization and the HTTP external effect are fixtures.
  The Go graph performed the fixture effect once; the answer was ACKed, epoch
  increased, and the previous worker's write was rejected. This is NOT the
  unknown-external-outcome window and NOT actual Tool Gateway replay.
- Changed input under a reused initial/answer message ID is refused. Other grant
  and subject cannot read the task. An unrelated runtime's pending run remains
  unclaimed. Invalid history options are rejected before admission.

Remaining: actual Python LangGraph composition, Tool Gateway crash window,
stream/reconnect, cancel/SLO, async admission and list capabilities, long-run JWT
renewal, host packaging/deployment and canonical whole checks. The old deployed
native OCI and `NewA2AHandler` path are not made durable by this candidate.
Full platform acceptance is still false.

Next Python seam (static source finding, not a test result):
`sdk/python/ahp_state/agent.py` `run_leased` enters `_guarded_turn`, starts its own
heartbeat, and `_drive` calls Suspend/Release itself. Do not naively call that
whole lifecycle underneath the Go host: the host would then try to publish after
Python has cleared its lease. Choose one lease/protocol owner and integrate the
actual graph/saver at the execution boundary. Pending writes and Command(resume)
must survive real Python process death, not just Go GraphFunc checkpoint fixtures.

## Files to follow

`pkg/sdk/a2a_handler.go`, `a2a_executor.go`, `a2a_input.go`, `lease.go`; `sdk/go/state/common.go` and inbox primitives; Python `sdk/python/ahp_state/agent.py` and checkpoint saver; `tests/space/sdk_native_hitl_realstate_test.go` and process recovery fixtures. Private operator/evidence remain outside Git. Common storage schema work does not itself implement native taskstore or workflow recovery.

## Implementation direction under evaluation

Use a State-owned native host rather than treating the upstream memory taskstore as durable. One native task maps to one State run; its public run ID stays distinct from the Identity grant. Initial admission must be idempotent under the verified grant/message key, even when the first A2A reply is lost. Store enough immutable native input to reconstruct the task after process loss, not just the graph's extracted text.

The host's protocol receiver accepts input; its execution worker owns lease/heartbeat and the actual Go or Python runtime. Native waiting/final snapshot, stream/outbox, processed inbox and run transition commit together through State. An old writer cannot publish a protocol terminal after losing its epoch. A2A transport stays the pinned native implementation where possible; transactional task ownership must not depend on its256-item local event buffer. Reads reconstruct from State; no per-process map is the authority.

Before selecting exact exported constructors, verify the outstanding seams: native Store.Update of a human message occurs before executor claim; response-version/CAS must remain correct; a new host must recover an orphan without a caller resend; terminal/duplicate initial sends must return the original task rather than add a second FIFO run. These are implementation requirements, not permission to narrow acceptance to history-only restoration.
