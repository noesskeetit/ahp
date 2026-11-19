# Native SDK shared host and ingress

Execute inline under existing autonomous authorization. User PDF rev3.2 §4.2 and its three deployment contacts remain the specification; no additional product requirements.

Goal: serve distinct users/tasks through one SDK instance without using another request's State credential. The host verifies the gateway certificate separately from the task token's target workload and checks active authorization on every ingress request.

- [x] Reproduce a shared SDK configured with a fixed other-subject State token; refuse before creating a run or executing graph code.
- [x] Guard the State-returned thread scope against the verified ingress principal.
- [x] Provide bounded native ingress middleware and request-bound token source; missing peer/JWT, wrong workload, human token and revoked grant fail closed.
- [x] Exercise two concurrent signed users sharing one SDK/State client on actual A2A/mTLS/PostgreSQL; each checkpoint and history remain in their scope.
- [x] Keep native Registry/HTTP/HITL regressions; run the whole control on the final candidate, report skips and deployment limits.

Design: `NativeIngressConfig` names the receiving tenant/deployment, trusted gateway deployment, JWT audience/verifier, and mandatory active-grant authorizer. `NewNativeIngress` installs only verified Principal and a private request credential. `NativeTaskTokenSource` supplies that credential to State; no global token mutation. Peer identity comes from verified certificate chains, never headers. Authorization callbacks remain explicit dependencies. Token renewal and durable upstream taskstore are separate open profiles.

Constraints: only owned Mac Linux stand, one heavy job, no secrets in output/Git, no remote publication. Do not describe Go GraphFunc tests as real Python LangGraph or inference/process-recovery proof.

Evidence: `native-sdk-foreign-state-token-red` actually executed Bob's graph under Alice and created a run in Alice's scope. `native-sdk-foreign-state-token-green`: 3/0/0 with real A2A/mTLS/State/PG. `native-sdk-ingress-missing-red` is explicitly a missing-API build failure, not a functional failure count. `native-sdk-shared-ingress-green`: 114/0/0, source `e9e54a1b`, full Go SDK/State packages plus five real-State cases. Concurrent graphs deliberately share task/context names; each checkpoint/readback is checked against its signed subject. Current Identity authorization is the named test fixture. Author self-review checked TLS/JWT trust boundaries, immutable request credentials, State-scope guard and overlap synchronization. No independent reviewer was run through an unverified gateway.

Whole control on `eff4b61e34376ff721bd0f6800765c5f853935e1`: 4380 PASS / 0 FAIL / 51 SKIP, 668.344 s; original feature tags/venv, Go1.26.11 and unchanged Mac/Linux tracked sources. `native-sdk-ingress-network-smoke` passed against the unchanged `cededfb0` deployment. One additional conditional `std_alphabet` SKIP had no effective random mutation; its separate three subtest repetitions passed (6 parent/subtest records). The original whole result remains 51 SKIP.

Scoped checkpoint: Downloads/AHP-PDF-Closure-20261115/checkpoints/native-sdk-ingress-20261116 (16-file manifest). Full platform remains unaccepted. The PDF audit separately confirmed 0 synchronous replicas on the live PG: section2.5 guarantee6 requires a new HA/failover proof, not just process restart.
