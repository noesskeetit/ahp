# AHP Run Coordinator

The coordinator accepts input for a deployment, watches the compact State API for runnable turns, and asks the Runtime controller to start a harness VM. PostgreSQL holds durable state in the existing six journal tables. Local memory holds only active SSE subscriptions.

## Service boundaries

- State API owns admission, the ordered journal, checkpoints, lease generation and fenced requeue.
- The coordinator starts newly admitted cold turns immediately and retries queued starts in the background. Queued starts and expired leases have separate paginated sweeps. For an expired VM lease it fences the physical execution ID stored as the lease owner, requeues that exact turn and epoch, and starts the next generation.
- Runtime owns the VM, the external auth-injector and their lifecycle. It serves `POST /internal/v1/executions:ensure` and `POST /internal/v1/executions:fence` over mTLS. Repeated ensure for one execution ID must converge on one live VM, including when boot failed before claim. Fence returns success after the old execution has lost access.
- The host claims a queued VM turn through `/v1/state/sessions/{session}/vm:claim`. The task identity must be its execution ID. The host passes the current turn sequence with each journal mutation and saves the steer acknowledgment in the same transaction as the recovered state incorporating that input.

SSE supports both a session feed and `?turn_seq=T`. The latter ends after the durable completion event for T. PostgreSQL notifications prompt clients to read faster. The journal cursor remains authoritative.

## Run one scoped replica

Build `./cmd/run-coordinator` in the output of `python3 assemble.py <new-directory>`. Every replica serves exactly the configured tenant, deployment and subject. Replicas for the same scope can run concurrently.

| Configuration | Use |
|---|---|
| `AHP_RC_TENANT`, `AHP_RC_DEPLOYMENT`, `AHP_RC_SUBJECT` | Verified scope of the replica |
| `AHP_RC_STATE_URL`, `AHP_RC_RUNTIME_URL` | Bare HTTPS origins |
| `AHP_RC_TLS_CERT`, `AHP_RC_TLS_KEY`, `AHP_RC_CA` | Certificate files for ingress and egress mutual TLS |
| `AHP_RC_STATE_TOKEN_FILE` | Rotated, scope-bound service task token |
| `AHP_RC_JWT_ISSUER`, `AHP_RC_JWT_KID`, `AHP_RC_JWT_PUBLIC_KEY` | Ingress task-token verification |
| `AHP_RC_NOTIFY_DSN` | PostgreSQL connection for journal notifications |
| `AHP_RC_ADDR`, `AHP_RC_OTLP_ENDPOINT` | Optional listen address and trace collector |

The coordinator ingress expects `run.coordinate`. Its State API token needs `state` and `state.coordinate`. Only trusted VM task tokens should receive `state.vm_claim`. Production credentials and Runtime launch claims are supplied by the deployment and identity owners.

## Acceptance

Run journal, coordinator and Go SDK tests with `-race` on a disposable PostgreSQL 18 database. Run packages sequentially when using the same database because journal tests intentionally rerun migrations. The coordinator tests exercise real PostgreSQL with an in-process Runtime test double. Hardware-backed fencing, cancellation latency, reconnect to sandbox storage and real gateway SSE behavior require an integrated Runtime deployment.
