# AHP Run Coordinator

One coordinator accepts input across tenants and deployments, watches the compact State API for runnable turns, and asks the Runtime controller to start a harness VM. PostgreSQL holds durable state in the existing six journal tables. Local memory holds only active SSE subscriptions.

## Service boundaries

- State API owns admission, the ordered journal, checkpoints, lease generation and fenced requeue.
- The coordinator starts newly admitted cold turns immediately and retries queued starts in the background. Queued starts and expired leases have separate global sweeps ordered by timestamp, tenant and internal session ID. For an expired VM lease the coordinator fences the stored physical execution ID and epoch, then requeues the current turn and starts its next generation. Ensure sends the current turn as `launch_turn_seq`.
- Runtime owns the VM, the external auth-injector and their lifecycle. It serves `POST /internal/v1/executions:ensure` and `POST /internal/v1/executions:fence` over mTLS. Repeated ensure for one execution ID must converge on one live VM, including when boot failed before claim. Fence returns success after the old execution has lost access.
- The host claims a queued VM turn through `/v1/state/sessions/{session}/vm:claim`. The task identity must be its execution ID. The host passes the current turn sequence with each journal mutation and saves the steer acknowledgment in the same transaction as the recovered state incorporating that input.

The public routes use `/v1/agents/{agent}/sessions/{conversation}` with `:send`, `:cancel`, GET status and `/stream`. The coordinator derives an internal session ID from the verified tenant, target deployment, verified subject and public conversation ID. Public JSON responses use `conversation_id`. The send receipt also returns the public value under `session_id` for existing clients. The State API, Runtime and journal notifications use the internal ID.

SSE supports a session feed and `?turn_seq=T`. The latter ends after the durable completion event for T. PostgreSQL notifications prompt clients to read faster. The journal cursor remains authoritative.

## Run a coordinator replica

Build `./cmd/run-coordinator` in the output of `python3 assemble.py <new-directory>`. Replicas use their own coordinator workload credentials and share the cross-tenant recovery queues.

| Configuration | Use |
|---|---|
| `AHP_RC_STATE_URL`, `AHP_RC_RUNTIME_URL` | Bare HTTPS origins |
| `AHP_RC_TLS_CERT`, `AHP_RC_TLS_KEY`, `AHP_RC_CA` | Certificate files for ingress and egress mutual TLS |
| `AHP_RC_STATE_TOKEN_FILE` | Rotated coordinator task token with `state` and `state.coordinate` |
| `AHP_RC_RUNTIME_TOKEN_FILE` | Rotated coordinator task token with `runtime.dispatch` |
| `AHP_RC_JWT_ISSUER`, `AHP_RC_JWT_KID`, `AHP_RC_JWT_PUBLIC_KEY` | Ingress token verification |
| `AHP_RC_AGENT_ACCESS_FILE` | JSON file listing active installations and their subject and broker grants |
| `AHP_RC_BROKER_WORKLOAD_IDS` | Comma-separated exact SPIFFE IDs allowed to delegate to another deployment in their tenant |
| `AHP_RC_USER_AUDIENCE` | Verified human token audience, for example `ahp-browser` |
| `AHP_RC_PLATFORM_EDGE_WORKLOAD_IDS` | Comma-separated exact SPIFFE IDs of cross-tenant platform edges |
| `AHP_RC_USER_EDGE_DEPLOYMENTS` | Comma-separated deployment IDs for edges that serve their certificate's tenant |
| `AHP_RC_NOTIFY_DSN` | PostgreSQL connection for journal notifications |
| `AHP_RC_ADDR`, `AHP_RC_OTLP_ENDPOINT` | Optional listen address and trace collector |

Configure the coordinator certificate's exact SPIFFE ID in the State API coordinator allowlist and the Runtime coordinator allowlist. Direct task callers need `run.coordinate` and a bearer token bound to their peer certificate. A broker also needs both its workload allowlist entry and a grant on the target installation. Human credentials pass through a permitted edge using the tenant and subject from the signed token. Providing a human audience and at least one permitted edge enables human access.

The operator keeps `AHP_RC_AGENT_ACCESS_FILE` in sync with installed agents and subject grants. A deployment absent from the file or with an empty subject list rejects access. Broker names in each record are deployment IDs in that record's tenant. The coordinator loads the file at startup. Restart replicas after changing the policy.

```json
{
  "agents": [
    {
      "tenant": "tenant-a",
      "deployment": "agent-1",
      "subjects": ["alice", "bob"],
      "brokers": ["space-broker"]
    }
  ]
}
```

## Managed execution lifecycle

Build `./cmd/ahp-vm-runtime` for a Linux host with KVM and writable delegated cgroup v2. This entry point uses one configured execution host per Runtime endpoint. It records the execution and attempt in PostgreSQL and the host's occupied slot on persistent disk. A fence permanently closes the slot, revokes the execution grant even if it had not been created, and waits for the cgroup to empty before returning 200. An unreachable host returns 503.

| Configuration | Use |
|---|---|
| `AHP_VM_RUNTIME_HOST_ID`, `AHP_VM_RUNTIME_SLOT_DIR` | Stable host identity and persistent private directory for slot state |
| `AHP_VM_RUNTIME_DRIVER` | `firecracker` by default. Use `process` with a trusted fixture binary for integration on a host without KVM |
| `AHP_VM_RUNTIME_WORKER_BINARY` | Absolute executable path for the trusted process driver |
| `AHP_VM_RUNTIME_CGROUP_ROOT` | Delegated cgroup v2 subtree on that host |
| `AHP_VM_RUNTIME_DSN`, `AHP_VM_RUNTIME_IDENTITY_DSN` | Runtime and Identity PostgreSQL connections |
| `AHP_VM_RUNTIME_TLS_CERT`, `AHP_VM_RUNTIME_TLS_KEY`, `AHP_VM_RUNTIME_CA` | Runtime server certificate and coordinator client CA |
| `AHP_VM_RUNTIME_JWT_ISSUER`, `AHP_VM_RUNTIME_JWT_KID`, `AHP_VM_RUNTIME_JWT_PUBLIC_KEY` | Dispatch token verification |
| `AHP_VM_RUNTIME_JWT_PRIVATE_KEY` | Host-side Ed25519 PKCS#8 PEM for the injector's short-lived task tokens |
| `AHP_VM_RUNTIME_STATE_URL`, `AHP_VM_RUNTIME_STATE_CA` | State API HTTPS origin and its CA |
| `AHP_VM_RUNTIME_WORKLOAD_CERT_DIR` | Host-side certificate tree with `tenant-<tenant>/deployment-<deployment>/tls.crt` and `tls.key` per workload |
| `AHP_VM_RUNTIME_COORDINATOR_WORKLOAD_IDS` | Comma-separated coordinator SPIFFE IDs |
| `AHP_VM_RUNTIME_ADDR` | Bind address, default `127.0.0.1:8454` |

Use the Firecracker settings documented in the compute supervisor configuration. Preserve the slot directory and host identity across restarts. One Runtime daemon owns a given slot directory. Each execution attempt has a separate AuthInjector, which applies the session policy and adds the workload certificate and task token to outbound State requests. The standalone State API needs `AHP_STATE_IDENTITY_DSN` to check the same live grants.

A trusted process worker receives `AHP_INJECTOR_SOCKET` and uses Go SDK `NewUnixInjectorClient` and `RuntimeBootstrap`. A Firecracker guest uses `NewVSockInjectorClient(RuntimeInjectorVSockPort)` and the same bootstrap operation. The guest image supplies its worker and supports Linux virtio-vsock. Firecracker connects the guest port to the host listener for the current attempt. [Firecracker vsock protocol](https://github.com/firecracker-microvm/firecracker/blob/main/docs/vsock.md#guest-initiated-connections)

## Tool calls from managed VMs

Enable tools for specific deployments through Runtime. Each generation gets a new task grant. The same tenant, deployment, subject and session produce one Gateway context across replacement generations, so a replacement can look up the original receipt.

| Configuration | Use |
|---|---|
| `AHP_VM_RUNTIME_TOOLS_DEPLOYMENTS` | Exact comma-separated `tenant/deployment` entries allowed to use tools |
| `AHP_VM_RUNTIME_TOOLS_URL`, `AHP_VM_RUNTIME_TOOLS_CA` | Fixed Tool Gateway HTTPS origin and optional separate trust root |
| `AHP_STATE_TOOL_LEASE_ISSUER`, `AHP_STATE_TOOL_LEASE_KID`, `AHP_STATE_TOOL_LEASE_KEY` | State API issuer, signing key ID and private Ed25519 PEM path |
| `AHP_TOOLS_LEASE_ISSUER`, `AHP_TOOLS_LEASE_KID`, `AHP_TOOLS_LEASE_PUBLIC_KEY` | Matching Tool Gateway verifier configuration and public PEM path |
| `AHP_TOOLS_LEASE_REQUIRED=1` | Require proof for all task calls on a dedicated leased gateway. Omit on an origin shared with legacy runners |

Drain or fence old executions before enabling a deployment. The next execution receives the updated grant. The Gateway requires live Identity authorization. The injector forwards only the leased call and lookup plus connection status to its configured origin.

The Go guest creates a `journal.VMLease` from the claim. `RunVMHeartbeatWithControl` refreshes it and `ApplyFinish` installs the proof for a promoted follow-up. The guest prepares the intent in State and passes its stable call ID, operation key, tool name and original argument bytes to `tools.Client.SettleLeasedCall`. State checks the epoch on every durable mutation. The Gateway checks the signed deadline and epoch again when it commits the right to dispatch. After a restart the guest resolves the same intent by key. A known receipt advances the journal, an in-flight call stays recoverable, an approval or connection gate parks the turn, and an unknown external outcome remains guarded for operator reconciliation.

For results over 256 KiB, a Gateway with `JournalArtifacts` configured publishes a versioned reference. The Go SDK saves that compact descriptor inline in the journal. The standalone platform Gateway mount does not provision an artifact owner for tools. Configure an artifact owner before running tools that return larger results. If a Gateway has already stored an oversized inline receipt, the SDK leaves the State intent dispatched and the owner retains the original receipt for reconciliation.

## Acceptance

Run the State, Coordinator, Runtime, Compute and Go SDK suites with `-race` on disposable PostgreSQL 18 and MinIO. Use separate disposable databases for State, Gateway and the coordinator integration tests. The coordinator scans all VM sessions in its database during reconciliation. The process integration tests exercise the coordinator, State, Identity and Runtime over mutual TLS and the guest SDK over its Unix socket. Firecracker boot, vsock inside a real guest and hypervisor isolation need a separate Linux host with `/dev/kvm` and delegated cgroup v2.

### Live PostgreSQL and Firecracker acceptance

On a Linux KVM test host, compile the assembled Run Coordinator test package for the host and guest architecture.

```sh
CGO_ENABLED=0 go test -c -o /tmp/ahp-runcoord.test ./internal/runcoord
```

Put that binary in the guest's ext4 image. The image starts the guest test as a child of init, with sysfs mounted and a clean environment. Init stays alive while the guest owns the turn.

```sh
env -i PATH=/bin HOME=/tmp AHP_FC_E2E_GUEST=1 /bin/ahp-runcoord.test -test.run '^TestV32FirecrackerGuest$'
```

In the Linux test host, configure `AHP_FC_BINARY`, `AHP_FC_KERNEL` and `AHP_FC_ROOTFS`. Point `AHP_FC_E2E_CGROUP` at a writable delegated cgroup v2 directory and `AHP_JOURNAL_TEST_DATABASE_URL` at a disposable PostgreSQL 18 database with the `ahp_carbon_compact` prefix. A guest kernel needs built-in virtio-vsock and ext4 support. The test creates short per-attempt paths under `/tmp` and disables guest network interfaces other than loopback.

```sh
AHP_FC_E2E=1 /tmp/ahp-runcoord.test -test.run '^TestV32FirecrackerPostgresE2E$' -test.count=2 -test.v
```

The test uses the real coordinator, mTLS handlers, workload grants and State Store. Two tenants use the same public conversation ID. One guest processes a follow-up in place. The other saves a checkpoint, records a dispatch and freezes. The replacement reads the checkpoint and the pending intent, checks the dispatch decision, and finishes the SSE stream. A test event counts fixture tool invocations. The assertions also verify that the old grant is revoked and Firecracker resources are retired.


For leased tools, select `TestV41FirecrackerGuest` in the guest init and run:

```sh
AHP_FC_E2E=1 /tmp/ahp-runcoord.test -test.run '^TestV41FirecrackerToolGatewayPostgres$' -test.count=2 -test.v
```

The v4.1 process tests cover pre-dispatch recovery, a lost Gateway reply, a lost upstream MCP reply, late completion, policy refusal and human approval or decline. The Firecracker test uses a live loopback MCP server through the Gateway, rotates the warm follow-up proof across expiry, restores the original receipt on the replacement guest and checks the SSE cursor and runtime cleanup. With local MinIO configured, `TestV41LargeToolResultPostgres` also checks the versioned descriptor for a result over the State inline limit.
