# AHP SDK — Custom Agent Integration

The SDK implements the platform contract from the architecture PDF (rev 3.2, section 4)
for custom Go workloads and Python LangGraph agents deployed as containers on AHP.

Current installation, tested examples and remaining gaps: [SDK conformance](conformance.md).
CrewAI/ADK adapters are not implemented; this page does not claim their support.

## Architecture role

Per PDF section 4.2, `cloudru-ahp-sdk` is a library **inside** the container, not a
platform service. It implements:

| Responsibility | Component |
|---|---|
| Native ingress peer/JWT/active-grant checks, request credential | `pkg/sdk.NewNativeIngress`, `pkg/sdk.NativeTaskTokenSource` |
| Native A2A principal and task-owner binding | `pkg/sdk.NewA2AHandler` |
| Native A2A task → thread/run lifecycle | `pkg/sdk.LangGraphExecutor` |
| Named run lifecycle | `pkg/sdk.App` |
| Lease management (ClaimRun, Heartbeat, Release, Suspend) | `pkg/sdk.ActiveLease` |
| LangGraph checkpoint persistence | `pkg/sdk.CheckpointSaver`, `sdk/python/ahp_state.AHPCheckpointSaver` |
| KV state (LangGraph Store replacement) | `pkg/sdk.AHPStore`, `sdk/python/ahp_state.AHPStore` |
| Operation keys (intent recording) | `pkg/sdk.IntentRecorder`, `pkg/sdk.OperationKey` |
| State API wire client | `sdk/go/state` |

## Package layout

```
sdk/go/state/          # Wire-level HTTP client for State API
pkg/sdk/               # High-level Go SDK: lease, checkpointer, app, intents
sdk/python/           # Python adapter: LangGraph BaseCheckpointSaver + BaseStore
tests/sdk/             # Integration tests (mock server, no DB required)
acceptance/mac-sdk-ci/ # CI gate script
docs/sdk/              # This documentation
```

## Usage (Go)

```go
import (
    "github.com/ahp-platform/ahp/pkg/sdk"
    sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

// 1. Build State API client with mTLS and token source.
client, _ := sdkstate.New(sdkstate.Config{...})

// 2. Configure SDK.
cfg := &sdk.Config{
    StateClient:       client,
    Worker:            hostname(),
    LeaseTTL:          30 * time.Second,
    HeartbeatInterval: 5 * time.Second,
}

// 3. Create App with your graph function.
app, _ := sdk.NewApp(cfg, func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
    // rc.CheckpointSaver — read/write graph checkpoints
    // rc.Store — KV state
    // rc.Intents — record side-effect operation keys
    // rc.Lease — active lease (heartbeat automatic)
    return json.RawMessage(`{"result":"done"}`), nil
})

// 4. Serve named tasks. There is no "claim whatever the queue hands me" path:
// a run is executed only under the task that owns it, and both App entry
// points verify run <-> task/payload against State before claiming.
task, _ := app.EnsureThreadAndRun(ctx, contextID, taskID, payload) // durable Task
res, _ := app.RunOnceFor(ctx, task)                                // single task
_ = app.RunTasks(ctx, []sdk.Task{task}, 2*time.Second)             // until terminal
```

## Native A2A server (Go)

Use the SDK composition to bind native task ownership before the upstream server
reads task data or appends incoming history:

```go
executor, err := sdk.NewLangGraphExecutor(cfg, runGraph)
if err != nil {
    return err
}
handler := a2asrv.NewJSONRPCHandler(sdk.NewA2AHandler(executor))
ingress, err := sdk.NewNativeIngress(sdk.NativeIngressConfig{
    Tenant: tenant, Deployment: deployment, GatewayDeployment: gatewayDeployment,
    Audience: taskAudience, Verifier: verifier,
    Authorize: checkActiveGrant, // mandatory current policy check, fail closed
}, handler)
if err != nil {
    return err
}
// Mount ingress on the workload TLS server with RequireAndVerifyClientCert.
// Mount peer-authenticated readiness separately; it does not carry a task JWT.
```

The host verifies the gateway peer and the task JWT's target workload separately,
checks the active grant, and installs `transport.Principal` from that verified
identity. JSON metadata cannot establish a principal. A human JWT is not an agent
ingress credential. Clients use `A2A-Version: 1.0` and the authorized contextId.

`NewA2AHandler` derives upstream authenticated owner identity from the verified
tenant/deployment/subject/task/context. Fresh JWTs for the same logical task keep
the same owner; other scopes cannot read, list, append to, subscribe to or cancel
that task. The State client's TokenSource must return the current request's
verified task credential when the host serves multiple tasks concurrently.

Build that shared client with `TokenSource: sdk.NativeTaskTokenSource`.
`NewNativeIngress` checks the gateway certificate's single SPIFFE identity and
verifies the JWT against the **receiving** workload, then calls the mandatory
active-grant authorizer. It places the verified credential only in that request's
context. It never mutates a process-wide token. Missing/unverified request context
cannot fall back to a bootstrap token. ClaimRun subsequently supplies its own
lease credential; this middleware does not renew long-running JWTs.

The native executor additionally rejects a State-returned thread whose scope
differs from the verified ingress principal, before creating a run or executing
graph code. A wrongly configured fixed-token client can still have created an
empty thread before that check; configure the request-bound source to avoid it.

The default upstream taskstore is in-memory. A custom durable store must preserve
the authenticated owner, version-CAS and no-mutation guarantees of `taskstore.Store`.
This constructor adds ownership binding; it does not add process recovery or token
renewal. Those profiles must be verified separately.

## Usage (Python / LangGraph)

```python
from ahp_state import StateAPIClient, AHPCheckpointSaver, AHPStore

client = StateAPIClient("https://state-api.internal", token=task_token)
fence = {"run_id": run_id, "worker": worker_id, "epoch": epoch}

saver = AHPCheckpointSaver(client, thread_id=thread_id, fence=fence)
store = AHPStore(client, fence=fence)

graph = builder.compile(checkpointer=saver, store=store)
result = graph.invoke(input, config={"configurable": {"thread_id": thread_id}}, durability="sync")
```

## Key contracts (PDF rev 3.2)

- **Idempotency**: `contextId` → thread (replay returns existing); `taskId` → run.
- **Lease fencing**: every write carries the epoch; stale epoch → `STALE_EPOCH`.
- **Operation keys**: `RecordIntent(run, node, call)` is idempotent; same fingerprint
  returns existing key; different fingerprint → `KEY_REUSE_MISMATCH`.
- **Orphan recovery**: expired lease → `ClaimRun` returns task to another replica
  with new epoch; graph resumes from last `super-step` checkpoint.
- **Interrupt/resume**: `Suspend(wait_for)` parks the run; inbox event wakes it.
- **No raw DB access**: all persistence through State API; SDK never holds DB credentials.

## CI gate

```bash
bash acceptance/mac-sdk-ci/check.sh
```

Runs build, unit tests, and vet without external dependencies.
