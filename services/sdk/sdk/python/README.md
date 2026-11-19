# AHP Python SDK - custom LangGraph agents on the platform

The container-side half of the contract in the architecture PDF (rev 3.2 §2.3, §4.2,
§9.2). **You** own the graph; this package owns the durable relationship between
that graph and the AHP State API. It is not a client for a platform-side runner:
LangGraph's own Pregel executor runs your nodes inside this process.

## Installation

The framework-neutral State/Recovery clients need `pip install ./sdk/python`.
For this LangGraph container use `pip install './sdk/python[langgraph]'` (published
package: `pip install 'ahp-langgraph[langgraph]'`). The adapter/recovery test
environment needs `pip install './sdk/python[dev,langgraph]'`.
See [conformance and standalone examples](../../docs/sdk/conformance.md) for the
current verified scope. Installing the base package alone does not install the
container's LangGraph runtime.

## Two things to provide

```python
# my_agent/agent.py
def build(ctx):                                   # AHP_AGENT_ENTRY
    graph = ...                                   # your StateGraph, your nodes
    return graph.compile(checkpointer=ctx.saver, store=ctx.store)
```

```bash
# the build context must be sdk/python: every COPY below resolves inside it, and
# that is the root at which .dockerignore applies (it is what keeps the test
# harness - raw sockets, subprocesses, a direct PostgreSQL reader - out of an image)
docker build -f sdk/python/Dockerfile --build-arg AGENT_DIR=examples/billing_agent --build-arg AGENT_MODULE=examples.billing_agent.agent -t ahp-billing-agent:dev sdk/python
# the digest the platform pins is the sha256 of this tar (there is no registry here)
docker save ahp-billing-agent:dev -o ahp-billing-agent.tar && sha256sum ahp-billing-agent.tar
```

The platform starts `python -m ahp_state.container`, which reads the hand-out from
the environment (`AHP_STATE_HANDSHAKE` or `AHP_STATE_URL`/`AHP_STATE_TOKEN`,
`AHP_AGENT_MODULE`, `AHP_WORKER_ID`, `AHP_LEASE_TTL_MS`, `AHP_HEARTBEAT_MS`),
claims runs, and stops writing the moment the lease is gone.

`AHP_IDLE_WAIT_S` sets the pause between empty `ClaimRun` calls and is **seconds,
fractions allowed** (`0.2`); the `_MS` knobs are integers. An entry that cannot
parse its environment dies before its first claim, and the queued run does not
show that - it just stays `pending`.

## What `ctx` gives a node

- `ctx.saver` / `ctx.store` - `AHPCheckpointSaver` / `AHPStore` bound to this
  lease. The only things allowed to hold durable state.
- `ctx.effects.call(tool, args, call_id=state["tool_call_id"])` journals under
  `(run_id, deterministic LangGraph task_id, persisted tool_call_id)`. Model and
  mutating tool must be separate nodes: call IDs come from checkpointed model
  output, never a fresh model attempt inside the mutation node. Reusing `call_0`
  in a new task is valid; replay of the same task meets the same journal row.
  Without a model call ID, create a fresh `ctx.effects.bind("billing")` view
  inside each node invocation and use its positional IDs. Unbound positional
  calls in graph context are rejected; carrying a view to another task is also
  rejected. UNKNOWN outcomes require reconciliation and never auto-redispatch.
- `ctx.take_steering()` - steering this node consumes itself; leave steering alone
  and the container folds it into the state at the next superstep boundary.
- `ctx.client` / `ctx.fence` - the fenced State client, for the rare direct call.

## Durability rules the container enforces

- turns run with `durability="sync"`: a superstep is durable before the next
  starts, so a killed pod loses nothing that was already reported.
- exactly one writer per superstep. Steering is folded at a boundary with the
  executor parked - never while a superstep is in flight (that is a
  `KEY_REUSE_MISMATCH` waiting to happen), and never on top of a pending
  interrupt (that would make the open question invisible).
- a restart *continues* from the durable checkpoint. It re-applies the input only
  when nothing was ever checkpointed; a lost lease stops the turn rather than
  letting a zombie write.
- `interrupt()` is durable: the question is written to the thread inbox as a
  `hitl_request` event and the run parks with `wait_for="hitl_response"` - the
  event kind `POST /v1/space/RespondHITL` emits, which is what wakes this run
  (same run id, new epoch) so the resume is applied to that same checkpoint.
  Parking on any other name is silent forever: nothing in the platform enqueues it.
- An answer can beat the park, and that must not lose the turn. State wakes a parked
  run only in the moment an event is *inserted*, so a decision written after the
  question is published and before the park lands wakes nothing, and the park then
  waits for a signal that has already passed. Space closed that window for the gates
  it submits itself (`577e2a9`); a container's gate never goes through `SubmitHITL`,
  so after parking this service re-reads its own thread and rings the platform's own
  doorbell - kind `hitl_response`, key `hitl_response:<hitl_id>:wake`, naming the event
  it re-signals - and the run comes back to read the single decision its gate holds.
  Residual, and the plane's to close: an answer acknowledged inside that window is
  invisible to an unacked-only inbox read, so such a run stays parked until the
  acked-inclusive read is decided.

Known limit, stated rather than hidden: the fold-then-ack of a steering message is
*at-least-once*. A pod that dies between the state commit and its journal write
folds that message once more on resume; the ack itself is exactly-once, and the
durable fold journal prevents the ordinary restart duplicate.

## Gate the image at build time

```bash
python -m ahp_state.check ./my_agent        # or: ahp-sdk-check ./my_agent
```

Exit `0` clean, `1` violation, `2` the check could not run. It fails the build on
foreign checkpointers/stores, `compile()` without `checkpointer=`/`store=`,
`durability` weaker than `sync`, direct DB drivers, disk writes, egress,
subprocesses, model SDKs called directly, pickle-style serialization, raw State
operations, and the task token reaching a log, a subprocess, the environment or a
model call. `--json` for CI, `--list-rules` for the full list. It never prints
source text.

## Tests

```bash
cd sdk/python && PYTHONPATH=. python -m pytest -q          # tests/ + ahp_state/tests/
PYTHONPATH=. python -m pytest ahp_state/tests -q           # the container/durability suite
```

`ahp_state/tests/` runs against a contract double that mirrors
`internal/state` (leases and epochs, checkpoint fingerprints, part references,
pending-write uniqueness, KV CAS, intent statuses, inbox) and includes real
subprocess restarts. `tests/test_state_adapter.py` is a live-stand acceptance test
for the first client surface and `tests/_live_stand.py` is the single place that
decides which stand a module talks to: one hand-out file, `AHP_STATE_HANDSHAKE`,
carrying `base_url`, `ca_file`, `cert_file`, `key_file` and `task_token`. Nothing
turns a missing stand into a pass - a module skips with a reason naming the exact
input that is absent.

### Against the platform's own stand

The chain a lane is allowed to use, measured end to end against a live aggregate:

1. mint a short-lived workload certificate for the lane:
   `tools/stand-mint-scoped-cert.sh <dir> <name>:<tenant>:<deployment>` - one day,
   keys `0600`, under the project devCA.
2. turn a stand human's session into a task token: login on the human edge
   (`POST /u-ahp/v1/auth/login`, then `POST /u-ahp/v1/auth/token`), then
   `POST /v1/identity/control/exchange` **with the exchanger's own certificate**.
   Identity serves only the deployments listed in `AHP_IDENTITY_EXCHANGER_DEPLOYMENTS`
   (anything else is `403 azp_mismatch`), and the token it mints stays bound to that
   exchanger: the same token behind a certificate of a different deployment answers
   `FORBIDDEN: auth: invalid token: token workload identity does not match the
   verified scope`. So the certificate the SDK presents has to be the identity the
   token was minted for, and the token lives 300 s - re-mint it for a longer run.
   `POST /v1/identity/roots` is where a human chooses the hosting deployment, but it
   wants a human token of the API audience: a browser access token is refused with
   `403 token audience does not match`, so a lane cannot root its own deployment
   from the browser surface.
3. run the suite with `AHP_STATE_HANDSHAKE` pointing at the hand-out. Keep
   `AHP_SDK_PGDATA`/`AHP_SDK_PGCTL`/`AHP_SDK_PGPORT` unset on a stand you do not
   own: those modules stop and start PostgreSQL to prove durability, which on someone
   else's aggregate is an outage rather than a test. The second-tenant isolation
   check needs `foreign_cert_file`/`foreign_key_file`/`foreign_task_token` in the
   hand-out, which only the stand owner can issue; without them it skips and says so.

A `127.0.0.1` forward over the sync lane is enough to reach the aggregate, and mTLS
still terminates at the aggregate itself - the server certificate is issued for
`IP:127.0.0.1`, so the hand-out's hostname check stays strict.

**Local green means the contract is implemented and self-consistent. It is not
platform readiness**: that needs the live State stand, the real Tool/Model
gateways, and a pod that really gets killed.
