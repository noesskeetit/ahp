# Native Core → Tool Gateway bridge candidate

Base: `fb278f6a8acc061339a1d0eba87fc3cc26872619`. This is a separate candidate;
the benchmark baseline, its binaries, images and results are unchanged.

The existing Tool Gateway already resolves scoped credentials, authorizes every
task request, applies Guardian/approval policy, journals effects, calls MCP, and
revalidates result ownership through `Call` and `GetResult`. The missing link was
from as-runner's Core tool definitions/executor to that owner API.

## Deployment configuration

The bridge is optional. Existing deployments with no descriptor variables retain
their prior local tools and policy. To enable it, configure:

* `AS_TOOLS_URL`: the bare HTTPS ToolsService owner origin, without path/query.
* `AS_REGISTRY_URL`: normal authenticated Registry owner origin.
* Existing `AS_WORKLOAD_CERT/KEY/CA`, `AS_AUTH_ISSUER/KEYS`, task audience.
* `AS_TOOLS_DESCRIPTOR_FILE`: a read-only, operator-owned JSON file.
* `AS_TOOLS_DESCRIPTOR_SHA256`: mandatory lowercase SHA256 of its exact bytes.

The descriptor file has the following bootstrap contract:

```json
{"version":1,"tools":[{"name":"calendar_read","description":"Read calendar entries","parameters":{"type":"object","properties":{},"additionalProperties":false}}]}
```

Names are existing Tool Gateway aliases. The gateway independently maps them to
MCP connection/tool names. No URLs, bearer, Vault path, credential or permission
flags belong in this manifest. Definitions are not accepted from user/model
requests. Maximum: 1 MiB, 128 unique tool names, object JSON schemas.

The operator must configure the SAME aliases through the existing
`AHP_ENABLE_TOOLS=1` / `AHP_TOOLS_CONFIG` gateway mount and its existing credential
source. Live task grants require `tools` capability in addition to the existing
State/model/Registry grants. Enabling a descriptor does not grant it. Side effects
and approvals remain the gateway's policy; this bridge never auto-approves.

## Pin and execution

Before admission, the server's pinner combines the authenticated Registry Dot
snapshot with the descriptor subset selected by its `AllowedTools`. State carries
the descriptor bytes, selected digest and source-file digest in the optional
`AgentConfigRef.tool_manifest` field. Each run gets independent copies. Cold
restoration reads that snapshot, not the latest file or Registry configuration.
An invalid digest, disabled name or local-tool collision fails before inference.

Core's normal per-run registry advertises the pinned definitions. Execution
requires the run's verified task token, exact scope/context and a mutation-journal
coordinate. The stable operation key hashes authority/run/coordinate identifiers;
it contains neither arguments nor credentials. It deliberately excludes arguments
and tool name so changing an action at one coordinate cannot mint another effect.

The bridge sends one `POST /v1/tools/Call`, then reads a successful operation with
`POST /v1/tools/GetResult`. Both use the same current task token over workload
mTLS and propagate only the validated `traceparent`. The result's key, target,
canonical action fingerprint and context provenance must match. No automatic
Call retries, redirects, deployment bearer fallback or direct MCP connections
exist in the runner bridge. Ambiguous Call outcomes retain `SIDE_EFFECT_UNKNOWN`.

Inline results preserve the gateway's canonical MCP CallToolResult JSON. Large
out-of-line results preserve the immutable artifact reference returned by the
owner; fetching its bytes is a separate, authorized Artifacts operation. The
bridge never fabricates an inline result or silently truncates it. Errors expose
fixed text and known error classes, never upstream error bodies, URLs or headers.

## Verification and scope

Focused contract tests cover exact file pinning, isolated concurrent snapshots,
scope/context rejection before network, owner denial, trace propagation, deadline,
no retry/redirect and secret canaries. `TestGatewayNativeThreeLLMTwoMCP` uses real
PostgreSQL Identity/Registry/State/ToolsService, task grants and workload mTLS,
actual MCP SDK StreamableHTTP client/server, and a deterministic LLM protocol
fixture. It requires an OWNED database supplied by
`AHP_MCP_BRIDGE_TEST_DATABASE_URL`; unset means explicitly skipped.

That test admits an exactly 100 KiB user input including its marker, modifies
mutable configuration, restores on a cold coordinator, and requires three
durable model frames, two tool frames, one done frame, two distinct actual MCP
receipts, denied side effects/capability, foreign result rejection and no raw
credential in logs. It proves native orchestration, not real-model inference or
capacity. The benchmark profile provider/MCP binaries are a separate operator-run
integration experiment and must supply their own artifacts.

This candidate implements immutable deployment bootstrap descriptors. Automatic
MCP discovery and a Registry tool-descriptor catalog are NOT implemented or
claimed. Gateway configuration, credentials, current policy and user consent
remain authoritative and can still refuse a pinned tool at execution time.

`max_steps` counts model iterations in Core. Three model rounds with two tool
calls can finish in three iterations; the benchmark declares eight as its profile
limit, not because a tool call consumes a separate model iteration.
