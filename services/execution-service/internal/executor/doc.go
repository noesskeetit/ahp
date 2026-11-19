// Package executor is the canonical Qwen+tools -> a2a gateway.Executor for the
// execution plane. It is the production executor the durable A2A gateway runs
// behind (the gateway's injection point passes it through NewService's exec
// argument). It replaces the gateway's honest fail-closed "no executor
// configured" default with a REAL native-Qwen model turn plus a tool
// round-trip that produces a real, sha256-sealed object-storage artifact.
//
// It composes the already-accepted execution-plane seams and changes none of
// their semantics:
//   - internal/core.Runtime   — the canonical bounded model/tool loop (the
//     "space coreRuntime seam"); the model may emit tool calls and the loop
//     feeds tool results back until the turn completes.
//   - internal/model secretstore + the OpenAI-compatible Qwen client — a real
//     native-Qwen turn. Credentials come ONLY from the scoped Vault runtime
//     secret (never the operator's ambient env); there is NO OpenAI/Astra/echo
//     fallback: an unconfigured secret fails closed.
//   - internal/tools Guardian — an independent policy decision is consulted
//     before any tool side effect runs.
//   - internal/artifacts.Store — real MinIO artifacts sealed with a verified
//     sha256; tenant isolation is enforced by the store (a foreign tenant's
//     read of a sealed id returns NOT_FOUND).
//
// The gateway itself still owns durability: it journals the ExecResult (state,
// message, artifacts) in its service-owned ledger + event journal, so a result
// and its artifact reference survive a gateway restart and are never replayed
// as a fresh side effect.
package executor
