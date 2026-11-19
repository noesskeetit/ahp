# two-agents — SDK journey over two Space deployments

This example is a **pure SDK client**. It shows the developer/human journey of
running two independent Agents Space deployments and delegating a turn from one
to the other. It does **not** embed a State service or an LLM — both live behind
each deployment (`as-runner` + the State API + a configured model).

## What it exercises

1. `AcceptTurn` on deployment **A** (orchestrator).
2. `Dispatch` A and follow its thread **stream** until a terminal `status`
   event (`done` / `failed` / `canceled`).
3. `Delegate` a follow-up turn from A to deployment **B** (agent-to-agent).
4. `Dispatch` B, follow its stream, and assert the two deployments produced
   **distinct** turn and thread ids.

## Prerequisites

Two running `as-runner` deployments, each wired to its own State service and
model. For the local harness these must be pointed at an allowlisted test
database (e.g. `ahp_hive_space`) in the isolated `ahp-handbook-go-postgres-1`
container — never a shared/production database.

## Configuration (environment only)

| Var | Meaning |
| --- | --- |
| `AS_A_URL`   | Deployment A base URL, e.g. `http://127.0.0.1:8080` |
| `AS_A_TOKEN` | Bearer token for A (scoped principal). Optional for local dev. |
| `AS_B_URL`   | Deployment B base URL, e.g. `http://127.0.0.1:8082` |
| `AS_B_TOKEN` | Bearer token for B. |

Tokens are read from the environment and never printed by the example.

## Run

```sh
cd examples/two-agents
AS_A_URL=http://127.0.0.1:8080 AS_A_TOKEN="$TOKEN_A" \
AS_B_URL=http://127.0.0.1:8082 AS_B_TOKEN="$TOKEN_B" \
go run .
```

Expected output ends with:

```
OK: two independent deployments, distinct turns/threads, delegation succeeded
```

## Honesty about scope

This example proves the **client-side** delegation/streaming contract. The
end-to-end leg that requires a **real LLM + real tools + artifact download** and
the **State persistence** (checkpoints, intents, inbox, KV) depends on the
State/identity/execution/data contracts being live and on Emil `mcp_auth`
provenance access — those are tracked as remaining acceptance requirements, not
closed here.
