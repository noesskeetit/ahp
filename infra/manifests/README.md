# Deployment Manifests — Levels of Supply (PDF §10)

Authoritative source: `ahp-agents-space-architecture (2) (1).pdf`, rev 3.2, §10
"Уровни поставки" + Рис. 12. SHA256 `044e045b73b7c34b642bd3c9eb5701b93eed470873a1e0a44e8b1d024821a7e2`.

**A level (Tier) is defined by the boundary between the Cloud.ru cloud and the
customer's perimeter, NOT by scale/replica count.** Per §10: "Сервисы и контракт
одинаковы на всех уровнях; меняется только граница между облаком и контуром
заказчика." The same `cmd/ahp` binary and the same service contract run at every
level; only *which components run in the cloud vs. in the customer perimeter* and
*where data at rest lives* changes.

> Historical note: an earlier draft of this directory framed Tier 0–2B as a
> scaling ladder (local → VM → K8s → K8s HA). That framing is superseded by PDF
> §10 boundary semantics and is no longer authoritative. The operational files
> under each tier dir are retained as topology building blocks, re-interpreted
> through the boundary lens below.

## Boundary matrix (verbatim §10, condensed)

| Level | In Cloud.ru cloud | In customer perimeter | Honest IB guarantee |
|-------|-------------------|-----------------------|---------------------|
| **Tier 0 · Shared SaaS** | Everything; shared pool, RLS by tenant, KMS keys per tenant | Corporate APIs via VPC relay | Logical + cryptographic isolation (SMB, developers) |
| **Tier 1 · Namespace** | Everything, but in dedicated namespaces, runner pools and microVMs, separate State Store and bucket | VPC peering; customer key (BYOK) | No noisy neighbors, customer encryption keys |
| **Tier 2A · Customer storage** | Gateway, Agents Space (coordinator + runners), custom deploys, Model Gateway + LLM, Tool Gateway, Memory + Artifacts services, telemetry without content | PostgreSQL (State Store, memory), S3, microVM pool, KMS | Residency at rest: files, chats and vectors live with the customer; plaintext is processed in cloud RAM |
| **Tier 2B · Sovereign perimeter** | Optional: signed releases, licenses, aggregates without content | The whole cell incl. local LLM (vLLM or SGLang) | Plaintext and prompts never leave the perimeter (CII, banks) |

Tier 2B == full private cell of draft 3.0: P0 with a local model, or P1 with
explicitly allowed external inference. Tier 2A is closest to draft variant P2:
storage + execution at the customer, processing at the vendor — **it must NOT be
sold as "the platform is deployed inside the customer VPC"**.

**Three mandatory conditions for Tier 2A** (§10, verbatim):
1. telemetry without content;
2. per-tenant prefix-cache salt (e.g. `cache_salt` in vLLM) **and** prohibition on
   flushing KV to disk;
3. written fixation of the processing boundary for the customer's IB.

Cross-cell / cross-level agent calls are a separate federation and are **out of
scope** (§10). Management plane is off the hot path at all levels.

## Directory layout

Each tier directory contains topology manifests plus a `README.md` runbook that
states the boundary, what each file demonstrates, and the apply/verify/rollback
procedure. The topology files are generic K8s/systemd building blocks; the tier
`README.md` is what pins them to the PDF §10 boundary they represent.

- `tier0/` — Shared SaaS: shared pool, RLS-by-tenant, KMS-per-tenant.
- `tier1/` — Namespace: per-tenant dedicated processes/namespace, BYOK, own State Store + bucket.
- `tier2a/` — Customer storage: cloud control+compute, customer PostgreSQL/S3/KMS; 3 mandatory conditions.
- `tier2b/` — Sovereign: full cell incl. local LLM; default-deny net, HPA, PDB, RBAC.

## Evidence discipline (source ≠ deployment)

| Level | What the manifests here PROVE (L1/L2 static) | What they do NOT prove (needs L4 target runtime) |
|-------|----------------------------------------------|--------------------------------------------------|
| 0 | YAML well-formed; RLS + per-tenant KMS expressed in config | A live shared-pool deploy with tenant isolation under load |
| 1 | systemd unit structure; per-tenant process isolation args | systemd restart + health probes on ≥2 real VMs |
| 2A | Deployments/Services/NetPol well-formed; State API → customer PostgreSQL; 3 conditions encoded | `kubectl apply` + rollout + cross-region State latency measured on a pilot |
| 2B | HPA/PDB/NetPol(default-deny)/RBAC well-formed | HPA scale event, chaos pod-kill, NetPol deny on a real cluster + local LLM quality eval |

Static YAML parse is reported as L2_LOCAL_TEST (structural). **No level is
reported as L4 target-runtime proof without a real run on the target runtime.**
Absence of a runtime is reported as `NOT_VERIFIED`, never as pass. Local
`npm run check`/`smoke` exercise the service contract on a single host; they are
evidence for the *contract*, not for a tier's *deployment topology*.

## Target-runtime verification (turnkey, non-mutating)

`scripts/verify-tier-target.sh` gives the VM-plane / integration-validation a
single reproducible command that emits a machine-readable evidence report per
tier. Three layers, never conflated:

1. **STATIC (default, safe on any host)** — parses every manifest, asserts each
   document has `kind` + `metadata.name`, and checks the documented topology
   (tier2A `state`/`identity`/`model` Deployments+Services+ServiceAccounts +
   `ahp` Namespace; tier2B HPA×3, `default-deny-ingress`+`ahp-allow-internal`+
   `ahp-allow-egress-dns`, PDB×3, `ahp-secret-reader` Role + per-role
   RoleBindings; tier0 RLS/KMS ConfigMap + shared Deployment/Service/SA; tier1
   systemd unit structure). This is **L1/L2 structural only**.
2. **SERVER-DRY-RUN (opt-in)** — `AHP_TARGET_RUNTIME=1 --context <ctx>` runs
   `kubectl apply --dry-run=server` (validates admission, does NOT persist).
   The script refuses to run without an explicit `--context`, so it never
   touches a neighbour cluster. This is **L3 admission**, still not a rollout.
3. **L4 target-runtime** — apply + `rollout status` + measured cross-region
   State latency + restart/HA. The verifier NEVER claims this; it stays
   `pending` until captured on a real cluster per each tier README.

```
bash scripts/verify-tier-target.sh                 # static layer, JSON report
AHP_TARGET_RUNTIME=1 bash scripts/verify-tier-target.sh --context <ctx>   # + admission
```

Source ≠ deployment: a passing static/admission layer is reported as
L1/L2/L3, never as L4. Absence of a target runtime is reported `pending`, never
as pass.

