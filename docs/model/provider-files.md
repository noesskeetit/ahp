# Model-owned provider files

PDF rev3.2 §2.1 assigns provider file copies to Model Gateway. This optional
component owns copies of already sealed Artifacts. It is not another storage
service, and ordinary chat generation does not automatically upload attachments.

The current Qwen chat configuration does not advertise Files support. Until an
operator configures an actual compatible Files API, `CopyFile`, `GetFile` and
`DeleteFile` return `UNAVAILABLE / provider_files_not_supported`.

## Enablement

The composed `ahp_exec,ahp_data` binary accepts `AHP_MODEL_FILES_CONFIG` as bounded
JSON (16 providers maximum). Provider names must match the existing label-aware
Model routing table. All endpoints and allowlists are operator-controlled.

```json
{
  "providers": [{
    "name": "approved-provider-copy",
    "base_url": "https://files.example.com/v1",
    "account": "account-reference",
    "secret_path": "secret/data/ahp/model/files",
    "allowed_hosts": ["files.example.com"],
    "private_hosts": []
  }],
  "ttl_seconds": 3600,
  "timeout_seconds": 30
}
```

Vault supplies `api_key`; no raw credential belongs in this configuration or a
request. Account plus endpoint identifies the remote namespace. Credential
rotation preserves that identity; changing account/endpoint cannot delete a copy
from the wrong namespace. Cleanup stays pending until the old owner is restored.
The Files driver uses multipart `POST /files` (`purpose=user_data`) and exact-ID
`DELETE /files/{id}`, with no redirect following and the standard egress policy.
The protocol is documented by the upstream [create](https://developers.openai.com/api/reference/resources/files/methods/create)
and [delete](https://developers.openai.com/api/reference/resources/files/methods/delete)
contracts. Compatibility with a particular configured provider still requires
provider acceptance testing.

## Task API and authorization

All routes are authenticated POSTs under `/v1/model/`, using the same task-token,
mTLS workload and live Identity checks as Model generation.

| Method | Body | Result |
| --- | --- | --- |
| CopyFile | `{"key":"stable-key","artifact_id":"sealed-artifact-id"}` | Opaque Model copy reference |
| GetFile | `{"id":"model-copy-id"}` | Current copy state |
| DeleteFile | `{"key":"delete-key","id":"model-copy-id"}` | Deleted or pending outcome |

The verified tenant/deployment/subject/task/context binds the copy. Provider IDs
are server-side only. A request cannot select a provider, endpoint, credentials,
read labels or owner scope. Model asks the owning Artifact Store for the exact
sealed version, records its labels through Identity, re-evaluates live routing,
and verifies SHA256 and length before dispatch. Private data must satisfy the
configured provider audience policy. Copy size is bounded to 8 MiB; TTL is 1 s–24 h
and provider timeout 1–60 s.

## Recovery and deletion

The existing Model operations ledger grants one upload dispatch. An upload whose
acknowledgement is lost stays `SIDE_EFFECT_UNKNOWN` and is never automatically
uploaded again, including after restart. A positive receipt committed before an
inventory crash is adopted on retry. Original executor receipts may reconcile a
fenced in-flight attempt; an unproven upload remains pending and requires trusted
provider-side evidence, not an arbitrary ID supplied through task HTTP.

Known exact provider IDs are deleted idempotently. Lost delete acknowledgements
are retried; unknown uploads block final erasure. A background owner sweeper runs
every 30 s with a 45 s pass deadline, 100-copy limit and retry scheduling so one
unknown outcome cannot starve other expired copies.

Model participates in required subject/tenant control deletion. Permanent barriers
block late admission and dispatch. Subject cleanup includes generations (prompts
and outputs), settled reservations/budgets, aliases and terminal effect records;
local content and ledger removal commit together. Active or unknown expenses keep
the operation pending. Legal holds preserve Model content and keep the composed
subject operation pending until released. Temporary provider copies retain their
ordinary TTL; the held canonical Artifact is preserved by its owner.

The default text-only configuration still installs the Model subject owner. If a
copy inventory exists but its driver is no longer mounted, deletion refuses to
finalize until the owner is restored. No disappearance of configuration counts
as proof that external files disappeared. A cached control receipt created before
an owner was wired returns `deletion_owner_upgrade_required`; an administrator
uses a new operation key to include all current required owners.

## Verification boundary

Automated tests use isolated real PostgreSQL and real MinIO for source Artifacts.
HTTP provider tests are deterministic protocol fixtures, not live SaaS or LLM
acceptance. Existing stream/reservation/chaos tests cover the shared generation
path. This component does not close the separate remote-model terminal-only
transport gap: upstream streaming alone is not evidence of incremental delivery
to the end user.
