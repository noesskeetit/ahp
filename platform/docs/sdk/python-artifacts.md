# Public Python Artifacts

`ahp_clients.ArtifactsClient` and `AsyncArtifactsClient` use the existing
Artifacts owner API. Both require a caller-owned verified mTLS `httpx` pool and
current task-token callback. Async accepts a sync or async callback. No framework,
S3 credential, SQL connection or Hindsight admin access enters these clients.
The caller closes its owner pool; each presigned upload owns a short-lived,
credential-free pool. No owner or blob request follows redirects.

## Wire parity

| Public Go | Python sync / async | Actual owner route / purpose |
|---|---|---|
| BeginUpload | begin_upload(key,name,mime,sha256,size,labels=...) | BeginUpload; task upload authority, scope/context from token |
| Upload | upload(upload,data) | Exact presigned PUT, no bearer/cookies/workload certificate |
| Seal | seal(id) | Seal; verifies actual immutable MinIO version/hash/length |
| Put | put(key,name,mime,data,labels=...) | BeginUpload + Upload + Seal, verifies returned ID/hash/length |
| Get | get(id) | Get; current read authority and scope/binding |
| Download | download(id) | Get + Download; exact size/SHA-256, no silent truncation |
| ReadRange | read_range(id,offset,length) | Get + Download with Range; status206, exact Content-Range/ETag/length |
| TransferGrant | transfer_grant(ref,target_deployment,target_context,target_task) | TransferGrant; current source+target authority, same tenant/subject |
| CopyFromTransferGrant | copy_from_transfer_grant(grant,key) | CopyFromTransferGrant; owner copy preserves provenance and target labels |
| Readiness | readiness(id) | Readiness; independent content/extraction/search/quarantine states |
| Process | process(id) | Process; current derive authority, configured owner sandbox parser |
| ReadTextChunk | read_text_chunk(id,version,offset,limit,context_budget) | ReadTextChunk; exact version, bounded UTF-8 and context byte budget |
| SearchText | search_text(id,version,query) | SearchText; current read authority and owner index |
| Resolve | resolve(sha256) | Resolve; readable exact versions, fields include `artifact_id` |

Both languages cap artifact bytes at 64 MiB and metadata at 1 MiB. Python checks
reference scope fields as well as ID/version/hash/size; optional context/task
fields remain owner data. Metadata limits are fixed, not caller overrides.
Range length is explicitly clamped at EOF as in Go; a short transport response,
wrong ETag, oversized body or non206 response is an error. A range cannot prove
a full-object digest; its exact owner SHA-256 ETag and Content-Range pin it.

There is no `Artifacts.Export` route. `download` exports bytes through current
authority. Administrative State `ExportThread` is a different existing control
plane contract in the State SDK. `DownloadGrant` / `OpenDownloadGrant` exist at
the owner for constrained task-authorized proxies but are not methods in the Go
client or this parity block. No human/manager authority is implied by this task
client. GC, retention, legal hold and publication control are also not exposed.

## Failure and recovery

Keys must be persisted before mutations. No automatic retries are performed.
A lost BeginUpload response is reconciled by the **same key and declaration**;
a lost Seal response by the **same artifact ID**. A replayed Put can create an
additional unsealed S3 object version; the owner returns the original immutable
sealed reference, matching the existing Go helper. Use separated stages to avoid
unnecessary PUT after an already known seal. A changed declaration under a key
is an owner refusal, preserving `code` and `reason=KEY_REUSE_MISMATCH`.

`APIError.outcome_unknown` preserves uncertainty after mutation transport loss,
5xx or malformed success. Transfer `reason=transfer_outcome_unknown` is preserved;
reconcile that same grant/key, never dispatch a new logical copy. Grant issuance
itself has no idempotency key in the owner protocol. Caller cancellation raises
`CancelledError`; cancellation of an HTTP call does not prove a mutation was
undispatched. Read errors never become empty successful bytes. Error bodies and
responses are bounded. Presigned PUT errors do not echo capability URLs.

The owner pool is trusted caller configuration: use verified TLS and a current
workload certificate. Python does not inspect private httpx transport internals
to prove the pool was configured correctly. `upload_ca_file` can add blob trust
roots; it builds a fresh SSL context and cannot inherit a workload certificate.

## Validation and example

[python.py](../../sdk/examples/artifacts/python.py) provides sync/async file
upload and verified readback. Runtime token refresh remains the caller's duty;
callbacks read current task authority for **every** metadata and byte request.

`TestPythonArtifactsRealOwners` runs real PostgreSQL, versioned MinIO, Identity,
HTTP/mTLS and a freshly compiled Linux seccomp parser. It exercises pure Python
sync/async upload, explicit stages, sealed replay, exact bytes/ranges, private
label transfer and Go crossreads in both directions. It also covers foreign
scope/context, workload mismatch, revoked grants (including revoke after Get
before Download), mismatched hash, tampered transfer reference/version, lost
BeginUpload/Seal/CopyFromTransferGrant ACK reconciliation, real text extraction/index readiness,
unsupported media and malformed JSON quarantine. No SQL update sets ready.
The live scenario also reads an 18,900,000-byte binary file and checks a TLS
PUT sink receives no task token, owner cookie or client certificate in both
sync and async modes. Transport unit tests inject corrupt/short/oversize replies, wrong range ETag,
redirects, token rotation, cancellation and credential traps.

The Linux parser supports text/plain, text/markdown, text/csv and application/json.
Sealing unsupported binary media confirms content bytes without claiming text or
search readiness. This is storage/parser/authority evidence, not LLM or microVM
E2E. Exact candidate hashes and logs live in the private evidence report.


The Go public client now has typed `Readiness`, `Process`, `ReadTextChunk`,
`SearchText` and `Resolve` responses matching these owner methods. `ReadTextChunk`
returns byte offsets, `next_offset`, `total_bytes` and `eof`; use the owner's next
offset, preserving UTF-8 boundaries and the context byte budget. `ResolveResult`
uses `ResolvedVersion.ArtifactID` (`artifact_id` on the wire). `Process` remains
a task-authorized derive request; no control-plane authority is added. Go's
existing verified-mTLS/token callback, response bounds and no-retry transport
remain unchanged. A lost Process reply can be checked with Readiness on the
same ID; sealing alone still does not imply extraction/search readiness.

`TestGoArtifactsPythonCreatedTextRealOwners` creates documents with the public
Python client over actual owner HTTP/mTLS, then checks typed Go readiness,
sandbox processing, multi-chunk UTF-8 readback, search and digest resolution.
It preserves wrong-version/budget, quarantine, foreign tenant/deployment/subject
and current grant-revocation refusals for every new helper. This Linux test uses
real PostgreSQL, MinIO, Identity and a compiled seccomp parser; no SQL sets ready.
