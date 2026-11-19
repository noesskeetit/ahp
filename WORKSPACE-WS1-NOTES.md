# WS1 — workspace file upload: mount + owner/editor/viewer rights (final)

Lane: vm2-as-workspace. Thread 01a1912a-c3ef-7a03-abc5-c2a94b85d35c.
Branch: vm2/as-workspace-workspacemount over pinned base 9b6b007.
Updated 2026-11-01T11:53:53Z. Own resources only: HTTP 127.0.0.1:18302,
private PostgreSQL 127.0.0.1:25471 (db ahp_ws1), private MinIO 127.0.0.1:16910.
Never used 18080-18082, 18097, 18110, 18280-18282, nor other lanes' PG/MinIO.

## 0. Confirmed at 9b6b007
Package internal/workspace/workspaceapi is referenced NOWHERE outside itself.
compute-host mounts it only on the DIVERGED compute head 75d530a (not an
ancestor of 9b6b007). So at my base the routes are unmounted in every binary.

## 1. Routes (exact) — POST /v1/workspace/{Operation}
Dispatch: CutPrefix(path,"/v1/workspace/") then h.operations[rest]; method must
be POST; nested "/" or unknown -> package contract 404 "workspaceapi: unknown
route" (returned BEFORE the transport guard, so it is observable unauthenticated).

Operations map has EXACTLY 21 tokens (api.go:46-68):
Create, AddMember, RemoveMember, ListMembers, GetWorkspace, CreateCandidate,
UpdateCandidateFiles, SetCheck, GetCandidate, ListCandidates, RejectCandidate,
Publish, GetHead, GetRevision, ListRevisions, ListFiles, ReadFile, Pin, Unpin,
GetBranch, PurgeRevision.
CORRECTION to my first-step post: there is NO "VerifyIntegrity" and NO
"ListConflictBranches" operation at this base; the earlier count of 23 was wrong.
The probe token "/v1/workspace/head" is NOT an operation (canonical is "GetHead");
once mounted it answers with the package 404, which is the success sign.

## 2. Mount (done) — the composed DATA plane, ahp_data build tag
cmd/ahp/role_data.go now builds BOTH planes from shared stores and registers
/v1/artifacts/ AND /v1/workspace/ (helper mountDataHandlers, used by both
serveData and tryMountData). buildDataHandler -> (artifactsHandler,
workspaceHandler, error); construction is split from migration in newDataPlanes so
the real handlers/mux can be exercised offline. The workspace store is wired to
the artifact store, and the artifact store gets the workspace membership hook
(WithWorkspaceAccess(wsStore.IsMember)). Coarse gate at the face mirrors the
artifact plane: task token + capability "workspace" + full scope.

## 3. Rights (done) — owner/editor/viewer is the source of truth, is_owner now gates MANY rules
Storage: ahp_workspace.members gained a `role` column (owner/editor/viewer, CHECK
constrained; idempotent ALTER also added). Create records the creator as owner;
AddMember grants editor; is_owner is kept as a derived flag (role == owner).
Policy table actionAllows() (store.go): viewer=read; editor=read+write+candidate
create/list/write/check; owner=also publish, candidate:reject, purge, member:*.
Unknown role/action -> deny (fail closed). authorizeMember() loads the caller's
role in-workspace-in-tenant and checks it; every seam (requireMember, getCandidate,
Publish) now runs coarse authorize + authorizeMember. Cross-tenant/unknown resolve
to 404 BEFORE membership, so 403 vs 404 are distinguishable with no existence leak.
The upload hole is closed: UpdateCandidateFiles -> getCandidate("candidate:write"),
SetCheck -> "candidate:check", RejectCandidate -> "candidate:reject", GetCandidate
-> "read". Purge tightened from "write" to owner-only "purge".

## 4. Verification (executed, own resources)
- go build (default) ./cmd/ahp OK; go build -tags ahp_data,ahp_exec OK; go vet -tags ahp_data clean; gofmt clean.
- Full internal/workspace suite PASS (no regression: copy-on-write, CAS/conflict,
  pin/purge, publication leak gate, cross-tenant-not-found).
- cmd/ahp suite PASS both default and -tags ahp_data,ahp_exec.
- Offline mount proof TestWS1WorkspaceRoutesMounted PASS (package 404 for unknown/
  nested/non-POST; known op dispatched to transport guard).
- TestWS1RoleTierPolicy PASS (pure policy matrix).
- TestWS1UploadRightsMatrix PASS on real PG+MinIO: owner & editor stage files;
  viewer 403; same-tenant non-member 403; cross-tenant 404; editor Publish 403;
  ListMembers reports owner/editor/viewer.
- LIVE socket on 127.0.0.1:18302 (role=data):
    POST /v1/workspace/head  -> 404 {"code":"NOT_FOUND","message":"... workspaceapi: unknown route"}
    POST /v1/workspace/GetHead -> 403 transport: a verified client certificate is required
    POST /v1/workspace/a/b   -> 404 workspaceapi: unknown route
    CONTROL /v1/space/x      -> 404 "404 page not found"  (edge-style, distinguishable)
  => routes answer with the PACKAGE's own codes, not "no route or asset" / "route not found".

## 5. Recorded gaps (NOT invented) — objective item 3
- INVITATIONS: none. Only AddMember/RemoveMember/ListMembers exist (membership,
  not invitations). Location: internal/workspace/workspaceapi/api.go:46-68 and
  internal/workspace/store.go (AddMember/RemoveMember/ListMembers).
- ROLE-SETTING OPERATION: none via the mounted API. Roles are enforced and stored,
  but AddMember always grants editor and there is no operation to assign/change
  viewer (or demote owner). The viewer tier is real in storage+policy (the test
  promotes a member by writing members.role directly) but cannot be created through
  the HTTP surface at this base. Not fabricated; flagged for the contract owner.
- EDGE ROUTING: the browser single_origin.go edge proxies only /v1/space/ and
  /u-ahp/v1/artifacts/; all other /v1/* -> spaOnly 404 "route not found". Making
  the workspace face reachable for real users needs the DIRECTOR to add an edge
  proxy prefix (e.g. /u-ahp/v1/workspace/ -> data plane, remapped to /v1/workspace/).
  I did not touch the live composed origin (18080-18082 are another zone).

## 6. Files touched
- cmd/ahp/role_data.go (mount both planes; split construction/migration; mountDataHandlers; dataHandlers)
- cmd/ahp/role_data_ws1_test.go (offline mount proof, ahp_data)
- internal/workspace/store.go (role column, actionAllows, authorizeMember, requireMember/Create/ListMembers)
- internal/workspace/candidate.go (getCandidate action param; upload/check/reject/read tiers)
- internal/workspace/publish.go (Publish gates on authorizeMember "publish")
- internal/workspace/revision.go (PurgeRevision -> owner-only "purge")
- internal/workspace/roles_ws1_test.go (pure policy matrix + DB-backed rights matrix, skip-without-env)
Forbidden zones untouched: internal/space, internal/core, adapters/emil, internal/compute.
