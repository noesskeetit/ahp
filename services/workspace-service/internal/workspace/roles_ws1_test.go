package workspace

import (
	"context"
	"os"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestWS1RoleTierPolicy is a pure unit test of the owner/editor/viewer policy
// table. It needs no database, so it runs in any environment and pins the
// contract: viewer=read only; editor=read+write+candidate work; owner=also
// publish/reject/purge/member management. Unknown roles/actions fail closed.
func TestWS1RoleTierPolicy(t *testing.T) {
	cases := []struct {
		role, action string
		want         bool
	}{
		{RoleViewer, "read", true},
		{RoleViewer, "write", false},
		{RoleViewer, "candidate:create", false},
		{RoleViewer, "candidate:write", false},
		{RoleViewer, "candidate:check", false},
		{RoleViewer, "publish", false},
		{RoleViewer, "member:add", false},

		{RoleEditor, "read", true},
		{RoleEditor, "write", true},
		{RoleEditor, "candidate:create", true},
		{RoleEditor, "candidate:write", true},
		{RoleEditor, "candidate:check", true},
		{RoleEditor, "candidate:list", true},
		{RoleEditor, "publish", false},
		{RoleEditor, "candidate:reject", false},
		{RoleEditor, "purge", false},
		{RoleEditor, "member:add", false},
		{RoleEditor, "member:list", false},

		{RoleOwner, "read", true},
		{RoleOwner, "write", true},
		{RoleOwner, "candidate:write", true},
		{RoleOwner, "candidate:reject", true},
		{RoleOwner, "publish", true},
		{RoleOwner, "purge", true},
		{RoleOwner, "member:add", true},
		{RoleOwner, "member:remove", true},
		{RoleOwner, "member:list", true},

		// fail closed
		{"", "read", false},
		{"admin", "read", false},
		{RoleOwner, "not-an-action", false},
	}
	for _, c := range cases {
		if got := actionAllows(c.role, c.action); got != c.want {
			t.Errorf("actionAllows(%q,%q) = %v, want %v", c.role, c.action, got, c.want)
		}
	}
}

// requireIntegrationEnv mirrors newEnv's requirements; it skips (rather than
// fails) when this package's integration harness is not configured, so the pure
// tests above still run in a bare checkout.
func requireIntegrationEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" || os.Getenv("AHP_S3_ENDPOINT") == "" ||
		os.Getenv("AHP_MINIO_ROOT_USER") == "" || os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Skip("workspace integration env not configured; skipping DB-backed rights test")
	}
}

// TestWS1UploadRightsMatrix proves the second half of the assignment end to end
// against real Postgres: owner and editor may stage candidate files, a viewer
// (a member, wrong tier) and a same-tenant non-member are refused FORBIDDEN, a
// cross-tenant caller is refused NOT_FOUND (403 vs 404 distinguishable), and the
// owner tier alone may publish — so membership now drives many rules, not one.
//
// Staging candidate files writes no artifact bytes, so this exercises the rights
// seams with Postgres alone (no object I/O). It is skipped without the harness.
func TestWS1UploadRightsMatrix(t *testing.T) {
	requireIntegrationEnv(t)
	e := newEnv(t)
	ctx := context.Background()

	owner := "alice"
	editor := "bob"
	viewer := "carol"
	nonMember := "dave"

	w, err := e.ws.Create(ctx, e.scope(owner), "proj")
	mustOK(t, err)

	// owner adds editor and viewer (AddMember grants the editor tier by default).
	mustOK(t, e.ws.AddMember(ctx, e.scope(owner), w.Name, editor))
	mustOK(t, e.ws.AddMember(ctx, e.scope(owner), w.Name, viewer))

	// Promote carol to the viewer tier: the mounted AddMember API has no
	// role-setting operation (recorded gap), so the test sets the authoritative
	// column directly. This is exactly the storage the store authorizes against.
	_, err = e.pool.Exec(ctx,
		`UPDATE ahp_workspace.members SET role='viewer', is_owner=false WHERE workspace_id=$1 AND subject=$2`,
		w.ID, viewer)
	mustOK(t, err)

	// membership list now exposes roles (owner is authoritative, viewer visible).
	members, err := e.ws.ListMembers(ctx, e.scope(owner), w.Name)
	mustOK(t, err)
	byRole := map[string]string{}
	for _, m := range members {
		byRole[m.Subject] = m.Role
	}
	if byRole[owner] != RoleOwner || byRole[editor] != RoleEditor || byRole[viewer] != RoleViewer {
		t.Fatalf("roles not persisted/reported: %+v", members)
	}

	upload := []File{{Path: "a.txt", ArtifactID: "art-1", Mode: 0o644, Digest: "deadbeef"}}

	// owner may stage files.
	oc, err := e.ws.CreateCandidate(ctx, e.scope(owner), w.Name, "")
	mustOK(t, err)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(owner), oc.ID, upload)
	mustOK(t, err)

	// editor may stage files (the fix: upload is a write-tier action).
	ec, err := e.ws.CreateCandidate(ctx, e.scope(editor), w.Name, "")
	mustOK(t, err)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(editor), ec.ID, upload)
	mustOK(t, err)

	// viewer may READ (candidate:list is not viewer, but candidate read is a
	// member read) yet may NOT open a candidate or stage files.
	_, err = e.ws.CreateCandidate(ctx, e.scope(viewer), w.Name, "")
	mustErr(t, err, string(contracts.ErrForbidden))
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(viewer), ec.ID, upload)
	mustErr(t, err, string(contracts.ErrForbidden))

	// same-tenant NON-member: refused FORBIDDEN (membership).
	_, err = e.ws.CreateCandidate(ctx, e.scope(nonMember), w.Name, "")
	mustErr(t, err, string(contracts.ErrForbidden))
	_, err = e.ws.GetWorkspace(ctx, e.scope(nonMember), w.Name)
	mustErr(t, err, string(contracts.ErrForbidden))

	// cross-tenant: refused NOT_FOUND — distinct from the 403 above, no leak.
	foreign := contracts.Scope{Tenant: e.tenant + "-other", Deployment: "d-1", Subject: owner}
	_, err = e.ws.GetWorkspace(ctx, foreign, w.Name)
	mustErr(t, err, string(contracts.ErrNotFound))

	// The owner tier alone may publish. An editor is refused at the publish gate
	// (is_owner/role now gates publish too, not just owner-removal).
	mustOK(t, e.ws.SetCheck(ctx, e.scope(editor), ec.ID, "ci", "passed", ""))
	_, err = e.ws.Publish(ctx, e.scope(editor), ec.ID, "")
	mustErr(t, err, string(contracts.ErrForbidden))
}
