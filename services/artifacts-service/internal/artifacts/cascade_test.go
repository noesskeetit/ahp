package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// shaHexOf is the sha256 hex the store demands in UploadRequest.
func shaHexOf(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// uploadSealBound uploads and seals one body under a binding and returns the id.
func (e *env) uploadSealBound(t *testing.T, b Binding, body string) string {
	t.Helper()
	ctx := context.Background()
	up, err := e.store.BeginUploadWithBinding(ctx, e.scope, b, "k-"+t.Name()+"-"+body, UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream",
		SHA256: shaHexOf(body), Size: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("BeginUploadWithBinding: %v", err)
	}
	put(t, up.PresignedURL, body)
	ref, err := e.store.Seal(ctx, e.scope, up.ID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return ref.ID
}

func mustReadable(t *testing.T, e *env, id string) {
	t.Helper()
	if _, err := e.store.Get(context.Background(), e.scope, id); err != nil {
		t.Fatalf("Get(%s) must still succeed: %v", id, err)
	}
}

func mustGone(t *testing.T, e *env, id string) {
	t.Helper()
	ctx := context.Background()
	if _, err := e.store.Get(ctx, e.scope, id); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("Get(%s) after cascade: want NOT_FOUND, got %v", id, err)
	}
	if _, _, err := e.store.Open(ctx, e.scope, id); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("Open(%s) after cascade: want NOT_FOUND, got %v", id, err)
	}
}

// TestCascadeByContext is the measured defect: deleting a thread must make the
// artifacts of that context unreadable and undownloadable, with no agent hook.
func TestCascadeByContext(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	inContext := e.uploadSealBound(t, Binding{ContextID: "ctx-thread-1"}, "body-in-context")
	byTask := e.uploadSealBound(t, Binding{TaskID: "task-of-thread-1"}, "body-by-task")
	elsewhere := e.uploadSealBound(t, Binding{ContextID: "ctx-thread-2"}, "body-other-thread")
	unbound := e.uploadSealBound(t, Binding{}, "body-without-binding")

	res, err := e.store.DeleteByThread(ctx, e.scope, []string{"ctx-thread-1"}, []string{"task-of-thread-1"})
	if err != nil {
		t.Fatalf("DeleteByThread: %v", err)
	}
	if res.Tombstoned != 2 || res.Held != 0 || res.AlreadyDeleted != 0 {
		t.Fatalf("first cascade = %+v, want tombstoned=2 held=0 already=0", res)
	}
	mustGone(t, e, inContext)
	mustGone(t, e, byTask)
	mustReadable(t, e, elsewhere)
	mustReadable(t, e, unbound)

	// Idempotent: the same cascade again changes nothing and says so.
	again, err := e.store.DeleteByThread(ctx, e.scope, []string{"ctx-thread-1"}, []string{"task-of-thread-1"})
	if err != nil {
		t.Fatalf("repeat DeleteByThread: %v", err)
	}
	if again.Tombstoned != 0 || again.AlreadyDeleted != 2 {
		t.Fatalf("repeat cascade = %+v, want tombstoned=0 already_deleted=2", again)
	}
	mustGone(t, e, inContext)
}

// TestCascadeLeavesLegalHoldAlone pins the rule the PDF states in the same
// sentence as the cascade: a legal hold blocks deletion. The held bytes stay
// readable after the cascade; the unprotected ones do not.
func TestCascadeLeavesLegalHoldAlone(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	held := e.uploadSealBound(t, Binding{ContextID: "ctx-held"}, "held-by-legal-hold")
	free := e.uploadSealBound(t, Binding{ContextID: "ctx-held"}, "not-held")
	if err := e.store.SetLegalHold(ctx, e.scope, held, true); err != nil {
		t.Fatalf("SetLegalHold: %v", err)
	}

	res, err := e.store.DeleteByContext(ctx, e.scope, "ctx-held")
	if err != nil {
		t.Fatalf("DeleteByContext: %v", err)
	}
	if res.Tombstoned != 1 || res.Held != 1 {
		t.Fatalf("cascade = %+v, want tombstoned=1 held=1", res)
	}
	mustReadable(t, e, held)
	mustGone(t, e, free)
	lc, err := e.store.GetLifecycle(ctx, e.scope, held)
	if err != nil || lc.Deleted || !lc.LegalHold {
		t.Fatalf("held lifecycle = %+v err %v, want deleted=false legal_hold=true", lc, err)
	}

	// A purge is blocked by the same hold, and releasing the hold lets the
	// repeated cascade reach the artifact at last.
	if err := e.store.Purge(ctx, e.scope, held); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("Purge under hold: want CONFLICT, got %v", err)
	}
	if err := e.store.SetLegalHold(ctx, e.scope, held, false); err != nil {
		t.Fatalf("release hold: %v", err)
	}
	res, err = e.store.DeleteByContext(ctx, e.scope, "ctx-held")
	if err != nil {
		t.Fatalf("cascade after release: %v", err)
	}
	if res.Tombstoned != 1 {
		t.Fatalf("cascade after release = %+v, want tombstoned=1", res)
	}
	mustGone(t, e, held)
}

// TestCascadeCannotLeaveItsScope: a foreign subject, deployment or tenant asking
// for the same context id touches nothing, and learns nothing.
func TestCascadeCannotLeaveItsScope(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	owned := e.uploadSealBound(t, Binding{ContextID: "shared-context"}, "alice-bytes")
	for name, foreign := range map[string]contracts.Scope{
		"subject":    e.foreign,
		"deployment": {Tenant: e.scope.Tenant, Deployment: "other-deployment", Subject: e.scope.Subject},
		"tenant":     {Tenant: "other-tenant", Deployment: e.scope.Deployment, Subject: e.scope.Subject},
	} {
		res, err := e.store.DeleteByContext(ctx, foreign, "shared-context")
		if err != nil {
			t.Fatalf("foreign %s DeleteByContext: %v", name, err)
		}
		if res.Tombstoned != 0 || res.Held != 0 {
			t.Fatalf("foreign %s cascade touched %+v", name, res)
		}
	}
	mustReadable(t, e, owned)

	if _, err := e.store.DeleteBySubject(ctx, e.foreign); err != nil {
		t.Fatalf("foreign DeleteBySubject: %v", err)
	}
	mustReadable(t, e, owned)

	res, err := e.store.DeleteBySubject(ctx, e.scope)
	if err != nil {
		t.Fatalf("own DeleteBySubject: %v", err)
	}
	if res.Tombstoned != 1 {
		t.Fatalf("own DeleteBySubject = %+v, want tombstoned=1", res)
	}
	mustGone(t, e, owned)
}

// TestBindingIsRecordedOnTheRow: the PDF's support for deletion, retention and
// legal hold is that every record knows its tenant, deployment, subject and
// context. A reader of one artifact must be able to see them.
func TestBindingIsRecordedOnTheRow(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := e.uploadSealBound(t, Binding{ContextID: "ctx-visible", TaskID: "task-visible"}, "binding-visible")
	ref, err := e.store.Get(ctx, e.scope, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ref.Tenant != e.scope.Tenant || ref.Deployment != e.scope.Deployment || ref.Subject != e.scope.Subject {
		t.Fatalf("ref scope = %+v, want %+v", ref, e.scope)
	}
	if ref.ContextID != "ctx-visible" || ref.TaskID != "task-visible" {
		t.Fatalf("ref binding = %+v, want ctx-visible/task-visible", ref)
	}

	listed, err := e.store.ListByContext(ctx, e.scope, "ctx-visible")
	if err != nil || len(listed) != 1 || listed[0].ID != id {
		t.Fatalf("ListByContext = %v err %v, want exactly the one artifact", listed, err)
	}
	// After the cascade the same read reports nothing rather than a tombstone.
	if _, err := e.store.DeleteByContext(ctx, e.scope, "ctx-visible"); err != nil {
		t.Fatalf("DeleteByContext: %v", err)
	}
	if listed, err := e.store.ListByContext(ctx, e.scope, "ctx-visible"); err != nil || len(listed) != 0 {
		t.Fatalf("ListByContext after cascade = %v err %v, want empty", listed, err)
	}
}

// TestBindingIsNotAnAuthorizationInput: a malformed binding is refused, and an
// empty binding is legal - it just cannot be reached by a context cascade.
func TestBindingValidation(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	for _, bad := range []Binding{
		{ContextID: "has/slash"},
		{TaskID: " has space"},
		{ContextID: string(make([]byte, 129))},
	} {
		_, err := e.store.BeginUploadWithBinding(ctx, e.scope, bad, "k-bads", UploadRequest{
			Name: "a.bin", MIME: "application/octet-stream",
			SHA256: shaHexOf("x"), Size: 1,
		})
		if contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("binding %+v: want INVALID, got %v", bad, err)
		}
	}
	// A selector that names nothing is refused, not silently treated as
	// "nothing to do": an empty context id would otherwise select every
	// artifact that carries no binding at all, and an empty task set would be
	// a cascade only in the caller's imagination.
	if _, err := e.store.DeleteByContext(ctx, e.scope, ""); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("empty context cascade: want INVALID, got %v", err)
	}
	if _, err := e.store.DeleteByTasks(ctx, e.scope, nil); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("empty task cascade: want INVALID, got %v", err)
	}
	// A malformed id inside an otherwise usable set is refused whole, so no
	// part of a cascade can be built from an unvalidated selector.
	if _, err := e.store.DeleteByTasks(ctx, e.scope, []string{"ok-task", "bad/task"}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("partly malformed task cascade: want INVALID, got %v", err)
	}
}

// TestCascadeRespectsRevokedAuthorization keeps the cascade inside the existing
// authorization gate: with the policy off, not even the owner may cascade.
func TestCascadeRespectsRevokedAuthorization(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id := e.uploadSealBound(t, Binding{ContextID: "ctx-authz"}, "authz-bytes")

	e.allow.Store(false)
	if _, err := e.store.DeleteByContext(ctx, e.scope, "ctx-authz"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("cascade without authorization: want FORBIDDEN, got %v", err)
	}
	e.allow.Store(true)
	mustReadable(t, e, id)
}
