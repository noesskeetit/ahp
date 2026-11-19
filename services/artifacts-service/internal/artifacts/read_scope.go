package artifacts

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Read admission has two boundaries and they belong to different principals.
//
// The SUBJECT is the human's boundary: a person sees their own artifacts, and
// that is what the label admission and the scope predicate enforce. The
// CONTEXT is the task's boundary: a task token is issued for one conversation
// (auth.Claims.Context, required on every task token by pkg/auth), so it may
// read the artifacts written into that conversation and no others. Without
// this rule a task token of the same subject reaches any artifact of that
// subject by id - measured on the stand as
// artifact-download-20261031T072257Z: same_subject_other_task_download=200.
//
// Rows written before the binding existed carry an empty context_id and cannot
// be scoped by it; they stay reachable by their subject. They are the residue
// of the migration, they are named in the acceptance record, and State's
// DeleteSubject still removes them.
type callerContextKey struct{}

// WithCallerContext annotates ctx with the context claim of the token
// performing the call. The API layer installs it from the VERIFIED claims,
// never from a request body; a caller that could name its own context would
// simply name the victim's.
func WithCallerContext(ctx context.Context, contextID string) context.Context {
	return context.WithValue(ctx, callerContextKey{}, contextID)
}

// CallerContext returns the installed context claim ("" when absent, which
// disables the check for that call - internal callers and unbound rows).
func CallerContext(ctx context.Context) string {
	v, _ := ctx.Value(callerContextKey{}).(string)
	return v
}

// checkReadScope is the context-scoped read gate. It runs inside every read
// path (Get, Download/OpenRange) after tenant, deletion and capability checks.
func (s *Store) checkReadScope(ctx context.Context, ref ArtifactRef) error {
	caller := CallerContext(ctx)
	if caller == "" || ref.ContextID == "" || caller == ref.ContextID {
		return nil
	}
	// The one widening the owner itself granted: a conversation the owning
	// conversation admitted (GrantContext). The source binding above stays
	// authoritative for everything else, and the lookup fails closed.
	if s.contextGrantAllowsRead(ctx, ref.Tenant, ref.ID, caller) {
		return nil
	}
	return contracts.NewError(contracts.ErrForbidden,
		"artifact belongs to another context")
}
