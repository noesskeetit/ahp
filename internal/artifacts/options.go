package artifacts

import (
	"context"
	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// GrantLabels reports the current label set of the caller's live grant. It is
// consulted on every read, seal and publication check, so a label added to (or
// revoked from) a grant takes effect on the next call, never from a stale copy.
type GrantLabels func(ctx context.Context, scope contracts.Scope) (labels.Set, error)

// WorkspaceAccess reports whether the caller may read data that is restricted
// to one workspace. Artifact storage has no membership model of its own: the
// check is supplied by the caller that owns membership (the workspace service).
type WorkspaceAccess func(ctx context.Context, scope contracts.Scope, workspace string) error

// Option configures optional collaborators of a Store. Without them the store
// keeps its fail-closed defaults: no grant labels and no workspace reads.
type Option func(*Store)

// WithGrantLabels makes every visibility decision include the caller's current
// grant labels in addition to the labels stored on the artifact.
func WithGrantLabels(f GrantLabels) Option {
	return func(s *Store) { s.grantLabels = f }
}

// WithWorkspaceAccess enables reads of workspace-restricted artifacts. A
// workspace-labelled artifact is denied while no accessor is configured.
func WithWorkspaceAccess(f WorkspaceAccess) Option {
	return func(s *Store) { s.wsAccess = f }
}

// callerLabels returns the live grant labels of a scope. An unconfigured hook
// yields the empty set, which is neutral under labels.Merge.
func (s *Store) callerLabels(ctx context.Context, scope contracts.Scope) (labels.Set, error) {
	if s.grantLabels == nil {
		return labels.Set{}, nil
	}
	set, err := s.grantLabels(ctx, scope)
	if err != nil {
		return labels.Set{}, err
	}
	return set, nil
}

// admit decides whether a reader in scope may see data carrying the stored
// label set. Stored labels and the caller's current grant labels are merged
// first: a restriction that either side adds must hold.
//
// A set that names only the caller's subject is a private read. A set
// restricted to one workspace additionally requires the membership hook, so
// workspace visibility is never implied by knowing an artifact id.
func (s *Store) admit(ctx context.Context, scope contracts.Scope, stored labels.Set) (labels.Set, error) {
	grant, err := s.callerLabels(ctx, scope)
	if err != nil {
		return labels.Set{}, err
	}
	effective := labels.Merge(stored, grant)
	if err := labels.CheckWrite(effective, labels.Audience{
		Kind: labels.KindPrivate, Subject: scope.Subject,
	}); err != nil {
		return labels.Set{}, contracts.NewError(contracts.ErrForbidden,
			"artifact labels do not admit this subject")
	}
	if len(distinctStrings(effective.Workspaces)) > 0 && len(effective.Subjects) == 0 {
		if s.wsAccess == nil {
			return labels.Set{}, contracts.NewError(contracts.ErrForbidden,
				"workspace-restricted artifact requires a configured membership check")
		}
		for _, ws := range distinctStrings(effective.Workspaces) {
			if err := s.wsAccess(ctx, scope, ws); err != nil {
				return labels.Set{}, err
			}
		}
	}
	return effective, nil
}

func distinctStrings(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, v := range in {
		if v == "" {
			continue
		}
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// WithReferenceKeeper gives the deletion cascade the same durable-reference
// authority the garbage collector has, so a cascade cannot destroy what a
// workspace candidate, a revision, a turn frame or a memory record still names
// (DEF-H24-15). It is the same ReferenceScan the GC probe uses; a plane that
// already built one for the sweep needs no second object.
func WithReferenceKeeper(scan ReferenceScan) Option {
	return func(s *Store) { s.keep = scan }
}

// WithUploadClient signs PUT URLs for a browser-reachable storage endpoint.
// The backend read client remains private; no GET capabilities are issued.
func WithUploadClient(client *minio.Client) Option { return func(s *Store) { s.uploadClient = client } }

// WithObjectCipher mounts the at-rest object seam (internal/atrest/objectcipher)
// onto the artifact plane's actual put/get points. Absent or disabled the store
// is byte-for-byte the pre-encryption store; enabled, Seal transforms verified
// plaintext into sealed versions and every plaintext delivery decrypts
// transparently. See atrest_mount.go for the invariants this mount preserves.
func WithObjectCipher(c *objectcipher.Cipher) Option { return func(s *Store) { s.cipher = c } }
