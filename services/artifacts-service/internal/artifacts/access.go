package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// ErrRangeNotSatisfiable marks a byte range that starts past the end of the
// artifact. The HTTP layer maps it to 416; the code is kept as invalid so it
// round-trips through the contract error codes.
var ErrRangeNotSatisfiable = contracts.NewError(contracts.ErrInvalid, "range not satisfiable")

// RangeNotSatisfiableError is ErrRangeNotSatisfiable plus the one fact the
// HTTP layer needs and the sentinel cannot carry: the complete length of the
// object the range was asked against. RFC 9110 requires a 416 to answer
// Content-Range "bytes */<complete length>", and only the store has just read
// that size, so it is the only place that can state it truthfully - a second
// lookup from the handler would re-run the read gate and, on the grant path,
// would report the CURRENT sealed size instead of the size the grant pins.
//
// It unwraps to the sentinel, so errors.Is(err, ErrRangeNotSatisfiable) and
// contracts.Code(err) keep answering exactly as before for every existing
// caller (artifactsapi's two 416 branches, the workspace read that maps the
// same error to 400).
type RangeNotSatisfiableError struct {
	Size int64
}

func (e *RangeNotSatisfiableError) Error() string { return ErrRangeNotSatisfiable.Error() }

func (e *RangeNotSatisfiableError) Unwrap() error { return ErrRangeNotSatisfiable }

// OpenRange proxies a byte range of the exact sealed object version. It
// re-checks authorization and labels first. count == 0 reads from offset to
// the end of the object; offset must be within the artifact size.
func (s *Store) OpenRange(ctx context.Context, scope contracts.Scope, id string, offset, count int64) (io.ReadCloser, ArtifactRef, error) {
	ref, err := s.get(ctx, scope, id, true)
	if err != nil {
		return nil, ArtifactRef{}, err
	}
	object, err := s.objectPath(ctx, id)
	if err != nil {
		return nil, ArtifactRef{}, err
	}
	return s.openVersionRange(ctx, object, ref, offset, count)
}

// objectPath returns the object path one artifact id was written under.
func (s *Store) objectPath(ctx context.Context, id string) (string, error) {
	var object string
	if err := s.pool.QueryRow(ctx,
		`SELECT object FROM ahp_artifacts.artifacts WHERE id = $1`, id).Scan(&object); err != nil {
		return "", storageError("read artifact object path", err)
	}
	return object, nil
}

// openVersionRange streams a byte range of one exact sealed object version. It
// authorizes nothing of its own: the caller has already admitted the caller
// against the artifact row, which is what keeps the read gate in one place
// while a redemption (grants) may use a different admission decision.
func (s *Store) openVersionRange(ctx context.Context, object string, ref ArtifactRef, offset, count int64) (io.ReadCloser, ArtifactRef, error) {
	if err := s.checkContent(ctx, ref.ID); err != nil {
		return nil, ArtifactRef{}, err
	}
	if offset < 0 || count < 0 {
		return nil, ArtifactRef{}, contracts.NewError(contracts.ErrInvalid, "offset and count must be non-negative")
	}
	if offset >= ref.Size {
		return nil, ArtifactRef{}, &RangeNotSatisfiableError{Size: ref.Size}
	}
	if s.cipherOn(ref.Tenant) {
		data, err := s.cipher.GetRange(ctx, ref.Tenant, s.bucket, object, ref.Version, offset, count)
		if err != nil {
			return nil, ArtifactRef{}, storageError("open object range", err)
		}
		// Same attestation as Open: the range the durable record owes is a computable length,
		// and a shorter answer means the stored bytes are not what the row attests.
		if err := attestRange(ref.ID, attestedRangeLen(ref.Size, offset, count), len(data)); err != nil {
			return nil, ArtifactRef{}, err
		}
		return io.NopCloser(bytes.NewReader(data)), ref, nil
	}
	var opts minio.GetObjectOptions
	opts.VersionID = ref.Version
	if offset != 0 || count != 0 {
		// end is inclusive; count == 0 means "to the end" (bytes=offset-).
		if count == 0 {
			if err := opts.SetRange(offset, 0); err != nil {
				return nil, ArtifactRef{}, &RangeNotSatisfiableError{Size: ref.Size}
			}
		} else {
			end := offset + count - 1
			if end >= ref.Size {
				end = ref.Size - 1
			}
			if err := opts.SetRange(offset, end); err != nil {
				return nil, ArtifactRef{}, &RangeNotSatisfiableError{Size: ref.Size}
			}
		}
	}
	obj, err := s.client.GetObject(ctx, s.bucket, object, opts)
	if err != nil {
		return nil, ArtifactRef{}, storageError("open object range", err)
	}
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		return nil, ArtifactRef{}, storageError("open object range version", err)
	}
	return obj, ref, nil
}

// CheckPublication reports whether every artifact in ids may be written to the
// sink audience under its effective labels (stored labels merged with the
// caller's current grant labels). The first artifact that fails yields a
// FORBIDDEN naming that artifact, so publication is all-or-nothing.
//
// The caller must already be able to read every artifact; publication never
// grants access it does not already have.
func (s *Store) CheckPublication(ctx context.Context, scope contracts.Scope, ids []string, sink labels.Audience) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	if err := labels.CheckWrite(labels.Set{}, sink); err != nil {
		// Reject a malformed sink once, before touching the database.
		return err
	}
	for _, id := range ids {
		stored, err := s.storedLabelsForPublication(ctx, scope, id)
		if err != nil {
			return err
		}
		effective, err := s.admit(ctx, scope, stored)
		if err != nil {
			return contracts.NewError(contracts.ErrForbidden,
				"artifact "+id+" is not readable by this caller")
		}
		if err := labels.CheckWrite(effective, sink); err != nil {
			return contracts.NewError(contracts.ErrForbidden,
				"artifact "+id+" labels do not permit publication to this sink")
		}
	}
	return nil
}

// storedLabelsForPublication loads one artifact's stored labels after a tenant
// and read-authorization check. The label admission itself is left to the
// caller via admit so the merged effective set is used for the sink check.
func (s *Store) storedLabelsForPublication(ctx context.Context, scope contracts.Scope, id string) (labels.Set, error) {
	if err := validateID(id); err != nil {
		return labels.Set{}, err
	}
	var (
		tenant string
		doc    []byte
		sealed string
		del    bool
		ctxID  string
	)
	err := s.pool.QueryRow(ctx,
		`SELECT tenant, labels, COALESCE(version, ''), deleted, context_id
		   FROM ahp_artifacts.artifacts WHERE id = $1`, id).
		Scan(&tenant, &doc, &sealed, &del, &ctxID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return labels.Set{}, contracts.NewError(contracts.ErrNotFound, "artifact not found")
	case err != nil:
		return labels.Set{}, storageError("read artifact for publication", err)
	}
	if del || tenant != scope.Tenant || sealed == "" {
		return labels.Set{}, contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	if err := s.authorize(ctx, scope, "read", "artifact:"+id); err != nil {
		return labels.Set{}, err
	}
	// Publication writes the bytes somewhere else, so it is a read of them and
	// carries the same conversation gate: otherwise a token bound to one
	// context could publish another context's artifact to a public sink and
	// never call Get at all.
	if err := s.checkReadScope(ctx, ArtifactRef{ID: id, ContextID: ctxID}); err != nil {
		return labels.Set{}, err
	}
	var stored labels.Set
	if len(doc) > 0 {
		if err := json.Unmarshal(doc, &stored); err != nil {
			return labels.Set{}, contracts.NewError(contracts.ErrConflict, "stored labels are unreadable")
		}
	}
	return stored, nil
}

// PublishToWorkspace re-classifies one owner-private artifact so that members
// of the named workspace may read it. It is the only path that widens an
// artifact beyond its owning subject, and it refuses:
//   - artifacts the caller does not own;
//   - artifacts already shared with any other subject;
//   - secret- or external-tainted artifacts;
//   - artifacts already published to a different workspace (CONFLICT);
//
// On success the stored labels become a single-workspace set, which admission
// then gates with the configured membership hook on every read.
func (s *Store) PublishToWorkspace(ctx context.Context, scope contracts.Scope, id, workspace string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := validateID(id); err != nil {
		return err
	}
	if !idPattern.MatchString(workspace) {
		return contracts.NewError(contracts.ErrInvalid, "workspace name is malformed")
	}
	var (
		tenant  string
		subject string
		doc     []byte
		sealed  string
		del     bool
	)
	err := s.pool.QueryRow(ctx,
		`SELECT tenant, subject, labels, COALESCE(version, ''), deleted
		   FROM ahp_artifacts.artifacts WHERE id = $1`, id).
		Scan(&tenant, &subject, &doc, &sealed, &del)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return contracts.NewError(contracts.ErrNotFound, "artifact not found")
	case err != nil:
		return storageError("read artifact for publish", err)
	}
	if del || tenant != scope.Tenant || sealed == "" {
		return contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	if subject != scope.Subject {
		return contracts.NewError(contracts.ErrForbidden, "only the owning subject may publish an artifact")
	}
	if err := s.authorize(ctx, scope, "publish", "artifact:"+id); err != nil {
		return err
	}
	var stored labels.Set
	if len(doc) > 0 {
		if err := json.Unmarshal(doc, &stored); err != nil {
			return contracts.NewError(contracts.ErrConflict, "stored labels are unreadable")
		}
	}
	// Refuse to widen anything already shared beyond the owner or tainted
	// as secret/external.
	for _, sub := range stored.Subjects {
		if sub != scope.Subject {
			return contracts.NewError(contracts.ErrForbidden,
				"artifact is shared with another subject and cannot be published")
		}
	}
	for _, t := range stored.Taints {
		if t == labels.TaintSecret || t == labels.TaintExternal || t == labels.TaintUntrustedExternal {
			return contracts.NewError(contracts.ErrForbidden,
				"tainted artifacts cannot be published to a workspace")
		}
	}
	for _, ws := range stored.Workspaces {
		if ws != workspace {
			return contracts.NewError(contracts.ErrConflict,
				"artifact is already published to a different workspace")
		}
	}
	published := labels.Set{Workspaces: []string{workspace}}
	if err := labels.CheckWrite(published, labels.Audience{
		Kind: labels.KindWorkspace, Workspace: workspace,
	}); err != nil {
		return err
	}
	newDoc, err := json.Marshal(published)
	if err != nil {
		return contracts.NewError(contracts.ErrConflict, "labels cannot be stored")
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx,
			`UPDATE ahp_artifacts.artifacts SET labels = $2::jsonb
		   WHERE id = $1 AND tenant = $3 AND subject = $4 AND NOT deleted`,
			id, newDoc, scope.Tenant, scope.Subject)
		if err != nil {
			return storageError("publish artifact", err)
		}
		if tag.RowsAffected() == 0 {
			return contracts.NewError(contracts.ErrNotFound, "artifact not found")
		}
		return nil
	})
}

// idPattern mirrors the identifier shape accepted by pkg/labels so workspace
// names validated here are accepted downstream by CheckWrite.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}$`)

// IsValidContextID reports whether value can name a context (thread) whose runs
// produce artifacts.
//
// The context id is written into the artifact row as the artifact's binding, so
// a context this plane cannot name is a conversation whose run can never store
// an artifact: the upload refuses it with INVALID_ARGUMENT at the moment a tool
// tries to save output, which is far too late to explain to a user. Control
// planes that create conversations validate against THIS function instead of
// repeating the pattern, so every id they accept can carry artifacts. An empty
// value is not a binding at all and stays valid.
func IsValidContextID(value string) bool {
	return value == "" || idPattern.MatchString(value)
}
