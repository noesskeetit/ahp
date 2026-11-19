package artifacts

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Context grants: the owner's own answer to "this conversation's bytes are
// needed by another of MY conversations".
//
// Read admission has two boundaries (read_scope.go): the subject is the human's,
// the context is the task's. The context boundary is what makes a cross-context
// State import unable to hydrate: the checkpoint names the artifact, the
// imported conversation may not read it (measured:
// TestCrossContextImportDoesNotHydrateArtifactReads). Refusing that forever is
// not a product answer, and rewriting the reference from State would be label
// laundering - the importer would be choosing which conversation may read bytes
// it does not own.
//
// So the widening is an operation of the artifact owner, here, and it is narrow:
//
//   - only the owning subject of the owning tenant may grant, and a deleted or
//     unsealed artifact is not visible at all;
//   - the grant must be made BY the conversation that currently owns the bytes
//     (the verified context claim equals the artifact's binding context). A task
//     of some third conversation cannot pull an artifact of its subject into
//     itself - that is the exact hole read_scope.go was written to close
//     (same_subject_other_task_download=200);
//   - labels that are already shared with another subject, or tainted
//     secret/external/untrusted, are refused: a grant may not launder them;
//   - the artifact's binding context is NEVER rewritten. The source stays
//     immutable, and every grant row records which context the bytes came from,
//     who granted it, and the operation key that asked for it.
//
// Revocation exists so a grant is reversible without touching the artifact.
const ddlContextGrants = `CREATE TABLE IF NOT EXISTS ` + Schema + `.context_grants(
 artifact_id text NOT NULL,
 tenant      text NOT NULL,
 subject     text NOT NULL,
 context_id  text NOT NULL,
 source_context text NOT NULL,
 granted_by  text NOT NULL,
 op_key      text NOT NULL,
 created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
 revoked_at  timestamptz,
 PRIMARY KEY (tenant, artifact_id, context_id)
);
CREATE TABLE IF NOT EXISTS ` + Schema + `.context_grant_audit(
 tenant text NOT NULL,
 deployment text NOT NULL,
 subject text NOT NULL,
 op_key text NOT NULL,
 artifact_id text NOT NULL,
 action text NOT NULL,
 context_id text NOT NULL,
 source_context text NOT NULL,
 granted_by text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (tenant, deployment, subject, op_key)
);`

// ddlContextGrantAuditUpgrade converges an audit table that was created with
// op_key as a global primary key. An idempotency key belongs to the scope that
// used it -- the same convention as (tenant, deployment, subject, idem_key) on
// uploads -- because a global key lets one tenant's key collide with another's
// and turns an unrelated replay into a CONFLICT for whoever writes second.
// The conversion keeps every row; it only adds the missing column and moves the
// key. It is guarded so a correctly shaped table is left completely alone.
const ddlContextGrantAuditUpgrade = `ALTER TABLE ` + Schema + `.context_grant_audit
	ADD COLUMN IF NOT EXISTS deployment text NOT NULL DEFAULT '';
DO $ahp$ BEGIN
	IF EXISTS (SELECT 1 FROM pg_constraint
		WHERE contype = 'p'
		  AND conrelid = '` + Schema + `.context_grant_audit'::regclass
		  AND array_length(conkey, 1) = 1) THEN
		EXECUTE 'ALTER TABLE ` + Schema + `.context_grant_audit DROP CONSTRAINT ' ||
			(SELECT quote_ident(conname) FROM pg_constraint
				WHERE contype = 'p'
				  AND conrelid = '` + Schema + `.context_grant_audit'::regclass);
		EXECUTE 'ALTER TABLE ` + Schema + `.context_grant_audit
			ADD PRIMARY KEY (tenant, deployment, subject, op_key)';
	END IF;
END $ahp$;`

// contextGrantRequest is one grant attempt.
type contextGrantRequest struct {
	ID     string
	Target string
	OpKey  string
	Revoke bool
}

// grantKeyReuseErr mirrors the platform-wide idempotency rule: the same key
// with the same arguments replays, with different arguments it is refused.
var errGrantKeyReuse = contracts.NewError(contracts.ErrConflict,
	"artifacts: idempotency key was already used for a different context grant")

// GrantContext lets the conversation that owns an artifact's bytes admit one
// more of the SAME subject's conversations to read them. It never moves the
// artifact and never changes its binding context.
func (s *Store) GrantContext(ctx context.Context, scope contracts.Scope, id, targetContext, opKey string) error {
	return s.applyContextGrant(ctx, scope, contextGrantRequest{ID: id, Target: targetContext, OpKey: opKey})
}

// RevokeContext removes a grant: the admitted conversation loses the read again,
// the artifact and its own conversation are untouched.
func (s *Store) RevokeContext(ctx context.Context, scope contracts.Scope, id, targetContext, opKey string) error {
	return s.applyContextGrant(ctx, scope, contextGrantRequest{ID: id, Target: targetContext, OpKey: opKey, Revoke: true})
}

func (s *Store) applyContextGrant(ctx context.Context, scope contracts.Scope, in contextGrantRequest) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := validateID(in.ID); err != nil {
		return err
	}
	// An empty value is "no binding at all" for the read gate, so a grant to it
	// would be a grant to nobody (or to everybody). Refuse it as malformed.
	if in.Target == "" || !IsValidContextID(in.Target) {
		return contracts.NewError(contracts.ErrInvalid, "target context id is malformed")
	}
	if in.OpKey == "" {
		return contracts.NewError(contracts.ErrInvalid, "an idempotency key is required")
	}
	action := "grant"
	if in.Revoke {
		action = "revoke"
	}

	var (
		tenant     string
		subject    string
		bindingCtx string
		doc        []byte
		sealed     string
		del        bool
	)
	err := s.pool.QueryRow(ctx,
		`SELECT tenant, subject, COALESCE(context_id, ''), labels, COALESCE(version, ''), deleted
		   FROM `+Schema+`.artifacts WHERE id = $1`, in.ID).
		Scan(&tenant, &subject, &bindingCtx, &doc, &sealed, &del)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return contracts.NewError(contracts.ErrNotFound, "artifact not found")
	case err != nil:
		return storageError("read artifact for context grant", err)
	}
	if del || tenant != scope.Tenant || sealed == "" {
		return contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	if subject != scope.Subject {
		return contracts.NewError(contracts.ErrForbidden,
			"only the owning subject may grant access to an artifact")
	}
	// Consent: the caller IS the conversation the bytes belong to. Without this
	// a task could widen any artifact of its subject into its own conversation.
	caller := CallerContext(ctx)
	if bindingCtx != "" && caller != bindingCtx {
		return contracts.NewError(contracts.ErrForbidden,
			"a context grant must be made by the conversation that owns the artifact")
	}
	if err := s.authorize(ctx, scope, action, "artifact:"+in.ID); err != nil {
		return err
	}
	if !in.Revoke {
		var stored labels.Set
		if len(doc) > 0 {
			if err := json.Unmarshal(doc, &stored); err != nil {
				return contracts.NewError(contracts.ErrConflict, "stored labels are unreadable")
			}
		}
		for _, sub := range stored.Subjects {
			if sub != scope.Subject {
				return contracts.NewError(contracts.ErrForbidden,
					"artifact is shared with another subject and cannot be granted to a conversation")
			}
		}
		for _, t := range stored.Taints {
			if t == labels.TaintSecret || t == labels.TaintExternal || t == labels.TaintUntrustedExternal {
				return contracts.NewError(contracts.ErrForbidden,
					"tainted artifacts cannot be granted to another conversation")
			}
		}
		// A grant is a read widened to a private audience of the same subject:
		// it must satisfy the same write check every other widening passes.
		if err := labels.CheckWrite(stored, labels.Audience{
			Kind: labels.KindPrivate, Subject: scope.Subject,
		}); err != nil {
			return err
		}
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		// Idempotency first: the same key with the same arguments replays.
		var seenAction, seenArtifact, seenTarget string
		err := tx.QueryRow(ctx, `SELECT action, artifact_id, context_id FROM `+Schema+`.context_grant_audit
			WHERE op_key = $1 AND tenant = $2 AND deployment = $3 AND subject = $4`,
			in.OpKey, scope.Tenant, scope.Deployment, scope.Subject).
			Scan(&seenAction, &seenArtifact, &seenTarget)
		replay := false
		switch {
		case err == nil:
			if seenAction != action || seenArtifact != in.ID || seenTarget != in.Target {
				return errGrantKeyReuse
			}
			replay = true
		case !errors.Is(err, pgx.ErrNoRows):
			return storageError("read context grant audit", err)
		}
		if !replay {
			if in.Revoke {
				tag, err := tx.Exec(ctx, `UPDATE `+Schema+`.context_grants SET revoked_at = clock_timestamp()
					WHERE tenant=$1 AND artifact_id=$2 AND context_id=$3 AND subject=$4 AND revoked_at IS NULL`,
					scope.Tenant, in.ID, in.Target, scope.Subject)
				if err != nil {
					return storageError("revoke context grant", err)
				}
				if tag.RowsAffected() == 0 {
					return contracts.NewError(contracts.ErrNotFound, "no such context grant")
				}
			} else {
				// ON CONFLICT DO NOTHING: a grant already in place is not a
				// second effect, and the audit row keeps the first one.
				if _, err := tx.Exec(ctx, `INSERT INTO `+Schema+`.context_grants
						(artifact_id, tenant, subject, context_id, source_context, granted_by, op_key)
					VALUES ($1,$2,$3,$4,$5,$6,$7)
					ON CONFLICT (tenant, artifact_id, context_id) DO NOTHING`,
					in.ID, scope.Tenant, scope.Subject, in.Target, bindingCtx, caller, in.OpKey); err != nil {
					return storageError("insert context grant", err)
				}
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+Schema+`.context_grant_audit
					(op_key, tenant, deployment, subject, artifact_id, action, context_id,
					 source_context, granted_by)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
				in.OpKey, scope.Tenant, scope.Deployment, scope.Subject, in.ID, action, in.Target,
				bindingCtx, caller); err != nil {
				return storageError("record context grant", err)
			}
		}
		return nil
	})
}

// contextGrantAllowsRead is the read-gate consultation: a caller admitted to an
// artifact by a live grant may read it from that conversation. Fail closed - a
// storage error is not an answer that permits a read.
func (s *Store) contextGrantAllowsRead(ctx context.Context, tenant, artifactID, caller string) bool {
	if s == nil || s.pool == nil || caller == "" || artifactID == "" {
		return false
	}
	var allowed bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+Schema+`.context_grants
		WHERE artifact_id=$1 AND context_id=$2 AND revoked_at IS NULL
		  AND ($3 = '' OR tenant = $3))`, artifactID, caller, tenant).Scan(&allowed)
	if err != nil {
		return false
	}
	return allowed
}

// ContextGrant is the durable shape of one grant, readable by an operator.
type ContextGrant struct {
	ArtifactID    string `json:"artifact_id"`
	ContextID     string `json:"context_id"`
	SourceContext string `json:"source_context"`
	GrantedBy     string `json:"granted_by"`
	OpKey         string `json:"op_key"`
	Revoked       bool   `json:"revoked"`
}

// GetContextGrant reads one grant row out of the owner's own table.
func (s *Store) GetContextGrant(ctx context.Context, scope contracts.Scope, id, targetContext string) (ContextGrant, error) {
	if err := validateID(id); err != nil {
		return ContextGrant{}, err
	}
	if err := s.authorize(ctx, scope, "read", "artifact:"+id); err != nil {
		return ContextGrant{}, err
	}
	var g ContextGrant
	err := s.pool.QueryRow(ctx, `SELECT artifact_id, context_id, source_context, granted_by, op_key,
			revoked_at IS NOT NULL FROM `+Schema+`.context_grants
		WHERE tenant=$1 AND subject=$2 AND artifact_id=$3 AND context_id=$4`,
		scope.Tenant, scope.Subject, id, targetContext).
		Scan(&g.ArtifactID, &g.ContextID, &g.SourceContext, &g.GrantedBy, &g.OpKey, &g.Revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return ContextGrant{}, contracts.NewError(contracts.ErrNotFound, "context grant not found")
	}
	if err != nil {
		return ContextGrant{}, storageError("read context grant", err)
	}
	return g, nil
}
