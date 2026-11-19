package artifacts

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// ddlLifecycle extends the artifacts table with lifecycle columns and the
// derivations table. It is idempotent and runs after ddl in Migrate.
const ddlLifecycle = `
ALTER TABLE ahp_artifacts.artifacts
    ADD COLUMN IF NOT EXISTS deleted_at   timestamptz,
    ADD COLUMN IF NOT EXISTS retain_until timestamptz,
    ADD COLUMN IF NOT EXISTS legal_hold   boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS context_id   text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS task_id      text NOT NULL DEFAULT '';

-- The cascade selector: a thread or subject deletion must find the artifacts it
-- owns by (scope, context) or (scope, task) without a full table scan. Rows
-- written before the binding existed carry the empty string and are excluded.
CREATE INDEX IF NOT EXISTS artifacts_context_idx
    ON ahp_artifacts.artifacts (tenant, deployment, subject, context_id)
    WHERE context_id <> '';
CREATE INDEX IF NOT EXISTS artifacts_task_idx
    ON ahp_artifacts.artifacts (tenant, deployment, subject, task_id)
    WHERE task_id <> '';

CREATE TABLE IF NOT EXISTS ahp_artifacts.derivations (
    artifact_id text NOT NULL REFERENCES ahp_artifacts.artifacts(id) ON DELETE CASCADE,
    kind        text NOT NULL,
    status      text NOT NULL CHECK (status IN ('pending', 'ready', 'failed')),
    sha256      text NOT NULL DEFAULT '',
    size        bigint NOT NULL DEFAULT 0,
    detail      text NOT NULL DEFAULT '',
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (artifact_id, kind)
);
`

// Derivation is one derived representation of an artifact (thumbnail, index,
// transcript, ...). Readiness of an artifact depends on every derivation
// reaching the ready state.
type Derivation struct {
	Kind   string `json:"kind"`
	Status string `json:"status"`
	SHA256 string `json:"sha256,omitempty"`
	Size   int64  `json:"size,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// Readiness separates verified bytes, extraction, indexed search and quarantine.
// Ready requires the ingestion stages and every additional derivation.
type Readiness struct {
	Sealed           bool         `json:"sealed"`
	ContentReady     bool         `json:"content_ready"`
	ExtractionReady  bool         `json:"extraction_ready"`
	SearchReady      bool         `json:"search_ready"`
	Quarantined      bool         `json:"quarantined"`
	ExtractionStatus string       `json:"extraction_status"`
	SearchStatus     string       `json:"search_status"`
	SearchDetail     string       `json:"search_detail,omitempty"`
	Detail           string       `json:"detail,omitempty"`
	Ready            bool         `json:"ready"`
	Derivations      []Derivation `json:"derivations"`
}

// Lifecycle is the observable deletion/retention state of one artifact.
type Lifecycle struct {
	ID          string       `json:"id"`
	Deleted     bool         `json:"deleted"`
	DeletedAt   *time.Time   `json:"deleted_at,omitempty"`
	RetainUntil *time.Time   `json:"retain_until,omitempty"`
	LegalHold   bool         `json:"legal_hold"`
	Derivations []Derivation `json:"derivations"`
}

// lifecycleVerbRead names the only lifecycle verb that is a READ rather than an
// action, and is therefore the only one subject to the B114 read-parity mask and
// to the conversation read gate.
const lifecycleVerbRead = "read"

// loadLifecycle reads one artifact's lifecycle row after tenant, current
// authorization, label admission and - for the READ verb only - the
// conversation read gate. The label gate is applied even to tombstones, so
// lifecycle metadata never leaks across subjects.
func (s *Store) loadLifecycle(ctx context.Context, scope contracts.Scope, id, action string) (Lifecycle, string, error) {
	if err := scope.Validate(); err != nil {
		return Lifecycle{}, "", err
	}
	if err := validateID(id); err != nil {
		return Lifecycle{}, "", err
	}
	var (
		lc     Lifecycle
		tenant string
		object string
		doc    []byte
	)
	var contextID string
	err := s.pool.QueryRow(ctx, `
SELECT id, tenant, object, labels, deleted, deleted_at, retain_until, legal_hold, context_id
FROM ahp_artifacts.artifacts WHERE id = $1`, id).
		Scan(&lc.ID, &tenant, &object, &doc, &lc.Deleted, &lc.DeletedAt,
			&lc.RetainUntil, &lc.LegalHold, &contextID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Lifecycle{}, "", contracts.NewError(contracts.ErrNotFound, "artifact not found")
	case err != nil:
		return Lifecycle{}, "", storageError("read lifecycle", err)
	}
	if tenant != scope.Tenant {
		return Lifecycle{}, "", contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	var stored labels.Set
	if len(doc) > 0 {
		if err := json.Unmarshal(doc, &stored); err != nil {
			return Lifecycle{}, "", contracts.NewError(contracts.ErrConflict, "stored labels are unreadable")
		}
	}
	if err := s.authorize(ctx, scope, action, "artifact:"+id); err != nil {
		return Lifecycle{}, "", err
	}
	if _, err := s.admit(ctx, scope, stored); err != nil {
		// B114 read parity, the same rule Store.load applies to content: a
		// metadata READ by a subject the labels exclude answers byte-for-byte as
		// a missing id does, so existence cannot be probed through this face.
		// Every other verb here is an action (retain, hold, purge, derive) and
		// keeps the real refusal, and an authorization refusal is never masked -
		// it happens above, before admission is consulted.
		if action == lifecycleVerbRead {
			return Lifecycle{}, "", contracts.NewError(contracts.ErrNotFound, "artifact not found")
		}
		return Lifecycle{}, "", err
	}
	if action == lifecycleVerbRead {
		// The same conversation gate the content read applies, in one place, so
		// GetLifecycle and Readiness cannot drift apart again. A scope refusal is
		// NOT masked: this caller owns the artifact and only its conversation
		// claim differs, which is the task-token boundary, not a secret.
		if err := s.checkReadScope(ctx, ArtifactRef{ID: id, Tenant: tenant, ContextID: contextID}); err != nil {
			return Lifecycle{}, "", err
		}
	}
	lc.Derivations, err = s.listDerivations(ctx, id)
	if err != nil {
		return Lifecycle{}, "", err
	}
	return lc, object, nil
}

// MarkDerivation records or replaces one derivation of an artifact. A ready
// derivation should carry the sha256/size of the derived bytes so consumers
// can bind to them; failed derivations keep the artifact not-ready.
func (s *Store) MarkDerivation(ctx context.Context, scope contracts.Scope, id string, d Derivation) error {
	if d.Kind == "extraction" || d.Kind == "search" || d.Kind == "quarantine" || d.Kind == "content" {
		return contracts.NewError(contracts.ErrForbidden, "ingestion stages are service-owned")
	}
	if d.Kind == "" || len(d.Kind) > 64 {
		return contracts.NewError(contracts.ErrInvalid, "derivation kind must be 1..64 bytes")
	}
	switch d.Status {
	case "pending", "ready", "failed":
	default:
		return contracts.NewError(contracts.ErrInvalid, "derivation status must be pending, ready or failed")
	}
	if _, _, err := s.loadLifecycle(ctx, scope, id, "derive"); err != nil {
		return err
	}
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := checkArtifactWritable(ctx, tx, scope, id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
INSERT INTO ahp_artifacts.derivations (artifact_id, kind, status, sha256, size, detail, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, now())
ON CONFLICT (artifact_id, kind)
DO UPDATE SET status = excluded.status, sha256 = excluded.sha256,
              size = excluded.size, detail = excluded.detail, updated_at = now()`,
			id, d.Kind, d.Status, d.SHA256, d.Size, d.Detail)
		return err
	})
	return storageError("mark derivation", err)
}

// Readiness reports independent content, extraction, search and quarantine stages.
// A sealed object without an extraction is never reported ready for agent text.
func (s *Store) Readiness(ctx context.Context, scope contracts.Scope, id string) (Readiness, error) {
	lc, _, err := s.loadLifecycle(ctx, scope, id, lifecycleVerbRead)
	if err != nil {
		return Readiness{}, err
	}
	if lc.Deleted {
		return Readiness{}, contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	var r Readiness
	var cached bool
	// The conversation read gate already ran inside loadLifecycle (read verb), so
	// this scan no longer needs the binding: one gate, one place, both read verbs.
	err = s.pool.QueryRow(ctx, `SELECT a.version IS NOT NULL AND a.sha256 IS NOT NULL,a.ingestion_state,a.ingestion_detail,
 c.sha256 IS NOT NULL,COALESCE(c.search_state,'pending'),COALESCE(c.search_detail,'')
 FROM ahp_artifacts.artifacts a LEFT JOIN ahp_artifacts.extraction_cache c
 ON c.tenant=a.tenant AND c.sha256=a.sha256 AND c.mime=a.mime AND c.revision=$2 WHERE a.id=$1`, id, parserRevision).Scan(&r.Sealed, &r.ExtractionStatus, &r.Detail, &cached, &r.SearchStatus, &r.SearchDetail)
	if err != nil {
		return Readiness{}, storageError("read ingestion status", err)
	}
	r.ContentReady = r.Sealed
	r.Quarantined = r.ExtractionStatus == "quarantined"
	r.ExtractionReady = r.Sealed && r.ExtractionStatus == "ready" && cached
	r.SearchReady = r.ExtractionReady && r.SearchStatus == "ready"
	if r.ExtractionStatus == "unsupported" {
		r.SearchStatus = "unsupported"
	}
	r.Ready = r.ContentReady && r.ExtractionReady && r.SearchReady && !r.Quarantined
	r.Derivations = lc.Derivations
	for _, d := range r.Derivations {
		if d.Status != "ready" {
			r.Ready = false
		}
	}
	return r, nil
}

// GetLifecycle returns the current deletion/retention/hold state.
func (s *Store) GetLifecycle(ctx context.Context, scope contracts.Scope, id string) (Lifecycle, error) {
	lc, _, err := s.loadLifecycle(ctx, scope, id, lifecycleVerbRead)
	return lc, err
}

// SetRetention sets (or clears, when until is nil) the earliest time after
// which a deleted artifact may be purged.
func (s *Store) SetRetention(ctx context.Context, scope contracts.Scope, id string, until *time.Time) error {
	if _, _, err := s.loadLifecycle(ctx, scope, id, "retain"); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE ahp_artifacts.artifacts SET retain_until = $2 WHERE id = $1`, id, until)
	return storageError("set retention", err)
}

// SetLegalHold pins an artifact against deletion and purge until released.
func (s *Store) SetLegalHold(ctx context.Context, scope contracts.Scope, id string, on bool) error {
	if _, _, err := s.loadLifecycle(ctx, scope, id, "hold"); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE ahp_artifacts.artifacts SET legal_hold = $2 WHERE id = $1`, id, on)
	return storageError("set legal hold", err)
}

// Purge irreversibly removes one tombstoned artifact: its metadata row and
// every stored version of its object. It refuses while a legal hold is set,
// while the row is not deleted, while a retention floor has not elapsed, and
// while a live context, thread, task or run still refers to it - the same
// refusal list the sweep applies, so an explicit purge can never be a way to
// get around a pin.
func (s *Store) Purge(ctx context.Context, scope contracts.Scope, id string) error {
	lc, object, err := s.loadLifecycle(ctx, scope, id, "purge")
	if err != nil {
		return err
	}
	if !lc.Deleted {
		return contracts.NewError(contracts.ErrConflict, "artifact is not deleted; purge requires a tombstone")
	}
	if lc.LegalHold {
		return contracts.NewError(contracts.ErrConflict, "artifact is under legal hold and cannot be purged")
	}
	if lc.RetainUntil != nil && lc.RetainUntil.After(time.Now().UTC()) {
		return contracts.NewError(contracts.ErrConflict, "retention floor has not elapsed")
	}
	var c candidate
	err = s.pool.QueryRow(ctx, `
SELECT tenant, deployment, subject, COALESCE(sha256, ''), COALESCE(version, ''),
       COALESCE(size, 0), context_id, task_id
  FROM ahp_artifacts.artifacts WHERE id = $1`, id).
		Scan(&c.tenant, &c.deploy, &c.subject, &c.sha, &c.version,
			&c.size, &c.contextID, &c.taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	if err != nil {
		return storageError("read artifact for purge", err)
	}
	c.id, c.object = id, object
	res, err := s.collectOne(ctx, c, "", GCEventPurge, "explicit_purge", false, s.keep)
	if err != nil {
		return err
	}
	if res.collected {
		return nil
	}
	switch res.refusal {
	case "gone":
		return contracts.NewError(contracts.ErrNotFound, "artifact not found")
	case "legal_hold":
		return contracts.NewError(contracts.ErrConflict, "artifact is under legal hold and cannot be purged")
	case "live_reference":
		return contracts.NewError(contracts.ErrConflict,
			"artifact is pinned by a live context, task or run and cannot be purged")
	default:
		return contracts.NewError(contracts.ErrConflict, "artifact state changed, purge aborted")
	}
}

func (s *Store) listDerivations(ctx context.Context, id string) ([]Derivation, error) {
	rows, err := s.pool.Query(ctx, `
SELECT kind, status, sha256, size, detail
FROM ahp_artifacts.derivations WHERE artifact_id = $1 ORDER BY kind`, id)
	if err != nil {
		return nil, storageError("list derivations", err)
	}
	defer rows.Close()
	var out []Derivation
	for rows.Next() {
		var d Derivation
		if err := rows.Scan(&d.Kind, &d.Status, &d.SHA256, &d.Size, &d.Detail); err != nil {
			return nil, storageError("scan derivation", err)
		}
		out = append(out, d)
	}
	return out, storageError("iterate derivations", rows.Err())
}
