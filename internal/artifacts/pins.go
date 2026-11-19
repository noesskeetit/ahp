package artifacts

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The PDF names four columns as the whole basis of deletion, retention and
// legal hold ("Каждая запись знает tenant, деплой, субъект и контекст - этого
// хватает", с. 33), and it names one more rule everywhere data is removed:
// bytes that something alive still refers to stay put. For a Workspace
// revision it is stated literally - "Ревизия не удаляется, пока её использует
// хотя бы одна рабочая копия" (с. 48) - and for artifacts the same rule is
// implied by the ordering of с. 56: a chat deletion first cancels the run and
// revokes the grant, and only then deletes. The GC therefore needs an answer
// to "is this context or task still alive?" that it does not invent: the
// control plane registers the references it holds, State stays the authority
// on which of them are live, and the sweep refuses everything else.
//
// Two mechanisms, deliberately:
//
//   - gc_pins is the durable registry. A pin is either reference-level ("this
//     context / thread / task / run is alive, so every artifact marked with it
//     is protected") or artifact-level (one specific artifact). It is what a
//     restore-then-pause or a HITL wait looks like from the storage side, and
//     it is checked under a lock, so it cannot be raced past.
//   - Liveness is an optional probe into the authority that actually knows
//     (State). It is a pre-filter, evaluated fail-closed: a probe error marks
//     every candidate it asked about as live, because guessing kills journey
//     artifacts.

// Pin kinds. The set mirrors the names a thread is known by in this platform
// (see DeleteByThread): contexts and threads select on context_id, tasks and
// runs select on task_id.
const (
	PinKindContext = "context"
	PinKindThread  = "thread"
	PinKindTask    = "task"
	PinKindRun     = "run"
)

// PinRefKind is one reference-level pin: a live context, thread, task or run.
type PinRefKind struct {
	Kind  string `json:"kind"`
	RefID string `json:"ref_id"`
}

// Pin is one registered reference. ArtifactID is empty for a reference-level
// pin, which protects every artifact carrying that marker.
type Pin struct {
	Kind       string    `json:"kind"`
	RefID      string    `json:"ref_id"`
	ArtifactID string    `json:"artifact_id,omitempty"`
	Tenant     string    `json:"tenant"`
	CreatedAt  time.Time `json:"created_at"`
}

// ValidPinKind reports whether kind is one of the four reference names the
// cascade matches on. An unbounded kind string would let a writer invent a
// marker the sweep cannot reason about.
func ValidPinKind(kind string) bool {
	switch kind {
	case PinKindContext, PinKindThread, PinKindTask, PinKindRun:
		return true
	}
	return false
}

// ddlGC is the storage-GC schema: the reference registry, the append-only
// audit log of the deletion plane, and the idempotency ledger of sweeps. It is
// idempotent and runs after ddlLifecycle in Migrate.
const ddlGC = `
CREATE TABLE IF NOT EXISTS ahp_artifacts.checkpoint_pins (
    operation_id text NOT NULL,
    tenant text NOT NULL,
    artifact_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    checked_at timestamptz NOT NULL DEFAULT 'epoch',
    PRIMARY KEY (operation_id, artifact_id)
);
ALTER TABLE ahp_artifacts.checkpoint_pins ADD COLUMN IF NOT EXISTS checked_at timestamptz NOT NULL DEFAULT 'epoch';
CREATE INDEX IF NOT EXISTS checkpoint_pins_artifact_idx ON ahp_artifacts.checkpoint_pins(tenant,artifact_id);

CREATE TABLE IF NOT EXISTS ahp_artifacts.gc_pins (
    tenant      text NOT NULL,
    kind        text NOT NULL CHECK (kind IN ('context','thread','task','run')),
    ref_id      text NOT NULL,
    artifact_id text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant, kind, ref_id, artifact_id)
);

-- The deletion-plane audit log. One row per observable decision, append-only:
-- identifiers, digests and sizes only, never names and never content, which is
-- exactly the logging rule the PDF gives for the storage plane ("только
-- идентификаторы и размеры", с. 34).
CREATE TABLE IF NOT EXISTS ahp_artifacts.gc_events (
    id          bigserial PRIMARY KEY,
    at          timestamptz NOT NULL DEFAULT now(),
    sweep_id    text NOT NULL DEFAULT '',
    event       text NOT NULL,
    tenant      text NOT NULL DEFAULT '',
    deployment  text NOT NULL DEFAULT '',
    subject     text NOT NULL DEFAULT '',
    artifact_id text NOT NULL DEFAULT '',
    object      text NOT NULL DEFAULT '',
    version     text NOT NULL DEFAULT '',
    sha256      text NOT NULL DEFAULT '',
    size        bigint NOT NULL DEFAULT 0,
    reason      text NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS gc_events_artifact_idx ON ahp_artifacts.gc_events (artifact_id, id);
CREATE INDEX IF NOT EXISTS gc_events_sweep_idx    ON ahp_artifacts.gc_events (sweep_id, id);
CREATE INDEX IF NOT EXISTS gc_events_tenant_idx   ON ahp_artifacts.gc_events (tenant, id);

-- Sweep ledger: the same idempotency key returns the same report instead of
-- sweeping a second time, which is what makes a lost ack harmless.
CREATE TABLE IF NOT EXISTS ahp_artifacts.gc_runs (
    key         text PRIMARY KEY,
    sweep_id    text NOT NULL,
    tenant      text NOT NULL,
    report      jsonb NOT NULL,
    finished_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS artifacts_gc_tombstone_idx
    ON ahp_artifacts.artifacts (tenant, deleted_at) WHERE deleted;
CREATE INDEX IF NOT EXISTS artifacts_gc_staged_idx
    ON ahp_artifacts.artifacts (tenant, created_at) WHERE NOT deleted AND version IS NULL;
`

// refColumn maps a pin kind to the artifact column its marker lives in.
func refColumn(kind string) string {
	switch kind {
	case PinKindContext, PinKindThread:
		return "context_id"
	default:
		return "task_id"
	}
}

// markerLockKeys are the advisory-lock keys of the markers one artifact row
// carries. A pin transaction takes the same key, which is what makes "the
// reference is live" and "the sweep collected it" mutually exclusive instead
// of a race that whoever loses simply loses bytes in.
func markerLockKeys(tenant, contextID, taskID string) []string {
	var out []string
	if contextID != "" {
		out = append(out, tenant+"|"+PinKindContext+"|"+contextID, tenant+"|"+PinKindThread+"|"+contextID)
	}
	if taskID != "" {
		out = append(out, tenant+"|"+PinKindTask+"|"+taskID, tenant+"|"+PinKindRun+"|"+taskID)
	}
	return out
}

// lockMarkers takes the transaction-scoped advisory locks of a marker set, in
// a stable order so two sweeps collecting overlapping marker sets cannot
// deadlock on each other.
func lockMarkers(ctx context.Context, tx pgx.Tx, keys []string) error {
	ordered := append([]string(nil), keys...)
	sortStrings(ordered)
	for _, k := range ordered {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, k); err != nil {
			return storageError("lock marker", err)
		}
	}
	return nil
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// pinExists reports whether any registered pin protects this row: an
// artifact-level pin on its id, or a reference-level pin whose marker the row
// carries. contextID/taskID come from the locked row, so the answer describes
// exactly the bytes about to be removed.
func pinExists(ctx context.Context, tx pgx.Tx, tenant, artifactID, contextID, taskID string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM ahp_artifacts.gc_pins p
     WHERE p.tenant = $1 AND (
         p.artifact_id = $2
         OR (p.artifact_id = '' AND (
               (p.kind IN ('context','thread') AND $3 <> '' AND p.ref_id = $3)
            OR (p.kind IN ('task','run')      AND $4 <> '' AND p.ref_id = $4)))))
 OR EXISTS (SELECT 1 FROM ahp_artifacts.checkpoint_pins WHERE tenant=$1 AND artifact_id=$2)`,
		tenant, artifactID, contextID, taskID).Scan(&exists)
	if err != nil {
		return false, storageError("check pins", err)
	}
	return exists, nil
}

// PinRef registers a live reference: every artifact of the caller's tenant
// marked with this context/thread/task/run becomes untouchable by the GC until
// the pin is released. A pin taken while a sweep is mid-collection is refused
// rather than silently missing the bytes it means to protect.
func (s *Store) PinRef(ctx context.Context, scope contracts.Scope, kind, refID string) error {
	return s.pin(ctx, scope, kind, refID, "")
}

// PinArtifact registers a reference against one specific artifact.
func (s *Store) PinArtifact(ctx context.Context, scope contracts.Scope, artifactID, kind, refID string) error {
	if err := validateID(artifactID); err != nil {
		return err
	}
	return s.pin(ctx, scope, kind, refID, artifactID)
}

func (s *Store) pin(ctx context.Context, scope contracts.Scope, kind, refID, artifactID string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if !ValidPinKind(kind) {
		return contracts.NewError(contracts.ErrInvalid,
			"artifacts: pin kind must be context, thread, task or run")
	}
	if err := requireBindingValue("pin reference", refID); err != nil {
		return err
	}
	if err := s.authorize(ctx, scope, "pin", "artifacts:pins"); err != nil {
		return err
	}
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			scope.Tenant+"|"+kind+"|"+refID); err != nil {
			return storageError("lock pin reference", err)
		}
		if artifactID != "" {
			// An artifact-scoped pin names ONE object, so that object has to belong
			// to the caller's tenant: the registry and the gc_events trail it writes
			// are what retention decisions are audited through, and a row naming an
			// artifact the caller has no rights to makes that trail assert something
			// false. Cross-tenant access answers "artifact not found" everywhere else
			// in this plane (restore.go, access.go) and does the same here, so
			// pinning is not an existence oracle either. A reference-level pin carries
			// no artifact id and stays unconditional on purpose: it is taken while a
			// conversation has no artifacts yet, which is the whole point of it, and
			// the sweep only ever consults pins under the swept tenant.
			var one int
			if err := tx.QueryRow(ctx,
				`SELECT 1 FROM ahp_artifacts.artifacts WHERE tenant = $1 AND id = $2`,
				scope.Tenant, artifactID).Scan(&one); errors.Is(err, pgx.ErrNoRows) {
				return contracts.NewError(contracts.ErrNotFound, "artifact not found")
			} else if err != nil {
				return storageError("check artifact for pin", err)
			}
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO ahp_artifacts.gc_pins (tenant, kind, ref_id, artifact_id)
VALUES ($1, $2, $3, $4)
ON CONFLICT DO NOTHING`,
			scope.Tenant, kind, refID, artifactID); err != nil {
			return storageError("insert pin", err)
		}
		return s.logGCEventTx(ctx, tx, GCEvent{
			Event: GCEventPin, Tenant: scope.Tenant, Deployment: scope.Deployment,
			Subject: scope.Subject, ArtifactID: artifactID,
			Reason: kind + ":" + refID,
		})
	})
	return err
}

// UnpinRef releases a reference-level pin. Releasing is the control plane
// saying "nothing alive refers to this any more", which is what makes the
// artifacts it marked collectable; retention still decides when.
func (s *Store) UnpinRef(ctx context.Context, scope contracts.Scope, kind, refID string) error {
	return s.unpin(ctx, scope, kind, refID, "")
}

// UnpinArtifact releases one artifact-level pin.
func (s *Store) UnpinArtifact(ctx context.Context, scope contracts.Scope, artifactID, kind, refID string) error {
	if err := validateID(artifactID); err != nil {
		return err
	}
	return s.unpin(ctx, scope, kind, refID, artifactID)
}

func (s *Store) unpin(ctx context.Context, scope contracts.Scope, kind, refID, artifactID string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if !ValidPinKind(kind) {
		return contracts.NewError(contracts.ErrInvalid,
			"artifacts: pin kind must be context, thread, task or run")
	}
	if err := requireBindingValue("pin reference", refID); err != nil {
		return err
	}
	if err := s.authorize(ctx, scope, "pin", "artifacts:pins"); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			scope.Tenant+"|"+kind+"|"+refID); err != nil {
			return storageError("lock pin reference", err)
		}
		if _, err := tx.Exec(ctx, `
DELETE FROM ahp_artifacts.gc_pins
 WHERE tenant = $1 AND kind = $2 AND ref_id = $3 AND artifact_id = $4`,
			scope.Tenant, kind, refID, artifactID); err != nil {
			return storageError("delete pin", err)
		}
		return s.logGCEventTx(ctx, tx, GCEvent{
			Event: GCEventUnpin, Tenant: scope.Tenant, Deployment: scope.Deployment,
			Subject: scope.Subject, ArtifactID: artifactID,
			Reason: kind + ":" + refID,
		})
	})
}

// ListPins returns the pins of the caller's tenant, optionally narrowed to one
// reference. The control plane uses it to see what it still holds alive; the
// GC uses the same rows, so the answer and the enforcement cannot disagree.
func (s *Store) ListPins(ctx context.Context, scope contracts.Scope, kind, refID string) ([]Pin, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if kind != "" && !ValidPinKind(kind) {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"artifacts: pin kind must be context, thread, task or run")
	}
	if err := s.authorize(ctx, scope, "read", "artifacts:pins"); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
SELECT kind, ref_id, artifact_id, created_at FROM ahp_artifacts.gc_pins
 WHERE tenant = $1 AND ($2 = '' OR kind = $2) AND ($3 = '' OR ref_id = $3)
 ORDER BY created_at, kind, ref_id, artifact_id`,
		scope.Tenant, kind, refID)
	if err != nil {
		return nil, storageError("list pins", err)
	}
	defer rows.Close()
	var out []Pin
	for rows.Next() {
		var p Pin
		if err := rows.Scan(&p.Kind, &p.RefID, &p.ArtifactID, &p.CreatedAt); err != nil {
			return nil, storageError("scan pin", err)
		}
		p.Tenant = scope.Tenant
		out = append(out, p)
	}
	return out, storageError("iterate pins", rows.Err())
}
