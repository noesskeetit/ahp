package artifacts

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Storage garbage collection.
//
// What the PDF gives this plane is narrow and strict at once. It owns
// "retention и удаление по чату, сотруднику и tenant" (с. 48); deletion is a
// cascade over tenant/deployment/subject/context with legal hold named in the
// same sentence (с. 33); a sealed artifact is one exact immutable version that
// outlives the VM it came from ("скачивания независимо от жизни VM", с. 47,
// "Seal фиксирует версию, размер, digest и метки", с. 48); bytes something
// alive still uses are not deleted ("Ревизия не удаляется, пока её использует
// хотя бы одна рабочая копия", с. 48); deletion during a live turn is ordered
// cancel-then-delete, never delete-and-hope (с. 56); and the plane is audited
// with identifiers and sizes only ("только идентификаторы и размеры", с. 34).
//
// So the sweep is specified by what it refuses:
//
//	sealed   - a row with version AND sha256 recorded is a sha-record. While it
//	           is not tombstoned no age at all makes it garbage, and its pinned
//	           object version is never a candidate even among orphan versions.
//	live     - a row whose context/thread/task/run marker is pinned, or that the
//	           configured Liveness authority reports alive (or fails to answer
//	           about), is never collected - not even tombstoned with retention
//	           elapsed. This is the "ссылки на живые задачи/контексты" rule.
//	held     - a legal hold stops collection exactly as it stops a cascade.
//	young    - a staged upload younger than the staged grace, or an object
//	           version younger than the orphan grace, stays: an in-flight
//	           journey is indistinguishable from an abandoned one until it is
//	           old enough to be one.
//
// What it collects: tombstones past their retention floor, uploads that were
// never sealed and carry no live marker, and object versions no row of this
// tenant refers to any more. Every refusal is counted and reported, because a
// sweep that reports only what it deleted proves nothing about what it kept.
//
// Collection order per row is deliberate: the metadata row goes first, inside
// one transaction that re-evaluated every refusal against the locked row, and
// the object bytes only afterwards, when no row can name them. A crash between
// the two leaves ordinary orphan bytes for the orphan phase, never a row whose
// bytes vanished.

// Audit event kinds of the deletion plane.
const (
	GCEventTombstone      = "tombstone"
	GCEventRestore        = "restore"
	GCEventPurge          = "purge"
	GCEventStaged         = "staged_collected"
	GCEventObjectsRemoved = "objects_removed"
	GCEventObjectsFailed  = "objects_failed"
	GCEventVersionRemoved = "version_removed"
	GCEventPin            = "pin"
	GCEventUnpin          = "unpin"
)

// GCEvent is one append-only audit record: identifiers, digests and sizes,
// never a filename and never content.
type GCEvent struct {
	At         time.Time `json:"at"`
	SweepID    string    `json:"sweep_id,omitempty"`
	Event      string    `json:"event"`
	Tenant     string    `json:"tenant"`
	Deployment string    `json:"deployment,omitempty"`
	Subject    string    `json:"subject,omitempty"`
	ArtifactID string    `json:"artifact_id,omitempty"`
	Object     string    `json:"object,omitempty"`
	Version    string    `json:"version,omitempty"`
	SHA256     string    `json:"sha256,omitempty"`
	Size       int64     `json:"size,omitempty"`
	Reason     string    `json:"reason,omitempty"`
}

// GCAuditFilter narrows a read of the audit trail.
type GCAuditFilter struct {
	Event      string `json:"event,omitempty"`
	ArtifactID string `json:"artifact_id,omitempty"`
	SweepID    string `json:"sweep_id,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

// Ref is one marker the Liveness authority is asked about.
type Ref struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Liveness asks the service that actually knows - State, for a context or a
// task - whether references are still alive. It is a pre-filter and it is
// fail-closed: an error, a missing answer or a partial answer all mean alive.
type Liveness interface {
	RefsAlive(ctx context.Context, tenant string, refs []Ref) (map[Ref]bool, error)
}

// LivenessFunc adapts a function to Liveness.
type LivenessFunc func(ctx context.Context, tenant string, refs []Ref) (map[Ref]bool, error)

// RefsAlive implements Liveness.
func (f LivenessFunc) RefsAlive(ctx context.Context, tenant string, refs []Ref) (map[Ref]bool, error) {
	return f(ctx, tenant, refs)
}

// GCOption configures a GC.
type GCOption func(*GC)

// WithLiveness points the sweep at the authority that decides whether a context
// or task is alive. Without it only registered pins protect a marker, and a
// staged upload carrying any marker at all is left alone: with no authority to
// ask, "nobody refers to this" is not something this package may assert.
func WithLiveness(l Liveness) GCOption {
	return func(g *GC) { g.liveness = l }
}

// ArtifactQuery is one candidate the reference scanner is asked about: its row
// id and the digest of the bytes it sealed (empty while still staged).
type ArtifactQuery struct {
	ID     string
	Digest string
}

// ReferenceScan answers a question Liveness cannot: Liveness is asked about
// markers (a context, a task), while a durable row in another plane can name
// one artifact directly - a message or turn frame that carries the artifact id,
// a workspace file of a revision that is still head or pinned, a memory record
// whose evidence names the artifact or its digest. A row that names an artifact
// is a live reference even when every marker of that artifact has gone away, so
// the sweep asks before it collects.
//
// The answer is a set of the queried ids that are still referenced. Like
// Liveness it is fail-closed: an error marks the whole batch referenced,
// because a storage plane that cannot read its consumers must not delete.
type ReferenceScan interface {
	ArtifactsReferenced(ctx context.Context, tenant string, arts []ArtifactQuery) (map[string]bool, error)
}

// ReferenceScanFunc adapts a function to ReferenceScan.
type ReferenceScanFunc func(ctx context.Context, tenant string, arts []ArtifactQuery) (map[string]bool, error)

// ArtifactsReferenced implements ReferenceScan.
func (f ReferenceScanFunc) ArtifactsReferenced(ctx context.Context, tenant string, arts []ArtifactQuery) (map[string]bool, error) {
	return f(ctx, tenant, arts)
}

// WithReferenceScan points the sweep at the planes that hold artifact content
// by id or digest. Without it only markers and pins protect a row.
func WithReferenceScan(s ReferenceScan) GCOption {
	return func(g *GC) { g.scan = s }
}

// WithJournalRetention walks expired State tombstones before the artifact
// purge. It receives the authenticated tenant and the same retention floor.
func WithJournalRetention(sweep func(context.Context, string, time.Time, int) error) GCOption {
	return func(g *GC) { g.journalRetention = sweep }
}

// GC is a sweep engine over one Store. It is safe for concurrent use: every
// collection is a guarded single-row statement, so two overlapping sweeps
// cannot both take one row, and the loser finds nothing to collect.
type GC struct {
	store            *Store
	liveness         Liveness
	scan             ReferenceScan
	journalRetention func(context.Context, string, time.Time, int) error
}

// NewGC wires a sweep engine to a store.
func NewGC(store *Store, opts ...GCOption) (*GC, error) {
	if store == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "artifacts: gc needs a store")
	}
	g := &GC{store: store, scan: store.keep}
	for _, opt := range opts {
		opt(g)
	}
	return g, nil
}

// GCPolicy bounds one sweep. Every floor here is a refusal to delete, so the
// defaults sit at the conservative end of everything the PDF leaves open: it
// mandates retention and deletion without naming numbers.
type GCPolicy struct {
	// Key makes a sweep idempotent: the same key returns the report of the
	// sweep that already ran instead of sweeping again, which is what makes a
	// lost ack harmless. Empty means "sweep now, nothing ledgered".
	Key string
	// StagedGrace is the minimum age before an abandoned upload is collectable.
	// It starts at seal for a sealed journal-stage artifact. Zero defaults to
	// 2h and a negative value is refused.
	StagedGrace time.Duration
	// TombstoneRetention is the floor applied to a tombstone that carries no
	// explicit retain_until. It defaults to 7 days; an explicit retain_until
	// always wins, because the control plane set it deliberately.
	TombstoneRetention time.Duration
	// OrphanGrace is the minimum age of an object version before it may be
	// removed as unreferenced. Defaults to 24h.
	OrphanGrace time.Duration
	// BatchSize is how many rows one phase reads per pass (default 200).
	BatchSize int
	// MaxRows caps how many rows and versions one sweep may remove in total.
	MaxRows int
	// CollectUnpinnedVersions also removes extra object versions written at a
	// sealed key after its seal. The pinned version of a sealed row is never a
	// candidate either way; this flag only decides the surplus ones.
	CollectUnpinnedVersions bool
	// DryRun evaluates and reports every decision and removes nothing.
	DryRun bool
}

const (
	defaultStagedGrace    = 2 * time.Hour
	defaultOrphanGrace    = 24 * time.Hour
	defaultRetentionFloor = 7 * 24 * time.Hour
	defaultGCBatchSize    = 200
	defaultGCMaxRows      = 10000
	// maxListedObjects bounds one orphan listing so a sweep cannot be made
	// expensive by somebody else's bucket.
	maxListedObjects = 100000
)

func (p *GCPolicy) normalise() error {
	if p.StagedGrace == 0 {
		p.StagedGrace = defaultStagedGrace
	}
	if p.OrphanGrace == 0 {
		p.OrphanGrace = defaultOrphanGrace
	}
	if p.TombstoneRetention == 0 {
		p.TombstoneRetention = defaultRetentionFloor
	}
	if p.BatchSize == 0 {
		p.BatchSize = defaultGCBatchSize
	}
	if p.MaxRows == 0 {
		p.MaxRows = defaultGCMaxRows
	}
	switch {
	case p.StagedGrace < 0:
		return contracts.NewError(contracts.ErrInvalid, "artifacts: staged grace must not be negative")
	case p.OrphanGrace < 0:
		return contracts.NewError(contracts.ErrInvalid, "artifacts: orphan grace must not be negative")
	case p.TombstoneRetention < 0:
		return contracts.NewError(contracts.ErrInvalid, "artifacts: retention floor must not be negative")
	case p.BatchSize < 1 || p.BatchSize > 1000:
		return contracts.NewError(contracts.ErrInvalid, "artifacts: batch size must be 1..1000")
	case p.MaxRows < 1:
		return contracts.NewError(contracts.ErrInvalid, "artifacts: max rows must be positive")
	case len(p.Key) > 128:
		return contracts.NewError(contracts.ErrInvalid, "artifacts: sweep key must be at most 128 bytes")
	}
	return nil
}

// GCReport is the outcome of one sweep. The refusal counters are half the
// point: they are the evidence that a journey's artifacts were left alone.
type GCReport struct {
	SweepID    string    `json:"sweep_id"`
	Key        string    `json:"key,omitempty"`
	Tenant     string    `json:"tenant"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Replayed   bool      `json:"replayed,omitempty"`
	DryRun     bool      `json:"dry_run,omitempty"`

	ScannedRows      int `json:"scanned_rows"`
	PurgedTombstoned int `json:"purged_tombstoned"`
	CollectedStaged  int `json:"collected_staged"`
	// VersionsRemoved counts object versions this sweep deleted, including the
	// surplus ones of a key that still exists.
	VersionsRemoved   int `json:"versions_removed"`
	ProtectedVersions int `json:"protected_sealed_versions"`

	SkippedSealed     int `json:"skipped_sealed"`
	SkippedLive       int `json:"skipped_live_reference"`
	SkippedHold       int `json:"skipped_legal_hold"`
	SkippedRetention  int `json:"skipped_retention"`
	SkippedConcurrent int `json:"skipped_concurrent_change"`
	SkippedYoung      int `json:"skipped_too_young"`
	Errors            int `json:"errors"`
}

// RunOnce performs one sweep of the caller's tenant and no other. The tenant
// comes from the verified scope, never from the policy: the blast radius of a
// garbage collector is a security property, and "another tenant's objects are
// untouched" has to hold by construction rather than by configuration.
func (g *GC) RunOnce(ctx context.Context, scope contracts.Scope, policy GCPolicy) (GCReport, error) {
	if err := scope.Validate(); err != nil {
		return GCReport{}, err
	}
	if err := policy.normalise(); err != nil {
		return GCReport{}, err
	}
	if err := g.store.authorize(ctx, scope, "gc", "artifacts:gc"); err != nil {
		return GCReport{}, err
	}
	if !policy.DryRun {
		if err := g.store.ReconcileCheckpointPins(ctx, scope.Tenant); err != nil {
			return GCReport{}, err
		}
	}
	if policy.Key != "" {
		cached, found, err := g.lookupRun(ctx, scope.Tenant, policy.Key)
		if err != nil {
			return GCReport{}, err
		}
		if found {
			return cached, nil
		}
	}

	now := time.Now().UTC()
	rep := GCReport{
		SweepID:   contracts.NewID(),
		Key:       policy.Key,
		Tenant:    scope.Tenant,
		StartedAt: now,
		DryRun:    policy.DryRun,
	}
	floor := now.Add(-policy.TombstoneRetention)
	stagedBefore := now.Add(-policy.StagedGrace)

	if g.journalRetention != nil && !policy.DryRun {
		if err := g.journalRetention(ctx, scope.Tenant, floor, policy.BatchSize); err != nil {
			return rep, err
		}
	}

	// Order matters: tombstones first (the control plane already decided about
	// those), then abandoned uploads, then the object-storage leftovers of
	// both - so a row collected in phase one has its metadata gone before
	// phase three decides what counts as unreferenced.
	if err := g.purgeTombstoned(ctx, scope, policy, floor, &rep); err != nil {
		return rep, err
	}
	if err := g.collectStaged(ctx, scope, policy, stagedBefore, &rep); err != nil {
		return rep, err
	}
	if err := g.collectOrphanVersions(ctx, scope, policy, &rep); err != nil {
		return rep, err
	}
	rep.FinishedAt = time.Now().UTC()

	if policy.Key != "" && !policy.DryRun {
		if err := g.recordRun(ctx, scope.Tenant, policy.Key, rep); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// candidate is one row a phase proposed for collection, before its own
// transaction re-evaluates every predicate against the locked row.
type candidate struct {
	id          string
	object      string
	tenant      string
	deploy      string
	subject     string
	version     string
	sha         string
	size        int64
	contextID   string
	taskID      string
	staged      bool
	sealedStage bool
}

func (c candidate) markers() []Ref {
	var out []Ref
	if c.contextID != "" {
		out = append(out, Ref{Kind: PinKindContext, ID: c.contextID})
	}
	if c.taskID != "" {
		out = append(out, Ref{Kind: PinKindTask, ID: c.taskID})
	}
	return out
}

// collected reports whether one candidate left the table.
type collectOutcome struct {
	collected bool
	refusal   string
}

// collectOne decides one row end to end: lock its markers and the row, re-read
// every refusal from the locked row, delete the metadata row and its audit
// record in the same transaction, then remove the object versions.
//
// The marker locks are what makes a concurrent pin a defined outcome instead
// of lost bytes: a pin transaction takes the same advisory key, so either the
// pin is visible here (the row stays) or the pin transaction runs after this
// one committed, at which point the row it meant to protect is already gone and
// the pin is refused rather than granted over removed bytes.
func (s *Store) collectOne(ctx context.Context, c candidate, sweepID, event, reason string, dry bool, scan ReferenceScan) (collectOutcome, error) {
	var out collectOutcome
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockMarkers(ctx, tx, []string{artifactLockKey(c.tenant, c.id)}); err != nil {
			return err
		}
		if err := lockMarkers(ctx, tx, markerLockKeys(c.tenant, c.contextID, c.taskID)); err != nil {
			return err
		}
		var (
			deleted, hold, sealed, journalStage bool
			object, contextID                   string
			taskID                              string
			version                             string
		)
		err := tx.QueryRow(ctx, `
SELECT deleted, legal_hold, version IS NOT NULL AND sha256 IS NOT NULL,
       object, context_id, task_id, COALESCE(version, ''), journal_stage
  FROM ahp_artifacts.artifacts WHERE id = $1 FOR UPDATE`, c.id).
			Scan(&deleted, &hold, &sealed, &object, &contextID, &taskID, &version, &journalStage)
		if errors.Is(err, pgx.ErrNoRows) {
			out.refusal = "gone"
			return nil
		}
		if err != nil {
			return storageError("lock candidate", err)
		}
		// The row can have moved since the phase read it. Each of these is a
		// reason to leave it exactly where it is.
		switch {
		case c.staged && deleted:
			out.refusal = "deleted"
			return nil
		case !c.staged && !deleted:
			out.refusal = "restored"
			return nil
		case c.staged && sealed && (!c.sealedStage || !journalStage || c.version != version || scan == nil):
			out.refusal = "sealed"
			return nil
		case hold:
			out.refusal = "legal_hold"
			return nil
		}
		pinned, err := pinExists(ctx, tx, c.tenant, c.id, contextID, taskID)
		if err != nil {
			return err
		}
		if pinned {
			out.refusal = "live_reference"
			return nil
		}
		// The batch scan may precede a newly committed State reference. Under
		// the same lock used by provisional pins, consult the owner again.
		if scan != nil {
			current, err := scan.ArtifactsReferenced(ctx, c.tenant, []ArtifactQuery{{ID: c.id, Digest: c.sha}})
			if err != nil {
				return err
			}
			if current[c.id] {
				out.refusal = "live_reference"
				return nil
			}
		}
		if dry {
			out.refusal = "dry_run"
			return nil
		}
		tag, err := tx.Exec(ctx, `DELETE FROM ahp_artifacts.artifacts WHERE id = $1`, c.id)
		if err != nil {
			return storageError("collect row", err)
		}
		if tag.RowsAffected() != 1 {
			out.refusal = "changed"
			return nil
		}
		// Derived cache follows the source retention lifecycle. Shared digests
		// remain only while another retained artifact still references them.
		if _, err := tx.Exec(ctx, `DELETE FROM ahp_artifacts.extraction_cache c WHERE c.tenant=$1 AND c.sha256=$2 AND NOT EXISTS(SELECT 1 FROM ahp_artifacts.artifacts a WHERE a.tenant=c.tenant AND a.sha256=c.sha256 AND a.mime=c.mime)`, c.tenant, c.sha); err != nil {
			return storageError("collect extraction cache", err)
		}
		out.collected = true
		out.refusal = ""
		return s.logGCEventTx(ctx, tx, GCEvent{
			SweepID: sweepID, Event: event, Tenant: c.tenant, Deployment: c.deploy,
			Subject: c.subject, ArtifactID: c.id, Object: object,
			Version: version, SHA256: c.sha, Size: c.size, Reason: reason,
		})
	})
	if err != nil || !out.collected {
		return out, err
	}
	// Every version of exactly this key. The row is gone and its removal was
	// atomic, so no reader can be handed a version this sweep is about to
	// remove.
	if err := s.removeObjectVersions(ctx, c.object); err != nil {
		_ = s.logGCEvent(context.WithoutCancel(ctx), GCEvent{
			SweepID: sweepID, Event: GCEventObjectsFailed, Tenant: c.tenant,
			Deployment: c.deploy, Subject: c.subject, ArtifactID: c.id,
			Object: c.object, Version: c.version, SHA256: c.sha, Size: c.size,
			Reason: err.Error(),
		})
		return out, err
	}
	_ = s.logGCEvent(context.WithoutCancel(ctx), GCEvent{
		SweepID: sweepID, Event: GCEventObjectsRemoved, Tenant: c.tenant,
		Deployment: c.deploy, Subject: c.subject, ArtifactID: c.id,
		Object: c.object, Version: c.version, SHA256: c.sha, Size: c.size, Reason: reason,
	})
	return out, nil
}

// removeObjectVersions deletes every stored version of exactly one key, which
// is what keeps one tenant's sweep inside one tenant's prefix.
func (s *Store) removeObjectVersions(ctx context.Context, object string) error {
	var versions []string
	for obj := range s.client.ListObjects(ctx, s.bucket,
		minio.ListObjectsOptions{Prefix: object, Recursive: true, WithVersions: true}) {
		if obj.Err != nil {
			return storageError("list object versions", obj.Err)
		}
		if obj.Key != object || obj.VersionID == "" {
			continue
		}
		versions = append(versions, obj.VersionID)
	}
	for _, v := range versions {
		if err := s.client.RemoveObject(ctx, s.bucket, object,
			minio.RemoveObjectOptions{VersionID: v}); err != nil {
			return storageError("remove object version", err)
		}
	}
	return nil
}

// eligibleSQL is the retention rule, stated once: an explicit retain_until
// wins, because the control plane set it deliberately; otherwise the tombstone
// itself has to be older than the sweep's floor.
const eligibleSQL = `
       (retain_until IS NOT NULL AND retain_until <= $1)
        OR (retain_until IS NULL AND deleted_at <= $2) AS eligible`

// purgeTombstoned collects rows the control plane already tombstoned and whose
// retention floor has elapsed. Rows are walked with a keyset cursor, so a sweep
// is bounded and cannot spin on rows it decides to keep.
func (g *GC) purgeTombstoned(ctx context.Context, scope contracts.Scope, p GCPolicy,
	floor time.Time, rep *GCReport) error {
	var (
		cursorAt time.Time
		cursorID string
	)
	for {
		rows, err := g.store.pool.Query(ctx, `
SELECT id, deleted_at, object, tenant, deployment, subject,
       COALESCE(sha256, ''), COALESCE(version, ''), COALESCE(size, 0),
       context_id, task_id, legal_hold,`+eligibleSQL+`
  FROM ahp_artifacts.artifacts
 WHERE tenant = $3 AND deleted AND (deleted_at, id) > ($4, $5)
 ORDER BY deleted_at, id
 LIMIT $6`,
			time.Now().UTC(), floor, scope.Tenant, cursorAt, cursorID, p.BatchSize)
		if err != nil {
			return storageError("scan tombstones", err)
		}
		var cands []candidate
		n := 0
		for rows.Next() {
			var (
				c          candidate
				deleted    time.Time
				hold, elig bool
			)
			if err := rows.Scan(&c.id, &deleted, &c.object, &c.tenant, &c.deploy, &c.subject,
				&c.sha, &c.version, &c.size, &c.contextID, &c.taskID, &hold, &elig); err != nil {
				rows.Close()
				return storageError("scan tombstone", err)
			}
			n++
			cursorAt, cursorID = deleted, c.id
			rep.ScannedRows++
			switch {
			case hold:
				rep.SkippedHold++
				continue
			case !elig:
				rep.SkippedRetention++
				continue
			}
			cands = append(cands, c)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return storageError("iterate tombstones", err)
		}
		rows.Close()

		live, err := g.liveMarkers(ctx, scope.Tenant, cands, false)
		if err != nil {
			return err
		}
		for i, c := range cands {
			if rep.PurgedTombstoned+rep.CollectedStaged >= p.MaxRows {
				return nil
			}
			if live[i] {
				rep.SkippedLive++
				continue
			}
			res, err := g.store.collectOne(ctx, c, rep.SweepID, GCEventPurge, "retention_elapsed", p.DryRun, g.scan)
			if err != nil {
				rep.Errors++
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				continue
			}
			if res.collected {
				rep.PurgedTombstoned++
				rep.VersionsRemoved++
				continue
			}
			rep.SkippedConcurrent++
		}
		if n < p.BatchSize {
			return nil
		}
	}
}

// collectStaged collects abandoned unsealed uploads and sealed journal stages
// whose writer and references are absent after the grace interval.
func (g *GC) collectStaged(ctx context.Context, scope contracts.Scope, p GCPolicy,
	stagedBefore time.Time, rep *GCReport) error {
	var (
		cursorAt time.Time
		cursorID string
	)
	for {
		rows, err := g.store.pool.Query(ctx, `
SELECT id, object, tenant, deployment, subject, expected_sha, context_id, task_id,
       legal_hold, COALESCE(version, ''), expected_size, COALESCE(sealed_at, created_at)
  FROM ahp_artifacts.artifacts
 WHERE tenant = $1 AND NOT deleted AND (version IS NULL OR (journal_stage AND $6))
   AND COALESCE(sealed_at, created_at) < $2
   AND (COALESCE(sealed_at, created_at), id) > ($3, $4)
 ORDER BY COALESCE(sealed_at, created_at), id
 LIMIT $5`, scope.Tenant, stagedBefore, cursorAt, cursorID, p.BatchSize, g.scan != nil)
		if err != nil {
			return storageError("scan staged", err)
		}
		var cands []candidate
		n := 0
		for rows.Next() {
			var c candidate
			var hold bool
			var created time.Time
			if err := rows.Scan(&c.id, &c.object, &c.tenant, &c.deploy, &c.subject,
				&c.sha, &c.contextID, &c.taskID, &hold, &c.version, &c.size, &created); err != nil {
				rows.Close()
				return storageError("scan staged row", err)
			}
			c.staged = true
			c.sealedStage = c.version != ""
			n++
			rep.ScannedRows++
			cursorAt, cursorID = created, c.id
			if hold {
				rep.SkippedHold++
				continue
			}
			cands = append(cands, c)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return storageError("iterate staged", err)
		}
		rows.Close()

		live, err := g.liveMarkers(ctx, scope.Tenant, cands, true)
		if err != nil {
			return err
		}
		for i, c := range cands {
			if rep.PurgedTombstoned+rep.CollectedStaged >= p.MaxRows {
				return nil
			}
			if live[i] {
				rep.SkippedLive++
				continue
			}
			reason := "upload_never_sealed"
			if c.sealedStage {
				reason = "journal_stage_unreferenced"
			}
			res, err := g.store.collectOne(ctx, c, rep.SweepID, GCEventStaged, reason, p.DryRun, g.scan)
			if err != nil {
				rep.Errors++
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				continue
			}
			if res.collected {
				rep.CollectedStaged++
				rep.VersionsRemoved++
				continue
			}
			rep.SkippedConcurrent++
		}
		if n < p.BatchSize {
			return nil
		}
	}
}

// liveMarkers asks both authorities about the markers of a batch and answers,
// per candidate index, which rows must be left alone.
//
// The pins always apply. The Liveness probe is asked when it is configured, and
// it fails closed: an error, a missing answer or a partial answer all mean
// alive. When no authority is configured the answer depends on what the row is.
//
// A staged, never-sealed upload carrying a context or task marker stays: nothing
// has decided about it yet, its bytes cannot be served from, and with nobody to
// ask, "no run is still writing this" is a claim this package may not make. A
// tombstoned row is different - the tombstone is itself the control plane's
// decision, and the cascade that wrote it had the authority available by
// definition - so there the pins are the protection. That asymmetry is what
// keeps a journey's in-flight uploads safe on a deployment that has not wired
// State into the collector, without letting undeleted tombstones accumulate
// forever.
func (g *GC) liveMarkers(ctx context.Context, tenant string, cands []candidate, unsealed bool) (map[int]bool, error) {
	live := make(map[int]bool, len(cands))
	if len(cands) == 0 {
		return live, nil
	}
	pinnedRefs, pinnedArtifacts, err := g.store.pinnedFor(ctx, tenant, cands)
	if err != nil {
		return nil, err
	}
	asked := make(map[Ref]struct{})
	for i, c := range cands {
		for _, m := range c.markers() {
			asked[m] = struct{}{}
			if pinnedRefs[m] {
				live[i] = true
			}
		}
		if pinnedArtifacts[c.id] {
			live[i] = true
		}
	}
	// A durable row in another plane that names this artifact (a turn frame, a
	// head revision's file, a memory's evidence) is a live reference even when
	// every marker of the row is gone. Fail-closed like the marker probe: an
	// unreadable consumer set protects the whole batch.
	if g.scan != nil {
		asked := make([]ArtifactQuery, 0, len(cands))
		for _, c := range cands {
			asked = append(asked, ArtifactQuery{ID: c.id, Digest: c.sha})
		}
		referenced, err := g.scan.ArtifactsReferenced(ctx, tenant, asked)
		if err != nil {
			for i := range cands {
				live[i] = true
			}
			return live, nil
		}
		for i, c := range cands {
			if referenced[c.id] {
				live[i] = true
			}
		}
	}
	if g.liveness == nil {
		if unsealed {
			for i, c := range cands {
				if len(c.markers()) > 0 {
					live[i] = true
				}
			}
		}
		return live, nil
	}
	if len(asked) == 0 {
		return live, nil
	}
	refs := make([]Ref, 0, len(asked))
	for r := range asked {
		refs = append(refs, r)
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Kind == refs[j].Kind {
			return refs[i].ID < refs[j].ID
		}
		return refs[i].Kind < refs[j].Kind
	})
	answer, err := g.liveness.RefsAlive(ctx, tenant, refs)
	if err != nil {
		for i, c := range cands {
			if len(c.markers()) > 0 {
				live[i] = true
			}
		}
		return live, nil
	}
	for i, c := range cands {
		for _, m := range c.markers() {
			alive, known := answer[m]
			if !known || alive {
				live[i] = true
			}
		}
	}
	return live, nil
}

// pinnedFor reads the durable registry for a batch of candidates: which
// reference markers are pinned, and which specific artifacts are.
func (s *Store) pinnedFor(ctx context.Context, tenant string, cands []candidate) (map[Ref]bool, map[string]bool, error) {
	byRef := map[Ref]bool{}
	byArtifact := map[string]bool{}
	refs := map[Ref]struct{}{}
	ids := make([]string, 0, len(cands))
	for _, c := range cands {
		ids = append(ids, c.id)
		if c.contextID != "" {
			refs[Ref{Kind: PinKindContext, ID: c.contextID}] = struct{}{}
			refs[Ref{Kind: PinKindThread, ID: c.contextID}] = struct{}{}
		}
		if c.taskID != "" {
			refs[Ref{Kind: PinKindTask, ID: c.taskID}] = struct{}{}
			refs[Ref{Kind: PinKindRun, ID: c.taskID}] = struct{}{}
		}
	}
	if len(refs) > 0 {
		kinds := make([]string, 0, len(refs))
		values := make([]string, 0, len(refs))
		for r := range refs {
			kinds = append(kinds, r.Kind)
			values = append(values, r.ID)
		}
		rows, err := s.pool.Query(ctx, `
SELECT kind, ref_id FROM ahp_artifacts.gc_pins
 WHERE tenant = $1 AND artifact_id = ''
   AND (kind, ref_id) IN (SELECT k, v FROM unnest($2::text[], $3::text[]) AS t(k, v))`,
			tenant, kinds, values)
		if err != nil {
			return nil, nil, storageError("read pins", err)
		}
		for rows.Next() {
			var r Ref
			if err := rows.Scan(&r.Kind, &r.ID); err != nil {
				rows.Close()
				return nil, nil, storageError("scan pin", err)
			}
			byRef[r] = true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, nil, storageError("iterate pins", err)
		}
		rows.Close()
	}
	if len(ids) > 0 {
		rows, err := s.pool.Query(ctx,
			`SELECT artifact_id FROM ahp_artifacts.gc_pins WHERE tenant = $1 AND artifact_id <> '' AND artifact_id = ANY($2)`,
			tenant, ids)
		if err != nil {
			return nil, nil, storageError("read artifact pins", err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, nil, storageError("scan artifact pin", err)
			}
			byArtifact[id] = true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, nil, storageError("iterate artifact pins", err)
		}
		rows.Close()
	}
	return byRef, byArtifact, nil
}

// listedVersion is one object-storage version under one tenant prefix, as the
// orphan listing saw it.
type listedVersion struct {
	key      string
	id       string
	size     int64
	modified time.Time
	marker   bool
}

// objectRef is what one row says about one object key: whether it still exists
// at all, and which version a sealed row pins.
type objectRef struct {
	pinnedVersion string
	sealed        bool
}

// collectOrphanVersions removes object versions no row of this tenant refers to
// any more: the bytes an abandoned upload left, the versions of a row that was
// purged by an earlier pass, and - when the policy asks - the surplus versions
// written at a sealed key after its seal.
//
// Two invariants hold regardless of policy: a version whose key no row has is
// only removed once it is older than the orphan grace, and the version a sealed
// row pins is never a candidate, so the sha-record a sealed artifact promises
// to serve forever cannot be collected out from under it.
func (g *GC) collectOrphanVersions(ctx context.Context, scope contracts.Scope, p GCPolicy, rep *GCReport) error {
	prefix := scope.Tenant + "/"
	var (
		all     []listedVersion
		listErr error
		count   int
	)
	for obj := range g.store.client.ListObjects(ctx, g.store.bucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true, WithVersions: true}) {
		if obj.Err != nil {
			listErr = storageError("list bucket versions", obj.Err)
			break
		}
		// Two structural guards: the key must sit under this tenant's prefix,
		// and it must be a single path segment below it, which is the shape
		// BeginUpload writes. Anything else belongs to somebody else's layout.
		if !strings.HasPrefix(obj.Key, prefix) ||
			strings.Contains(strings.TrimPrefix(obj.Key, prefix), "/") {
			continue
		}
		count++
		if count > maxListedObjects {
			break
		}
		if obj.VersionID == "" {
			continue
		}
		all = append(all, listedVersion{key: obj.Key, id: obj.VersionID,
			size: obj.Size, modified: obj.LastModified, marker: obj.IsDeleteMarker})
	}
	if listErr != nil {
		return listErr
	}

	keys := make([]string, 0, len(all))
	seen := make(map[string]struct{}, len(all))
	for _, v := range all {
		if _, dup := seen[v.key]; dup {
			continue
		}
		seen[v.key] = struct{}{}
		keys = append(keys, v.key)
	}
	refs, err := g.store.objectRefs(ctx, scope.Tenant, keys)
	if err != nil {
		return err
	}
	cutoff := time.Now().UTC().Add(-p.OrphanGrace)
	for _, v := range all {
		if rep.VersionsRemoved >= p.MaxRows {
			return nil
		}
		ref, referenced := refs[v.key]
		switch {
		case !referenced:
			// No row of this tenant names this key: nothing can be served from
			// it, and nothing can start to, because a PUT path only exists once
			// BeginUpload has committed its row. Age still gates the removal, so
			// a sweep cannot outrun an upload in flight.
			if v.modified.After(cutoff) {
				rep.SkippedYoung++
				continue
			}
		case ref.sealed && v.id == ref.pinnedVersion:
			// The version a sha-record pins. Never, under any policy.
			rep.ProtectedVersions++
			continue
		case ref.sealed:
			if !p.CollectUnpinnedVersions || v.modified.After(cutoff) {
				rep.SkippedSealed++
				continue
			}
		default:
			// A staged row that still exists owns every version of its key: its
			// PUT may not have landed yet, and the staged phase is the one that
			// decides whether that upload is dead.
			rep.SkippedLive++
			continue
		}
		if p.DryRun {
			continue
		}
		if err := g.store.client.RemoveObject(ctx, g.store.bucket, v.key,
			minio.RemoveObjectOptions{VersionID: v.id}); err != nil {
			rep.Errors++
			continue
		}
		rep.VersionsRemoved++
		_ = g.store.logGCEvent(context.WithoutCancel(ctx), GCEvent{
			SweepID: rep.SweepID, Event: GCEventVersionRemoved, Tenant: scope.Tenant,
			Deployment: scope.Deployment, Subject: scope.Subject, Object: v.key,
			Version: v.id, Size: v.size, Reason: orphanReason(referenced, ref),
		})
	}
	return nil
}

func orphanReason(referenced bool, ref objectRef) string {
	switch {
	case !referenced:
		return "no_row_refers_to_key"
	case ref.sealed:
		return "surplus_version_of_sealed_key"
	default:
		return "unreferenced_version"
	}
}

// objectRefs resolves listed object keys against this tenant's rows: which keys
// a row still names, and which version a sealed row pins. Keys are unique per
// row (artifacts_object_uidx), so one key yields at most one answer.
func (s *Store) objectRefs(ctx context.Context, tenant string, keys []string) (map[string]objectRef, error) {
	out := make(map[string]objectRef, len(keys))
	const chunk = 200
	for start := 0; start < len(keys); start += chunk {
		stop := start + chunk
		if stop > len(keys) {
			stop = len(keys)
		}
		rows, err := s.pool.Query(ctx, `
SELECT object, COALESCE(version, ''), version IS NOT NULL AND sha256 IS NOT NULL
  FROM ahp_artifacts.artifacts WHERE tenant = $1 AND object = ANY($2)`,
			tenant, keys[start:stop])
		if err != nil {
			return nil, storageError("read object refs", err)
		}
		for rows.Next() {
			var key, pinned string
			var sealed bool
			if err := rows.Scan(&key, &pinned, &sealed); err != nil {
				rows.Close()
				return nil, storageError("scan object ref", err)
			}
			out[key] = objectRef{pinnedVersion: pinned, sealed: sealed}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, storageError("iterate object refs", err)
		}
		rows.Close()
	}
	return out, nil
}

func (s *Store) logGCEventTx(ctx context.Context, tx pgx.Tx, ev GCEvent) error {
	if ev.Event == "" {
		return contracts.NewError(contracts.ErrInvalid, "artifacts: gc event needs a kind")
	}
	_, err := tx.Exec(ctx, `
INSERT INTO ahp_artifacts.gc_events
    (sweep_id, event, tenant, deployment, subject, artifact_id, object, version, sha256, size, reason)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		ev.SweepID, ev.Event, ev.Tenant, ev.Deployment, ev.Subject, ev.ArtifactID,
		ev.Object, ev.Version, ev.SHA256, ev.Size, ev.Reason)
	return storageError("log gc event", err)
}

// logGCEvent appends one audit record outside any transaction. Content never
// travels through this path: the columns hold identifiers, digests and sizes.
func (s *Store) logGCEvent(ctx context.Context, ev GCEvent) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return s.logGCEventTx(ctx, tx, ev)
	})
}

// GCAudit returns the deletion-plane audit trail of the caller's tenant, oldest
// first. It is the out-of-band answer to "what did the collector take and what
// did it refuse", readable by the same control-plane caller that runs a sweep.
func (s *Store) GCAudit(ctx context.Context, scope contracts.Scope, filter GCAuditFilter) ([]GCEvent, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, scope, "read", "artifacts:gc-audit"); err != nil {
		return nil, err
	}
	limit := filter.Limit
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
SELECT at, sweep_id, event, tenant, deployment, subject, artifact_id, object, version, sha256, size, reason
  FROM ahp_artifacts.gc_events
 WHERE tenant = $1
   AND ($2 = '' OR event = $2)
   AND ($3 = '' OR artifact_id = $3)
   AND ($4 = '' OR sweep_id = $4)
 ORDER BY id DESC
 LIMIT $5`, scope.Tenant, filter.Event, filter.ArtifactID, filter.SweepID, limit)
	if err != nil {
		return nil, storageError("read gc audit", err)
	}
	defer rows.Close()
	var out []GCEvent
	for rows.Next() {
		var ev GCEvent
		if err := rows.Scan(&ev.At, &ev.SweepID, &ev.Event, &ev.Tenant, &ev.Deployment,
			&ev.Subject, &ev.ArtifactID, &ev.Object, &ev.Version, &ev.SHA256, &ev.Size, &ev.Reason); err != nil {
			return nil, storageError("scan gc event", err)
		}
		out = append(out, ev)
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, storageError("iterate gc events", rows.Err())
}

// ledgerKey namespaces a caller's idempotency key by tenant, so tenant A can
// never occupy the ledger entry tenant B is about to retry. The sweep key itself
// stays the caller's string: it is what the report echoes back.
func ledgerKey(tenant, key string) string { return tenant + "|" + key }

func (g *GC) lookupRun(ctx context.Context, tenant, key string) (GCReport, bool, error) {
	var doc []byte
	err := g.store.pool.QueryRow(ctx,
		`SELECT report FROM ahp_artifacts.gc_runs WHERE key = $1 AND tenant = $2`,
		ledgerKey(tenant, key), tenant).Scan(&doc)
	if errors.Is(err, pgx.ErrNoRows) {
		return GCReport{}, false, nil
	}
	if err != nil {
		return GCReport{}, false, storageError("read sweep ledger", err)
	}
	var rep GCReport
	if err := json.Unmarshal(doc, &rep); err != nil {
		return GCReport{}, false, contracts.NewError(contracts.ErrConflict, "stored sweep report is unreadable")
	}
	rep.Replayed = true
	return rep, true, nil
}

func (g *GC) recordRun(ctx context.Context, tenant, key string, rep GCReport) error {
	doc, err := json.Marshal(rep)
	if err != nil {
		return storageError("encode sweep report", err)
	}
	_, err = g.store.pool.Exec(ctx, `
INSERT INTO ahp_artifacts.gc_runs (key, sweep_id, tenant, report)
VALUES ($1, $2, $3, $4::jsonb)
ON CONFLICT (key) DO NOTHING`, ledgerKey(tenant, key), rep.SweepID, tenant, doc)
	return storageError("record sweep", err)
}
