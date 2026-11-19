package state

// Checkpoint history bounds.
//
// WHY THIS EXISTS. A thread's checkpoint history grew without any bound: every
// PutCheckpoint appended a header row, every new channel version appended a
// blob, and nothing in the product ever removed either. The only limit in the
// package was maxCheckpointList - a cap on ONE ListCheckpoints call - so a
// deep history was not merely expensive to keep, it was also partly
// unreachable: rows older than the read window can never be listed again, yet
// they occupied storage forever and were invisible to whoever might want to
// make a deliberate decision about them.
//
// The PDF makes retention a control-plane responsibility: Lifecycle
// (DeleteThread, DeleteSubject, Export, retention, legal hold) is executed by
// AHP's control plane, not by the agent (accept/space-pdf-rev32.txt:1787-1792),
// and every record knows tenant/deployment/subject/context precisely so that
// retention and deletion can be applied (ibid.:1820-1824). So bounding history
// is an operator action with an explicit target - never a side effect of a
// write, and never silently lossy.
//
// WHAT IS KEPT, and why each rule is there:
//
//   - the newest `limit` checkpoints of every namespace, in exactly the order
//     ListCheckpoints returns them, so storage matches the readable window;
//     the newest row (the head a writer builds on) is therefore always kept;
//   - any checkpoint with pending writes: a step is still being assembled
//     around it, truncating it would delete in-flight work;
//   - any checkpoint that carries an artifact descriptor. Releasing such a row
//     would remove a live reference the Artifacts owner is entitled to see,
//     and State must not weaken another owner's liveness authority as a side
//     effect of its own retention. Those rows are REFUSED, counted and
//     reported; the durable way to release them is the thread cascade, which
//     asks the Artifacts owner first.
//
// A surviving checkpoint whose parent was pruned gets parent_id = NULL: the
// chain is honestly truncated ("history begins here"), and the FK is ON DELETE
// CASCADE, so the NULL must happen before the delete or the whole surviving
// chain would be dragged out with the parent.
//
// The durable administrative journal (ahp_state.legalhold, ahp_state.deletions)
// is a different table family and is never touched here, so an operation stays
// reviewable after retention has been applied.

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// UnboundedCheckpointHistory disables the store's history limit explicitly.
// It is the pre-existing behaviour and it is opt-in: an operator who wants
// retention left to the cascade configures this value, and PruneCheckpointHistory
// then refuses to act without an explicit per-request limit.
const UnboundedCheckpointHistory = -1

// DefaultCheckpointHistoryLimit keeps as many checkpoints per
// (thread, namespace) as one ListCheckpoints call can return. Deeper history
// is unreachable through the read API, so keeping it durably serves no reader;
// an operator who needs a deeper window raises the limit, not the read cap.
const DefaultCheckpointHistoryLimit = maxCheckpointList

// CheckpointPruneRequest names the retention target. Namespace is exact
// ("" is the default namespace); AllNamespaces bounds every namespace of the
// thread in one pass, which is what an operator rotating a thread's history
// actually wants, and is the only way a namespace the caller never learned
// about stops being a leak.
type CheckpointPruneRequest struct {
	ThreadID      string `json:"thread_id"`
	Namespace     string `json:"namespace,omitempty"`
	AllNamespaces bool   `json:"all_namespaces,omitempty"`
	// Limit overrides the store's configured bound for this call. It must be
	// positive when the store is configured unbounded.
	Limit int `json:"limit,omitempty"`
	// PreserveAncestors is the recovery-aware mode for delta/event journals.
	// The operator must provide every live published root after coordinating
	// writers; State treats their payloads as opaque, not as native snapshots.
	PreserveAncestors bool                      `json:"preserve_ancestors,omitempty"`
	ProtectedRoots    []CheckpointRetentionRoot `json:"protected_roots,omitempty"`
}

type CheckpointRetentionRoot struct {
	Namespace string `json:"namespace"`
	ID        string `json:"id"`
}

// CheckpointPruneReport is what one retention pass did. Every refusal is a
// number, never a silent omission: an operator deciding whether history is
// bounded has to be able to see what the bound did not touch and why.
type CheckpointPruneReport struct {
	ThreadID     string `json:"thread_id"`
	Limit        int    `json:"limit"`
	Kept         int    `json:"kept"`
	Pruned       int    `json:"pruned"`
	PrunedWrites int    `json:"pruned_writes"`
	PrunedBlobs  int    `json:"pruned_blobs"`
	// RefusedReferenceBearing counts checkpoints that hold an artifact
	// descriptor and were therefore left in place.
	RefusedReferenceBearing int `json:"refused_reference_bearing"`
	// TruncatedChains counts survivors whose parent was pruned and whose
	// parent link was therefore set to NULL.
	TruncatedChains int `json:"truncated_chains"`
	// RefusedDependencies counts candidates kept because a retained root or
	// snapshot still depends on them, including an explicitly protected root.
	RefusedDependencies int `json:"refused_dependencies"`
}

func WithCheckpointHistory(limit int) StoreOption {
	return func(s *Store) { s.checkpointHistoryLimit = limit }
}

// CheckpointHistoryLimit reports the configured bound: a positive number of
// checkpoints kept per (thread, namespace), or UnboundedCheckpointHistory.
func (s *Store) CheckpointHistoryLimit() int { return s.checkpointHistoryLimit }

func checkpointHistoryLockKey(threadID string) string {
	return "ahp_state:checkpoint-history:" + threadID
}

// PruneCheckpointHistory applies the history bound to one thread. It is
// idempotent: a second pass with the same limit prunes nothing. It takes an
// advisory transaction lock per thread so two operators cannot race over the
// same history, and it does one transaction, so a partial truncation is not
// observable.
func (s *Store) PruneCheckpointHistory(ctx context.Context, scope contracts.Scope, req CheckpointPruneRequest) (CheckpointPruneReport, error) {
	report := CheckpointPruneReport{ThreadID: req.ThreadID}
	if err := scope.Validate(); err != nil {
		return report, err
	}
	if err := validIdentifier(req.ThreadID, "thread id"); err != nil {
		return report, err
	}
	if !req.AllNamespaces && len(req.Namespace) > maxNamespaceBytes {
		return report, invalid("checkpoint namespace exceeds 512 bytes")
	}
	if req.PreserveAncestors && (len(req.ProtectedRoots) == 0 || len(req.ProtectedRoots) > 256) {
		return report, invalid("recovery-aware retention requires 1..256 published roots")
	}
	if !req.PreserveAncestors && len(req.ProtectedRoots) > 0 {
		return report, invalid("protected roots require recovery-aware retention")
	}
	for _, root := range req.ProtectedRoots {
		if err := validIdentifier(root.ID, "protected checkpoint id"); err != nil {
			return report, err
		}
		if len(root.Namespace) > maxNamespaceBytes || (!req.AllNamespaces && root.Namespace != req.Namespace) {
			return report, invalid("protected root is outside selected namespace")
		}
	}
	limit := req.Limit
	if limit == 0 {
		limit = s.checkpointHistoryLimit
	}
	if limit < 1 {
		return report, invalid("checkpoint history limit must be positive; this store keeps history unbounded, pass an explicit limit")
	}
	report.Limit = limit
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getThread(ctx, tx, scope, req.ThreadID, ""); err != nil {
			return err
		}
		// This route prunes legacy checkpoint rows. Common snapshots have
		// independent interval/Artifact roots and require another retention
		// protocol; returning an empty successful report would mislead the host.
		if s.commonRecords {
			var common bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.common_cuts WHERE thread_id=$1 AND ($2 OR namespace=$3))`, req.ThreadID, req.AllNamespaces, req.Namespace).Scan(&common); err != nil {
				return err
			}
			if common {
				return invalid("common snapshot retention requires a separate root-aware protocol; legacy checkpoint pruning cannot process it")
			}
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
			checkpointHistoryLockKey(req.ThreadID)); err != nil {
			return err
		}
		threadID := req.ThreadID
		protected := []string{}
		for _, root := range req.ProtectedRoots {
			id, err := resolveCheckpoint(ctx, tx, threadID, root.Namespace, root.ID)
			if err != nil {
				return err
			}
			protected = append(protected, id)
		}
		// Rank every checkpoint of the thread in exactly the order a reader
		// sees them, and mark the two things that must survive regardless of
		// depth: an in-flight step, and an artifact descriptor.
		if _, err := tx.Exec(ctx, `
CREATE TEMP TABLE cp_prune ON COMMIT DROP AS
WITH ranked AS (
    SELECT c.id,
           row_number() OVER (PARTITION BY c.namespace
                              ORDER BY c.created_at DESC, c.id DESC) AS rn,
           EXISTS(SELECT 1 FROM ahp_state.checkpoint_writes w
                   WHERE w.checkpoint_id = c.id)                   AS in_flight,
           (c.metadata_encoding <> '' OR EXISTS(
                SELECT 1
                  FROM ahp_state.checkpoint_parts p
                  JOIN ahp_state.checkpoint_blobs b
                    ON b.thread_id = c.thread_id AND b.namespace = c.namespace
                   AND b.channel = p.channel AND b.version = p.version
                 WHERE p.checkpoint_id = c.id AND b.encoding <> '')) AS reference_bearing
      FROM ahp_state.checkpoints c
     WHERE c.thread_id = $1 AND ($2 OR c.namespace = $3))
SELECT id, reference_bearing FROM ranked
 WHERE rn > $4 AND NOT in_flight`, threadID, req.AllNamespaces, req.Namespace, limit); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM cp_prune WHERE reference_bearing`).Scan(&report.RefusedReferenceBearing); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM cp_prune WHERE reference_bearing`); err != nil {
			return err
		}
		if req.PreserveAncestors {
			// Seed with explicitly published roots and all checkpoints already
			// retained by the size, pending-write and artifact-reference rules.
			// UNION (not UNION ALL) also bounds a malformed legacy graph cycle.
			tag, err := tx.Exec(ctx, `WITH RECURSIVE retained(id,parent_id) AS (
 SELECT c.id,c.parent_id FROM ahp_state.checkpoints c
 WHERE c.thread_id=$1 AND ($2 OR c.namespace=$3)
   AND (c.id=ANY($4::text[]) OR NOT EXISTS(SELECT 1 FROM cp_prune p WHERE p.id=c.id))
 UNION
 SELECT p.id,p.parent_id FROM ahp_state.checkpoints p
 JOIN retained r ON p.id=r.parent_id WHERE p.thread_id=$1)
 DELETE FROM cp_prune WHERE id IN (SELECT id FROM retained)`, threadID, req.AllNamespaces, req.Namespace, protected)
			if err != nil {
				return err
			}
			report.RefusedDependencies = int(tag.RowsAffected())
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM cp_prune`).Scan(&report.Pruned); err != nil {
			return err
		}
		// Truncate chains before deleting their parents: the parent FK cascades.
		tag, err := tx.Exec(ctx, `
UPDATE ahp_state.checkpoints c SET parent_id = NULL
 WHERE c.parent_id IN (SELECT id FROM cp_prune) AND c.id NOT IN (SELECT id FROM cp_prune)`)
		if err != nil {
			return err
		}
		report.TruncatedChains = int(tag.RowsAffected())
		tag, err = tx.Exec(ctx, `
DELETE FROM ahp_state.checkpoint_writes w
 WHERE w.checkpoint_id IN (SELECT id FROM cp_prune)`)
		if err != nil {
			return err
		}
		report.PrunedWrites = int(tag.RowsAffected())
		// Remember which blob versions the pruned rows referenced: those are the
		// only ones retention may reclaim, and only once nothing keeps pointing
		// at them.
		if _, err := tx.Exec(ctx, `
CREATE TEMP TABLE cp_blobref ON COMMIT DROP AS
SELECT DISTINCT b.thread_id, b.namespace, b.channel, b.version
  FROM ahp_state.checkpoint_parts p
  JOIN ahp_state.checkpoints c ON c.id = p.checkpoint_id
  JOIN ahp_state.checkpoint_blobs b
    ON b.thread_id = c.thread_id AND b.namespace = c.namespace
   AND b.channel = p.channel AND b.version = p.version
 WHERE p.checkpoint_id IN (SELECT id FROM cp_prune)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM ahp_state.checkpoints c
 WHERE c.id IN (SELECT id FROM cp_prune)`); err != nil {
			return err
		}
		tag, err = tx.Exec(ctx, `
DELETE FROM ahp_state.checkpoint_blobs b
 WHERE (b.thread_id, b.namespace, b.channel, b.version) IN (SELECT * FROM cp_blobref)
   AND NOT EXISTS(SELECT 1 FROM ahp_state.checkpoint_parts p
                    JOIN ahp_state.checkpoints c ON c.id = p.checkpoint_id
                   WHERE c.thread_id = b.thread_id AND c.namespace = b.namespace
                     AND p.channel = b.channel AND p.version = b.version)`)
		if err != nil {
			return err
		}
		report.PrunedBlobs = int(tag.RowsAffected())
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM ahp_state.checkpoints WHERE thread_id = $1`,
			threadID).Scan(&report.Kept); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		// inTx already carries the contract code (NOT_FOUND for a thread that
		// is not this caller's, CONFLICT for a lost race); re-wrapping here
		// would turn a precise refusal into a generic internal error.
		return report, err
	}
	return report, nil
}
