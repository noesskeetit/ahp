package artifacts

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// GCEventCascadeHeld names the audit event of one artifact a deletion cascade
// matched and deliberately did NOT tombstone, because a durable reference outside
// the artifact plane still names it. It is recorded in the same transaction as
// the cascade itself, so the trail cannot disagree with the sweep.
const GCEventCascadeHeld = "cascade_held_by_reference"

// cascade runs one tombstone sweep. An empty matcher sweeps the whole caller
// scope (the subject cascade). Held rows are excluded by the UPDATE itself, so a
// legal hold set concurrently with this call is honoured rather than raced past.
//
// The sweep also excludes rows the reference authority still sees in use
// (WithReferenceKeeper) -- that is the DEF-H24-15 contract. Deleting a chat is a
// cascade, and the cascade must run; but a workspace candidate or revision that
// names one of the chat's artifacts is a separate durable fact, and tombstoning
// the bytes anyway leaves the workspace holding a reference that answers 404
// when it goes to Publish. So the artifact stays: no deletion is refused for it,
// and no second pin row is invented for it, because W-1 made the durable
// reference itself the authority. It becomes collectable when that reference
// goes away, through the ordinary retention path.
//
// Without a configured authority this behaves byte for byte as it did before the
// option existed. With an authority that CANNOT ANSWER, the cascade fails and
// touches nothing: silently removing bytes that might still be in use is the one
// outcome this plane does not choose on its own.
//
// Rows are locked first (FOR UPDATE) and tombstoned by id in the same
// transaction, so the check and the removal cannot half-apply. The residual
// window is one statement wide: a workspace reference committed after the probe
// and before this transaction commits is not seen. Closing it would mean the
// workspace write path taking the same row lock, which is a contract decision of
// its own rather than a detail of this function.
func (s *Store) cascade(ctx context.Context, b *cascadeBuilder) (CascadeResult, error) {
	var res CascadeResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		where, args := b.where(), b.args
		// Discover a bounded cohort before taking row locks, then serialize on
		// the same object locks as State publication. New arrivals are handled
		// by the next coordinated cascade rather than bypassing those locks.
		cohort, err := tx.Query(ctx, `SELECT id FROM ahp_artifacts.artifacts WHERE `+where+` AND NOT deleted AND NOT legal_hold`, args...)
		if err != nil {
			return storageError("cascade cohort", err)
		}
		idsToLock := []string{}
		locks := []string{}
		for cohort.Next() {
			var id string
			if err := cohort.Scan(&id); err != nil {
				cohort.Close()
				return err
			}
			idsToLock = append(idsToLock, id)
			locks = append(locks, artifactLockKey(b.tenant, id))
		}
		err = cohort.Err()
		cohort.Close()
		if err != nil {
			return err
		}
		if err := lockMarkers(ctx, tx, locks); err != nil {
			return err
		}
		lockedArgs := append(append([]any{}, args...), idsToLock)

		type swept struct {
			id, object, sha, version string
			size                     int64
		}
		rows, err := tx.Query(ctx, `
SELECT id, object, COALESCE(sha256, ''), COALESCE(version, ''), COALESCE(size, 0)
  FROM ahp_artifacts.artifacts
 WHERE `+where+` AND NOT deleted AND NOT legal_hold AND id=ANY($`+strconv.Itoa(len(lockedArgs))+`::text[])
   FOR UPDATE`, lockedArgs...)
		if err != nil {
			return storageError("cascade select", err)
		}
		var matched []swept
		for rows.Next() {
			var row swept
			if err := rows.Scan(&row.id, &row.object, &row.sha, &row.version, &row.size); err != nil {
				rows.Close()
				return storageError("cascade select scan", err)
			}
			matched = append(matched, row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return storageError("cascade select rows", err)
		}
		rows.Close()

		referenced := map[string]bool{}
		if len(matched) > 0 {
			query := make([]ArtifactQuery, 0, len(matched))
			for _, row := range matched {
				query = append(query, ArtifactQuery{ID: row.id, Digest: row.sha})
			}
			referenced, err = s.tombstoneReferences(ctx, tx, b.tenant, query)
			if err != nil {
				return fmt.Errorf("artifacts: cascade refused, live references unreadable: %w", err)
			}
		}

		var taken []swept
		var kept []swept
		ids := make([]string, 0, len(matched))
		for _, row := range matched {
			if referenced[row.id] {
				kept = append(kept, row)
				continue
			}
			taken = append(taken, row)
			ids = append(ids, row.id)
		}

		if len(ids) > 0 {
			tag, err := tx.Exec(ctx, `
UPDATE ahp_artifacts.artifacts
   SET deleted = true, deleted_at = COALESCE($2::timestamptz,now())
 WHERE id = ANY($1) AND NOT deleted AND NOT legal_hold`, ids, b.deletedAt)
			if err != nil {
				return storageError("cascade tombstone", err)
			}
			// A concurrent purge or delete can take a row between the locked
			// select and this statement; the cascade is idempotent, so the honest
			// answer is the number actually tombstoned, not the number matched.
			if n := int(tag.RowsAffected()); n < len(taken) {
				taken = taken[:n]
			}
		}
		res.Tombstoned = len(taken)
		res.Referenced = len(kept)

		// Both decisions are audited in the transaction that made them.
		for _, row := range taken {
			if err := s.logGCEventTx(ctx, tx, GCEvent{
				Event: GCEventTombstone, Tenant: b.tenant, Deployment: b.deployment,
				Subject: b.subject, ArtifactID: row.id, Object: row.object,
				Version: row.version, SHA256: row.sha, Size: row.size, Reason: b.reason,
			}); err != nil {
				return err
			}
		}
		for _, row := range kept {
			if err := s.logGCEventTx(ctx, tx, GCEvent{
				Event: GCEventCascadeHeld, Tenant: b.tenant, Deployment: b.deployment,
				Subject: b.subject, ArtifactID: row.id, Object: row.object,
				Version: row.version, SHA256: row.sha, Size: row.size,
				Reason: b.reason + ":held-by-reference",
			}); err != nil {
				return err
			}
		}

		var held, deleted int
		if err := tx.QueryRow(ctx, `
SELECT COUNT(*) FILTER (WHERE legal_hold AND NOT deleted), COUNT(*) FILTER (WHERE deleted)
  FROM ahp_artifacts.artifacts WHERE `+where, args...).Scan(&held, &deleted); err != nil {
			return storageError("count cascade remainder", err)
		}
		res.Held = held
		// Rows this sweep just tombstoned are counted in `deleted` too; the
		// already-deleted figure is what was deleted before this call, and Held
		// counts only a hold that is still serving bytes, so the three numbers
		// never double-count one row.
		res.AlreadyDeleted = deleted - held - res.Tombstoned
		if res.AlreadyDeleted < 0 {
			res.AlreadyDeleted = 0
		}
		return nil
	})
	if err != nil {
		return CascadeResult{}, err
	}
	return res, nil
}
