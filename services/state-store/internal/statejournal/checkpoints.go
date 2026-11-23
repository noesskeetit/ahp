package statejournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
)

const langGraphCodec = "langgraph-jsonplus-v1"

type PendingWrite = journal.PendingWrite

type Checkpoint = journal.Checkpoint

type CheckpointCommit = journal.CheckpointCommit

type CheckpointTuple = journal.CheckpointTuple

func (s *Store) PutCheckpoint(ctx context.Context, scope contracts.Scope, session string, lease Lease, req CheckpointCommit) (string, error) {
	receipt, err := s.CommitCheckpoint(ctx, scope, session, lease, req)
	return receipt.ID, err
}

func (s *Store) CommitCheckpoint(ctx context.Context, scope contracts.Scope, session string, lease Lease, req CheckpointCommit) (journal.CheckpointReceipt, error) {
	cp := req.Checkpoint
	if err := scope.Validate(); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	if !validID(req.Key) || !validCheckpointKey(cp.Namespace, cp.ID) || !validID(cp.Codec) ||
		(cp.ParentID != "" && (!validID(cp.ParentID) || cp.ParentID == cp.ID)) ||
		cp.SnapshotSeq < 0 || !json.Valid(cp.Metadata) || !json.Valid(cp.ChannelVersions) ||
		!validPayload(cp.Payload) || cp.ChatEventID != "" {
		return journal.CheckpointReceipt{}, contracts.NewError(contracts.ErrInvalid, "invalid checkpoint commit")
	}
	if err := validateWrites(req.Writes); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	if err := validateIntents(req.Intents); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	if err := validateOutcomes(req.ToolOutcomes); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	inline := len(cp.Payload.Inline) + inlineWritesSize(req.Writes)
	for _, outcome := range req.ToolOutcomes {
		if outcome.Result != nil {
			inline += len(outcome.Result.Inline)
		}
	}
	if inline > 2<<20 {
		return journal.CheckpointReceipt{}, contracts.NewError(contracts.ErrInvalid, "checkpoint exceeds 2 MiB of inline payload")
	}
	chat, err := encodeChatDelta(cp.Codec, req.Chat)
	if err != nil {
		return journal.CheckpointReceipt{}, err
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return journal.CheckpointReceipt{}, err
	}
	legacyDigest := sha256.Sum256(encoded)
	// PostgreSQL compares the two JSONB fields directly on replay. Hash the
	// remaining, typed fields so the comparison preserves JSONB's number range
	// and still detects changes to opaque bytes, writes and tool intents.
	replayRequest := req
	replayRequest.Checkpoint.Metadata = nil
	replayRequest.Checkpoint.ChannelVersions = nil
	replayBody, err := json.Marshal(replayRequest)
	if err != nil {
		return journal.CheckpointReceipt{}, err
	}
	canonicalDigest := sha256.Sum256(replayBody)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return journal.CheckpointReceipt{}, err
	}
	defer tx.Rollback(ctx)
	head, turn, err := lockSessionState(ctx, tx, scope, session, lease)
	if err != nil {
		return journal.CheckpointReceipt{}, err
	}
	if err := turn.validate(req.TurnSeq, req.SteerAck); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	if cp.SnapshotSeq > head {
		return journal.CheckpointReceipt{}, contracts.NewError(contracts.ErrConflict, "checkpoint references uncommitted journal events")
	}
	var id string
	var chatID string
	if len(chat) != 0 {
		chatID = chatEventID(cp.Namespace, cp.ID)
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO ahp_journal.checkpoints
			(tenant_id, session_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
			 snapshot_seq, metadata, channel_versions, codec, inline_payload, blob_ref, operation_key,
			 request_digest, canonical_request_digest, chat_event_id)
		SELECT $1,$2,$3,$4,nullif($5,''),$6,$7,$8,$9,$10,$11,$12,$13,$14,nullif($15,'')
		WHERE $5='' OR EXISTS (
			SELECT 1 FROM ahp_journal.checkpoints
			WHERE tenant_id=$1 AND session_id=$2 AND checkpoint_ns=$3 AND checkpoint_id=$5)
		ON CONFLICT (tenant_id, session_id, operation_key) DO NOTHING RETURNING checkpoint_id`,
		scope.Tenant, session, cp.Namespace, cp.ID, cp.ParentID, cp.SnapshotSeq,
		cp.Metadata, cp.ChannelVersions, cp.Codec, cp.Payload.Inline, blobValue(cp.Payload),
		req.Key, legacyDigest[:], canonicalDigest[:], chatID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		var priorLegacy, priorCanonical []byte
		var sameJSON bool
		err = tx.QueryRow(ctx, `
			SELECT checkpoint_id, request_digest, canonical_request_digest,
			       metadata=$4::jsonb AND channel_versions=$5::jsonb
			FROM ahp_journal.checkpoints
			WHERE tenant_id=$1 AND session_id=$2 AND operation_key=$3`,
			scope.Tenant, session, req.Key, cp.Metadata, cp.ChannelVersions).
			Scan(&id, &priorLegacy, &priorCanonical, &sameJSON)
		if errors.Is(err, pgx.ErrNoRows) {
			return journal.CheckpointReceipt{}, contracts.NewError(contracts.ErrInvalid, "checkpoint parent must exist in the same session and namespace")
		}
		if err != nil {
			return journal.CheckpointReceipt{}, err
		}
		if sameJSON && bytes.Equal(priorCanonical, canonicalDigest[:]) {
			control, err := peekControl(ctx, tx, scope, session, turn)
			return journal.CheckpointReceipt{ID: id, Control: control}, err
		}
		if priorCanonical != nil || !bytes.Equal(priorLegacy, legacyDigest[:]) {
			return journal.CheckpointReceipt{}, contracts.NewError(contracts.ErrKeyReuse, "checkpoint key has a different request digest")
		}
		// An exact replay supplies the original request for a pre-upgrade
		// checkpoint. Add its canonical digest without changing the old one.
		if _, err := tx.Exec(ctx, `
			UPDATE ahp_journal.checkpoints SET canonical_request_digest=$4
			WHERE tenant_id=$1 AND session_id=$2 AND operation_key=$3`,
			scope.Tenant, session, req.Key, canonicalDigest[:]); err != nil {
			return journal.CheckpointReceipt{}, err
		}
		return journal.CheckpointReceipt{ID: id}, tx.Commit(ctx)
	}
	if err != nil {
		return journal.CheckpointReceipt{}, classifyWrite(err)
	}
	refs := addOutcomeBlobs(addWriteBlobs(addBlob(nil, cp.Payload), req.Writes), req.ToolOutcomes)
	if err := s.pinBlobs(ctx, tx, scope, session, refs); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	if err := putWrites(ctx, tx, scope.Tenant, session, cp.Namespace, cp.ID, req.Writes); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	if err := prepareIntents(ctx, tx, scope.Tenant, session, lease.Epoch, req.Intents); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	if err := commitOutcomes(ctx, tx, scope.Tenant, session, turn.Mode == journal.ExecutionVMManaged, req.ToolOutcomes); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	if err := applySteerAck(ctx, tx, scope, session, &turn, req.SteerAck); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	if chatID != "" {
		if err := appendChatDelta(ctx, tx, scope, session, cp, head, chatID, chat, req.TurnSeq); err != nil {
			return journal.CheckpointReceipt{}, err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE ahp_journal.sessions SET head_checkpoint_id=$3, updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND session_id=$2`, scope.Tenant, session, id); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	control, err := peekControl(ctx, tx, scope, session, turn)
	if err != nil {
		return journal.CheckpointReceipt{}, err
	}
	if turn.Mode == journal.ExecutionVMManaged && chatID != "" {
		if err := notifyJournal(ctx, tx, scope, session); err != nil {
			return journal.CheckpointReceipt{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return journal.CheckpointReceipt{}, err
	}
	return journal.CheckpointReceipt{ID: id, Control: control}, nil
}

func (s *Store) PutWrites(ctx context.Context, scope contracts.Scope, session string, lease Lease, namespace, checkpointID string, writes []PendingWrite) error {
	return s.PutTurnWrites(ctx, scope, session, lease, namespace, checkpointID, writes, 0)
}

func (s *Store) PutTurnWrites(ctx context.Context, scope contracts.Scope, session string, lease Lease, namespace, checkpointID string, writes []PendingWrite, turnSeq int64) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if !validCheckpointKey(namespace, checkpointID) {
		return contracts.NewError(contracts.ErrInvalid, "invalid checkpoint key")
	}
	if err := validateWrites(writes); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, turn, err := lockSessionState(ctx, tx, scope, session, lease)
	if err != nil {
		return err
	}
	if err := turn.validate(turnSeq, nil); err != nil {
		return err
	}
	if err := s.pinBlobs(ctx, tx, scope, session, addWriteBlobs(nil, writes)); err != nil {
		return err
	}
	if err := putWrites(ctx, tx, scope.Tenant, session, namespace, checkpointID, writes); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func putWrites(ctx context.Context, tx pgx.Tx, tenant, session, namespace, checkpointID string, writes []PendingWrite) error {
	for _, write := range writes {
		args := []any{tenant, session, namespace, checkpointID, write.TaskID, write.Index,
			write.TaskPath, write.Channel, write.Codec, write.Payload.Inline, blobValue(write.Payload)}
		tag, err := tx.Exec(ctx, `
			INSERT INTO ahp_journal.checkpoint_writes
				(tenant_id,session_id,checkpoint_ns,checkpoint_id,task_id,idx,task_path,
				 channel,codec,inline_payload,blob_ref)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING`, args...)
		if err != nil {
			return classifyWrite(err)
		}
		if tag.RowsAffected() != 0 {
			continue
		}
		// LangGraph derives the same nonnegative write slot when it forks from
		// a historical checkpoint. Its saver keeps the first slot value. The
		// alternate update is materialized in a distinct child checkpoint.
		var match bool
		err = tx.QueryRow(ctx, `
			SELECT task_path=$7 AND channel=$8 AND codec=$9 AND (
			  (idx>=0 AND codec=$12)
			  OR (inline_payload IS NOT DISTINCT FROM $10::bytea
			      AND blob_ref IS NOT DISTINCT FROM $11::jsonb))
			FROM ahp_journal.checkpoint_writes
			WHERE tenant_id=$1 AND session_id=$2 AND checkpoint_ns=$3 AND checkpoint_id=$4
			  AND task_id=$5 AND idx=$6`, append(args, langGraphCodec)...).Scan(&match)
		if err != nil {
			return err
		}
		if !match {
			return contracts.NewError(contracts.ErrKeyReuse, "pending write key has different data")
		}
	}
	return nil
}

func (s *Store) GetCheckpoint(ctx context.Context, scope contracts.Scope, session, namespace, checkpointID string) (CheckpointTuple, error) {
	if err := scope.Validate(); err != nil {
		return CheckpointTuple{}, err
	}
	if (namespace != "" && !validID(namespace)) || (checkpointID != "" && !validID(checkpointID)) {
		return CheckpointTuple{}, contracts.NewError(contracts.ErrInvalid, "invalid checkpoint key")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return CheckpointTuple{}, err
	}
	defer tx.Rollback(ctx)
	var result CheckpointTuple
	cp := &result.Checkpoint
	cp.Namespace = namespace
	// If the ID is omitted, use the newest native sortable identifier in this namespace.
	args := []any{scope.Tenant, session, scope.Deployment, scope.Subject, namespace}
	filter := "ORDER BY c.checkpoint_id DESC LIMIT 1"
	if checkpointID != "" {
		filter = "AND c.checkpoint_id=$6"
		args = append(args, checkpointID)
	}
	err = tx.QueryRow(ctx, `
		SELECT c.checkpoint_id, coalesce(c.parent_checkpoint_id,''), c.snapshot_seq, c.metadata,
		       c.channel_versions, c.codec, c.inline_payload, c.blob_ref, COALESCE(c.chat_event_id,'')
		FROM ahp_journal.checkpoints c JOIN ahp_journal.sessions t USING (tenant_id, session_id)
		WHERE c.tenant_id=$1 AND c.session_id=$2 AND t.deployment_id=$3 AND t.subject_id=$4
		  AND c.checkpoint_ns=$5 AND t.status <> 'DELETED' `+filter, args...).
		Scan(&cp.ID, &cp.ParentID, &cp.SnapshotSeq, &cp.Metadata, &cp.ChannelVersions, &cp.Codec, &cp.Payload.Inline, &cp.Payload.Blob, &cp.ChatEventID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrNotFound, "checkpoint not found")
	}
	if err != nil {
		return CheckpointTuple{}, err
	}
	rows, err := tx.Query(ctx, `
		SELECT task_id, task_path, idx, channel, codec, inline_payload, blob_ref
		FROM ahp_journal.checkpoint_writes
		WHERE tenant_id=$1 AND session_id=$2 AND checkpoint_ns=$3 AND checkpoint_id=$4
		ORDER BY task_id, idx`, scope.Tenant, session, namespace, cp.ID)
	if err != nil {
		return CheckpointTuple{}, err
	}
	result.Writes, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (PendingWrite, error) {
		var write PendingWrite
		err := row.Scan(&write.TaskID, &write.TaskPath, &write.Index, &write.Channel, &write.Codec,
			&write.Payload.Inline, &write.Payload.Blob)
		return write, err
	})
	if err != nil {
		return CheckpointTuple{}, err
	}
	return result, tx.Commit(ctx)
}

func (s *Store) ListCheckpointIDs(ctx context.Context, scope contracts.Scope, session, namespace, before string, limit int) ([]string, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if !validID(session) || (namespace != "" && !validID(namespace)) ||
		(before != "" && !validID(before)) || limit < 1 || limit > 1000 {
		return nil, contracts.NewError(contracts.ErrInvalid, "invalid checkpoint page")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT c.checkpoint_id
		FROM ahp_journal.checkpoints c JOIN ahp_journal.sessions t USING (tenant_id, session_id)
		WHERE c.tenant_id=$1 AND c.session_id=$2 AND t.deployment_id=$3 AND t.subject_id=$4
		  AND c.checkpoint_ns=$5 AND t.status <> 'DELETED' AND ($6='' OR c.checkpoint_id<$6)
		ORDER BY c.checkpoint_id DESC LIMIT $7`,
		scope.Tenant, session, scope.Deployment, scope.Subject, namespace, before, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func validCheckpointKey(namespace, checkpointID string) bool {
	return validID(checkpointID) && (namespace == "" || validID(namespace))
}

func validateWrites(writes []PendingWrite) error {
	if len(writes) > 256 {
		return contracts.NewError(contracts.ErrInvalid, "pending write batch exceeds 256 items")
	}
	if inlineWritesSize(writes) > 2<<20 {
		return contracts.NewError(contracts.ErrInvalid, "pending writes exceed 2 MiB of inline payload")
	}
	for _, write := range writes {
		if !validID(write.TaskID) || !validID(write.Channel) || !validID(write.Codec) ||
			len(write.TaskPath) > 1024 || !validPayload(write.Payload) {
			return contracts.NewError(contracts.ErrInvalid, "invalid pending write")
		}
	}
	return nil
}

func inlineWritesSize(writes []PendingWrite) int {
	var size int
	for _, write := range writes {
		size += len(write.Payload.Inline)
	}
	return size
}

type CheckpointPage struct {
	IDs    []string          `json:"checkpoint_ids"`
	Tuples []CheckpointTuple `json:"checkpoint_tuples"`
}

// ListCheckpointTuples reads a single consistent page and its writes in two
// batch queries. It never opens one transaction or performs a read per tuple.
func (s *Store) ListCheckpointTuples(ctx context.Context, scope contracts.Scope, session, namespace, before string, limit int, metadata json.RawMessage, includeUnindexed bool) (CheckpointPage, error) {
	page := CheckpointPage{IDs: []string{}, Tuples: []CheckpointTuple{}}
	if err := scope.Validate(); err != nil {
		return page, err
	}
	if !validID(session) || (namespace != "" && !validID(namespace)) ||
		(before != "" && !validID(before)) || limit < 1 || limit > 1000 {
		return page, contracts.NewError(contracts.ErrInvalid, "invalid checkpoint page")
	}
	if len(metadata) == 0 {
		metadata = json.RawMessage("{}")
	}
	var object map[string]json.RawMessage
	if len(metadata) > 16<<10 || json.Unmarshal(metadata, &object) != nil || object == nil {
		return page, contracts.NewError(contracts.ErrInvalid, "metadata filter must be a JSON object up to 16 KiB")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return page, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
      SELECT c.checkpoint_id,coalesce(c.parent_checkpoint_id,''),c.snapshot_seq,c.metadata,
        c.channel_versions,c.codec,c.inline_payload,c.blob_ref,coalesce(c.chat_event_id,'')
      FROM ahp_journal.checkpoints c JOIN ahp_journal.sessions t USING(tenant_id,session_id)
      WHERE c.tenant_id=$1 AND c.session_id=$2 AND t.deployment_id=$3 AND t.subject_id=$4
        AND c.checkpoint_ns=$5 AND t.status<>'DELETED' AND ($6='' OR c.checkpoint_id<$6)
        AND (c.metadata @> $8::jsonb OR ($9 AND c.codec='langgraph-jsonplus-v1' AND c.metadata='{}'::jsonb))
      ORDER BY c.checkpoint_id DESC LIMIT $7`,
		scope.Tenant, session, scope.Deployment, scope.Subject, namespace, before, limit, metadata, includeUnindexed)
	if err != nil {
		return page, classifyWrite(err)
	}
	defer rows.Close()
	indexes := make(map[string]int, limit)
	total := 0
	for rows.Next() {
		tuple := CheckpointTuple{Checkpoint: Checkpoint{Namespace: namespace}, Writes: []PendingWrite{}}
		cp := &tuple.Checkpoint
		if err := rows.Scan(&cp.ID, &cp.ParentID, &cp.SnapshotSeq, &cp.Metadata, &cp.ChannelVersions, &cp.Codec,
			&cp.Payload.Inline, &cp.Payload.Blob, &cp.ChatEventID); err != nil {
			return page, err
		}
		total += len(cp.Payload.Inline) + len(cp.Metadata) + len(cp.ChannelVersions)
		if total > 16<<20 {
			return page, contracts.NewError(contracts.ErrInvalid, "checkpoint page exceeds 16 MiB; choose a smaller limit")
		}
		indexes[cp.ID] = len(page.Tuples)
		page.IDs = append(page.IDs, cp.ID)
		page.Tuples = append(page.Tuples, tuple)
	}
	if err := rows.Err(); err != nil {
		return page, classifyWrite(err)
	}
	if len(page.Tuples) == 0 {
		return page, tx.Commit(ctx)
	}
	writes, err := tx.Query(ctx, `
       SELECT checkpoint_id,task_id,task_path,idx,channel,codec,inline_payload,blob_ref
       FROM ahp_journal.checkpoint_writes
       WHERE tenant_id=$1 AND session_id=$2 AND checkpoint_ns=$3 AND checkpoint_id=ANY($4)
       ORDER BY checkpoint_id,task_id,idx`, scope.Tenant, session, namespace, page.IDs)
	if err != nil {
		return page, err
	}
	defer writes.Close()
	for writes.Next() {
		var id string
		var w PendingWrite
		if err := writes.Scan(&id, &w.TaskID, &w.TaskPath, &w.Index, &w.Channel, &w.Codec, &w.Payload.Inline, &w.Payload.Blob); err != nil {
			return page, err
		}
		total += len(w.Payload.Inline)
		if total > 16<<20 {
			return page, contracts.NewError(contracts.ErrInvalid, "checkpoint page exceeds 16 MiB; choose a smaller limit")
		}
		i := indexes[id]
		page.Tuples[i].Writes = append(page.Tuples[i].Writes, w)
	}
	if err := writes.Err(); err != nil {
		return page, err
	}
	return page, tx.Commit(ctx)
}
