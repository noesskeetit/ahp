package state

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// checkpointHeaderSQL reads one checkpoint header by INTERNAL id (or the newest
// one of the namespace when internalID is empty) and reports the PUBLIC id of
// both the row and its parent. Rows created before the public/internal split
// carry no external_id, so the internal id is their public id.
const checkpointHeaderSQL = `
SELECT c.id, COALESCE(c.external_id, c.id), c.namespace,
       COALESCE(COALESCE(p.external_id, p.id), ''),
       c.version, c.metadata::text, c.metadata_encoding, c.created_at
  FROM ahp_state.checkpoints c
  LEFT JOIN ahp_state.checkpoints p ON p.id = c.parent_id
 WHERE c.thread_id = $1 AND c.namespace = $2
   AND ($3 = '' OR c.id = $3)
 ORDER BY c.created_at DESC, c.id DESC
 LIMIT 1`

// checkpointRef is the internal identity of one public checkpoint id inside a
// (thread, namespace) plus the idempotency data needed by the replay path.
type checkpointRef struct {
	internalID string
	digest     string
	key        string
}

// findCheckpointByPublicID resolves a public checkpoint id inside one
// (thread, namespace). Rows created after the isolation fix are found by their
// deterministic internal id (the digest of the scope triple), which makes the
// lookup a single primary-key read; rows created before it stored the public id
// as their internal id and are matched by the legacy fallback. found=false is
// not an error: the caller decides between NOT_FOUND and insert.
func findCheckpointByPublicID(ctx context.Context, tx pgx.Tx, threadID, namespace, publicID string) (checkpointRef, bool, error) {
	scopedID, err := internalCheckpointID(threadID, namespace, publicID)
	if err != nil {
		return checkpointRef{}, false, err
	}
	var ref checkpointRef
	err = tx.QueryRow(ctx, `
SELECT id, body_digest, key FROM ahp_state.checkpoints
 WHERE id = $1 AND thread_id = $2 AND namespace = $3`,
		scopedID, threadID, namespace).Scan(&ref.internalID, &ref.digest, &ref.key)
	if err == nil {
		return ref, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return checkpointRef{}, false, err
	}
	err = tx.QueryRow(ctx, `
SELECT id, body_digest, key FROM ahp_state.checkpoints
 WHERE external_id = $1 AND thread_id = $2 AND namespace = $3`,
		publicID, threadID, namespace).Scan(&ref.internalID, &ref.digest, &ref.key)
	if errors.Is(err, pgx.ErrNoRows) {
		return checkpointRef{}, false, nil
	}
	if err != nil {
		return checkpointRef{}, false, err
	}
	return ref, true, nil
}

// resolveCheckpoint maps a public checkpoint id to its internal storage id
// inside one (thread, namespace); a foreign or missing id is NOT_FOUND.
func resolveCheckpoint(ctx context.Context, tx pgx.Tx, threadID, namespace, publicID string) (string, error) {
	if publicID == "" {
		return "", nil
	}
	ref, found, err := findCheckpointByPublicID(ctx, tx, threadID, namespace, publicID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", notFound("checkpoint")
	}
	return ref.internalID, nil
}

// loadCheckpoint reads one checkpoint header plus its part references of one
// thread namespace by INTERNAL id: the exact row when given, otherwise the
// newest checkpoint of the namespace. Both the id and the parent reported to
// the caller are public ids; the internal storage id is never exposed. The
// caller has already verified the thread scope.
func loadCheckpoint(ctx context.Context, tx pgx.Tx, threadID, namespace, internalID string) (Checkpoint, error) {
	var (
		header Checkpoint
		meta   string
	)
	err := tx.QueryRow(ctx, checkpointHeaderSQL, threadID, namespace, internalID).
		Scan(&internalID, &header.ID, &header.Namespace, &header.Parent,
			&header.Version, &meta, &header.MetadataEncoding, &header.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Checkpoint{}, notFound("checkpoint")
	}
	if err != nil {
		return Checkpoint{}, err
	}
	header.ThreadID = threadID
	header.Metadata = json.RawMessage(meta)
	parts, err := checkpointParts(ctx, tx, internalID)
	if err != nil {
		return Checkpoint{}, err
	}
	header.Parts = parts
	header.CreatedAt = utcOf(header.CreatedAt)
	return header, nil
}

// checkpointParts returns the part references of one checkpoint (internal id)
// ordered by channel. Reads never return blob bytes: only channel and version.
func checkpointParts(ctx context.Context, tx pgx.Tx, internalID string) ([]Part, error) {
	rows, err := tx.Query(ctx, `
SELECT channel, version FROM ahp_state.checkpoint_parts
 WHERE checkpoint_id = $1 ORDER BY channel`, internalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	parts := []Part{}
	for rows.Next() {
		var p Part
		if err := rows.Scan(&p.Channel, &p.Version); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

// checkpointPartsByIDs loads the part references of many checkpoints (internal
// ids) with one bounded query, grouped by internal id and ordered by channel.
func checkpointPartsByIDs(ctx context.Context, tx pgx.Tx, ids []string) (map[string][]Part, error) {
	out := make(map[string][]Part, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := tx.Query(ctx, `
SELECT checkpoint_id, channel, version FROM ahp_state.checkpoint_parts
 WHERE checkpoint_id = ANY($1)
 ORDER BY checkpoint_id, channel`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cpID string
			p    Part
		)
		if err := rows.Scan(&cpID, &p.Channel, &p.Version); err != nil {
			return nil, err
		}
		out[cpID] = append(out[cpID], p)
	}
	return out, rows.Err()
}

// GetPart returns the stored blob of one (thread, namespace, channel, version).
// The read is scope-checked and never leaks foreign blobs.
func (s *Store) GetPart(ctx context.Context, scope contracts.Scope, threadID, namespace, channel, version string) ([]byte, error) {
	out, err := s.GetPartEncoded(ctx, scope, threadID, namespace, channel, version)
	return out.Data, err
}

// EncodedPayload keeps the codec separate from arbitrary pre-upgrade bytes.
type EncodedPayload struct {
	Data     []byte `json:"data"`
	Encoding string `json:"encoding,omitempty"`
}

// GetPartEncoded returns bytes and the durable codec under the same scope check.
func (s *Store) GetPartEncoded(ctx context.Context, scope contracts.Scope, threadID, namespace, channel, version string) (EncodedPayload, error) {
	if err := scope.Validate(); err != nil {
		return EncodedPayload{}, err
	}
	if err := validIdentifier(threadID, "thread id"); err != nil {
		return EncodedPayload{}, err
	}
	if err := validIdentifier(channel, "channel"); err != nil {
		return EncodedPayload{}, err
	}
	if err := validPartVersion(version); err != nil {
		return EncodedPayload{}, err
	}
	var out EncodedPayload
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getThread(ctx, tx, scope, threadID, ""); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `
SELECT data, encoding FROM ahp_state.checkpoint_blobs
 WHERE thread_id = $1 AND namespace = $2 AND channel = $3 AND version = $4`,
			threadID, namespace, channel, version).Scan(&out.Data, &out.Encoding)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return EncodedPayload{}, notFound("checkpoint blob")
	}
	if err != nil {
		return EncodedPayload{}, err
	}
	return out, nil
}

// PutWrites records pending writes for one checkpoint of one thread namespace
// under the current lease. The checkpoint is addressed by public id and is
// bound to the leased thread and request namespace; the header row does not
// have to exist yet, because the same checkpoint id resolves to the same
// deterministic internal id (see the LangGraph put_writes/put race). (checkpoint,
// task, index) is unique: replaying the identical write is a no-op, a differing
// write under an existing key fails with KEY_REUSE_MISMATCH.
func (s *Store) PutWrites(ctx context.Context, scope contracts.Scope, f Fence, namespace, checkpointID string, writes []PendingWrite) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if len(namespace) > maxNamespaceBytes {
		return invalid("checkpoint namespace exceeds 512 bytes")
	}
	if err := validIdentifier(checkpointID, "checkpoint id"); err != nil {
		return err
	}
	seen := make(map[[2]string]struct{}, len(writes))
	total := 0
	for _, w := range writes {
		if err := validatePayloadEncoding(w.Encoding, w.Data); err != nil {
			return err
		}
		if err := validIdentifier(w.Task, "task"); err != nil {
			return err
		}
		if err := validIdentifier(w.Channel, "channel"); err != nil {
			return err
		}
		// Signed int32 indexes are legal, including the official LangGraph
		// WRITES_IDX_MAP specials -1..-4.
		if w.Index < math.MinInt32 || w.Index > math.MaxInt32 {
			return invalid("write index must fit a signed int32")
		}
		if len(w.Data) > MaxCheckpointPartBytes {
			return invalid("pending write exceeds 1MiB")
		}
		total += len(w.Data)
		k := [2]string{w.Task, strconv.FormatInt(int64(w.Index), 10)}
		if _, dup := seen[k]; dup {
			return invalid("duplicate pending write for the same task and index")
		}
		seen[k] = struct{}{}
	}
	if total > MaxCheckpointBatchBytes {
		return invalid("pending write batch exceeds 8MiB")
	}
	ctx, err := checkpointReferenceContext(ctx, scope, nil, writes)
	if err != nil {
		return err
	}
	return s.withLease(ctx, scope, f.RunID, f.Worker, f.Epoch, func(tx pgx.Tx, l *Lease) error {
		// The internal id is a deterministic digest of (thread, namespace,
		// public id), so a pending write can be stored before its checkpoint
		// header is flushed. The LangGraph executor races put_writes ahead of
		// put for the same checkpoint; requiring the header to exist first
		// rejected those legitimate writes. The thread is taken from the lease,
		// so the write is bound to the leased thread's scope and cascades with
		// it, even if the header never arrives (client crash between writes).
		internalID, err := internalCheckpointID(l.Thread.ID, namespace, checkpointID)
		if err != nil {
			return err
		}
		for _, w := range writes {
			var (
				channel  string
				encoding string
				data     []byte
			)
			err := tx.QueryRow(ctx, `
SELECT channel, data, encoding FROM ahp_state.checkpoint_writes
 WHERE checkpoint_id = $1 AND task = $2 AND idx = $3`,
				internalID, w.Task, w.Index).Scan(&channel, &data, &encoding)
			switch {
			case err == nil:
				if channel != w.Channel || string(data) != string(w.Data) || encoding != w.Encoding {
					return contracts.NewError(contracts.ErrKeyReuse,
						"pending write key reused with a different body")
				}
				continue
			case !errors.Is(err, pgx.ErrNoRows):
				return err
			}
			// Data == nil means the empty byte string: the column is NOT NULL,
			// so the nil must be materialised as a real empty slice.
			data = w.Data
			if err := markCheckpointPublication(ctx, w.Encoding, data); err != nil {
				return err
			}
			if data == nil {
				data = []byte{}
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO ahp_state.checkpoint_writes (checkpoint_id, thread_id, task, idx, channel, data, encoding)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				internalID, l.Thread.ID, w.Task, w.Index, w.Channel, data, w.Encoding); err != nil {
				if isUniqueViolation(err) {
					return contracts.NewError(contracts.ErrKeyReuse,
						"pending write key reused concurrently")
				}
				return err
			}
		}
		return nil
	})
}

// GetWrites returns the pending writes of one checkpoint of one thread
// namespace ordered by task and index. The read is scope-checked; a foreign
// checkpoint is NOT_FOUND.
func (s *Store) GetWrites(ctx context.Context, scope contracts.Scope, threadID, namespace, checkpointID string) ([]PendingWrite, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if err := validIdentifier(threadID, "thread id"); err != nil {
		return nil, err
	}
	if len(namespace) > maxNamespaceBytes {
		return nil, invalid("checkpoint namespace exceeds 512 bytes")
	}
	if err := validIdentifier(checkpointID, "checkpoint id"); err != nil {
		return nil, err
	}
	var out []PendingWrite
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getThread(ctx, tx, scope, threadID, ""); err != nil {
			return err
		}
		// Resolve the write bucket by the same deterministic id PutWrites used,
		// so a read sees writes recorded before the header was flushed. The
		// thread scope was already verified by getThread above, so a foreign
		// thread is reported NOT_FOUND before this point.
		internalID, err := internalCheckpointID(threadID, namespace, checkpointID)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
SELECT task, idx, channel, data, encoding FROM ahp_state.checkpoint_writes
 WHERE checkpoint_id = $1 ORDER BY task, idx`, internalID)
		if err != nil {
			return err
		}
		defer rows.Close()
		out = []PendingWrite{}
		for rows.Next() {
			var w PendingWrite
			if err := rows.Scan(&w.Task, &w.Index, &w.Channel, &w.Data, &w.Encoding); err != nil {
				return err
			}
			out = append(out, w)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Task != out[j].Task {
			return out[i].Task < out[j].Task
		}
		return out[i].Index < out[j].Index
	})
	return out, nil
}

// insertCheckpointParts stores blob data and creates part-reference rows for a
// checkpoint under construction inside a transaction. It is the shared helper
// used by both PutCheckpoint and PutBatch.
func insertCheckpointParts(ctx context.Context, tx pgx.Tx, threadID, namespace, checkpointID string, parts []Part) error {
	for _, p := range parts {
		if p.Data == nil {
			var ok bool
			if err := tx.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM ahp_state.checkpoint_blobs
                WHERE thread_id = $1 AND namespace = $2 AND channel = $3 AND version = $4)`,
				threadID, namespace, p.Channel, p.Version).Scan(&ok); err != nil {
				return err
			}
			if !ok {
				return notFound("referenced checkpoint blob")
			}
		} else {
			tag, err := tx.Exec(ctx, `
INSERT INTO ahp_state.checkpoint_blobs (thread_id, namespace, channel, version, data, encoding)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (thread_id, namespace, channel, version) DO NOTHING`,
				threadID, namespace, p.Channel, p.Version, p.Data, p.Encoding)
			if err != nil {
				return err
			}
			if tag.RowsAffected() > 0 {
				if err := markCheckpointPublication(ctx, p.Encoding, p.Data); err != nil {
					return err
				}
			}
			var stored []byte
			var encoding string
			if err := tx.QueryRow(ctx, `
SELECT data, encoding FROM ahp_state.checkpoint_blobs
 WHERE thread_id = $1 AND namespace = $2 AND channel = $3 AND version = $4`,
				threadID, namespace, p.Channel, p.Version).Scan(&stored, &encoding); err != nil {
				return err
			}
			if string(stored) != string(p.Data) || encoding != p.Encoding {
				return contracts.NewError(contracts.ErrKeyReuse,
					"channel version already stores different data")
			}
		}
	}
	for _, p := range parts {
		if _, err := tx.Exec(ctx, `
INSERT INTO ahp_state.checkpoint_parts (checkpoint_id, channel, version)
VALUES ($1, $2, $3)`, checkpointID, p.Channel, p.Version); err != nil {
			return err
		}
	}
	return nil
}
