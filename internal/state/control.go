package state

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ControlExport is the serialized thread representation for export/import.
// It is a faithful snapshot: runs, every checkpoint of every namespace (with
// the blob bytes of each part inlined so the snapshot is self-contained), the
// pending writes recorded against each checkpoint, the ordered stream and the
// inbox. Importing it into another scope reproduces all of that without any
// agent runtime hooks.
type ControlExport struct {
	Thread      Thread           `json:"thread"`
	Runs        []Run            `json:"runs"`
	Checkpoints []Checkpoint     `json:"checkpoints,omitempty"`
	Writes      []ExportedWrites `json:"writes,omitempty"`
	Stream      []StreamEvent    `json:"stream,omitempty"`
	Inbox       []InboxEvent     `json:"inbox,omitempty"`
	ExportedAt  time.Time        `json:"exported_at"`
}

// ExportedWrites carries the pending writes of one checkpoint. A checkpoint is
// addressed by (namespace, checkpoint id) because public ids are unique only
// within a (thread, namespace).
type ExportedWrites struct {
	Namespace    string         `json:"namespace"`
	CheckpointID string         `json:"checkpoint_id"`
	Writes       []PendingWrite `json:"writes"`
}

const legalholdSchema = `
CREATE TABLE IF NOT EXISTS ahp_state.legalhold (
  tenant      text NOT NULL,
  subject     text NOT NULL DEFAULT '',
  thread_id   text NOT NULL DEFAULT '',
  reason      text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant, subject, thread_id)
);
`

// MigrateControl creates control-plane tables. Idempotent.
// MigrateControl applies the control DDL on the store's own pool. Migrate calls the same body on
// the connection that already holds the migrate advisory lock, which is what
// keeps a MaxConns=1 pool from deadlocking at startup.
func (s *Store) MigrateControl(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return invalid("state: store is not initialised")
	}
	return s.migrateControl(ctx, s.pool)
}

func (s *Store) migrateControl(ctx context.Context, ex ddlExec) error {
	_, err := ex.Exec(ctx, legalholdSchema+deletionSchema+controlDeletionSchema+subjectFenceSchema)
	return err
}

// DeleteThread deletes a thread and all cascading data (runs, checkpoints,
// stream events, inbox events, intents). If the thread is under legalhold,
// the operation is denied.
func (s *Store) DeleteThread(ctx context.Context, scope contracts.Scope, threadID string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var held bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM ahp_state.legalhold
			 WHERE tenant = $1 AND (subject = '' OR subject = $2) AND (thread_id = '' OR thread_id = $3))`,
			scope.Tenant, scope.Subject, threadID).Scan(&held); err != nil {
			return err
		}
		if held {
			return contracts.NewError(contracts.ErrForbidden, "thread is under legalhold")
		}
		thread, err := scanThread(tx.QueryRow(ctx, `SELECT `+threadColumns+` FROM ahp_state.threads WHERE id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4 FOR UPDATE`, threadID, scope.Tenant, scope.Deployment, scope.Subject))
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("thread")
		}
		if err != nil {
			return err
		}
		if err = s.recordDeletion(ctx, tx, scope, "DeleteThread", threadID, []Thread{thread}, 0); err != nil {
			return err
		}
		// Receipts contain values too. They have no FK to threads, so removing
		// the KV row alone would leave a recoverable copy of deleted session data.
		if _, err := tx.Exec(ctx, `DELETE FROM ahp_state.kv_ops
			WHERE tenant=$1 AND deployment=$2 AND subject=$3
			  AND (result->'item'->>'thread_id'=$4 OR result->>'actor_thread_id'=$4)`,
			scope.Tenant, scope.Deployment, scope.Subject, threadID); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx,
			`DELETE FROM ahp_state.threads WHERE id = $1 AND tenant = $2 AND deployment = $3 AND subject = $4`,
			threadID, scope.Tenant, scope.Deployment, scope.Subject)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return contracts.NewError(contracts.ErrNotFound, "thread not found")
		}
		return nil
	})
}

// DeleteSubject deletes all threads belonging to a subject within scope.
// Threads under legalhold are skipped.
func (s *Store) DeleteSubject(ctx context.Context, scope contracts.Scope) (int64, error) {
	return s.DeleteSubjectWithKey(ctx, scope, contracts.NewID())
}

// SetLegalhold marks a scope-level legalhold. If threadID is empty, the hold
// covers all threads for the subject.
func (s *Store) SetLegalhold(ctx context.Context, scope contracts.Scope, threadID, reason string) error {
	subject := scope.Subject
	if subject == "" {
		return contracts.NewError(contracts.ErrInvalid, "subject required for legalhold")
	}
	if reason == "" {
		return contracts.NewError(contracts.ErrInvalid, "reason is required")
	}
	release, err := s.LockSubjectControl(ctx, scope.Tenant, scope.Subject)
	if err != nil {
		return err
	}
	defer release()
	_, err = s.pool.Exec(ctx,
		`INSERT INTO ahp_state.legalhold (tenant, subject, thread_id, reason)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (tenant, subject, thread_id) DO UPDATE SET reason = EXCLUDED.reason`,
		scope.Tenant, subject, threadID, reason)
	return err
}

// RemoveLegalhold removes a legalhold entry.
func (s *Store) RemoveLegalhold(ctx context.Context, scope contracts.Scope, threadID string) error {
	release, err := s.LockSubjectControl(ctx, scope.Tenant, scope.Subject)
	if err != nil {
		return err
	}
	defer release()
	_, err = s.pool.Exec(ctx,
		`DELETE FROM ahp_state.legalhold WHERE tenant = $1 AND subject = $2 AND thread_id = $3`,
		scope.Tenant, scope.Subject, threadID)
	return err
}

// ExportThread serializes the thread with all its associated data: runs, every
// checkpoint of every namespace with the bytes of each part inlined, the
// pending writes of each checkpoint, the ordered stream and the inbox. Reads
// never return blob bytes elsewhere, so export is the one place that collects
// them on purpose: the snapshot must be portable to another deployment without
// reaching back into the source database.
func (s *Store) ExportThread(ctx context.Context, scope contracts.Scope, threadID string) (ControlExport, error) {
	var exp ControlExport
	if err := scope.Validate(); err != nil {
		return exp, asInvalid(err)
	}
	if err := validIdentifier(threadID, "thread id"); err != nil {
		return exp, err
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// One snapshot across all thread entities.
		if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`); err != nil {
			return err
		}
		th, err := s.getThread(ctx, tx, scope, threadID, "")
		if err != nil {
			return err
		}
		exp.Thread = th
		if err := s.refuseCommonExport(ctx, tx, scope, threadID); err != nil {
			return err
		}

		runs, err := tx.Query(ctx, `SELECT `+runColumns+`
			FROM ahp_state.runs WHERE thread_id = $1 ORDER BY sequence`, threadID)
		if err != nil {
			return err
		}
		exp.Runs = []Run{}
		for runs.Next() {
			r, err := scanRun(runs)
			if err != nil {
				runs.Close()
				return err
			}
			exp.Runs = append(exp.Runs, r)
		}
		if err := runs.Err(); err != nil {
			runs.Close()
			return err
		}
		runs.Close()

		cpRows, err := tx.Query(ctx, `
SELECT c.id, COALESCE(c.external_id, c.id), c.namespace,
       COALESCE(p.external_id, p.id, ''), c.version, c.metadata::text, c.metadata_encoding, c.created_at
  FROM ahp_state.checkpoints c
  LEFT JOIN ahp_state.checkpoints par ON par.id = c.parent_id
  LEFT JOIN ahp_state.checkpoints p ON p.id = par.id
 WHERE c.thread_id = $1
 ORDER BY c.namespace, c.created_at, c.id`, threadID)
		if err != nil {
			return err
		}
		type cpKey struct{ internal, namespace, publicID string }
		var keys []cpKey
		var headers []Checkpoint
		exp.Checkpoints = []Checkpoint{}
		exp.Writes = []ExportedWrites{}
		for cpRows.Next() {
			var (
				internal, publicID, namespace, parent string
				version                               int64
				meta, encoding                        string
				createdAt                             time.Time
			)
			if err := cpRows.Scan(&internal, &publicID, &namespace, &parent,
				&version, &meta, &encoding, &createdAt); err != nil {
				cpRows.Close()
				return err
			}
			cp := Checkpoint{
				ID:               publicID,
				ThreadID:         threadID,
				Namespace:        namespace,
				Parent:           parent,
				Version:          version,
				Metadata:         json.RawMessage(meta),
				MetadataEncoding: encoding,
				CreatedAt:        utcOf(createdAt),
			}
			keys = append(keys, cpKey{internal, namespace, publicID})
			headers = append(headers, cp)
		}
		if err := cpRows.Err(); err != nil {
			cpRows.Close()
			return err
		}
		cpRows.Close()

		for i, k := range keys {
			parts, err := checkpointParts(ctx, tx, k.internal)
			if err != nil {
				return err
			}
			for j := range parts {
				var data []byte
				if err := tx.QueryRow(ctx, `
SELECT data, encoding FROM ahp_state.checkpoint_blobs
 WHERE thread_id = $1 AND namespace = $2 AND channel = $3 AND version = $4`,
					threadID, k.namespace, parts[j].Channel, parts[j].Version).Scan(&data, &parts[j].Encoding); err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						// A dangling reference should never exist; export it as
						// an empty blob rather than aborting the whole thread.
						parts[j].Data = []byte{}
						continue
					}
					return err
				}
				parts[j].Data = data
			}
			headers[i].Parts = parts

			writes, err := exportCheckpointWrites(ctx, tx, k.internal)
			if err != nil {
				return err
			}
			if len(writes) > 0 {
				exp.Writes = append(exp.Writes, ExportedWrites{
					Namespace: k.namespace, CheckpointID: k.publicID, Writes: writes,
				})
			}
			exp.Checkpoints = append(exp.Checkpoints, headers[i])
		}

		if exp.Stream, err = exportStream(ctx, tx, threadID); err != nil {
			return err
		}
		if exp.Inbox, err = exportInbox(ctx, tx, threadID); err != nil {
			return err
		}
		exp.ExportedAt = time.Now().UTC()
		return nil
	})
	if err != nil {
		return ControlExport{}, err
	}
	return exp, nil
}

// exportCheckpointWrites reads the pending writes of one checkpoint (internal
// id) inside the caller's transaction.
func exportCheckpointWrites(ctx context.Context, tx pgx.Tx, internalID string) ([]PendingWrite, error) {
	rows, err := tx.Query(ctx, `
SELECT task, idx, channel, data, encoding FROM ahp_state.checkpoint_writes
 WHERE checkpoint_id = $1 ORDER BY task, idx`, internalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingWrite
	for rows.Next() {
		var w PendingWrite
		if err := rows.Scan(&w.Task, &w.Index, &w.Channel, &w.Data, &w.Encoding); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// Stable import reasons. The HTTP class of an import refusal is fixed by its
// code, so the reason is what lets a client tell "these bytes belong to someone
// else" from "that context_id is taken" from "you did not retarget".
const (
	ReasonImportScopeMismatch = "EXPORT_SCOPE_MISMATCH"
	ReasonImportContextTaken  = "IMPORT_CONTEXT_TAKEN"
)

// ImportThread re-materialises an export as a NEW thread owned by the caller.
// It is a control-plane bulk operation: no lease fence, no agent runtime hooks.
// Runs, checkpoints (with blob bytes, part references, parent links and pending
// writes) are re-created and the exported stream is re-enqueued as inbox events.
//
// Two rules hold the identity of the plane together, because export bytes
// travel by definition (PDF rev3.2: hand-off between drivers):
//
//   - The export carries the scope it came from in `thread.scope`. Importing
//     into a different tenant or a different subject is refused: the caller
//     would otherwise materialise a durable clone of someone else's thread
//     under their own identity. Migration across deployments of the same
//     identity is what the operation is for, so deployment is not compared.
//   - The target context_id must be unowned inside (tenant, deployment), across
//     subjects. Context ids are how the human edge resolves threads, so two
//     subjects materialising the same context_id is an identity rewire on
//     ingress. Restoring into a context the caller already owns therefore takes
//     an explicit `contextID` (retarget); the thread id is always fresh.
func (s *Store) ImportThread(ctx context.Context, scope contracts.Scope, exp ControlExport, contextID string) (Thread, error) {
	if err := scope.Validate(); err != nil {
		return Thread{}, asInvalid(err)
	}
	origin := exp.Thread.Scope
	if origin.Tenant != scope.Tenant || origin.Subject != scope.Subject {
		return Thread{}, contracts.NewReasoned(contracts.ErrForbidden, ReasonImportScopeMismatch,
			"the export was produced by another identity: import refuses to materialise it under this one")
	}
	target := contextID
	if target == "" {
		target = exp.Thread.ContextID
	}
	if err := validIdentifier(target, "context id"); err != nil {
		return Thread{}, asInvalid(err)
	}
	// Format refusal must happen before CreateThread/CreateRun commits. Keep
	// legacy opaque payloads untouched and leave artifact authority to its owner.
	for _, cp := range exp.Checkpoints {
		if err := validatePayloadEncoding(cp.MetadataEncoding, cp.Metadata); err != nil {
			return Thread{}, err
		}
		for _, part := range cp.Parts {
			if err := validatePayloadEncoding(part.Encoding, part.Data); err != nil {
				return Thread{}, err
			}
		}
	}
	for _, bucket := range exp.Writes {
		for _, write := range bucket.Writes {
			if err := validatePayloadEncoding(write.Encoding, write.Data); err != nil {
				return Thread{}, err
			}
		}
	}
	if err := s.preflightImportReferences(ctx, scope, exp, target); err != nil {
		return Thread{}, err
	}
	taken, err := s.contextTakenInDeployment(ctx, scope.Tenant, scope.Deployment, target)
	if err != nil {
		return Thread{}, err
	}
	if taken {
		return Thread{}, contracts.NewReasoned(contracts.ErrConflict, ReasonImportContextTaken,
			"this deployment already owns the target context_id: import would rewire an existing identity or merge two histories into one thread")
	}
	th, err := s.CreateThread(ctx, scope, target,
		"import-"+exp.Thread.ID+"-"+contracts.NewID())
	if err != nil {
		return Thread{}, err
	}
	for _, r := range exp.Runs {
		if _, err := s.CreateRun(ctx, scope, th.ID, r.TaskID, "import-"+r.ID, r.Payload); err != nil {
			return Thread{}, err
		}
	}
	if err := s.importCheckpoints(ctx, scope, th.ID, exp); err != nil {
		return Thread{}, err
	}
	if err := s.importEvents(ctx, scope, th.ID, exp); err != nil {
		return Thread{}, err
	}
	return th, nil
}

// importCheckpoints inserts every exported checkpoint of an import under the
// new thread id, lease-free, in a single transaction. Blob bytes come from the
// inlined part data; parent links are rewired by the deterministic internal id
// of the parent public id inside the same namespace.
func (s *Store) importCheckpoints(ctx context.Context, scope contracts.Scope, threadID string, exp ControlExport) error {
	if len(exp.Checkpoints) == 0 {
		return nil
	}
	writesByKey := make(map[[2]string][]PendingWrite, len(exp.Writes))
	var guardedWrites []PendingWrite
	for _, w := range exp.Writes {
		writesByKey[[2]string{w.Namespace, w.CheckpointID}] = w.Writes
		guardedWrites = append(guardedWrites, w.Writes...)
	}
	ctx, err := checkpointReferenceContext(ctx, scope, exp.Checkpoints, guardedWrites)
	if err != nil {
		return err
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		// Pass 1: materialise blobs (every part carries its bytes in an export).
		for _, cp := range exp.Checkpoints {
			if _, _, err := validateCheckpointRequest(&cp); err != nil {
				return err
			}
			for _, p := range cp.Parts {
				data := p.Data
				if data == nil {
					data = []byte{}
				}
				tag, err := tx.Exec(ctx, `
INSERT INTO ahp_state.checkpoint_blobs (thread_id, namespace, channel, version, data, encoding)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (thread_id, namespace, channel, version) DO NOTHING`,
					threadID, cp.Namespace, p.Channel, p.Version, data, p.Encoding)
				if err != nil {
					return err
				}
				if tag.RowsAffected() > 0 {
					if err := markCheckpointPublication(ctx, p.Encoding, data); err != nil {
						return err
					}
				}
				var stored EncodedPayload
				if err := tx.QueryRow(ctx, `SELECT data,encoding FROM ahp_state.checkpoint_blobs WHERE thread_id=$1 AND namespace=$2 AND channel=$3 AND version=$4`, threadID, cp.Namespace, p.Channel, p.Version).Scan(&stored.Data, &stored.Encoding); err != nil {
					return err
				}
				if string(stored.Data) != string(data) || stored.Encoding != p.Encoding {
					return contracts.NewError(contracts.ErrKeyReuse, "import channel version already stores different data or encoding")
				}
			}
		}
		// Pass 2: insert headers without parents, so parent rows always exist
		// before the FK-bearing update.
		for _, cp := range exp.Checkpoints {
			internal, err := internalCheckpointID(threadID, cp.Namespace, cp.ID)
			if err != nil {
				return err
			}
			metadata := cp.Metadata
			if len(metadata) == 0 {
				metadata = json.RawMessage("null")
			}
			digest, err := checkpointFingerprintV2(cp, cp.Parts)
			if err != nil {
				return err
			}
			key := "import:" + internal
			tag, err := tx.Exec(ctx, `
INSERT INTO ahp_state.checkpoints
	(id, external_id, thread_id, namespace, parent_id, version, metadata, body_digest, key, metadata_encoding)
VALUES ($1, $2, $3, $4, NULL, $5, $6::jsonb, $7, $8, $9)
ON CONFLICT (id) DO NOTHING`,
				internal, cp.ID, threadID, cp.Namespace, cp.Version,
				string(metadata), digest, key, cp.MetadataEncoding)
			if err != nil {
				return err
			}
			if tag.RowsAffected() > 0 {
				if err := markCheckpointPublication(ctx, cp.MetadataEncoding, metadata); err != nil {
					return err
				}
			}
			for _, p := range cp.Parts {
				if _, err := tx.Exec(ctx, `
INSERT INTO ahp_state.checkpoint_parts (checkpoint_id, channel, version)
VALUES ($1, $2, $3)
ON CONFLICT (checkpoint_id, channel) DO NOTHING`,
					internal, p.Channel, p.Version); err != nil {
					return err
				}
			}
			for _, w := range writesByKey[[2]string{cp.Namespace, cp.ID}] {
				if err := validatePayloadEncoding(w.Encoding, w.Data); err != nil {
					return err
				}
				data := w.Data
				if data == nil {
					data = []byte{}
				}
				tag, err := tx.Exec(ctx, `
INSERT INTO ahp_state.checkpoint_writes (checkpoint_id, thread_id, task, idx, channel, data, encoding)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (checkpoint_id, task, idx) DO NOTHING`,
					internal, threadID, w.Task, w.Index, w.Channel, data, w.Encoding)
				if err != nil {
					return err
				}
				if tag.RowsAffected() > 0 {
					if err := markCheckpointPublication(ctx, w.Encoding, data); err != nil {
						return err
					}
				}
			}
		}
		// Pass 3: rewire parents (public ids are namespace-scoped).
		for _, cp := range exp.Checkpoints {
			if cp.Parent == "" {
				continue
			}
			internal, err := internalCheckpointID(threadID, cp.Namespace, cp.ID)
			if err != nil {
				return err
			}
			parentInternal, err := internalCheckpointID(threadID, cp.Namespace, cp.Parent)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`UPDATE ahp_state.checkpoints SET parent_id = $2 WHERE id = $1`,
				internal, parentInternal); err != nil {
				return err
			}
		}
		return nil
	})
}

// ExportJSON serializes a ControlExport to JSON bytes.
func ExportJSON(exp ControlExport) ([]byte, error) {
	return json.Marshal(exp)
}

// ImportJSON deserializes a ControlExport from JSON bytes.
func ImportJSON(data []byte) (ControlExport, error) {
	var exp ControlExport
	err := json.Unmarshal(data, &exp)
	return exp, err
}

// exportStream reads every stream event of the thread inside the tx, ordered.
func exportStream(ctx context.Context, tx pgx.Tx, threadID string) ([]StreamEvent, error) {
	rows, err := tx.Query(ctx, `
SELECT id, thread_id, key, kind, sequence, payload FROM ahp_state.stream_events
 WHERE thread_id = $1 ORDER BY sequence`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]StreamEvent, 0)
	for rows.Next() {
		var ev StreamEvent
		if err := rows.Scan(&ev.ID, &ev.ThreadID, &ev.Key, &ev.Kind,
			&ev.Sequence, &ev.Payload); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// exportInbox reads every inbox event (acked or not) inside the tx, ordered.
func exportInbox(ctx context.Context, tx pgx.Tx, threadID string) ([]InboxEvent, error) {
	rows, err := tx.Query(ctx, `
SELECT id, thread_id, key, kind, sequence, payload, acked FROM ahp_state.inbox_events
 WHERE thread_id = $1 ORDER BY sequence`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]InboxEvent, 0)
	for rows.Next() {
		var ev InboxEvent
		if err := rows.Scan(&ev.ID, &ev.ThreadID, &ev.Key, &ev.Kind,
			&ev.Sequence, &ev.Payload, &ev.Acked); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// importEvents appends the exported stream and inbox events under the new
// thread id without any agent runtime hooks: no lease, no run reactivation.
// Original keys and sequences are preserved so the snapshot order is faithful;
// new primary ids avoid global collisions and inserts are idempotent on
// (thread_id, key) so a re-import is a no-op.
func (s *Store) importEvents(ctx context.Context, scope contracts.Scope, threadID string, exp ControlExport) error {
	if len(exp.Stream) == 0 && len(exp.Inbox) == 0 {
		return nil
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		type kindPayload struct {
			Kind    string          `json:"kind"`
			Payload json.RawMessage `json:"payload"`
		}
		for _, ev := range exp.Stream {
			digest, err := contracts.Digest(kindPayload{Kind: ev.Kind, Payload: ev.Payload})
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO ahp_state.stream_events (id, thread_id, run_id, key, kind, sequence, payload, digest)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (thread_id, key) DO NOTHING`,
				contracts.NewID(), threadID, "import", ev.Key, ev.Kind, ev.Sequence, ev.Payload, digest); err != nil {
				return err
			}
		}
		for _, ev := range exp.Inbox {
			digest, err := contracts.Digest(kindPayload{Kind: ev.Kind, Payload: ev.Payload})
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO ahp_state.inbox_events (id, thread_id, key, kind, sequence, payload, digest, acked)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (thread_id, key) DO NOTHING`,
				contracts.NewID(), threadID, ev.Key, ev.Kind, ev.Sequence, ev.Payload, digest, ev.Acked); err != nil {
				return err
			}
		}
		return nil
	})
}
