package state

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Checkpoint limits and the checkpoint wire types.
const (
	// MaxCheckpointPartBytes bounds one checkpoint part or pending write.
	MaxCheckpointPartBytes = 1 << 20
	// MaxCheckpointBatchBytes bounds one PutCheckpoint/PutWrites batch.
	MaxCheckpointBatchBytes = 8 << 20
	// maxNamespaceBytes bounds a checkpoint namespace.
	maxNamespaceBytes = 512
)

// Fence is the lease token a checkpoint writer must present: the run, worker
// and epoch of the lease that currently owns the thread.
type Fence struct {
	RunID  string `json:"run_id"`
	Worker string `json:"worker"`
	Epoch  int64  `json:"epoch"`
}

// Part is one channel value of a checkpoint. Version is an opaque nonempty
// string (<=512 bytes, no NUL) matching the LangGraph ChannelVersions contract.
// Data == nil references the existing blob of (channel, Version); a non-nil
// value (including an empty slice) is a new blob stored under Version. Encoding
// is an explicit SDK codec; empty means opaque data, including legacy prefixes.
type Part struct {
	Channel  string `json:"channel"`
	Version  string `json:"version"`
	Data     []byte `json:"data"`
	Encoding string `json:"encoding,omitempty"`
}

// Checkpoint is one immutable snapshot of a thread namespace. ID and Parent are
// PUBLIC ids chosen by the caller; the internal storage id is never exposed.
type Checkpoint struct {
	ID               string          `json:"id"`
	ThreadID         string          `json:"thread_id"`
	Namespace        string          `json:"namespace"`
	Parent           string          `json:"parent,omitempty"`
	Version          int64           `json:"version"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
	MetadataEncoding string          `json:"metadata_encoding,omitempty"`
	Parts            []Part          `json:"parts,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
}

// PendingWrite is one partial channel result recorded for a checkpoint before
// the checkpoint that finalises it is written. Index is a signed int32: the
// official LangGraph WRITES_IDX_MAP reserves the negative indexes -1..-4 for
// special writes, so negative values are legal. Data == nil is stored as the
// empty byte string (the column is NOT NULL).
type PendingWrite struct {
	Task     string `json:"task"`
	Index    int32  `json:"index"`
	Channel  string `json:"channel"`
	Data     []byte `json:"data"`
	Encoding string `json:"encoding,omitempty"`
}

// internalCheckpointID is the stable storage id of one public checkpoint id.
// Public ids are caller-chosen, so they are only unique per
// (thread, namespace); the digest scopes them so no caller-chosen id can ever
// collide with another thread or namespace at the global primary key.
func internalCheckpointID(threadID, namespace, publicID string) (string, error) {
	digest, err := contracts.Digest([]string{threadID, namespace, publicID})
	if err != nil {
		return "", asInvalid(err)
	}
	return strings.ReplaceAll(digest, ":", "_"), nil
}

// checkpointFingerprint is the legacy receipt algorithm, retained for upgrade
// replays only. Its string conversion is lossy for non-UTF8 bytes; a matching
// legacy digest MUST also pass checkpointReplayMatches' exact blob comparison.
func checkpointFingerprint(cp Checkpoint, parts []Part) (string, error) {
	type partFP struct {
		Channel  string `json:"channel"`
		Version  string `json:"version"`
		Data     string `json:"data"`
		Encoding string `json:"encoding,omitempty"`
	}
	in := struct {
		ID               string          `json:"id"`
		Namespace        string          `json:"namespace"`
		Parent           string          `json:"parent"`
		Version          int64           `json:"version"`
		Metadata         json.RawMessage `json:"metadata"`
		MetadataEncoding string          `json:"metadata_encoding,omitempty"`
		Parts            []partFP        `json:"parts"`
	}{ID: cp.ID, Namespace: cp.Namespace, Parent: cp.Parent,
		Version: cp.Version, Metadata: cp.Metadata, MetadataEncoding: cp.MetadataEncoding}
	if len(cp.Metadata) == 0 {
		in.Metadata = json.RawMessage("null")
	}
	in.Parts = make([]partFP, len(parts))
	for i, p := range parts {
		in.Parts[i] = partFP{Channel: p.Channel, Version: p.Version, Encoding: p.Encoding}
		if p.Data != nil {
			in.Parts[i].Data = string(p.Data)
		}
	}
	return contracts.Digest(in)
}

// checkpointFingerprintV2 uses JSON's binary encoding, preserving every byte
// and distinguishing a nil reference from an explicitly empty new blob.
func checkpointFingerprintV2(cp Checkpoint, parts []Part) (string, error) {
	if parts == nil {
		parts = []Part{}
	}
	metadata := cp.Metadata
	if len(metadata) == 0 {
		metadata = json.RawMessage("null")
	}
	digest, err := contracts.Digest(map[string]any{
		"id": cp.ID, "namespace": cp.Namespace, "parent": cp.Parent,
		"version": cp.Version, "metadata": metadata,
		"metadata_encoding": cp.MetadataEncoding, "parts": parts,
	})
	if err != nil {
		return "", err
	}
	return "checkpoint-v2:" + digest, nil
}

// validateCheckpointRequest checks the static shape of a PutCheckpoint request
// and returns the canonical part list plus the request fingerprint.
func validateCheckpointRequest(cp *Checkpoint) ([]Part, string, error) {
	if err := validIdentifier(cp.ID, "checkpoint id"); err != nil {
		return nil, "", err
	}
	if len(cp.Namespace) > maxNamespaceBytes {
		return nil, "", invalid("checkpoint namespace exceeds 512 bytes")
	}
	if cp.Parent != "" && cp.Parent == cp.ID {
		return nil, "", invalid("checkpoint parent must differ from the checkpoint id")
	}
	if cp.Version < 1 {
		return nil, "", invalid("checkpoint version must be positive")
	}
	if len(cp.Metadata) > 0 && !json.Valid(cp.Metadata) {
		return nil, "", invalid("checkpoint metadata must be valid JSON")
	}
	if err := validatePayloadEncoding(cp.MetadataEncoding, cp.Metadata); err != nil {
		return nil, "", err
	}
	parts := make([]Part, len(cp.Parts))
	seen := make(map[string]struct{}, len(cp.Parts))
	total := 0
	for i, p := range cp.Parts {
		if err := validatePayloadEncoding(p.Encoding, p.Data); err != nil {
			return nil, "", err
		}
		if err := validIdentifier(p.Channel, "channel"); err != nil {
			return nil, "", err
		}
		if err := validPartVersion(p.Version); err != nil {
			return nil, "", err
		}
		if _, dup := seen[p.Channel]; dup {
			return nil, "", invalid("channel " + p.Channel + " appears more than once")
		}
		seen[p.Channel] = struct{}{}
		if len(p.Data) > MaxCheckpointPartBytes {
			return nil, "", invalid("part data exceeds 1MiB")
		}
		total += len(p.Data)
		parts[i] = p
	}
	if total > MaxCheckpointBatchBytes {
		return nil, "", invalid("checkpoint batch exceeds 8MiB")
	}
	digest, err := checkpointFingerprintV2(*cp, parts)
	if err != nil {
		return nil, "", asInvalid(err)
	}
	return parts, digest, nil
}

// PutCheckpoint stores one immutable checkpoint and its part references under
// the current lease. The header, the new blobs and the part references are
// applied atomically. The public id is scoped by the leased thread and the
// request namespace, so the same public id may exist in every other scope.
// Replaying the identical request under the same key returns the original
// checkpoint; a different body under that key fails with KEY_REUSE_MISMATCH,
// and so does reusing an existing version with different data. A foreign
// thread or parent is reported NOT_FOUND.
func (s *Store) PutCheckpoint(ctx context.Context, scope contracts.Scope, f Fence, cp Checkpoint, key string) (Checkpoint, error) {
	if err := scope.Validate(); err != nil {
		return Checkpoint{}, err
	}
	if err := validIdentifier(key, "idempotency key"); err != nil {
		return Checkpoint{}, err
	}
	parts, digest, err := validateCheckpointRequest(&cp)
	if err != nil {
		return Checkpoint{}, err
	}
	ctx, err = checkpointReferenceContext(ctx, scope, []Checkpoint{cp}, nil)
	if err != nil {
		return Checkpoint{}, err
	}
	metadata := cp.Metadata
	if len(metadata) == 0 {
		metadata = json.RawMessage("null")
	}
	var out Checkpoint
	err = s.withLease(ctx, scope, f.RunID, f.Worker, f.Epoch, func(tx pgx.Tx, l *Lease) error {
		threadID := l.Thread.ID
		// The storage id is deterministic in (thread, namespace, public id), so
		// it can only be computed once the leased thread is known.
		scopedID, err := internalCheckpointID(threadID, cp.Namespace, cp.ID)
		if err != nil {
			return err
		}

		var (
			eID     string
			eDigest string
		)
		qerr := tx.QueryRow(ctx, `
SELECT id, body_digest
  FROM ahp_state.checkpoints
 WHERE thread_id = $1 AND namespace = $2 AND key = $3`,
			threadID, cp.Namespace, key).Scan(&eID, &eDigest)
		if qerr == nil {
			matches, err := checkpointReplayMatches(ctx, tx, threadID, cp, parts, eDigest, digest)
			if err != nil {
				return err
			}
			if !matches {
				return contracts.NewError(contracts.ErrKeyReuse,
					"checkpoint key was already used with a different body")
			}
			loaded, err := loadCheckpoint(ctx, tx, threadID, cp.Namespace, eID)
			if err != nil {
				return err
			}
			out = loaded
			return nil
		}
		if !errors.Is(qerr, pgx.ErrNoRows) {
			return qerr
		}

		// The public id is immutable and unique per (thread, namespace): an
		// existing id replays only when the body is byte-identical; a differing
		// body under its original key is KEY_REUSE, under a fresh key CONFLICT.
		existing, found, err := findCheckpointByPublicID(ctx, tx, threadID, cp.Namespace, cp.ID)
		if err != nil {
			return err
		}
		if found {
			idDigest, idKey := existing.digest, existing.key
			matches, err := checkpointReplayMatches(ctx, tx, threadID, cp, parts, idDigest, digest)
			if err != nil {
				return err
			}
			if matches {
				loaded, err := loadCheckpoint(ctx, tx, threadID, cp.Namespace, existing.internalID)
				if err != nil {
					return err
				}
				out = loaded
				return nil
			}
			if idKey == key {
				return contracts.NewError(contracts.ErrKeyReuse,
					"checkpoint key was already used with a different body")
			}
			return contracts.NewError(contracts.ErrConflict,
				"checkpoint id already exists in this thread namespace")
		}

		parentInternal := ""
		if cp.Parent != "" {
			resolved, err := resolveCheckpoint(ctx, tx, threadID, cp.Namespace, cp.Parent)
			if err != nil {
				return err
			}
			parentInternal = resolved
		}

		if err := markCheckpointPublication(ctx, cp.MetadataEncoding, metadata); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO ahp_state.checkpoints
	(id, external_id, thread_id, namespace, parent_id, version, metadata, body_digest, key, metadata_encoding)
VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6, $7::jsonb, $8, $9, $10)`,
			scopedID, cp.ID, threadID, cp.Namespace, parentInternal,
			cp.Version, string(metadata), digest, key, cp.MetadataEncoding); err != nil {
			if isUniqueViolation(err) {
				return contracts.NewError(contracts.ErrConflict,
					"checkpoint id or key was taken concurrently")
			}
			return err
		}

		if err := insertCheckpointParts(ctx, tx, threadID, cp.Namespace, scopedID, parts); err != nil {
			return err
		}

		loaded, err := loadCheckpoint(ctx, tx, threadID, cp.Namespace, scopedID)
		if err != nil {
			return err
		}
		out = loaded
		return nil
	})
	if err != nil {
		return Checkpoint{}, err
	}
	return out, nil
}

// GetCheckpoint reads one checkpoint of a thread namespace: the exact public id
// when given, otherwise the most recently created checkpoint. Foreign threads
// and checkpoints are reported NOT_FOUND.
func (s *Store) GetCheckpoint(ctx context.Context, scope contracts.Scope, threadID, namespace, id string) (Checkpoint, error) {
	if err := scope.Validate(); err != nil {
		return Checkpoint{}, err
	}
	if err := validIdentifier(threadID, "thread id"); err != nil {
		return Checkpoint{}, err
	}
	if len(namespace) > maxNamespaceBytes {
		return Checkpoint{}, invalid("checkpoint namespace exceeds 512 bytes")
	}
	if id != "" {
		if err := validIdentifier(id, "checkpoint id"); err != nil {
			return Checkpoint{}, err
		}
	}
	var out Checkpoint
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getThread(ctx, tx, scope, threadID, ""); err != nil {
			return err
		}
		internalID := ""
		if id != "" {
			resolved, err := resolveCheckpoint(ctx, tx, threadID, namespace, id)
			if err != nil {
				return err
			}
			internalID = resolved
		}
		loaded, err := loadCheckpoint(ctx, tx, threadID, namespace, internalID)
		if err != nil {
			return err
		}
		out = loaded
		return nil
	})
	if err != nil {
		return Checkpoint{}, err
	}
	return out, nil
}

// maxCheckpointList is the hard cap of one ListCheckpoints call.
const maxCheckpointList = 1000

// ListCheckpoints returns full checkpoint snapshots (headers plus part
// references) of one thread namespace, newest first, capped at 1000 entries.
// The scan is bounded by LIMIT and part references are loaded with one query
// over exactly the returned internal ids; the thread scope is enforced first.
// Every returned id and parent is a public id.
func (s *Store) ListCheckpoints(ctx context.Context, scope contracts.Scope, threadID, namespace string, limit int) ([]Checkpoint, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if err := validIdentifier(threadID, "thread id"); err != nil {
		return nil, err
	}
	if len(namespace) > maxNamespaceBytes {
		return nil, invalid("checkpoint namespace exceeds 512 bytes")
	}
	if limit < 1 {
		limit = maxCheckpointList
	}
	if limit > maxCheckpointList {
		limit = maxCheckpointList
	}
	var out []Checkpoint
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getThread(ctx, tx, scope, threadID, ""); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
SELECT c.id, c.external_id, c.namespace, COALESCE(p.external_id, ''),
       c.version, c.metadata::text, c.metadata_encoding, c.created_at
  FROM ahp_state.checkpoints c
  LEFT JOIN ahp_state.checkpoints p ON p.id = c.parent_id
 WHERE c.thread_id = $1 AND c.namespace = $2
 ORDER BY c.created_at DESC, c.id DESC
 LIMIT $3`, threadID, namespace, limit)
		if err != nil {
			return err
		}
		out = []Checkpoint{}
		internalIDs := make([]string, 0, limit)
		for rows.Next() {
			var (
				cp       Checkpoint
				meta     string
				external *string
			)
			if err := rows.Scan(&cp.ID, &external, &cp.Namespace, &cp.Parent,
				&cp.Version, &meta, &cp.MetadataEncoding, &cp.CreatedAt); err != nil {
				rows.Close()
				return err
			}
			// cp.ID holds the internal storage id until the public id replaces
			// it; parts are keyed by the internal id.
			internalIDs = append(internalIDs, cp.ID)
			if external != nil {
				cp.ID = *external
			}
			cp.ThreadID = threadID
			cp.Metadata = json.RawMessage(meta)
			cp.CreatedAt = utcOf(cp.CreatedAt)
			out = append(out, cp)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		partsByInternal, err := checkpointPartsByIDs(ctx, tx, internalIDs)
		if err != nil {
			return err
		}
		for i := range out {
			out[i].Parts = partsByInternal[internalIDs[i]]
			if out[i].Parts == nil {
				out[i].Parts = []Part{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// validPartVersion checks the opaque part-version contract: nonempty, at most
// 512 bytes, no NUL byte.
func validPartVersion(v string) error {
	if v == "" {
		return invalid("part version must not be empty")
	}
	if len(v) > maxKeyBytes {
		return invalid("part version exceeds 512 bytes")
	}
	for i := 0; i < len(v); i++ {
		if v[i] == 0 {
			return invalid("part version must not contain NUL")
		}
	}
	return nil
}
