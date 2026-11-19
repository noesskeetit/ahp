package state

// Experimental generic records store. Runtime formats remain opaque. This
// option is off by default and is not an approved public platform contract.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

func WithCommonRecords() StoreOption        { return func(s *Store) { s.commonRecords = true } }
func (s *Store) CommonRecordsEnabled() bool { return s.commonRecords }

const commonRecordsDDL = `
CREATE TABLE IF NOT EXISTS ahp_state.common_cuts(
 thread_id text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
 namespace text NOT NULL,revision bigint NOT NULL CHECK(revision>0),id text NOT NULL UNIQUE,
 format text NOT NULL,profile text NOT NULL,root text NOT NULL,operation_key text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(thread_id,namespace,revision),UNIQUE(thread_id,namespace,operation_key));
CREATE TABLE IF NOT EXISTS ahp_state.common_heads(
 thread_id text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
 namespace text NOT NULL,revision bigint NOT NULL,
 PRIMARY KEY(thread_id,namespace),
 FOREIGN KEY(thread_id,namespace,revision) REFERENCES ahp_state.common_cuts DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE IF NOT EXISTS ahp_state.common_records(
 thread_id text NOT NULL,namespace text NOT NULL,revision bigint NOT NULL,id text NOT NULL,
 kind text NOT NULL,native_id bytea,sequence bigint,owner_hint text,presence smallint NOT NULL,
 attributes bytea NOT NULL,
 PRIMARY KEY(thread_id,namespace,revision,id),
 FOREIGN KEY(thread_id,namespace,revision) REFERENCES ahp_state.common_cuts ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS ahp_state.common_roles(
 thread_id text NOT NULL,namespace text NOT NULL,revision bigint NOT NULL,source text NOT NULL,role text NOT NULL,
 PRIMARY KEY(thread_id,namespace,revision,source,role),
 FOREIGN KEY(thread_id,namespace,revision,source) REFERENCES ahp_state.common_records ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS ahp_state.common_edges(
 thread_id text NOT NULL,namespace text NOT NULL,revision bigint NOT NULL,source text NOT NULL,role text NOT NULL,
 ordinal integer NOT NULL CHECK(ordinal>=0),target text NOT NULL,
 PRIMARY KEY(thread_id,namespace,revision,source,role,ordinal),
 FOREIGN KEY(thread_id,namespace,revision,source,role) REFERENCES ahp_state.common_roles ON DELETE CASCADE,
 FOREIGN KEY(thread_id,namespace,revision,target) REFERENCES ahp_state.common_records DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE IF NOT EXISTS ahp_state.common_receipts(
 thread_id text NOT NULL,namespace text NOT NULL,key text NOT NULL,revision bigint NOT NULL,
 digest text NOT NULL,result bytea NOT NULL,
 PRIMARY KEY(thread_id,namespace,key),
 FOREIGN KEY(thread_id,namespace,revision) REFERENCES ahp_state.common_cuts ON DELETE CASCADE);
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='common_cut_root_fk' AND conrelid='ahp_state.common_cuts'::regclass) THEN
 ALTER TABLE ahp_state.common_cuts ADD CONSTRAINT common_cut_root_fk
 FOREIGN KEY(thread_id,namespace,revision,root) REFERENCES ahp_state.common_records(thread_id,namespace,revision,id)
 DEFERRABLE INITIALLY DEFERRED;
 END IF;
END $$;`

type CommonCommit struct {
	Delta              *CommonDelta              `json:"delta,omitempty"`
	ArtifactReferences []CommonArtifactReference `json:"artifact_references,omitempty"`
	KV                 []KVPut                   `json:"kv,omitempty"`
	Namespace          string                    `json:"namespace"`
	Key                string                    `json:"key"`
	ExpectedRevision   int64                     `json:"expected_revision"`
	Document           CommonDocument            `json:"document,omitzero"`
	AckInbox           []string                  `json:"ack_inbox,omitempty"`
	Stream             *StreamOp                 `json:"stream,omitempty"`
	RunStatus          *RunStatusOp              `json:"run_status,omitempty"`
}
type CommonReceipt struct {
	KV          []KVItem  `json:"kv,omitempty"`
	CutID       string    `json:"cut_id"`
	RunID       string    `json:"run_id"`
	Namespace   string    `json:"namespace"`
	Revision    int64     `json:"revision"`
	Key         string    `json:"key"`
	RecordCount int       `json:"record_count"`
	CreatedAt   time.Time `json:"created_at"`
}
type CommonRead struct {
	ArtifactReferences []CommonArtifactReference `json:"artifact_references,omitempty"`
	Document           CommonDocument            `json:"document,omitzero"`
	Receipt            CommonReceipt             `json:"receipt"`
}

func commonNamespace(ns string) error {
	if ns == "" {
		return nil
	}
	return validIdentifier(ns, "common namespace")
}
func (s *Store) commonReceipt(ctx context.Context, tx pgx.Tx, thread, ns, key, digest string) (CommonReceipt, bool, error) {
	var raw []byte
	var old string
	var result CommonReceipt
	err := tx.QueryRow(ctx, `SELECT digest,result FROM ahp_state.common_receipts WHERE thread_id=$1 AND namespace=$2 AND key=$3`, thread, ns, key).Scan(&old, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, false, nil
	}
	if err != nil {
		return result, false, err
	}
	if old != digest {
		return result, true, contracts.NewError(contracts.ErrKeyReuse, "common mutation key reused with different body")
	}
	if err = json.Unmarshal(raw, &result); err != nil {
		return result, true, invalid("corrupt common receipt")
	}
	return result, true, nil
}

// CommitRecords publishes an immutable cut and its current frontier together.
// A committed replay is a scoped receipt read, including after terminal release.
// A fresh mutation always passes the existing database-clock lease fence.
func (s *Store) CommitRecords(ctx context.Context, scope contracts.Scope, f Fence, req CommonCommit) (CommonReceipt, error) {
	var out CommonReceipt
	if !s.commonRecords {
		return out, invalid("common records option is disabled")
	}
	if err := scope.Validate(); err != nil {
		return out, err
	}
	if err := validLeaseArgs(scope, f.RunID, f.Worker, f.Epoch); err != nil {
		return out, err
	}
	if err := commonNamespace(req.Namespace); err != nil {
		return out, err
	}
	if err := validIdentifier(req.Key, "common operation key"); err != nil {
		return out, err
	}
	if req.ExpectedRevision < 0 {
		return out, invalid("common expected revision must be nonnegative")
	}
	if req.Delta != nil {
		if !s.commonRecordVersions {
			return out, invalid("common delta layout is not enabled")
		}
		if req.Document.Format != "" || req.Document.Profile != "" || req.Document.Root != "" || req.Document.Records != nil {
			return out, invalid("delta and full document are mutually exclusive")
		}
		if err := req.Delta.Validate(); err != nil {
			return out, err
		}
	} else if err := req.Document.Validate(); err != nil {
		return out, err
	}
	if len(req.AckInbox) > 1000 {
		return out, invalid("common inbox acknowledgement count exceeds limit")
	}
	if req.Stream != nil {
		if req.Stream.Kind == "" {
			return out, invalid("common stream kind is required")
		}
		if err := checkPayload(req.Stream.Payload); err != nil {
			return out, err
		}
	}
	if err := validateAtomicRunStatus(req.RunStatus, "common run status"); err != nil {
		return out, err
	}
	var kvItems []kvPrepared
	if len(req.KV) > 0 {
		var err error
		kvItems, _, err = s.prepareKVPuts(scope, req.KV)
		if err != nil {
			return out, err
		}
	}
	// Lease epoch/worker may change when the same run is recovered. The run
	// identity must stay bound: replaying its terminal receipt cannot complete
	// a different successor run under a coincidentally reused client key.
	digest, err := commonMutationDigest(f.RunID, req)
	if err != nil {
		return out, asInvalid(err)
	}
	if req.Delta != nil {
		ctx, err = s.commonDeltaArtifactContext(ctx, scope, req)
	} else {
		ctx, err = s.commonArtifactContext(ctx, scope, req)
	}
	if err != nil {
		return out, err
	}
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		r, err := s.getRun(ctx, tx, scope, f.RunID)
		if err != nil {
			return err
		}
		thread := r.Thread.ID
		if len(kvItems) > 0 {
			namespaces := make([][]string, 0, len(kvItems))
			for _, item := range kvItems {
				namespaces = append(namespaces, item.put.Namespace)
			}
			if err := s.checkKVPolicies(ctx, tx, scope, namespaces); err != nil {
				return err
			}
		}
		// Read-only replays do not regain authority and never mutate the head.
		var found bool
		out, found, err = s.commonReceipt(ctx, tx, thread, req.Namespace, req.Key, digest)
		if err != nil || found {
			return err
		}
		l, leaseErr := s.currentLease(ctx, tx, scope, f.RunID, f.Worker, f.Epoch)
		if leaseErr != nil {
			// A concurrent identical terminal commit can have cleared the lease while
			// this request waited for the thread lock. Recheck its immutable receipt.
			out, found, err = s.commonReceipt(ctx, tx, thread, req.Namespace, req.Key, digest)
			if err != nil || found {
				return err
			}
			return leaseErr
		}
		out, found, err = s.commonReceipt(ctx, tx, thread, req.Namespace, req.Key, digest)
		if err != nil || found {
			return err
		}
		var current int64
		var previousFormat string
		err = tx.QueryRow(ctx, `SELECT h.revision,c.format FROM ahp_state.common_heads h JOIN ahp_state.common_cuts c USING(thread_id,namespace,revision) WHERE h.thread_id=$1 AND h.namespace=$2`, thread, req.Namespace).Scan(&current, &previousFormat)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if current != req.ExpectedRevision || current == math.MaxInt64 {
			return contracts.NewError(contracts.ErrConflict, "common expected revision differs from published head")
		}
		if previousFormat == commonVersionLayout && !s.commonRecordVersions {
			return contracts.NewError(contracts.ErrUnavailable, "versioned common writer is not configured")
		}
		incremental, e := s.tryCommonAppend(ctx, tx, thread, req.Namespace, current, req.Delta, req.ArtifactReferences)
		if e != nil {
			return e
		}
		var before *CommonDocument
		if incremental != nil {
			req.Document = CommonDocument{Format: CommonFormat, Profile: req.Delta.Profile, Root: req.Delta.Root, Records: req.Delta.Put}
		}
		if incremental == nil && s.commonRecordVersions && current > 0 {
			read, e := s.readCommonCutTx(ctx, tx, scope, thread, req.Namespace, current)
			if e != nil {
				return e
			}
			before = &read.Document
		}
		if req.Delta != nil && incremental == nil {
			req.Document, err = applyCommonDelta(before, *req.Delta)
			if err != nil {
				return err
			}
			if err = validateCommonArtifactReferences(scope, req.Document, req.ArtifactReferences); err != nil {
				return err
			}
		}
		physicalFormat := req.Document.Format
		if s.commonRecordVersions {
			physicalFormat = commonVersionLayout
		}
		revision := current + 1
		out = CommonReceipt{CutID: contracts.NewID(), RunID: f.RunID, Namespace: req.Namespace, Revision: revision, Key: req.Key, RecordCount: len(req.Document.Records)}
		if incremental != nil {
			out.RecordCount = incremental.root.Records
		}
		err = tx.QueryRow(ctx, `INSERT INTO ahp_state.common_cuts(thread_id,namespace,revision,id,format,profile,root,operation_key) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING created_at`, thread, req.Namespace, revision, out.CutID, physicalFormat, req.Document.Profile, req.Document.Root, req.Key).Scan(&out.CreatedAt)
		if err != nil {
			return err
		}
		// Match the persisted JSON receipt's UTC representation, independent of
		// the PostgreSQL driver's decode location (including local UTC zones).
		out.CreatedAt = out.CreatedAt.UTC()
		if incremental != nil {
			if err = s.putCommonAppend(ctx, tx, thread, req.Namespace, revision, incremental); err != nil {
				return err
			}
		} else if s.commonRecordVersions {
			storedBefore := before
			if previousFormat != commonVersionLayout {
				storedBefore = nil
			}
			if err = s.putCommonVersions(ctx, tx, thread, req.Namespace, revision, storedBefore, req.Document, req.ArtifactReferences); err != nil {
				return err
			}
		} else {
			keys := make([]string, 0, len(req.Document.Records))
			for id := range req.Document.Records {
				keys = append(keys, id)
			}
			sort.Strings(keys)
			records, roles, edges := [][]any{}, [][]any{}, [][]any{}
			payloads := map[string][]byte{}
			for _, id := range keys {
				row := req.Document.Records[id]
				seq, _ := commonSequence(row.Sequence)
				hint, _ := commonOwnerHint(row.OwnerHint)
				presence := int16(0)
				if len(row.Sequence) > 0 {
					presence |= 1
				}
				if len(row.OwnerHint) > 0 {
					presence |= 2
				}
				attributes := []byte(row.Attributes)
				var objectHash *string
				if s.commonPayloadObjects {
					sum := sha256.Sum256(attributes)
					hash := hex.EncodeToString(sum[:])
					objectHash = &hash
					payloads[hash] = attributes
					attributes = []byte{}
				}
				records = append(records, []any{thread, req.Namespace, revision, id, row.Kind, []byte(row.NativeID), seq, hint, presence, attributes, objectHash})
				for role, refs := range row.Relations {
					roles = append(roles, []any{thread, req.Namespace, revision, id, role})
					for i, target := range refs {
						edges = append(edges, []any{thread, req.Namespace, revision, id, role, i, target})
					}
				}
			}
			if len(payloads) > 0 {
				if err = s.putCommonPayloads(ctx, tx, thread, payloads); err != nil {
					return err
				}
			}
			if _, err = tx.CopyFrom(ctx, pgx.Identifier{"ahp_state", "common_records"}, []string{"thread_id", "namespace", "revision", "id", "kind", "native_id", "sequence", "owner_hint", "presence", "attributes", "attributes_hash"}, pgx.CopyFromRows(records)); err != nil {
				return err
			}
			if len(roles) > 0 {
				if _, err = tx.CopyFrom(ctx, pgx.Identifier{"ahp_state", "common_roles"}, []string{"thread_id", "namespace", "revision", "source", "role"}, pgx.CopyFromRows(roles)); err != nil {
					return err
				}
			}
			if len(edges) > 0 {
				if _, err = tx.CopyFrom(ctx, pgx.Identifier{"ahp_state", "common_edges"}, []string{"thread_id", "namespace", "revision", "source", "role", "ordinal", "target"}, pgx.CopyFromRows(edges)); err != nil {
					return err
				}
			}
		}
		if err = s.putCommonArtifactReferences(ctx, tx, scope, l, req.Namespace, revision, req.ArtifactReferences); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO ahp_state.common_heads VALUES($1,$2,$3) ON CONFLICT(thread_id,namespace) DO UPDATE SET revision=EXCLUDED.revision`, thread, req.Namespace, revision); err != nil {
			return err
		}
		if len(kvItems) > 0 {
			out.KV, err = s.applyPreparedKV(ctx, tx, l, kvItems)
			if err != nil {
				return err
			}
		}
		for _, id := range req.AckInbox {
			tag, err := tx.Exec(ctx, `UPDATE ahp_state.inbox_events SET acked=true WHERE id=$1 AND thread_id=$2 AND NOT acked`, id, thread)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				var exists bool
				if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.inbox_events WHERE id=$1 AND thread_id=$2)`, id, thread).Scan(&exists); err != nil {
					return err
				}
				if exists {
					return contracts.NewError(contracts.ErrConflict, "common input was already acknowledged by another operation")
				}
				return ErrForeignEvent
			}
		}
		if req.Stream != nil {
			if err = s.commonAppendStream(ctx, tx, thread, f.RunID, req.Key, req.Stream); err != nil {
				return err
			}
		}
		if req.RunStatus != nil {
			st := req.RunStatus.Status
			if err = applyAtomicRunStatus(ctx, tx, thread, f.RunID, req.RunStatus); err != nil {
				return err
			}
			if err = s.publishRunCompletionIfTerminal(ctx, tx, scope, f.RunID, st); err != nil {
				return err
			}
			if IsTerminalStatus(st) || st == RunPending || st == RunWaiting {
				if err = s.clearLease(ctx, tx, l.Thread.ID, f.RunID, f.Worker, f.Epoch); err != nil {
					return err
				}
			}
		}
		raw, err := json.Marshal(out)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO ahp_state.common_receipts VALUES($1,$2,$3,$4,$5,$6)`, thread, req.Namespace, req.Key, revision, digest, raw)
		return err
	})
	if err != nil {
		return CommonReceipt{}, err
	}
	return out, nil
}
func (s *Store) commonAppendStream(ctx context.Context, tx pgx.Tx, thread, run, key string, op *StreamOp) error {
	streamKey := op.Key
	if streamKey == "" {
		streamKey = key + ":s"
	}
	digest, err := contracts.Digest(struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{op.Kind, op.Payload})
	if err != nil {
		return err
	}
	var old []byte
	err = tx.QueryRow(ctx, `SELECT digest FROM ahp_state.stream_events WHERE thread_id=$1 AND key=$2`, thread, streamKey).Scan(&old)
	if err == nil {
		if !digestMatches(old, digest) {
			return ErrKeyReuse
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	var seq int64
	if err = tx.QueryRow(ctx, `SELECT GREATEST(COALESCE(MAX(sequence),0),COALESCE((SELECT stream_high_water FROM ahp_state.threads WHERE id=$1),0))+1 FROM ahp_state.stream_events WHERE thread_id=$1`, thread).Scan(&seq); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO ahp_state.stream_events(id,thread_id,run_id,key,kind,sequence,payload,digest) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, contracts.NewID(), thread, run, streamKey, op.Kind, seq, op.Payload, digest)
	return err
}

func (s *Store) ReadRecords(ctx context.Context, scope contracts.Scope, thread, ns string, revision int64) (CommonRead, error) {
	var out CommonRead
	if !s.commonRecords {
		return out, invalid("common records option is disabled")
	}
	if err := scope.Validate(); err != nil {
		return out, err
	}
	if err := validIdentifier(thread, "thread id"); err != nil {
		return out, err
	}
	if err := commonNamespace(ns); err != nil {
		return out, err
	}
	if revision < 0 {
		return out, invalid("common revision must be nonnegative")
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`); err != nil {
			return err
		}
		var err error
		out, err = s.readCommonCutTx(ctx, tx, scope, thread, ns, revision)
		return err
	})
	if err != nil {
		return CommonRead{}, err
	}
	return out, nil
}

func (s *Store) readCommonCutTx(ctx context.Context, tx pgx.Tx, scope contracts.Scope, thread, ns string, revision int64) (CommonRead, error) {
	var out CommonRead
	err := func() error {
		if _, err := s.getThread(ctx, tx, scope, thread, ""); err != nil {
			return err
		}
		if revision == 0 {
			if err := tx.QueryRow(ctx, `SELECT revision FROM ahp_state.common_heads WHERE thread_id=$1 AND namespace=$2`, thread, ns).Scan(&revision); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return notFound("common head")
				}
				return err
			}
		}
		var raw []byte
		if err := tx.QueryRow(ctx, `SELECT c.format,c.profile,c.root,r.result FROM ahp_state.common_cuts c JOIN ahp_state.common_receipts r USING(thread_id,namespace,revision) WHERE c.thread_id=$1 AND c.namespace=$2 AND c.revision=$3`, thread, ns, revision).Scan(&out.Document.Format, &out.Document.Profile, &out.Document.Root, &raw); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return notFound("common cut")
			}
			return err
		}
		if out.Document.Format == commonPrunedLayout {
			return notFound("common revision outside retained history")
		}
		if err := json.Unmarshal(raw, &out.Receipt); err != nil {
			return err
		}
		if out.Document.Format == commonVersionLayout {
			if err := s.readCommonVersions(ctx, tx, thread, ns, revision, &out); err != nil {
				return err
			}
			return s.readCommonArtifactReferences(ctx, tx, scope, thread, ns, revision, &out)
		}
		out.Document.Records = map[string]CommonRecord{}
		rows, err := tx.Query(ctx, `SELECT r.id,r.kind,r.native_id,r.sequence,r.owner_hint,r.presence,
 CASE WHEN r.attributes_hash IS NULL THEN r.attributes ELSE p.attributes END,r.attributes_hash
 FROM ahp_state.common_records r LEFT JOIN ahp_state.common_payloads p
 ON p.thread_id=r.thread_id AND p.sha256=r.attributes_hash
 WHERE r.thread_id=$1 AND r.namespace=$2 AND r.revision=$3`, thread, ns, revision)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id, kind string
			var nid, attrs []byte
			var seq *int64
			var hint *string
			var fields int16
			var objectHash *string
			if err = rows.Scan(&id, &kind, &nid, &seq, &hint, &fields, &attrs, &objectHash); err != nil {
				rows.Close()
				return err
			}
			if objectHash != nil {
				sum := sha256.Sum256(attrs)
				if hex.EncodeToString(sum[:]) != *objectHash {
					rows.Close()
					return contracts.NewError(contracts.ErrUnavailable, "common payload integrity check failed")
				}
			}
			r := CommonRecord{Kind: kind, NativeID: nid, Attributes: attrs, Relations: map[string][]string{}}
			if fields&1 != 0 {
				r.Sequence = json.RawMessage("null")
				if seq != nil {
					r.Sequence = json.RawMessage(strconv.FormatInt(*seq, 10))
				}
			}
			if fields&2 != 0 {
				r.OwnerHint = json.RawMessage("null")
				if hint != nil {
					r.OwnerHint, _ = json.Marshal(*hint)
				}
			}
			out.Document.Records[id] = r
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		rows, err = tx.Query(ctx, `SELECT source,role FROM ahp_state.common_roles WHERE thread_id=$1 AND namespace=$2 AND revision=$3`, thread, ns, revision)
		if err != nil {
			return err
		}
		for rows.Next() {
			var source, role string
			if err = rows.Scan(&source, &role); err != nil {
				rows.Close()
				return err
			}
			r := out.Document.Records[source]
			r.Relations[role] = []string{}
			out.Document.Records[source] = r
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		rows, err = tx.Query(ctx, `SELECT source,role,target FROM ahp_state.common_edges WHERE thread_id=$1 AND namespace=$2 AND revision=$3 ORDER BY source,role,ordinal`, thread, ns, revision)
		if err != nil {
			return err
		}
		for rows.Next() {
			var source, role, target string
			if err = rows.Scan(&source, &role, &target); err != nil {
				rows.Close()
				return err
			}
			r := out.Document.Records[source]
			r.Relations[role] = append(r.Relations[role], target)
			out.Document.Records[source] = r
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(out.Document.Records) != out.Receipt.RecordCount {
			return invalid("incomplete common cut")
		}
		if err = s.readCommonArtifactReferences(ctx, tx, scope, thread, ns, revision, &out); err != nil {
			return err
		}
		return out.Document.Validate()
	}()
	if err != nil {
		return CommonRead{}, err
	}
	return out, nil
}

// Legacy exports cannot silently omit the new record model. A versioned common
// export/import remains a separate capability, not an inferred guarantee.
func (s *Store) refuseCommonExport(ctx context.Context, tx pgx.Tx, sc contracts.Scope, thread string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('ahp_state.common_cuts') IS NOT NULL`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.common_cuts c JOIN ahp_state.threads t ON t.id=c.thread_id WHERE t.tenant=$1 AND t.deployment=$2 AND t.subject=$3 AND ($4='' OR t.id=$4))`, sc.Tenant, sc.Deployment, sc.Subject, thread).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return contracts.NewError(contracts.ErrConflict, "common records require a supported versioned export")
	}
	return nil
}
