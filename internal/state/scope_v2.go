package state

// This is an explicit logical owner format, not pg_dump. The PG driver below
// maps a fixed schema to it. Another driver must implement these mappings and
// pass the same native recovery suite; service-to-service is not driver-B proof.
import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/atrest"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const ScopeV2Format = "ahp-state-scope/2"
const ScopeV2Native = "ahp-state-owner/dbdd4ef1/1"
const MaxScopeV2Bytes = 16 << 20
const MaxScopeV2Rows = 10000

// Offline receives (context, scope, migrationID, sourceStoreID). It must bind
// the supplied physical source identity to its durable custody record.
// Offline must be supplied by a trusted control-plane coordinator, never from
// a task token or an unverified request boolean. It proves source API/workers,
// outbox consumers, GC and every other writer stopped/drained, and keeps them
// stopped until target activation. It is called both before and after snapshot,
// and before target commit. Missing coordinator fails closed. No deletion or
// legalhold marker substitutes for a migration fence.
type ScopeTransferConfig struct {
	// NativeBuild is an operator-admitted owner build, never supplied by caller alone.
	NativeBuild string
	Offline     func(context.Context, contracts.Scope, string, string) error
}

func WithScopeTransfer(cfg ScopeTransferConfig) StoreOption {
	return func(s *Store) { s.scopeTransfer = cfg }
}

type ScopeV2Manifest struct {
	SourceStoreID   string          `json:"source_store_id"`
	Format          string          `json:"format"`
	Native          string          `json:"native"`
	NativeBuild     string          `json:"native_build"`
	PayloadEncoding string          `json:"payload_encoding"`
	Scope           contracts.Scope `json:"scope"`
	MigrationID     string          `json:"migration_id"`
	Profile         string          `json:"profile"`
	ExportedAt      time.Time       `json:"exported_at"`
	Counts          map[string]int  `json:"counts"`
}
type ScopeV2 struct {
	Manifest ScopeV2Manifest `json:"manifest"`
	// Rows have fixed named fields, JSON numbers/timestamps and base64 data.
	// No SQL statements, ciphertext, DSN, bearer token or lease owner is carried.
	Entities           map[string][]json.RawMessage    `json:"entities"`
	ArtifactReferences []references.CheckpointArtifact `json:"artifact_references"`
	Digest             string                          `json:"digest"`
}
type ScopeV2Receipt struct {
	MigrationID string          `json:"migration_id"`
	Digest      string          `json:"digest"`
	Scope       contracts.Scope `json:"scope"`
	ImportedAt  time.Time       `json:"imported_at"`
}

const scopeV2DDL = `CREATE TABLE IF NOT EXISTS ahp_state.scope_store_v2 (singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), id text NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS ahp_state.scope_transfers_v2 (
 migration_id text PRIMARY KEY, digest text NOT NULL, tenant text NOT NULL,
 deployment text NOT NULL, subject text NOT NULL, imported_at timestamptz NOT NULL DEFAULT clock_timestamp());`

// Column lists form the versioned logical schema. Lease owners/expiry are
// deliberately absent; epochs and running/unknown statuses remain unchanged.
type scopeEntity struct {
	name, columns, where, order string
	binary                      bool
}

var scopeV2Entities = []scopeEntity{
	{"threads", "id,tenant,deployment,subject,context_id,epoch,created_at,idem_key,body_digest,stream_high_water", "x.tenant=$1 AND x.deployment=$2 AND x.subject=$3", "id", false},
	{"runs", "id,thread_id,task_id,status,wait_for,sequence,payload,body_digest,deadline,created_at,updated_at,idem_key,traceparent", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,sequence", false},
	{"checkpoint_blobs", "thread_id,namespace,channel,version,data,created_at,encoding", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,channel,version", true},
	{"checkpoints", "id,external_id,thread_id,namespace,parent_id,version,metadata,body_digest,key,created_at,metadata_encoding", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,created_at,id", false},
	{"checkpoint_parts", "checkpoint_id,channel,version", "x.checkpoint_id IN (SELECT id FROM ahp_state.checkpoints WHERE thread_id IN (SELECT id FROM scoped_threads))", "checkpoint_id,channel", false},
	{"checkpoint_writes", "checkpoint_id,thread_id,task,idx,channel,data,encoding", "x.thread_id IN (SELECT id FROM scoped_threads)", "checkpoint_id,task,idx", true},
	{"inbox_events", "id,thread_id,key,kind,sequence,payload,digest,acked,created_at", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,sequence", false},
	{"stream_events", "id,thread_id,run_id,key,kind,sequence,payload,digest,created_at", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,sequence", false},
	{"kv", "tenant,deployment,subject,namespace,key,value,version,deleted,thread_id,updated_at", "x.tenant=$1 AND x.deployment=$2 AND x.subject=$3", "namespace,key", false},
	{"kv_ops", "tenant,deployment,subject,operation_key,request_digest,result,created_at", "x.tenant=$1 AND x.deployment=$2 AND x.subject=$3", "operation_key", false},
	{"intents", "id,run_id,node,call,kind,target,digest,status,result,created_at,updated_at", "x.run_id IN (SELECT id FROM ahp_state.runs WHERE thread_id IN (SELECT id FROM scoped_threads))", "run_id,node,call", false},
	{"id_operations", "operation_key,resource_id,created_at,tenant,deployment,subject,run_id,node,call", "x.tenant=$1 AND x.deployment=$2 AND x.subject=$3", "operation_key", false},
	{"run_completions", "id,thread_id,sequence,body,status,created_at,next_at,attempts,error_code", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,sequence,id", false},
	{"scope_transfers_v2", "migration_id,digest,tenant,deployment,subject,imported_at", "x.tenant=$1 AND x.deployment=$2 AND x.subject=$3", "migration_id", false},
	{"deletions", "id,tenant,deployment,subject,operation,resource,context_ids,task_ids,deleted_count,held_count,artifacts_done,memory_done,created_at,completed_at", "x.tenant=$1 AND x.deployment=$2 AND x.subject=$3", "id", false},
}

// A JSON document null and SQL NULL are different durable values. Native JSON
// fields use {"json": <document>} while SQL NULL remains null. Omitting this
// wrapper loses tombstones and nullable intent results on populate_record.
func scopeV2JSONColumn(entity, column string) bool {
	switch entity + "." + column {
	case "runs.payload", "checkpoints.metadata", "inbox_events.payload", "stream_events.payload", "kv.value", "kv_ops.result", "intents.result", "run_completions.body":
		return true
	}
	return false
}

const scopeCTE = `WITH scoped_threads AS (SELECT id FROM ahp_state.threads WHERE tenant=$1 AND deployment=$2 AND subject=$3) `

func scopeV2Digest(e ScopeV2) (string, error) { e.Digest = ""; return contracts.Digest(e) }
func (s *Store) offlineScope(ctx context.Context, sc contracts.Scope, id, sourceStoreID string) error {
	if err := sc.Validate(); err != nil {
		return asInvalid(err)
	}
	if err := validIdentifier(id, "migration id"); err != nil {
		return err
	}
	if s.scopeTransfer.Offline == nil {
		return contracts.NewError(contracts.ErrForbidden, "scope transfer requires trusted offline source coordinator")
	}
	if err := validIdentifier(sourceStoreID, "source physical store id"); err != nil {
		return err
	}
	return s.scopeTransfer.Offline(ctx, sc, id, sourceStoreID)
}

// ExportScopeV2 reads every entity in one repeatable-read snapshot. Native
// payloads come from the State owner. The pinned State owner has NO mounted
// SQL payload cipher: encrypted rows are refused rather than copied as portable.
func (s *Store) ExportScopeV2(ctx context.Context, sc contracts.Scope, id, build string) (ScopeV2, error) {
	e := ScopeV2{Manifest: ScopeV2Manifest{Format: ScopeV2Format, Native: ScopeV2Native, NativeBuild: build, PayloadEncoding: "state-owner-plaintext/1", Scope: sc, MigrationID: id, Profile: "offline-same-scope-clean-db/1", ExportedAt: time.Now().UTC(), Counts: map[string]int{}}, Entities: map[string][]json.RawMessage{}, ArtifactReferences: []references.CheckpointArtifact{}}
	if err := validIdentifier(build, "native build"); err != nil {
		return e, err
	}
	if build != s.scopeTransfer.NativeBuild {
		return e, invalid("owner build not admitted by transfer configuration")
	}
	if err := s.pool.QueryRow(ctx, `SELECT id FROM ahp_state.scope_store_v2 WHERE singleton`).Scan(&e.Manifest.SourceStoreID); err != nil {
		return e, err
	}
	if err := s.offlineScope(ctx, sc, id, e.Manifest.SourceStoreID); err != nil {
		return e, err
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY`); err != nil {
			return err
		}
		if err := s.checkOwnerKVScopeExport(ctx, tx, sc); err != nil {
			return err
		}
		if err := s.refuseCommonExport(ctx, tx, sc, ""); err != nil {
			return err
		}

		// These tenant-wide control records cannot be faithfully mapped to a single
		// deployment scope. Refuse, do not omit or manufacture equivalent fences.
		if err := tx.QueryRow(ctx, `SELECT id FROM ahp_state.scope_store_v2 WHERE singleton`).Scan(&e.Manifest.SourceStoreID); err != nil {
			return err
		}
		if err := scopeV2SourceSchema(ctx, tx); err != nil {
			return err
		}
		var unsafe bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.legalhold WHERE tenant=$1 AND (subject='' OR subject=$2)) OR EXISTS(SELECT 1 FROM ahp_state.control_deletions WHERE tenant=$1 AND (subject=$2 OR kind='tenant')) OR EXISTS(SELECT 1 FROM ahp_state.erased_subjects WHERE tenant=$1 AND subject=$2) OR EXISTS(SELECT 1 FROM ahp_state.erased_tenants WHERE tenant=$1)`, sc.Tenant, sc.Subject).Scan(&unsafe); err != nil {
			return err
		}
		if unsafe {
			return contracts.NewError(contracts.ErrConflict, "tenant-wide control records require an owner migration contract")
		}
		var orphan bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.id_operations o WHERE tenant IS NULL OR deployment IS NULL OR subject IS NULL OR run_id IS NULL OR node IS NULL OR call IS NULL)`).Scan(&orphan); err != nil {
			return err
		}
		if orphan {
			return contracts.NewError(contracts.ErrConflict, "unattributed orphan operation receipts require owner scope metadata")
		}
		total := 0
		size := 0
		for _, spec := range scopeV2Entities {
			columns := strings.Split(spec.columns, ",")
			projectionParts := []string{}
			for _, column := range columns {
				expr := column
				if scopeV2JSONColumn(spec.name, column) {
					expr = "CASE WHEN " + column + " IS NULL THEN NULL ELSE jsonb_build_object('json'," + column + ") END AS " + column
				}
				if spec.binary && column == "data" {
					expr = "encode(data,'base64') AS data"
				}
				projectionParts = append(projectionParts, expr)
			}
			projection := strings.Join(projectionParts, ",")
			q := scopeCTE + `SELECT row_to_json(t) FROM (SELECT ` + projection + ` FROM ahp_state.` + spec.name + ` x WHERE ` + spec.where + ` ORDER BY ` + spec.order + `) t LIMIT 10001`
			rows, err := tx.Query(ctx, q, sc.Tenant, sc.Deployment, sc.Subject)
			if err != nil {
				return err
			}
			e.Entities[spec.name] = []json.RawMessage{}
			for rows.Next() {
				var b []byte
				if err = rows.Scan(&b); err != nil {
					rows.Close()
					return err
				}
				total++
				size += len(b)
				if total > MaxScopeV2Rows || size > MaxScopeV2Bytes {
					rows.Close()
					return invalid("scope export exceeds bounded v2 profile")
				}
				e.Entities[spec.name] = append(e.Entities[spec.name], json.RawMessage(b))
			}
			rows.Close()
			if err = rows.Err(); err != nil {
				return err
			}
			e.Manifest.Counts[spec.name] = len(e.Entities[spec.name])
		}
		return nil
	})
	if err != nil {
		return ScopeV2{}, err
	}
	refs, err := scopeV2References(e)
	if err != nil {
		return ScopeV2{}, err
	}
	e.ArtifactReferences = refs
	e.Digest, err = scopeV2Digest(e)
	if err != nil {
		return ScopeV2{}, err
	}
	if err = ValidateScopeV2(e, sc, build); err != nil {
		return ScopeV2{}, err
	}
	if err = s.offlineScope(ctx, sc, id, e.Manifest.SourceStoreID); err != nil {
		return ScopeV2{}, err
	}
	return e, nil
}

// Parse rejects duplicate keys through canonicalization before ordinary decode,
// unknown fields, trailing values, oversized input, format/digest corruption.
func ParseScopeV2(b []byte, sc contracts.Scope, build string) (ScopeV2, error) {
	var e ScopeV2
	if len(b) > MaxScopeV2Bytes {
		return e, invalid("scope archive too large")
	}
	if _, err := contracts.Digest(json.RawMessage(b)); err != nil {
		return e, err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return e, invalid("invalid scope archive")
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return e, invalid("scope archive trailing data")
	}
	return e, ValidateScopeV2(e, sc, build)
}
func ValidateScopeV2(e ScopeV2, sc contracts.Scope, build string) error {
	m := e.Manifest
	if err := sc.Validate(); err != nil {
		return asInvalid(err)
	}
	if m.Scope != sc {
		return contracts.NewError(contracts.ErrForbidden, "scope transfer cannot rewrite logical scope")
	}
	if m.Format != ScopeV2Format || m.Native != ScopeV2Native || m.NativeBuild != build || m.PayloadEncoding != "state-owner-plaintext/1" || m.Profile != "offline-same-scope-clean-db/1" {
		return invalid("unsupported scope format/native build/profile")
	}
	if err := validIdentifier(m.SourceStoreID, "source physical store id"); err != nil {
		return err
	}
	if err := validIdentifier(m.MigrationID, "migration id"); err != nil {
		return err
	}
	if m.ExportedAt.IsZero() || len(e.Entities) != len(scopeV2Entities) || len(m.Counts) != len(scopeV2Entities) {
		return invalid("incomplete scope manifest")
	}
	b, err := json.Marshal(e)
	if err != nil || len(b) > MaxScopeV2Bytes {
		return invalid("scope archive too large")
	}
	digest, err := scopeV2Digest(e)
	if err != nil {
		return err
	}
	if digest != e.Digest {
		return invalid("scope manifest digest mismatch")
	}
	total := 0
	threads := map[string]bool{}
	runs := map[string]bool{}
	cps := map[string]bool{}
	intents := map[string]bool{}
	for _, sp := range scopeV2Entities {
		rows, ok := e.Entities[sp.name]
		if !ok || len(rows) != m.Counts[sp.name] {
			return invalid("scope count mismatch")
		}
		total += len(rows)
		if total > MaxScopeV2Rows {
			return invalid("scope row limit")
		}
		fields := strings.Split(sp.columns, ",")
		for _, raw := range rows {
			if len(raw) > 2<<20 {
				return invalid("scope entity too large")
			}
			var r map[string]json.RawMessage
			if json.Unmarshal(raw, &r) != nil || len(r) != len(fields) {
				return invalid("invalid scope row fields")
			}
			for _, f := range fields {
				if _, ok := r[f]; !ok {
					return invalid("missing scope row field")
				}
			}
			for _, column := range fields {
				if scopeV2JSONColumn(sp.name, column) {
					if bytes.Equal(bytes.TrimSpace(r[column]), []byte("null")) {
						if sp.name != "intents" || column != "result" {
							return invalid("SQL NULL in required native JSON field")
						}
						continue
					}
					var wrapper map[string]json.RawMessage
					if json.Unmarshal(r[column], &wrapper) != nil || len(wrapper) != 1 || wrapper["json"] == nil {
						return invalid("native JSON field requires explicit document wrapper")
					}
				}
			}
			val := func(k string) string { var v string; _ = json.Unmarshal(r[k], &v); return v }
			if _, ok := r["tenant"]; ok && (val("tenant") != sc.Tenant || val("deployment") != sc.Deployment || val("subject") != sc.Subject) {
				return contracts.NewError(contracts.ErrForbidden, "foreign scope row")
			}
			if sp.name == "threads" {
				var epoch int64
				if json.Unmarshal(r["epoch"], &epoch) != nil || epoch < 0 || epoch == int64(^uint64(0)>>1) {
					return invalid("invalid epoch floor")
				}
				if threads[val("id")] {
					return invalid("duplicate thread")
				}
				threads[val("id")] = true
			}
			if v := val("thread_id"); v != "" && !threads[v] {
				return invalid("scope row references foreign thread")
			}
			if sp.name == "runs" {
				runs[val("id")] = true
			}
			if v := val("run_id"); sp.name != "id_operations" && v != "" && !runs[v] {
				return invalid("scope row references foreign run")
			}
			if sp.name == "checkpoints" {
				cps[val("id")] = true
			}
			if sp.name == "checkpoint_parts" && !cps[val("checkpoint_id")] {
				return invalid("checkpoint part has no header")
			}
			if sp.name == "intents" {
				intents[val("id")] = true
			}
			if sp.binary {
				var enc string
				if json.Unmarshal(r["data"], &enc) != nil {
					return invalid("invalid binary payload")
				}
				plain, err := base64.StdEncoding.DecodeString(enc)
				if err != nil || len(plain) > 1<<20 {
					return invalid("invalid binary payload")
				}
				if atrest.IsEnvelope(plain) {
					return invalid("encrypted SQL payload requires State owner crypto API")
				}
			}
		}
	}
	if err := validateScopeV2Graph(e, sc); err != nil {
		return err
	}
	refs, err := scopeV2References(e)
	if err != nil {
		return err
	}
	a, _ := contracts.Digest(refs)
	bref, _ := contracts.Digest(e.ArtifactReferences)
	if a != bref {
		return invalid("artifact reference manifest mismatch")
	}
	return nil
}

// Only explicit owner encoding labels are interpreted. Opaque native bytes,
// IDs and strings are never searched/re-written as artifact references.
func scopeV2References(e ScopeV2) ([]references.CheckpointArtifact, error) {
	refs := []references.CheckpointArtifact{}
	seen := map[string]bool{}
	for _, name := range []string{"checkpoints", "checkpoint_blobs", "checkpoint_writes"} {
		for _, b := range e.Entities[name] {
			var r map[string]json.RawMessage
			if json.Unmarshal(b, &r) != nil {
				return nil, invalid("bad payload row")
			}
			field, enc := "data", "encoding"
			if name == "checkpoints" {
				field, enc = "metadata", "metadata_encoding"
			}
			var label string
			_ = json.Unmarshal(r[enc], &label)
			data := []byte(r[field])
			if field == "metadata" {
				var wrapper map[string]json.RawMessage
				if json.Unmarshal(data, &wrapper) != nil {
					return nil, invalid("invalid native JSON wrapper")
				}
				data = wrapper["json"]
			}
			if field == "data" {
				var s string
				_ = json.Unmarshal(r[field], &s)
				var err error
				data, err = base64.StdEncoding.DecodeString(s)
				if err != nil {
					return nil, invalid("bad payload base64")
				}
				if atrest.IsEnvelope(data) {
					return nil, invalid("encrypted payload is not portable")
				}
			}
			if err := validatePayloadEncoding(label, data); err != nil {
				return nil, err
			}
			if label != "" {
				var ref references.CheckpointArtifact
				if json.Unmarshal(data, &ref) != nil {
					return nil, invalid("bad artifact reference")
				}
				d, _ := contracts.Digest(ref)
				if !seen[d] {
					seen[d] = true
					refs = append(refs, ref)
				}
			}
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		a, _ := contracts.Digest(refs[i])
		b, _ := contracts.Digest(refs[j])
		return a < b
	})
	return refs, nil
}

func (s *Store) ImportScopeV2(ctx context.Context, sc contracts.Scope, e ScopeV2, build string) (ScopeV2Receipt, error) {
	var receipt ScopeV2Receipt
	if build == "" || build != s.scopeTransfer.NativeBuild {
		return receipt, invalid("owner build not admitted by transfer configuration")
	}
	if err := s.checkOwnerKVScopeImport(sc, e); err != nil {
		return receipt, err
	}

	if err := ValidateScopeV2(e, sc, build); err != nil {
		return receipt, err
	}
	if err := s.offlineScope(ctx, sc, e.Manifest.MigrationID, e.Manifest.SourceStoreID); err != nil {
		return receipt, err
	}
	// Existing artifact publication guard pins ALL declared refs for the whole
	// transaction, including rollback/lost-ACK settlement. Owner must already have
	// immutable versions under the same logical context and scope.
	if len(e.ArtifactReferences) > 0 {
		for _, r := range e.ArtifactReferences {
			if r.Tenant != sc.Tenant || r.Deployment != sc.Deployment || r.Subject != sc.Subject {
				return receipt, contracts.NewError(contracts.ErrForbidden, "foreign artifact reference")
			}
		}
		ctx = context.WithValue(ctx, checkpointAttemptKey{}, &checkpointAttempt{id: contracts.NewID(), scope: sc, refs: e.ArtifactReferences})
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var targetStoreID string
		if err := tx.QueryRow(ctx, `SELECT id FROM ahp_state.scope_store_v2 WHERE singleton`).Scan(&targetStoreID); err != nil {
			return err
		}
		if targetStoreID == e.Manifest.SourceStoreID {
			return contracts.NewError(contracts.ErrConflict, "scope transfer requires separate physical State store")
		}
		names := []string{"ahp_state.legalhold", "ahp_state.control_deletions", "ahp_state.erased_subjects", "ahp_state.erased_tenants", "ahp_state.kv_namespace_owners"}
		for _, sp := range scopeV2Entities {
			names = append(names, "ahp_state."+sp.name)
		}
		// A physical clean-target profile serializes even writers that do not use
		// migration advisory locks. No staged rows become visible before COMMIT.
		if _, err := tx.Exec(ctx, `LOCK TABLE `+strings.Join(names, ",")+` IN ACCESS EXCLUSIVE MODE`); err != nil {
			return err
		}
		if found, err := kvScopeHasOwners(ctx, tx, sc); err != nil {
			return err
		} else if found {
			return invalid("scope-v2 does not support declared owner-scoped KV")
		}
		var old ScopeV2Receipt
		err := tx.QueryRow(ctx, `SELECT migration_id,digest,tenant,deployment,subject,imported_at FROM ahp_state.scope_transfers_v2 WHERE migration_id=$1`, e.Manifest.MigrationID).Scan(&old.MigrationID, &old.Digest, &old.Scope.Tenant, &old.Scope.Deployment, &old.Scope.Subject, &old.ImportedAt)
		if err == nil {
			if old.Digest != e.Digest || old.Scope != sc {
				return contracts.NewError(contracts.ErrKeyReuse, "migration id reused for different archive")
			}
			receipt = old
			if attempt, _ := ctx.Value(checkpointAttemptKey{}).(*checkpointAttempt); attempt != nil {
				attempt.refs = nil
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		for _, name := range names {
			var n bool
			if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+name+`)`).Scan(&n); err != nil {
				return err
			}
			if n {
				return contracts.NewError(contracts.ErrConflict, "scope v2 requires clean separate physical State database")
			}
		}
		// Insert checkpoint headers with null parent first, then wire original
		// parents. Preserve every opaque ID; never guess parent order by timestamps.
		for _, sp := range scopeV2Entities {
			for _, raw := range e.Entities[sp.name] {
				var row map[string]json.RawMessage
				_ = json.Unmarshal(raw, &row)
				if sp.binary {
					var b64 string
					_ = json.Unmarshal(row["data"], &b64)
					data, _ := base64.StdEncoding.DecodeString(b64)
					v, _ := json.Marshal(`\x` + hex.EncodeToString(data))
					row["data"] = v
				}
				if sp.name == "checkpoints" {
					row["parent_id"] = json.RawMessage(`null`)
				}
				payload, err := json.Marshal(row)
				if err != nil {
					return invalid("cannot encode entity")
				}
				selects := []string{}
				for _, column := range strings.Split(sp.columns, ",") {
					expr := column
					if scopeV2JSONColumn(sp.name, column) {
						expr = column + "->'json'"
					}
					selects = append(selects, expr)
				}
				_, err = tx.Exec(ctx, `INSERT INTO ahp_state.`+sp.name+` (`+sp.columns+`) SELECT `+strings.Join(selects, ",")+` FROM jsonb_populate_record(NULL::ahp_state.`+sp.name+`,$1::jsonb)`, string(payload))
				if err != nil {
					return invalid("scope entity violates native schema or identity constraints: " + sp.name)
				}
			}
		}
		for _, raw := range e.Entities["checkpoints"] {
			var r struct {
				ID     string  `json:"id"`
				Parent *string `json:"parent_id"`
			}
			_ = json.Unmarshal(raw, &r)
			if r.Parent != nil {
				tag, err := tx.Exec(ctx, `UPDATE ahp_state.checkpoints SET parent_id=$2 WHERE id=$1 AND EXISTS(SELECT 1 FROM ahp_state.checkpoints WHERE id=$2 AND thread_id=(SELECT thread_id FROM ahp_state.checkpoints WHERE id=$1))`, r.ID, *r.Parent)
				if err != nil || tag.RowsAffected() != 1 {
					return invalid("checkpoint parent absent or foreign")
				}
			}
		}
		if err = s.offlineScope(ctx, sc, e.Manifest.MigrationID, e.Manifest.SourceStoreID); err != nil {
			return err
		}
		receipt = ScopeV2Receipt{MigrationID: e.Manifest.MigrationID, Digest: e.Digest, Scope: sc}
		return tx.QueryRow(ctx, `INSERT INTO ahp_state.scope_transfers_v2(migration_id,digest,tenant,deployment,subject) VALUES($1,$2,$3,$4,$5) RETURNING imported_at`, receipt.MigrationID, receipt.Digest, sc.Tenant, sc.Deployment, sc.Subject).Scan(&receipt.ImportedAt)
	})
	return receipt, err
}

// Seal/Open use the existing owner crypto API to protect the complete portable
// plaintext archive. Source and target may use different keys: Open at source,
// validate plaintext, then Seal with the target owner. Raw encrypted SQL rows
// are never accepted by this format. This does not mount crypto in State SQL.
func SealScopeV2(ctx context.Context, codec *atrest.Codec, e ScopeV2) ([]byte, error) {
	if codec == nil {
		return nil, invalid("archive crypto owner required")
	}
	if err := ValidateScopeV2(e, e.Manifest.Scope, e.Manifest.NativeBuild); err != nil {
		return nil, err
	}
	b, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	return codec.Seal(ctx, e.Manifest.Scope.Tenant, ScopeV2Format, b)
}
func OpenScopeV2(ctx context.Context, codec *atrest.Codec, tenant string, b []byte, sc contracts.Scope, build string) (ScopeV2, error) {
	if codec == nil || tenant != sc.Tenant {
		return ScopeV2{}, contracts.NewError(contracts.ErrForbidden, "archive crypto scope mismatch")
	}
	if len(b) > MaxScopeV2Bytes+65536 {
		return ScopeV2{}, invalid("encrypted archive too large")
	}
	plain, err := codec.Open(ctx, tenant, ScopeV2Format, b)
	if err != nil {
		return ScopeV2{}, contracts.NewError(contracts.ErrInvalid, "archive owner decrypt refused")
	}
	return ParseScopeV2(plain, sc, build)
}

// Semantic references are validated before target publication, independently of
// SQL constraints. The manifest digest detects damage, not malicious re-signing
// by an authorized admin; these checks still reject an incoherent archive.
func validateScopeV2Graph(e ScopeV2, sc contracts.Scope) error {
	type obj map[string]json.RawMessage
	rows := func(name string) []obj {
		all := []obj{}
		for _, b := range e.Entities[name] {
			var o obj
			_ = json.Unmarshal(b, &o)
			all = append(all, o)
		}
		return all
	}
	str := func(o obj, k string) string { var s string; _ = json.Unmarshal(o[k], &s); return s }
	threads := map[string]obj{}
	runs := map[string]obj{}
	cps := map[string]obj{}
	blobs := map[string]bool{}
	intents := map[string]obj{}
	for _, pair := range []struct {
		name string
		dst  map[string]obj
	}{{"threads", threads}, {"runs", runs}, {"checkpoints", cps}, {"intents", intents}} {
		for _, o := range rows(pair.name) {
			id := str(o, "id")
			if err := validIdentifier(id, "logical entity id"); err != nil {
				return err
			}
			if pair.dst[id] != nil {
				return invalid("duplicate logical entity id")
			}
			pair.dst[id] = o
		}
	}
	tuple := func(v ...string) string { b, _ := json.Marshal(v); return string(b) }
	// GetWrites addresses deterministic (thread, namespace, public ID) buckets,
	// including writes that arrived before their header. GetCheckpoint alone
	// also supports legacy headers whose internal ID equals their public ID.
	// Accepting an arbitrary header/bucket pair makes retained writes invisible.
	buckets := map[string]obj{}
	for id, cp := range cps {
		public := str(cp, "external_id")
		if err := validIdentifier(public, "public checkpoint id"); err != nil {
			return err
		}
		bucket, err := internalCheckpointID(str(cp, "thread_id"), str(cp, "namespace"), public)
		if err != nil {
			return err
		}
		if id != bucket && id != public {
			return invalid("checkpoint storage identity is unsupported by native owner")
		}
		if buckets[bucket] != nil {
			return invalid("duplicate public checkpoint identity")
		}
		buckets[bucket] = cp
	}
	for _, write := range rows("checkpoint_writes") {
		bucket := str(write, "checkpoint_id")
		digest, scoped := strings.CutPrefix(bucket, "sha256_")
		decoded, err := hex.DecodeString(digest)
		if !scoped || err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != digest {
			return invalid("pending write storage identity is unsupported by native owner")
		}
		if cp := cps[bucket]; cp != nil {
			expected, err := internalCheckpointID(str(cp, "thread_id"), str(cp, "namespace"), str(cp, "external_id"))
			if err != nil {
				return err
			}
			if bucket != expected || str(write, "thread_id") != str(cp, "thread_id") {
				return invalid("pending write differs from native checkpoint bucket or thread")
			}
		}
		if cp := buckets[bucket]; cp != nil && str(write, "thread_id") != str(cp, "thread_id") {
			return invalid("pending write belongs to another checkpoint thread")
		}
		// No matching header is a supported writes-before-checkpoint race. The
		// opaque bucket cannot be inverted to recover its public ID/namespace;
		// preserve it without inventing a header or changing any native bytes.
	}
	for _, o := range rows("checkpoint_blobs") {
		key := tuple(str(o, "thread_id"), str(o, "namespace"), str(o, "channel"), str(o, "version"))
		if blobs[key] {
			return invalid("duplicate blob identity")
		}
		blobs[key] = true
	}
	for id, o := range cps {
		parent := str(o, "parent_id")
		if parent != "" && (cps[parent] == nil || str(cps[parent], "thread_id") != str(o, "thread_id") || str(cps[parent], "namespace") != str(o, "namespace")) {
			return invalid("checkpoint parent absent or foreign")
		}
		if id == parent {
			return invalid("checkpoint cycle")
		}
	}
	colors := map[string]int{}
	var visit func(string) error
	visit = func(id string) error {
		if colors[id] == 1 {
			return invalid("checkpoint cycle")
		}
		if colors[id] == 2 {
			return nil
		}
		colors[id] = 1
		if parent := str(cps[id], "parent_id"); parent != "" {
			if err := visit(parent); err != nil {
				return err
			}
		}
		colors[id] = 2
		return nil
	}
	for id := range cps {
		if err := visit(id); err != nil {
			return err
		}
	}
	for _, o := range rows("checkpoint_parts") {
		cp := cps[str(o, "checkpoint_id")]
		if cp == nil || !blobs[tuple(str(cp, "thread_id"), str(cp, "namespace"), str(o, "channel"), str(o, "version"))] {
			return invalid("checkpoint part references absent blob")
		}
	}
	for _, name := range []string{"stream_events", "run_completions"} {
		for _, o := range rows(name) {
			rid := str(o, "run_id")
			if name == "run_completions" {
				rid = str(o, "id")
			}
			if runs[rid] == nil || str(runs[rid], "thread_id") != str(o, "thread_id") {
				return invalid("event run and thread differ")
			}
		}
	}
	ops := map[string]string{}
	for _, o := range rows("id_operations") {
		id := str(o, "resource_id")
		intent := intents[id]
		if err := validIdentifier(id, "operation resource id"); err != nil {
			return err
		}
		for _, field := range []string{"run_id", "node", "call"} {
			if err := validIdentifier(str(o, field), "operation locator"); err != nil {
				return err
			}
		}
		if intent != nil && (str(intent, "run_id") != str(o, "run_id") || str(intent, "node") != str(o, "node") || str(intent, "call") != str(o, "call")) {
			return invalid("operation locator differs from intent")
		}
		if str(o, "operation_key") != contracts.OperationKey(sc, str(o, "run_id"), str(o, "node"), str(o, "call")) {
			return invalid("intent operation key identity mismatch")
		}
		if ops[id] != "" {
			return invalid("duplicate intent operation receipt")
		}
		ops[id] = str(o, "operation_key")
	}
	for id := range intents {
		if ops[id] == "" {
			return invalid("missing intent operation receipt")
		}
	}
	for _, o := range rows("scope_transfers_v2") {
		if str(o, "migration_id") == e.Manifest.MigrationID {
			return invalid("current migration id already in source history")
		}
	}
	for _, name := range []string{"checkpoints", "checkpoint_blobs", "checkpoint_writes"} {
		for _, o := range rows(name) {
			var label string
			field, encoding := "data", "encoding"
			if name == "checkpoints" {
				field, encoding = "metadata", "metadata_encoding"
			}
			_ = json.Unmarshal(o[encoding], &label)
			if label == "" {
				continue
			}
			var data []byte
			if field == "data" {
				var b64 string
				_ = json.Unmarshal(o[field], &b64)
				data, _ = base64.StdEncoding.DecodeString(b64)
			} else {
				var wrapper map[string]json.RawMessage
				_ = json.Unmarshal(o[field], &wrapper)
				data = wrapper["json"]
			}
			var ref references.CheckpointArtifact
			_ = json.Unmarshal(data, &ref)
			thread := threads[str(o, "thread_id")]
			if ref.Tenant != sc.Tenant || ref.Deployment != sc.Deployment || ref.Subject != sc.Subject || thread == nil || ref.ContextID != str(thread, "context_id") {
				return contracts.NewError(contracts.ErrForbidden, "artifact reference differs from native row context")
			}
		}
	}
	return nil
}

// Refuse schema drift instead of silently dropping a newly introduced owner
// table/column while still stamping the old native format/build.
func scopeV2SourceSchema(ctx context.Context, tx pgx.Tx) error {
	fields := map[string]map[string]bool{"scope_store_v2": {"singleton": true, "id": true}}
	for _, sp := range scopeV2Entities {
		fields[sp.name] = map[string]bool{}
		for _, column := range strings.Split(sp.columns, ",") {
			fields[sp.name][column] = true
		}
	}
	for _, column := range []string{"lease_run_id", "lease_worker", "lease_epoch", "lease_expires"} {
		fields["threads"][column] = true
	}
	for _, column := range []string{"lease_key", "lease_until"} {
		fields["run_completions"][column] = true
	}
	for _, table := range []string{"legalhold", "control_deletions", "erased_subjects", "erased_tenants"} {
		fields[table] = nil
	}
	rows, err := tx.Query(ctx, `SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='ahp_state' ORDER BY table_name,ordinal_position`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var table, column string
		if err = rows.Scan(&table, &column); err != nil {
			return err
		}
		columns, known := fields[table]
		if !known || (columns != nil && !columns[column]) {
			return invalid("State owner schema changed; scope format upgrade required")
		}
	}
	return rows.Err()
}
