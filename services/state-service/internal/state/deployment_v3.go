package state

// Experimental logical deployment transfer. This is a fixed data schema, not
// SQL text or pg_dump. Shared app state moves with the complete deployment.
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

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const DeploymentV3Format = "ahp-state-deployment/3"
const DeploymentV3Native = "ahp-state-common-owner/research-3"
const MaxDeploymentV3Bytes = 32 << 20
const MaxDeploymentV3Rows = 50000
const MaxDeploymentV3RowBytes = 12 << 20
const MaxDeploymentV3ExpandedBytes = 128 << 20

type DeploymentUnit struct {
	Tenant     string `json:"tenant"`
	Deployment string `json:"deployment"`
}

func (u DeploymentUnit) Validate() error {
	return (contracts.Scope{Tenant: u.Tenant, Deployment: u.Deployment, Subject: "control-plane"}).Validate()
}

type DeploymentCustody struct {
	Unit          DeploymentUnit `json:"unit"`
	MigrationID   string         `json:"migration_id"`
	SourceStoreID string         `json:"source_store_id"`
	Digest        string         `json:"digest"`
	Phase         string         `json:"phase"`
}

// Authorize is a trusted coordinator callback, never a request boolean. It
// binds custody/quiescence to source physical identity and archive digest.
// It must stop/drain API writers, consumers and GC until target activation.
type DeploymentTransferConfig struct {
	NativeBuild     string
	Authorize       func(context.Context, DeploymentCustody) error
	SharedArtifacts *SharedArtifactTransfer
}

func WithDeploymentTransfer(cfg DeploymentTransferConfig) StoreOption {
	return func(s *Store) { s.deploymentTransfer = cfg; s.commonRecords = true }
}

type DeploymentV3Manifest struct {
	Format          string         `json:"format"`
	Native          string         `json:"native"`
	NativeBuild     string         `json:"native_build"`
	Profile         string         `json:"profile"`
	PayloadEncoding string         `json:"payload_encoding"`
	Unit            DeploymentUnit `json:"unit"`
	Subjects        []string       `json:"subjects"`
	SourceStoreID   string         `json:"source_store_id"`
	MigrationID     string         `json:"migration_id"`
	ExportedAt      time.Time      `json:"exported_at"`
	Counts          map[string]int `json:"counts"`
}
type DeploymentV3 struct {
	Manifest DeploymentV3Manifest         `json:"manifest"`
	Entities map[string][]json.RawMessage `json:"entities"`
	Digest   string                       `json:"digest"`
}
type DeploymentV3Receipt struct {
	MigrationID   string         `json:"migration_id"`
	Digest        string         `json:"digest"`
	Unit          DeploymentUnit `json:"unit"`
	SourceStoreID string         `json:"source_store_id"`
	TargetStoreID string         `json:"target_store_id"`
	ImportedAt    time.Time      `json:"imported_at"`
}

const deploymentV3DDL = `CREATE TABLE IF NOT EXISTS ahp_state.deployment_transfers_v3(
 migration_id text PRIMARY KEY,digest text NOT NULL,tenant text NOT NULL,deployment text NOT NULL,
 source_store_id text NOT NULL,target_store_id text NOT NULL,imported_at timestamptz NOT NULL DEFAULT clock_timestamp());`
const deploymentCTE = `WITH scoped_threads AS (SELECT id FROM ahp_state.threads WHERE tenant=$1 AND deployment=$2) `

var deploymentV3Entities = func() []scopeEntity {
	out := []scopeEntity{{"kv_namespace_owners", "tenant,deployment,namespace,owner", "x.tenant=$1 AND x.deployment=$2", "namespace", false}}
	for _, sp := range scopeV2Entities {
		sp.where = strings.ReplaceAll(sp.where, " AND x.subject=$3", "")
		if sp.name == "kv" {
			sp.order = "subject,namespace,key"
		}
		if sp.name == "kv_ops" {
			sp.order = "subject,operation_key"
		}
		out = append(out, sp)
	}
	out = append(out,
		scopeEntity{"common_payloads", "thread_id,sha256,attributes", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,sha256", false},
		scopeEntity{"common_cuts", "thread_id,namespace,revision,id,format,profile,root,operation_key,created_at", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,revision", false},
		scopeEntity{"common_records", "thread_id,namespace,revision,id,kind,native_id,sequence,owner_hint,presence,attributes,attributes_hash", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,revision,id", false},
		scopeEntity{"common_roles", "thread_id,namespace,revision,source,role", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,revision,source,role", false},
		scopeEntity{"common_edges", "thread_id,namespace,revision,source,role,ordinal,target", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,revision,source,role,ordinal", false},
		scopeEntity{"common_heads", "thread_id,namespace,revision", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace", false},
		scopeEntity{"common_receipts", "thread_id,namespace,key,revision,digest,result", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,key", false},
		scopeEntity{"deployment_transfers_v3", "migration_id,digest,tenant,deployment,source_store_id,target_store_id,imported_at", "x.tenant=$1 AND x.deployment=$2", "migration_id", false})
	return out
}()

func deploymentBinary(entity, column string) bool {
	return (entity == "common_append_nodes" || entity == "common_append_roots") && column == "data" || entity == "common_record_versions" && column == "native_id" || (entity == "checkpoint_blobs" || entity == "checkpoint_writes") && column == "data" || entity == "common_records" && (column == "native_id" || column == "attributes") || entity == "common_payloads" && column == "attributes" || (entity == "common_receipts" || entity == "common_retention_ops") && column == "result"
}
func deploymentDigest(e DeploymentV3) (string, error) { e.Digest = ""; return contracts.Digest(e) }
func (s *Store) deploymentCustody(ctx context.Context, u DeploymentUnit, id, source, digest, phase string) error {
	if err := u.Validate(); err != nil {
		return err
	}
	if err := validIdentifier(id, "migration id"); err != nil {
		return err
	}
	if err := validIdentifier(source, "source physical store"); err != nil {
		return err
	}
	if s.deploymentTransfer.Authorize == nil {
		return contracts.NewError(contracts.ErrForbidden, "deployment transfer requires trusted offline coordinator")
	}
	return s.deploymentTransfer.Authorize(ctx, DeploymentCustody{Unit: u, MigrationID: id, SourceStoreID: source, Digest: digest, Phase: phase})
}
func deploymentSchema(ctx context.Context, tx pgx.Tx) error {
	return deploymentSchemaFor(ctx, tx, deploymentV3Entities)
}
func deploymentSchemaFor(ctx context.Context, tx pgx.Tx, specs []scopeEntity) error {
	fields := map[string]map[string]bool{"scope_store_v2": {"singleton": true, "id": true}}
	for _, sp := range specs {
		fields[sp.name] = map[string]bool{}
		for _, c := range strings.Split(sp.columns, ",") {
			fields[sp.name][c] = true
		}
	}
	for _, c := range []string{"lease_run_id", "lease_worker", "lease_epoch", "lease_expires"} {
		fields["threads"][c] = true
	}
	for _, c := range []string{"lease_key", "lease_until"} {
		fields["run_completions"][c] = true
	}
	for _, t := range []string{"legalhold", "control_deletions", "erased_subjects", "erased_tenants"} {
		fields[t] = nil
	}
	rows, e := tx.Query(ctx, `SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='ahp_state'`)
	if e != nil {
		return e
	}
	defer rows.Close()
	for rows.Next() {
		var t, c string
		if e = rows.Scan(&t, &c); e != nil {
			return e
		}
		columns, ok := fields[t]
		if !ok || columns != nil && !columns[c] {
			return invalid("State owner schema changed; deployment format upgrade required")
		}
	}
	return rows.Err()
}
func (s *Store) exportDeploymentState(ctx context.Context, u DeploymentUnit, id, build string, specs []scopeEntity) (DeploymentV3, error) {
	var image DeploymentV3
	if s.deploymentTransfer.Authorize == nil {
		return image, contracts.NewError(contracts.ErrForbidden, "deployment transfer requires trusted offline coordinator")
	}
	if build == "" || build != s.deploymentTransfer.NativeBuild {
		return image, invalid("owner build not admitted")
	}
	if err := validIdentifier(build, "native build"); err != nil {
		return image, err
	}
	var source string
	if err := s.pool.QueryRow(ctx, `SELECT id FROM ahp_state.scope_store_v2 WHERE singleton`).Scan(&source); err != nil {
		return image, err
	}
	if err := s.deploymentCustody(ctx, u, id, source, "", "before-export"); err != nil {
		return image, err
	}
	image = DeploymentV3{Manifest: DeploymentV3Manifest{Format: DeploymentV3Format, Native: DeploymentV3Native, NativeBuild: build, Profile: "offline-same-deployment-clean-db-inline/1", PayloadEncoding: "state-owner-plaintext/base64/1", Unit: u, SourceStoreID: source, MigrationID: id, ExportedAt: time.Now().UTC(), Subjects: []string{}, Counts: map[string]int{}}, Entities: map[string][]json.RawMessage{}}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ,READ ONLY`); e != nil {
			return e
		}
		if e := deploymentSchemaFor(ctx, tx, specs); e != nil {
			return e
		}
		if e := deploymentExportControls(ctx, tx, u); e != nil {
			return e
		}
		total, size := 0, 0
		for _, sp := range specs {
			projections := []string{}
			for _, c := range strings.Split(sp.columns, ",") {
				v := c
				if deploymentJSONColumn(sp.name, c) {
					v = "CASE WHEN " + c + " IS NULL THEN NULL ELSE jsonb_build_object('json'," + c + ") END AS " + c
				}
				if deploymentBinary(sp.name, c) {
					v = "encode(" + c + ",'base64') AS " + c
				}
				projections = append(projections, v)
			}
			rows, e := tx.Query(ctx, deploymentCTE+`SELECT row_to_json(t) FROM (SELECT `+strings.Join(projections, ",")+` FROM ahp_state.`+sp.name+` x WHERE `+sp.where+` ORDER BY `+sp.order+`) t LIMIT 50001`, u.Tenant, u.Deployment)
			if e != nil {
				return e
			}
			image.Entities[sp.name] = []json.RawMessage{}
			for rows.Next() {
				var raw []byte
				if e = rows.Scan(&raw); e != nil {
					rows.Close()
					return e
				}
				total++
				size += len(raw)
				if total > MaxDeploymentV3Rows || size > MaxDeploymentV3Bytes || len(raw) > MaxDeploymentV3RowBytes {
					rows.Close()
					return invalid("deployment export exceeds bounded profile")
				}
				image.Entities[sp.name] = append(image.Entities[sp.name], json.RawMessage(raw))
			}
			rows.Close()
			if e = rows.Err(); e != nil {
				return e
			}
			image.Manifest.Counts[sp.name] = len(image.Entities[sp.name])
		}
		return nil
	})
	if err != nil {
		return DeploymentV3{}, err
	}
	image.Manifest.Subjects = deploymentSubjects(image)
	image.Digest, err = deploymentDigest(image)
	if err != nil {
		return DeploymentV3{}, err
	}
	return image, nil
}
func (s *Store) ExportDeploymentV3(ctx context.Context, u DeploymentUnit, id, build string) (DeploymentV3, error) {
	image, err := s.exportDeploymentState(ctx, u, id, build, deploymentV3Entities)
	if err != nil {
		return DeploymentV3{}, err
	}
	source := image.Manifest.SourceStoreID
	if err = ValidateDeploymentV3(image, u, build); err != nil {
		return DeploymentV3{}, err
	}
	if err = s.deploymentPolicyConfig(image); err != nil {
		return DeploymentV3{}, err
	}
	if err = s.deploymentCustody(ctx, u, id, source, image.Digest, "after-export"); err != nil {
		return DeploymentV3{}, err
	}
	return image, nil
}

// Raw archives also pass through this decoder when nested in HTTP input.
// Opaque native bytes are base64, so duplicate logical fields are never needed.
func (e *DeploymentV3) UnmarshalJSON(raw []byte) error {
	if len(raw) > MaxDeploymentV3Bytes {
		return invalid("deployment archive too large")
	}
	if _, err := contracts.Digest(json.RawMessage(raw)); err != nil {
		return invalid("duplicate or invalid archive JSON")
	}
	type wire DeploymentV3
	var parsed wire
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&parsed); err != nil {
		return invalid("invalid deployment archive")
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return invalid("trailing deployment archive")
	}
	*e = DeploymentV3(parsed)
	return nil
}

func ParseDeploymentV3(raw []byte, u DeploymentUnit, build string) (DeploymentV3, error) {
	var e DeploymentV3
	if len(raw) > MaxDeploymentV3Bytes {
		return e, invalid("deployment archive too large")
	}
	if _, err := contracts.Digest(json.RawMessage(raw)); err != nil {
		return e, invalid("duplicate or invalid archive JSON")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return e, invalid("invalid deployment archive")
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return e, invalid("trailing deployment archive data")
	}
	return e, ValidateDeploymentV3(e, u, build)
}
func (s *Store) importDeploymentState(ctx context.Context, u DeploymentUnit, image DeploymentV3, build string, specs []scopeEntity, receiptDigest string, beforeCommit func(context.Context) error) (DeploymentV3Receipt, error) {
	var receipt DeploymentV3Receipt
	if s.deploymentTransfer.Authorize == nil {
		return receipt, contracts.NewError(contracts.ErrForbidden, "deployment transfer requires trusted offline coordinator")
	}
	if build == "" || build != s.deploymentTransfer.NativeBuild {
		return receipt, invalid("owner build not admitted")
	}
	m := image.Manifest
	if err := s.deploymentCustody(ctx, u, m.MigrationID, m.SourceStoreID, receiptDigest, "before-import"); err != nil {
		return receipt, err
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var target string
		if e := tx.QueryRow(ctx, `SELECT id FROM ahp_state.scope_store_v2 WHERE singleton`).Scan(&target); e != nil {
			return e
		}
		if target == m.SourceStoreID {
			return contracts.NewError(contracts.ErrConflict, "separate physical target State store required")
		}
		names := []string{"ahp_state.legalhold", "ahp_state.control_deletions", "ahp_state.erased_subjects", "ahp_state.erased_tenants"}
		for _, sp := range specs {
			names = append(names, "ahp_state."+sp.name)
		}
		if _, e := tx.Exec(ctx, `LOCK TABLE `+strings.Join(names, ",")+` IN ACCESS EXCLUSIVE MODE`); e != nil {
			return e
		}
		if e := deploymentSchemaFor(ctx, tx, specs); e != nil {
			return e
		}
		var old DeploymentV3Receipt
		e := tx.QueryRow(ctx, `SELECT migration_id,digest,tenant,deployment,source_store_id,target_store_id,imported_at FROM ahp_state.deployment_transfers_v3 WHERE migration_id=$1`, m.MigrationID).Scan(&old.MigrationID, &old.Digest, &old.Unit.Tenant, &old.Unit.Deployment, &old.SourceStoreID, &old.TargetStoreID, &old.ImportedAt)
		if e == nil {
			if old.Digest != receiptDigest || old.Unit != u || old.SourceStoreID != m.SourceStoreID || old.TargetStoreID != target {
				return contracts.NewError(contracts.ErrKeyReuse, "migration id reused for another image")
			}
			receipt = old
			return nil
		}
		if !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
		// Reviewed policies may be registered on a clean target. They must exactly
		// match the image and operator config; content tables must still be empty.
		if e = s.validateDeploymentPolicies(ctx, tx, image); e != nil {
			return e
		}
		for _, name := range names {
			if name == "ahp_state.kv_namespace_owners" {
				continue
			}
			var any bool
			if e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+name+`)`).Scan(&any); e != nil {
				return e
			}
			if any {
				return contracts.NewError(contracts.ErrConflict, "deployment transfer requires clean separate State database")
			}
		}
		for _, sp := range specs {
			for _, raw := range image.Entities[sp.name] {
				var row map[string]json.RawMessage
				_ = json.Unmarshal(raw, &row)
				for _, c := range strings.Split(sp.columns, ",") {
					if deploymentBinary(sp.name, c) && !bytes.Equal(bytes.TrimSpace(row[c]), []byte("null")) {
						var b64 string
						_ = json.Unmarshal(row[c], &b64)
						b, _ := base64.StdEncoding.DecodeString(b64)
						row[c], _ = json.Marshal(`\x` + hex.EncodeToString(b))
					}
				}
				if sp.name == "checkpoints" {
					row["parent_id"] = json.RawMessage(`null`)
				}
				encoded, e := json.Marshal(row)
				if e != nil {
					return invalid("cannot encode deployment entity")
				}
				selects := []string{}
				for _, c := range strings.Split(sp.columns, ",") {
					v := c
					if deploymentJSONColumn(sp.name, c) {
						v = c + "->'json'"
					}
					selects = append(selects, v)
				}
				suffix := ""
				if sp.name == "kv_namespace_owners" {
					suffix = " ON CONFLICT DO NOTHING"
				}
				if _, e = tx.Exec(ctx, `INSERT INTO ahp_state.`+sp.name+` (`+sp.columns+`) SELECT `+strings.Join(selects, ",")+` FROM jsonb_populate_record(NULL::ahp_state.`+sp.name+`,$1::jsonb)`+suffix, string(encoded)); e != nil {
					return invalid("deployment entity violates native schema: " + sp.name)
				}
			}
		}
		for _, raw := range image.Entities["checkpoints"] {
			var cp struct {
				ID     string  `json:"id"`
				Parent *string `json:"parent_id"`
			}
			_ = json.Unmarshal(raw, &cp)
			if cp.Parent != nil {
				tag, e := tx.Exec(ctx, `UPDATE ahp_state.checkpoints SET parent_id=$2 WHERE id=$1 AND EXISTS(SELECT 1 FROM ahp_state.checkpoints WHERE id=$2 AND thread_id=(SELECT thread_id FROM ahp_state.checkpoints WHERE id=$1))`, cp.ID, *cp.Parent)
				if e != nil || tag.RowsAffected() != 1 {
					return invalid("checkpoint parent absent or foreign")
				}
			}
		}
		if beforeCommit != nil {
			if e = beforeCommit(ctx); e != nil {
				return e
			}
		}
		if e = s.deploymentCustody(ctx, u, m.MigrationID, m.SourceStoreID, receiptDigest, "before-commit"); e != nil {
			return e
		}
		receipt = DeploymentV3Receipt{MigrationID: m.MigrationID, Digest: receiptDigest, Unit: u, SourceStoreID: m.SourceStoreID, TargetStoreID: target}
		return tx.QueryRow(ctx, `INSERT INTO ahp_state.deployment_transfers_v3(migration_id,digest,tenant,deployment,source_store_id,target_store_id) VALUES($1,$2,$3,$4,$5,$6) RETURNING imported_at`, receipt.MigrationID, receipt.Digest, u.Tenant, u.Deployment, receipt.SourceStoreID, target).Scan(&receipt.ImportedAt)
	})
	return receipt, err
}
func (s *Store) ImportDeploymentV3(ctx context.Context, u DeploymentUnit, image DeploymentV3, build string) (DeploymentV3Receipt, error) {
	if s.deploymentTransfer.Authorize == nil {
		return DeploymentV3Receipt{}, contracts.NewError(contracts.ErrForbidden, "deployment transfer requires trusted offline coordinator")
	}
	if build == "" || build != s.deploymentTransfer.NativeBuild {
		return DeploymentV3Receipt{}, invalid("owner build not admitted")
	}
	if err := ValidateDeploymentV3(image, u, build); err != nil {
		return DeploymentV3Receipt{}, err
	}
	return s.importDeploymentState(ctx, u, image, build, deploymentV3Entities, image.Digest, nil)
}
func deploymentSubjects(e DeploymentV3) []string {
	seen := map[string]bool{}
	for _, rows := range e.Entities {
		for _, raw := range rows {
			var row map[string]json.RawMessage
			_ = json.Unmarshal(raw, &row)
			var subject string
			if json.Unmarshal(row["subject"], &subject) == nil && subject != "" {
				seen[subject] = true
			}
		}
	}
	out := []string{}
	for sub := range seen {
		out = append(out, sub)
	}
	sort.Strings(out)
	return out
}

func deploymentJSONColumn(entity, column string) bool {
	return scopeV2JSONColumn(entity, column) || (entity == "common_artifact_refs" && column == "artifact")
}

func deploymentExportControls(ctx context.Context, tx pgx.Tx, u DeploymentUnit) error {
	var unsafe bool
	if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.legalhold WHERE tenant=$1) OR EXISTS(SELECT 1 FROM ahp_state.control_deletions WHERE tenant=$1) OR EXISTS(SELECT 1 FROM ahp_state.erased_subjects WHERE tenant=$1) OR EXISTS(SELECT 1 FROM ahp_state.erased_tenants WHERE tenant=$1)`, u.Tenant).Scan(&unsafe); e != nil {
		return e
	}
	if unsafe {
		return contracts.NewError(contracts.ErrConflict, "tenant-wide controls require a separate custody contract")
	}
	if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.id_operations WHERE tenant IS NULL OR deployment IS NULL OR subject IS NULL OR run_id IS NULL OR node IS NULL OR call IS NULL)`).Scan(&unsafe); e != nil {
		return e
	}
	if unsafe {
		return contracts.NewError(contracts.ErrConflict, "unattributed operation receipts require owner metadata")
	}
	return nil
}
