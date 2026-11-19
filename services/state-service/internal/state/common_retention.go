package state

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"math"
)

// WithCommonRetention admits only reviewed complete-snapshot native profiles.
// Native dependency closure is not inferred from opaque properties.
func WithCommonRetention(profiles []string) (StoreOption, error) {
	if len(profiles) < 1 || len(profiles) > 64 {
		return nil, invalid("retention requires 1..64 reviewed native profiles")
	}
	admitted := map[string]bool{}
	for _, profile := range profiles {
		if e := validIdentifier(profile, "retention profile"); e != nil {
			return nil, e
		}
		if admitted[profile] {
			return nil, invalid("duplicate retention profile")
		}
		admitted[profile] = true
	}
	return func(s *Store) {
		s.commonRecords = true
		s.commonArtifactReferences = true
		s.commonRetentionProfiles = map[string]bool{}
		for k, v := range admitted {
			s.commonRetentionProfiles[k] = v
		}
	}, nil
}

type CommonRootRequest struct {
	Namespace       string `json:"namespace"`
	Key             string `json:"key"`
	OperationKey    string `json:"operation_key"`
	ExpectedVersion int64  `json:"expected_version"`
	Revision        *int64 `json:"revision"`
}
type CommonRoot struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Version   int64  `json:"version"`
	Revision  *int64 `json:"revision"`
}
type CommonPruneRequest struct {
	ThreadID         string `json:"thread_id"`
	Namespace        string `json:"namespace"`
	Key              string `json:"key"`
	ExpectedRevision int64  `json:"expected_revision"`
	KeepFromRevision int64  `json:"keep_from_revision"`
	Limit            int    `json:"limit,omitempty"`
}
type CommonPruneReport struct {
	ThreadID              string `json:"thread_id"`
	Namespace             string `json:"namespace"`
	HeadRevision          int64  `json:"head_revision"`
	ReadableFrom          int64  `json:"readable_from"`
	ArchivedCuts          int    `json:"archived_cuts"`
	ReclaimedRecords      int64  `json:"reclaimed_records"`
	ReclaimedPayloads     int64  `json:"reclaimed_payloads"`
	ReleasedArtifactRoots int64  `json:"released_artifact_roots"`
	More                  bool   `json:"more"`
}

const commonPrunedLayout = "ahp-common-pruned/research-1"
const commonRetentionDDL = `
CREATE TABLE IF NOT EXISTS ahp_state.common_retention_floors(
 thread_id text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
 namespace text NOT NULL,readable_from bigint NOT NULL CHECK(readable_from>0),
 PRIMARY KEY(thread_id,namespace));
CREATE TABLE IF NOT EXISTS ahp_state.common_retention_roots(
 thread_id text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
 namespace text NOT NULL,key text NOT NULL,version bigint NOT NULL CHECK(version>0),revision bigint,
 PRIMARY KEY(thread_id,namespace,key),
 FOREIGN KEY(thread_id,namespace,revision) REFERENCES ahp_state.common_cuts(thread_id,namespace,revision) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS ahp_state.common_retention_ops(
 thread_id text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
 namespace text NOT NULL,key text NOT NULL,kind text NOT NULL CHECK(kind IN ('root','prune')),
 digest text NOT NULL,result bytea NOT NULL,PRIMARY KEY(thread_id,namespace,key));
CREATE INDEX IF NOT EXISTS common_version_payload_refs_idx ON ahp_state.common_record_versions(thread_id,attributes_hash);
CREATE INDEX IF NOT EXISTS common_anchor_payload_refs_idx ON ahp_state.common_records(thread_id,attributes_hash);`

func retentionOperation(ctx context.Context, tx pgx.Tx, thread, ns, key, kind, digest string, out any) (bool, error) {
	var oldDigest, oldKind string
	var raw []byte
	e := tx.QueryRow(ctx, `SELECT kind,digest,result FROM ahp_state.common_retention_ops WHERE thread_id=$1 AND namespace=$2 AND key=$3`, thread, ns, key).Scan(&oldKind, &oldDigest, &raw)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if oldDigest != digest || oldKind != kind {
		return false, contracts.NewError(contracts.ErrKeyReuse, "retention operation key reused with another request")
	}
	if json.Unmarshal(raw, out) != nil {
		return false, contracts.NewError(contracts.ErrUnavailable, "invalid retention operation receipt")
	}
	return true, nil
}
func putRetentionOperation(ctx context.Context, tx pgx.Tx, thread, ns, key, kind, digest string, out any) error {
	raw, e := json.Marshal(out)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `INSERT INTO ahp_state.common_retention_ops(thread_id,namespace,key,kind,digest,result) VALUES($1,$2,$3,$4,$5,$6)`, thread, ns, key, kind, digest, raw)
	return e
}
func (s *Store) retentionAvailable() error {
	if len(s.commonRetentionProfiles) == 0 {
		return contracts.NewError(contracts.ErrUnavailable, "reviewed complete-snapshot retention profiles must be configured")
	}
	return nil
}
func (s *Store) retentionProfile(profile string) error {
	if !s.commonRetentionProfiles[profile] {
		return contracts.NewError(contracts.ErrForbidden, "native profile has no reviewed complete-snapshot retention policy")
	}
	return nil
}
func (s *Store) PutCommonRoot(ctx context.Context, scope contracts.Scope, f Fence, req CommonRootRequest) (CommonRoot, error) {
	var out CommonRoot
	if e := s.retentionAvailable(); e != nil {
		return out, e
	}
	if e := scope.Validate(); e != nil {
		return out, e
	}
	for _, v := range []string{req.Key, req.OperationKey} {
		if e := validIdentifier(v, "root key"); e != nil {
			return out, e
		}
	}
	if e := commonNamespace(req.Namespace); e != nil {
		return out, e
	}
	if req.ExpectedVersion < 0 || req.ExpectedVersion == math.MaxInt64 || req.Revision != nil && *req.Revision < 1 {
		return out, invalid("invalid root version or revision")
	}
	digest, e := contracts.Digest(struct {
		Kind, Run string
		Request   CommonRootRequest
	}{"root", f.RunID, req})
	if e != nil {
		return out, e
	}
	e = s.inTx(ctx, func(tx pgx.Tx) error {
		run, e := s.getRun(ctx, tx, scope, f.RunID)
		if e != nil {
			return e
		}
		thread := run.Thread.ID
		if e := checkSubjectWritable(ctx, tx, scope); e != nil {
			return e
		}
		found, e := retentionOperation(ctx, tx, thread, req.Namespace, req.OperationKey, "root", digest, &out)
		if e != nil || found {
			return e
		}
		if _, e := s.currentLease(ctx, tx, scope, f.RunID, f.Worker, f.Epoch); e != nil {
			return e
		}
		found, e = retentionOperation(ctx, tx, thread, req.Namespace, req.OperationKey, "root", digest, &out)
		if e != nil || found {
			return e
		}
		var version int64
		e = tx.QueryRow(ctx, `SELECT version FROM ahp_state.common_retention_roots WHERE thread_id=$1 AND namespace=$2 AND key=$3`, thread, req.Namespace, req.Key).Scan(&version)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
		if version != req.ExpectedVersion {
			return contracts.NewError(contracts.ErrConflict, "root expected version differs")
		}
		if req.Revision != nil {
			var profile, format string
			e = tx.QueryRow(ctx, `SELECT profile,format FROM ahp_state.common_cuts WHERE thread_id=$1 AND namespace=$2 AND revision=$3`, thread, req.Namespace, *req.Revision).Scan(&profile, &format)
			if errors.Is(e, pgx.ErrNoRows) || format == commonPrunedLayout {
				return notFound("readable common root")
			}
			if e != nil {
				return e
			}
			if e := s.retentionProfile(profile); e != nil {
				return e
			}
		} else if version == 0 {
			return notFound("common root")
		}
		out = CommonRoot{Namespace: req.Namespace, Key: req.Key, Version: version + 1, Revision: req.Revision}
		if _, e := tx.Exec(ctx, `INSERT INTO ahp_state.common_retention_roots(thread_id,namespace,key,version,revision) VALUES($1,$2,$3,$4,$5) ON CONFLICT(thread_id,namespace,key) DO UPDATE SET version=EXCLUDED.version,revision=EXCLUDED.revision`, thread, req.Namespace, req.Key, out.Version, req.Revision); e != nil {
			return e
		}
		return putRetentionOperation(ctx, tx, thread, req.Namespace, req.OperationKey, "root", digest, out)
	})
	return out, e
}

func (s *Store) PruneCommonHistory(ctx context.Context, scope contracts.Scope, req CommonPruneRequest) (CommonPruneReport, error) {
	var out CommonPruneReport
	if e := s.retentionAvailable(); e != nil {
		return out, e
	}
	if e := scope.Validate(); e != nil {
		return out, e
	}
	for _, v := range []string{req.ThreadID, req.Key} {
		if e := validIdentifier(v, "common prune identifier"); e != nil {
			return out, e
		}
	}
	if e := commonNamespace(req.Namespace); e != nil {
		return out, e
	}
	if req.ExpectedRevision < 1 || req.KeepFromRevision < 1 || req.Limit < 0 || req.Limit > 512 {
		return out, invalid("invalid common retention bounds")
	}
	limit := req.Limit
	if limit == 0 {
		limit = 128
	}
	digest, e := contracts.Digest(struct {
		Kind    string
		Request CommonPruneRequest
	}{"prune", req})
	if e != nil {
		return out, e
	}
	e = s.inTx(ctx, func(tx pgx.Tx) error {
		// Same lock order as subject controls, all held on this transaction's one
		// connection: no nested pool acquire (also works with MaxConns=1).
		for _, key := range []string{"state-control-tenant/" + scope.Tenant, "state-control/" + scope.Tenant + "/" + scope.Subject} {
			sql := `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`
			if key == "state-control-tenant/"+scope.Tenant {
				sql = `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`
			}
			if _, e := tx.Exec(ctx, sql, key); e != nil {
				return e
			}
		}
		if e := checkSubjectWritable(ctx, tx, scope); e != nil {
			return e
		}
		var id string
		e := tx.QueryRow(ctx, `SELECT id FROM ahp_state.threads WHERE id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4 FOR UPDATE`, req.ThreadID, scope.Tenant, scope.Deployment, scope.Subject).Scan(&id)
		if errors.Is(e, pgx.ErrNoRows) {
			return notFound("thread")
		}
		if e != nil {
			return e
		}
		found, e := retentionOperation(ctx, tx, id, req.Namespace, req.Key, "prune", digest, &out)
		if e != nil || found {
			return e
		}
		var held, active, unsafe bool
		if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.legalhold WHERE tenant=$1 AND(subject=''OR subject=$2)AND(thread_id=''OR thread_id=$3))`, scope.Tenant, scope.Subject, id).Scan(&held); e != nil {
			return e
		}
		if held {
			return contracts.NewError(contracts.ErrForbidden, "common history is under legalhold")
		}
		if e := tx.QueryRow(ctx, `SELECT lease_expires IS NOT NULL AND lease_expires>clock_timestamp() FROM ahp_state.threads WHERE id=$1`, id).Scan(&active); e != nil {
			return e
		}
		if active {
			return contracts.NewError(contracts.ErrConflict, "common pruning requires an idle thread")
		}
		if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.intents i JOIN ahp_state.runs r ON r.id=i.run_id WHERE r.thread_id=$1 AND i.status IN('recorded','dispatched','unknown'))`, id).Scan(&unsafe); e != nil {
			return e
		}
		if unsafe {
			return contracts.NewError(contracts.ErrConflict, "unresolved operations retain common history")
		}
		var head int64
		var profile, format string
		e = tx.QueryRow(ctx, `SELECT h.revision,c.profile,c.format FROM ahp_state.common_heads h JOIN ahp_state.common_cuts c USING(thread_id,namespace,revision) WHERE h.thread_id=$1 AND h.namespace=$2`, id, req.Namespace).Scan(&head, &profile, &format)
		if errors.Is(e, pgx.ErrNoRows) {
			return notFound("common head")
		}
		if e != nil {
			return e
		}
		if head != req.ExpectedRevision {
			return contracts.NewError(contracts.ErrConflict, "common head changed before retention")
		}
		if e := s.retentionProfile(profile); e != nil {
			return e
		}
		if !s.commonIncrementalAppend {
			var needs bool
			if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.common_version_roots WHERE thread_id=$1 AND namespace=$2 AND digest LIKE 'mb1:%')`, id, req.Namespace).Scan(&needs); e != nil {
				return e
			}
			if needs {
				return contracts.NewError(contracts.ErrUnavailable, "incremental retention reader is not configured")
			}
		}
		if format == commonVersionLayout && !s.commonRecordVersions {
			return contracts.NewError(contracts.ErrUnavailable, "native version reader not configured")
		}
		if req.KeepFromRevision > head {
			return invalid("retention cannot remove current common head")
		}
		floor := int64(1)
		e = tx.QueryRow(ctx, `SELECT readable_from FROM ahp_state.common_retention_floors WHERE thread_id=$1 AND namespace=$2`, id, req.Namespace).Scan(&floor)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return e
		}
		target := req.KeepFromRevision
		var pinned *int64
		if e := tx.QueryRow(ctx, `SELECT min(revision) FROM ahp_state.common_retention_roots WHERE thread_id=$1 AND namespace=$2`, id, req.Namespace).Scan(&pinned); e != nil {
			return e
		}
		if pinned != nil && *pinned < target {
			target = *pinned
		}
		if target < floor {
			target = floor
		}
		if target-floor > int64(limit) {
			target = floor + int64(limit)
		}
		out = CommonPruneReport{ThreadID: id, Namespace: req.Namespace, HeadRevision: head, ReadableFrom: target, More: target < req.KeepFromRevision && (pinned == nil || target < *pinned)}
		rows, e := tx.Query(ctx, `SELECT c.revision,c.root,c.format,c.profile,r.result FROM ahp_state.common_cuts c LEFT JOIN ahp_state.common_receipts r USING(thread_id,namespace,revision) WHERE c.thread_id=$1 AND c.namespace=$2 AND c.revision>=$3 AND c.revision<$4 ORDER BY c.revision`, id, req.Namespace, floor, target)
		if e != nil {
			return e
		}
		type cut struct {
			rev                   int64
			root, format, profile string
			receipt               []byte
		}
		cuts := []cut{}
		for rows.Next() {
			var c cut
			if e := rows.Scan(&c.rev, &c.root, &c.format, &c.profile, &c.receipt); e != nil {
				rows.Close()
				return e
			}
			cuts = append(cuts, c)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		if int64(len(cuts)) != target-floor {
			return contracts.NewError(contracts.ErrUnavailable, "common retention history has a gap")
		}
		for _, c := range cuts {
			if c.format != CommonFormat && c.format != commonVersionLayout {
				return invalid("unsupported common layout in retention window")
			}
			if e := s.retentionProfile(c.profile); e != nil {
				return e
			}
			var receipt CommonReceipt
			if json.Unmarshal(c.receipt, &receipt) != nil || receipt.Revision != c.rev || receipt.Namespace != req.Namespace || receipt.RecordCount < 1 {
				return contracts.NewError(contracts.ErrUnavailable, "common retention cut has no valid receipt")
			}
			marker, e := json.Marshal(struct {
				Format         string `json:"format"`
				OriginalFormat string `json:"original_format"`
				RecordCount    int    `json:"record_count"`
			}{commonPrunedLayout, c.format, receipt.RecordCount})
			if e != nil {
				return e
			}
			// Explicit references are released only when their native cut is removed
			// from the readable window. Remaining cuts and named roots still protect.
			if s.commonArtifactReferences {
				tag, e := tx.Exec(ctx, `DELETE FROM ahp_state.common_artifact_refs WHERE thread_id=$1 AND namespace=$2 AND revision=$3`, id, req.Namespace, c.rev)
				if e != nil {
					return e
				}
				out.ReleasedArtifactRoots += tag.RowsAffected()
			}
			if _, e := tx.Exec(ctx, `DELETE FROM ahp_state.common_roles WHERE thread_id=$1 AND namespace=$2 AND revision=$3`, id, req.Namespace, c.rev); e != nil {
				return e
			}
			if _, e := tx.Exec(ctx, `UPDATE ahp_state.common_records SET kind='metadata',native_id=NULL,sequence=NULL,owner_hint=NULL,presence=0,attributes=$5,attributes_hash=NULL WHERE thread_id=$1 AND namespace=$2 AND revision=$3 AND id=$4`, id, req.Namespace, c.rev, c.root, marker); e != nil {
				return e
			}
			tag, e := tx.Exec(ctx, `DELETE FROM ahp_state.common_records WHERE thread_id=$1 AND namespace=$2 AND revision=$3 AND id<>$4`, id, req.Namespace, c.rev, c.root)
			if e != nil {
				return e
			}
			out.ReclaimedRecords += tag.RowsAffected()
			if _, e := tx.Exec(ctx, `DELETE FROM ahp_state.common_version_roots WHERE thread_id=$1 AND namespace=$2 AND revision=$3`, id, req.Namespace, c.rev); e != nil {
				return e
			}
			if _, e := tx.Exec(ctx, `UPDATE ahp_state.common_cuts SET format=$4 WHERE thread_id=$1 AND namespace=$2 AND revision=$3`, id, req.Namespace, c.rev, commonPrunedLayout); e != nil {
				return e
			}
			out.ArchivedCuts++
		}
		if e := s.pruneCommonAppend(ctx, tx, id, req.Namespace, target); e != nil {
			return e
		}
		for _, table := range []string{"common_record_versions", "common_role_versions", "common_edge_versions"} {
			tag, e := tx.Exec(ctx, `DELETE FROM ahp_state.`+table+` WHERE thread_id=$1 AND namespace=$2 AND to_revision<=$3`, id, req.Namespace, target)
			if e != nil {
				return e
			}
			out.ReclaimedRecords += tag.RowsAffected()
		}
		tag, e := tx.Exec(ctx, `DELETE FROM ahp_state.common_payloads p WHERE p.thread_id=$1 AND NOT EXISTS(SELECT 1 FROM ahp_state.common_records r WHERE r.thread_id=p.thread_id AND r.attributes_hash=p.sha256) AND NOT EXISTS(SELECT 1 FROM ahp_state.common_record_versions r WHERE r.thread_id=p.thread_id AND r.attributes_hash=p.sha256)`, id)
		if e != nil {
			return e
		}
		out.ReclaimedPayloads = tag.RowsAffected()
		if _, e := tx.Exec(ctx, `INSERT INTO ahp_state.common_retention_floors(thread_id,namespace,readable_from) VALUES($1,$2,$3) ON CONFLICT(thread_id,namespace) DO UPDATE SET readable_from=EXCLUDED.readable_from`, id, req.Namespace, target); e != nil {
			return e
		}
		return putRetentionOperation(ctx, tx, id, req.Namespace, req.Key, "prune", digest, out)
	})
	return out, e
}
