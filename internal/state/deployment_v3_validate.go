package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

type deploymentRow map[string]json.RawMessage

func deploymentRows(e DeploymentV3, name string) []deploymentRow {
	out := []deploymentRow{}
	for _, raw := range e.Entities[name] {
		var r deploymentRow
		_ = json.Unmarshal(raw, &r)
		out = append(out, r)
	}
	return out
}
func deploymentString(r deploymentRow, k string) string {
	var s string
	_ = json.Unmarshal(r[k], &s)
	return s
}
func deploymentNull(r deploymentRow, k string) bool {
	return bytes.Equal(bytes.TrimSpace(r[k]), []byte("null"))
}
func deploymentTuple(v ...any) string { b, _ := json.Marshal(v); return string(b) }
func deploymentInt(r deploymentRow, k string) (int64, error) {
	var n int64
	if json.Unmarshal(r[k], &n) != nil {
		return 0, invalid("invalid integer field: " + k)
	}
	return n, nil
}
func deploymentBytes(r deploymentRow, k string) ([]byte, error) {
	if deploymentNull(r, k) {
		if k == "native_id" {
			return nil, nil
		}
		return nil, invalid("SQL NULL in required bytes")
	}
	var s string
	if json.Unmarshal(r[k], &s) != nil {
		return nil, invalid("invalid portable bytes")
	}
	b, e := base64.StdEncoding.DecodeString(s)
	if e != nil {
		return nil, invalid("invalid portable base64")
	}
	return b, nil
}
func deploymentSHA(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func ValidateDeploymentV3(e DeploymentV3, u DeploymentUnit, build string) error {
	return validateDeploymentV3(e, u, build, false)
}
func validateDeploymentV3(e DeploymentV3, u DeploymentUnit, build string, allowPruned bool) error {
	if err := u.Validate(); err != nil {
		return err
	}
	m := e.Manifest
	if m.Unit != u {
		return contracts.NewError(contracts.ErrForbidden, "deployment transfer cannot rewrite logical unit")
	}
	if m.Format != DeploymentV3Format || m.Native != DeploymentV3Native || m.NativeBuild != build || m.Profile != "offline-same-deployment-clean-db-inline/1" || m.PayloadEncoding != "state-owner-plaintext/base64/1" {
		return invalid("unsupported deployment format/native/profile")
	}
	if err := validIdentifier(m.SourceStoreID, "source store id"); err != nil {
		return err
	}
	if err := validIdentifier(m.MigrationID, "migration id"); err != nil {
		return err
	}
	if m.ExportedAt.IsZero() || len(e.Entities) != len(deploymentV3Entities) || len(m.Counts) != len(deploymentV3Entities) {
		return invalid("incomplete deployment manifest")
	}
	raw, err := json.Marshal(e)
	if err != nil || len(raw) > MaxDeploymentV3Bytes {
		return invalid("deployment archive too large")
	}
	d, err := deploymentDigest(e)
	if err != nil || d != e.Digest {
		return invalid("deployment manifest digest mismatch")
	}
	total := 0
	for _, sp := range deploymentV3Entities {
		rows, ok := e.Entities[sp.name]
		if !ok || len(rows) != m.Counts[sp.name] {
			return invalid("deployment row count mismatch")
		}
		total += len(rows)
		if total > MaxDeploymentV3Rows {
			return invalid("deployment row bound exceeded")
		}
		fields := strings.Split(sp.columns, ",")
		seen := map[string]bool{}
		for _, raw := range rows {
			if len(raw) > MaxDeploymentV3RowBytes {
				return invalid("deployment row too large")
			}
			var row deploymentRow
			if json.Unmarshal(raw, &row) != nil || len(row) != len(fields) {
				return invalid("invalid deployment row fields")
			}
			for _, c := range fields {
				v, ok := row[c]
				if !ok {
					return invalid("missing deployment row field")
				}
				if scopeV2JSONColumn(sp.name, c) {
					if deploymentNull(row, c) {
						if sp.name != "intents" || c != "result" {
							return invalid("SQL NULL in required JSON")
						}
					} else {
						var w map[string]json.RawMessage
						if json.Unmarshal(v, &w) != nil || len(w) != 1 || w["json"] == nil {
							return invalid("native JSON wrapper required")
						}
					}
				}
				if deploymentBinary(sp.name, c) {
					if _, err := deploymentBytes(row, c); err != nil {
						return err
					}
				}
			}
			if _, ok := row["tenant"]; ok && (deploymentString(row, "tenant") != u.Tenant || deploymentString(row, "deployment") != u.Deployment) {
				return contracts.NewError(contracts.ErrForbidden, "foreign deployment row")
			}
			keys := []json.RawMessage{}
			for _, c := range strings.Split(sp.order, ",") {
				keys = append(keys, row[c])
			}
			key, _ := contracts.Digest(keys)
			if seen[key] {
				return invalid("duplicate deployment row identity: " + sp.name)
			}
			seen[key] = true
		}
	}
	subjects := deploymentSubjects(e)
	a, _ := contracts.Digest(subjects)
	b, _ := contracts.Digest(m.Subjects)
	if a != b {
		return invalid("deployment subject catalog differs")
	}
	for _, s := range subjects {
		if err := (contracts.Scope{Tenant: u.Tenant, Deployment: u.Deployment, Subject: s}).Validate(); err != nil {
			return err
		}
	}
	// Validate legacy entity semantics per actual subject, without forcing app
	// KV into the identity of a last-writing session.
	threads := map[string]string{}
	runs := map[string]string{}
	cps := map[string]string{}
	for _, r := range deploymentRows(e, "threads") {
		id := deploymentString(r, "id")
		if err := validIdentifier(id, "thread id"); err != nil {
			return err
		}
		if _, ok := threads[id]; ok {
			return invalid("duplicate thread id")
		}
		subject := deploymentString(r, "subject")
		if subject == "" {
			return invalid("thread has empty subject")
		}
		threads[id] = subject
		n, err := deploymentInt(r, "epoch")
		if err != nil || n < 0 || n == int64(^uint64(0)>>1) {
			return invalid("invalid epoch floor")
		}
	}
	for _, r := range deploymentRows(e, "runs") {
		id, tid := deploymentString(r, "id"), deploymentString(r, "thread_id")
		if threads[tid] == "" {
			return invalid("run references foreign thread")
		}
		if _, ok := runs[id]; ok {
			return invalid("duplicate run id")
		}
		runs[id] = threads[tid]
	}
	for _, r := range deploymentRows(e, "checkpoints") {
		cps[deploymentString(r, "id")] = threads[deploymentString(r, "thread_id")]
	}
	for _, sp := range deploymentV3Entities {
		for _, r := range deploymentRows(e, sp.name) {
			if tid := deploymentString(r, "thread_id"); tid != "" && sp.name != "kv" && threads[tid] == "" {
				return invalid("row references foreign thread")
			}
			if rid := deploymentString(r, "run_id"); rid != "" && sp.name != "id_operations" && runs[rid] == "" {
				return invalid("row references foreign run")
			}
		}
	}
	groups := map[string]*ScopeV2{}
	for _, subject := range subjects {
		groups[subject] = &ScopeV2{Manifest: ScopeV2Manifest{MigrationID: m.MigrationID}, Entities: map[string][]json.RawMessage{}}
	}
	for _, sp := range scopeV2Entities {
		for _, group := range groups {
			group.Entities[sp.name] = []json.RawMessage{}
		}
		for i, r := range deploymentRows(e, sp.name) {
			owner := deploymentString(r, "subject")
			if tid := deploymentString(r, "thread_id"); owner == "" && tid != "" {
				owner = threads[tid]
			}
			if rid := deploymentString(r, "run_id"); owner == "" && rid != "" {
				owner = runs[rid]
			}
			if sp.name == "checkpoint_parts" {
				owner = cps[deploymentString(r, "checkpoint_id")]
			}
			if g := groups[owner]; g != nil {
				g.Entities[sp.name] = append(g.Entities[sp.name], e.Entities[sp.name][i])
			}
		}
	}
	for _, subject := range subjects {
		if err := validateScopeV2Graph(*groups[subject], contracts.Scope{Tenant: u.Tenant, Deployment: u.Deployment, Subject: subject}); err != nil {
			return err
		}
	}

	refs, err := scopeV2References(ScopeV2{Entities: e.Entities})
	if err != nil {
		return err
	}
	if len(refs) > 0 {
		return invalid("deployment v3 inline profile requires explicit Artifact custody before transfer")
	}
	for _, r := range deploymentRows(e, "deployment_transfers_v3") {
		if deploymentString(r, "migration_id") == m.MigrationID {
			return invalid("current migration id exists in source history")
		}
	}
	if err := validateDeploymentKV(e, threads); err != nil {
		return err
	}
	return validateDeploymentCommonRetention(e, threads, runs, allowPruned)
}
func deploymentPolicies(e DeploymentV3) (map[string]string, error) {
	out := map[string]string{}
	for _, r := range deploymentRows(e, "kv_namespace_owners") {
		var ns []string
		if json.Unmarshal(r["namespace"], &ns) != nil || validKVNamespace(ns) != nil || len(ns) == 0 {
			return nil, invalid("invalid reviewed namespace")
		}
		owner := deploymentString(r, "owner")
		if owner != "subject" && owner != "deployment" {
			return nil, invalid("unsupported owner declaration")
		}
		id := deploymentTuple(ns)
		if out[id] != "" {
			return nil, invalid("duplicate owner declaration")
		}
		out[id] = owner
	}
	return out, nil
}
func validateDeploymentKV(e DeploymentV3, threads map[string]string) error {
	policies, err := deploymentPolicies(e)
	if err != nil {
		return err
	}
	for _, r := range deploymentRows(e, "kv") {
		var ns []string
		if json.Unmarshal(r["namespace"], &ns) != nil || validKVNamespace(ns) != nil || validKVKey(deploymentString(r, "key")) != nil {
			return invalid("invalid KV namespace/key")
		}
		owner := policies[deploymentTuple(ns)]
		subject, tid := deploymentString(r, "subject"), deploymentString(r, "thread_id")
		if owner == "deployment" {
			if subject != "" || !deploymentNull(r, "thread_id") {
				return invalid("deployment owner has session/user lifetime")
			}
		} else if owner == "subject" {
			if subject == "" || !deploymentNull(r, "thread_id") {
				return invalid("subject owner has session lifetime")
			}
		} else {
			if subject == "" || tid == "" || threads[tid] != subject {
				return invalid("thread KV owner is absent or foreign")
			}
		}
	}
	for _, r := range deploymentRows(e, "id_operations") {
		sc := contracts.Scope{Tenant: e.Manifest.Unit.Tenant, Deployment: e.Manifest.Unit.Deployment, Subject: deploymentString(r, "subject")}
		if sc.Validate() != nil || deploymentString(r, "operation_key") != contracts.OperationKey(sc, deploymentString(r, "run_id"), deploymentString(r, "node"), deploymentString(r, "call")) {
			return invalid("operation idempotency identity differs")
		}
	}
	return nil
}
func (s *Store) deploymentPolicyConfig(e DeploymentV3) error {
	declarations, err := deploymentPolicies(e)
	if err != nil {
		return err
	}
	expected := map[string]string{}
	for _, p := range s.kvOwnerPolicies {
		if p.Tenant == e.Manifest.Unit.Tenant && p.Deployment == e.Manifest.Unit.Deployment {
			expected[deploymentTuple(p.Namespace)] = p.Owner
		}
	}
	a, _ := contracts.Digest(declarations)
	b, _ := contracts.Digest(expected)
	if a != b {
		return invalid("target/source reviewed owner config differs from archive")
	}
	return nil
}
func (s *Store) validateDeploymentPolicies(ctx context.Context, tx pgx.Tx, e DeploymentV3) error {
	if err := s.deploymentPolicyConfig(e); err != nil {
		return err
	}
	declarations, _ := deploymentPolicies(e)
	rows, err := tx.Query(ctx, `SELECT tenant,deployment,namespace,owner FROM ahp_state.kv_namespace_owners`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var t, d, o string
		var ns []string
		if err = rows.Scan(&t, &d, &ns, &o); err != nil {
			return err
		}
		if t != e.Manifest.Unit.Tenant || d != e.Manifest.Unit.Deployment || declarations[deploymentTuple(ns)] != o {
			return invalid("target owner declaration is foreign or incompatible")
		}
	}
	return rows.Err()
}
func deploymentPayloadHash(raw []byte) string {
	v := sha256.Sum256(raw)
	return hex.EncodeToString(v[:])
}

// Stable ordered relation construction is shared by validation, not SQL query
// planning: native payload remains opaque and its raw bytes are never rewritten.
func deploymentSortedOrdinals(v map[int64]string) ([]string, error) {
	keys := []int{}
	for n := range v {
		if n < 0 || n > MaxDeploymentV3Rows {
			return nil, invalid("invalid relation ordinal")
		}
		keys = append(keys, int(n))
	}
	sort.Ints(keys)
	out := []string{}
	for i, n := range keys {
		if n != i {
			return nil, invalid("relation ordinal gap")
		}
		out = append(out, v[int64(n)])
	}
	return out, nil
}
