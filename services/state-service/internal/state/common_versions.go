package state

// Research-only interval layout. Logical cuts remain immutable; subsequent
// commits close changed metadata intervals under the existing thread lock.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"strconv"
	"strings"
)

const commonVersionLayout = "ahp-common-interval-layout/research-1"

func WithCommonRecordVersions() StoreOption {
	return func(s *Store) { s.commonRecords = true; s.commonPayloadObjects = true; s.commonRecordVersions = true }
}

const commonVersionDDL = `
CREATE TABLE IF NOT EXISTS ahp_state.common_version_roots(
 thread_id text NOT NULL,namespace text NOT NULL,revision bigint NOT NULL,digest text NOT NULL,
 PRIMARY KEY(thread_id,namespace,revision),
 FOREIGN KEY(thread_id,namespace,revision) REFERENCES ahp_state.common_cuts(thread_id,namespace,revision) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS ahp_state.common_record_versions(
 thread_id text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
 namespace text NOT NULL,id text NOT NULL,from_revision bigint NOT NULL CHECK(from_revision>0),
 to_revision bigint CHECK(to_revision>from_revision),kind text NOT NULL,native_id bytea,
 sequence bigint,owner_hint text,presence smallint NOT NULL,attributes_hash text NOT NULL,
 PRIMARY KEY(thread_id,namespace,id,from_revision),
 FOREIGN KEY(thread_id,attributes_hash) REFERENCES ahp_state.common_payloads(thread_id,sha256));
CREATE UNIQUE INDEX IF NOT EXISTS common_record_current_idx ON ahp_state.common_record_versions(thread_id,namespace,id) WHERE to_revision IS NULL;
CREATE INDEX IF NOT EXISTS common_record_asof_idx ON ahp_state.common_record_versions(thread_id,namespace,from_revision,to_revision);
CREATE TABLE IF NOT EXISTS ahp_state.common_role_versions(
 thread_id text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
 namespace text NOT NULL,source text NOT NULL,role text NOT NULL,
 from_revision bigint NOT NULL CHECK(from_revision>0),to_revision bigint CHECK(to_revision>from_revision),
 PRIMARY KEY(thread_id,namespace,source,role,from_revision));
CREATE UNIQUE INDEX IF NOT EXISTS common_role_current_idx ON ahp_state.common_role_versions(thread_id,namespace,source,role) WHERE to_revision IS NULL;
CREATE INDEX IF NOT EXISTS common_role_asof_idx ON ahp_state.common_role_versions(thread_id,namespace,from_revision,to_revision);
CREATE TABLE IF NOT EXISTS ahp_state.common_edge_versions(
 thread_id text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
 namespace text NOT NULL,source text NOT NULL,role text NOT NULL,ordinal integer NOT NULL CHECK(ordinal>=0),target text NOT NULL,
 from_revision bigint NOT NULL CHECK(from_revision>0),to_revision bigint CHECK(to_revision>from_revision),
 PRIMARY KEY(thread_id,namespace,source,role,ordinal,from_revision));
CREATE UNIQUE INDEX IF NOT EXISTS common_edge_current_idx ON ahp_state.common_edge_versions(thread_id,namespace,source,role,ordinal) WHERE to_revision IS NULL;
CREATE INDEX IF NOT EXISTS common_edge_asof_idx ON ahp_state.common_edge_versions(thread_id,namespace,from_revision,to_revision);`

type commonRoleKey struct{ source, role string }
type commonEdgeKey struct {
	source, role string
	ordinal      int
}

func commonMetadataEqual(a, b CommonRecord) bool {
	return a.Kind == b.Kind && bytes.Equal(a.NativeID, b.NativeID) && bytes.Equal(a.Sequence, b.Sequence) && bytes.Equal(a.OwnerHint, b.OwnerHint) && bytes.Equal(a.Attributes, b.Attributes)
}
func commonRelations(doc *CommonDocument) (map[commonRoleKey]bool, map[commonEdgeKey]string) {
	roles := map[commonRoleKey]bool{}
	edges := map[commonEdgeKey]string{}
	if doc != nil {
		for id, r := range doc.Records {
			for role, refs := range r.Relations {
				roles[commonRoleKey{id, role}] = true
				for i, target := range refs {
					edges[commonEdgeKey{id, role, i}] = target
				}
			}
		}
	}
	return roles, edges
}
func commonVersionFields(row CommonRecord) (*int64, *string, int16) {
	seq, _ := commonSequence(row.Sequence)
	hint, _ := commonOwnerHint(row.OwnerHint)
	var presence int16
	if len(row.Sequence) > 0 {
		presence |= 1
	}
	if len(row.OwnerHint) > 0 {
		presence |= 2
	}
	return seq, hint, presence
}
func (s *Store) putCommonVersions(ctx context.Context, tx pgx.Tx, thread, ns string, rev int64, before *CommonDocument, doc CommonDocument, refs []CommonArtifactReference) error {
	old := map[string]CommonRecord{}
	if before != nil {
		old = before.Records
	}
	closeIDs := []string{}
	records := [][]any{}
	payloads := map[string][]byte{}
	for id, row := range old {
		next, ok := doc.Records[id]
		if !ok || !commonMetadataEqual(row, next) {
			closeIDs = append(closeIDs, id)
		}
	}
	for id, row := range doc.Records {
		previous, ok := old[id]
		if ok && commonMetadataEqual(previous, row) {
			continue
		}
		sum := sha256.Sum256(row.Attributes)
		hash := hex.EncodeToString(sum[:])
		payloads[hash] = row.Attributes
		seq, hint, presence := commonVersionFields(row)
		records = append(records, []any{thread, ns, id, rev, row.Kind, []byte(row.NativeID), seq, hint, presence, hash})
	}
	// Keep one root and explicit Artifact anchors per cut for established FKs.
	anchorIDs := map[string]bool{doc.Root: true}
	for _, ref := range refs {
		anchorIDs[ref.RecordID] = true
	}
	anchors := [][]any{}
	for id := range anchorIDs {
		row := doc.Records[id]
		sum := sha256.Sum256(row.Attributes)
		hash := hex.EncodeToString(sum[:])
		payloads[hash] = row.Attributes
		seq, hint, presence := commonVersionFields(row)
		anchors = append(anchors, []any{thread, ns, rev, id, row.Kind, []byte(row.NativeID), seq, hint, presence, []byte{}, hash})
	}
	if err := s.putCommonPayloads(ctx, tx, thread, payloads); err != nil {
		return err
	}
	if len(closeIDs) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE ahp_state.common_record_versions SET to_revision=$3 WHERE thread_id=$1 AND namespace=$2 AND to_revision IS NULL AND id=ANY($4::text[])`, thread, ns, rev, closeIDs); err != nil {
			return err
		}
	}
	if len(records) > 0 {
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"ahp_state", "common_record_versions"}, []string{"thread_id", "namespace", "id", "from_revision", "kind", "native_id", "sequence", "owner_hint", "presence", "attributes_hash"}, pgx.CopyFromRows(records)); err != nil {
			return err
		}
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"ahp_state", "common_records"}, []string{"thread_id", "namespace", "revision", "id", "kind", "native_id", "sequence", "owner_hint", "presence", "attributes", "attributes_hash"}, pgx.CopyFromRows(anchors)); err != nil {
		return err
	}
	oldRoles, oldEdges := commonRelations(before)
	newRoles, newEdges := commonRelations(&doc)
	sources, roleNames := []string{}, []string{}
	roles := [][]any{}
	for key := range oldRoles {
		if !newRoles[key] {
			sources = append(sources, key.source)
			roleNames = append(roleNames, key.role)
		}
	}
	if len(sources) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE ahp_state.common_role_versions v SET to_revision=$3 FROM unnest($4::text[],$5::text[]) x(source,role) WHERE v.thread_id=$1 AND v.namespace=$2 AND v.to_revision IS NULL AND v.source=x.source AND v.role=x.role`, thread, ns, rev, sources, roleNames); err != nil {
			return err
		}
	}
	for key := range newRoles {
		if !oldRoles[key] {
			roles = append(roles, []any{thread, ns, key.source, key.role, rev})
		}
	}
	if len(roles) > 0 {
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"ahp_state", "common_role_versions"}, []string{"thread_id", "namespace", "source", "role", "from_revision"}, pgx.CopyFromRows(roles)); err != nil {
			return err
		}
	}
	sources, roleNames = []string{}, []string{}
	ordinals := []int32{}
	edges := [][]any{}
	for key, target := range oldEdges {
		next, ok := newEdges[key]
		if !ok || next != target {
			sources = append(sources, key.source)
			roleNames = append(roleNames, key.role)
			ordinals = append(ordinals, int32(key.ordinal))
		}
	}
	if len(sources) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE ahp_state.common_edge_versions v SET to_revision=$3 FROM unnest($4::text[],$5::text[],$6::integer[]) x(source,role,ordinal) WHERE v.thread_id=$1 AND v.namespace=$2 AND v.to_revision IS NULL AND v.source=x.source AND v.role=x.role AND v.ordinal=x.ordinal`, thread, ns, rev, sources, roleNames, ordinals); err != nil {
			return err
		}
	}
	for key, target := range newEdges {
		previous, ok := oldEdges[key]
		if !ok || previous != target {
			edges = append(edges, []any{thread, ns, key.source, key.role, key.ordinal, target, rev})
		}
	}
	if len(edges) > 0 {
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"ahp_state", "common_edge_versions"}, []string{"thread_id", "namespace", "source", "role", "ordinal", "target", "from_revision"}, pgx.CopyFromRows(edges)); err != nil {
			return err
		}
	}
	if s.commonIncrementalAppend {
		root, nodes, e := commonAppendFromDocument(doc)
		if e == nil {
			return s.saveCommonAppend(ctx, tx, thread, ns, rev, root, nodes)
		}
		if !errors.Is(e, errCommonAppendBound) {
			return e
		}
	}
	digest, err := commonVersionDigest(doc)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO ahp_state.common_version_roots VALUES($1,$2,$3,$4)`, thread, ns, rev, digest)
	return err
}
func (s *Store) readCommonVersions(ctx context.Context, tx pgx.Tx, thread, ns string, rev int64, out *CommonRead) error {
	if !s.commonRecordVersions {
		return contracts.NewError(contracts.ErrUnavailable, "versioned common reader is not configured")
	}
	out.Document.Format = CommonFormat
	out.Document.Records = map[string]CommonRecord{}
	rows, err := tx.Query(ctx, `SELECT r.id,r.kind,r.native_id,r.sequence,r.owner_hint,r.presence,p.attributes,r.attributes_hash FROM ahp_state.common_record_versions r LEFT JOIN ahp_state.common_payloads p ON p.thread_id=r.thread_id AND p.sha256=r.attributes_hash WHERE r.thread_id=$1 AND r.namespace=$2 AND r.from_revision<=$3 AND(r.to_revision IS NULL OR r.to_revision>$3)`, thread, ns, rev)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id string
		var row CommonRecord
		var seq *int64
		var hint *string
		var presence int16
		var hash string
		if err = rows.Scan(&id, &row.Kind, &row.NativeID, &seq, &hint, &presence, &row.Attributes, &hash); err != nil {
			rows.Close()
			return err
		}
		sum := sha256.Sum256(row.Attributes)
		if hex.EncodeToString(sum[:]) != hash {
			rows.Close()
			return contracts.NewError(contracts.ErrUnavailable, "common payload integrity check failed")
		}
		if _, duplicate := out.Document.Records[id]; duplicate {
			rows.Close()
			return invalid("overlapping common record versions")
		}
		row.Relations = map[string][]string{}
		if presence&1 != 0 {
			row.Sequence = json.RawMessage("null")
			if seq != nil {
				row.Sequence = json.RawMessage(strconv.FormatInt(*seq, 10))
			}
		}
		if presence&2 != 0 {
			row.OwnerHint = json.RawMessage("null")
			if hint != nil {
				row.OwnerHint, _ = json.Marshal(*hint)
			}
		}
		out.Document.Records[id] = row
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = tx.Query(ctx, `SELECT source,role FROM ahp_state.common_role_versions WHERE thread_id=$1 AND namespace=$2 AND from_revision<=$3 AND(to_revision IS NULL OR to_revision>$3)`, thread, ns, rev)
	if err != nil {
		return err
	}
	for rows.Next() {
		var source, role string
		if err = rows.Scan(&source, &role); err != nil {
			rows.Close()
			return err
		}
		row, ok := out.Document.Records[source]
		if !ok {
			rows.Close()
			return invalid("versioned role has no source")
		}
		if _, duplicate := row.Relations[role]; duplicate {
			rows.Close()
			return invalid("overlapping common role versions")
		}
		row.Relations[role] = []string{}
		out.Document.Records[source] = row
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = tx.Query(ctx, `SELECT source,role,ordinal,target FROM ahp_state.common_edge_versions WHERE thread_id=$1 AND namespace=$2 AND from_revision<=$3 AND(to_revision IS NULL OR to_revision>$3) ORDER BY source,role,ordinal`, thread, ns, rev)
	if err != nil {
		return err
	}
	for rows.Next() {
		var source, role, target string
		var ordinal int
		if err = rows.Scan(&source, &role, &ordinal, &target); err != nil {
			rows.Close()
			return err
		}
		row, ok := out.Document.Records[source]
		refs, roleOK := row.Relations[role]
		if !ok || !roleOK || ordinal != len(refs) {
			rows.Close()
			return invalid("versioned edge order/source mismatch")
		}
		row.Relations[role] = append(refs, target)
		out.Document.Records[source] = row
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(out.Document.Records) != out.Receipt.RecordCount {
		return invalid("incomplete versioned common cut")
	}
	if err := out.Document.Validate(); err != nil {
		return err
	}
	var wanted string
	if err := tx.QueryRow(ctx, `SELECT digest FROM ahp_state.common_version_roots WHERE thread_id=$1 AND namespace=$2 AND revision=$3`, thread, ns, rev).Scan(&wanted); err != nil {
		return contracts.NewError(contracts.ErrUnavailable, "versioned common root integrity record missing")
	}
	if strings.HasPrefix(wanted, commonAppendDigestPrefix) {
		return s.verifyCommonAppendRead(ctx, tx, thread, ns, rev, out.Document)
	}
	got, err := commonVersionDigest(out.Document)
	if err != nil {
		return err
	}
	if wanted != got {
		return contracts.NewError(contracts.ErrUnavailable, "versioned common cut integrity check failed")
	}
	return nil
}

// Opaque native bytes stay bytes in the digest carrier. Sequence/owner_hint
// are the typed optional envelope fields the SQL model already normalizes.
func commonVersionDigest(doc CommonDocument) (string, error) {
	type header struct {
		Kind                 string
		NativeID, Attributes []byte
		Sequence             *int64
		OwnerHint            *string
		Presence             int16
		Relations            map[string][]string
	}
	records := map[string]header{}
	for id, row := range doc.Records {
		seq, hint, presence := commonVersionFields(row)
		records[id] = header{row.Kind, row.NativeID, row.Attributes, seq, hint, presence, row.Relations}
	}
	raw, err := json.Marshal(struct {
		Domain, Format, Profile, Root string
		Records                       map[string]header
	}{"ahp-common-version-cut/1", doc.Format, doc.Profile, doc.Root, records})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
