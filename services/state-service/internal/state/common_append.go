package state

// A fixed SHA256 bucket map makes each retained root independent. Appends read
// only touched verified buckets, not prior native event payloads. Cold reads
// still reconstruct and verify the complete native document. Destructive edits
// use the existing full validation path. This mode is explicitly opt-in.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

var errCommonAppendBound = errors.New("common bucket exceeds bounded layout")

const commonAppendDigestPrefix = "mb1:"
const commonAppendFormat = "ahp-common-buckets/1"
const commonAppendDDL = `
CREATE TABLE IF NOT EXISTS ahp_state.common_append_nodes(
 thread_id text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
 sha256 text NOT NULL,data bytea NOT NULL,PRIMARY KEY(thread_id,sha256));
CREATE TABLE IF NOT EXISTS ahp_state.common_append_roots(
 thread_id text NOT NULL,namespace text NOT NULL,revision bigint NOT NULL,data bytea NOT NULL,
 PRIMARY KEY(thread_id,namespace,revision),
 FOREIGN KEY(thread_id,namespace,revision) REFERENCES ahp_state.common_cuts(thread_id,namespace,revision) ON DELETE CASCADE);`

func WithCommonIncrementalAppend() StoreOption {
	return func(s *Store) { WithCommonRecordVersions()(s); s.commonIncrementalAppend = true }
}

type commonAppendRoot struct {
	Format  string      `json:"format"`
	Profile string      `json:"profile"`
	Root    string      `json:"root"`
	Records int         `json:"records"`
	Edges   int         `json:"edges"`
	Budget  int         `json:"budget"`
	Hashes  [256]string `json:"hashes"`
}
type commonAppendRecord struct {
	Kind           string
	NativeID       []byte
	Sequence       *int64
	OwnerHint      *string
	Presence       int16
	AttributesHash string
	Weight         int
}
type commonAppendRole struct{ Length int }
type commonAppendPlan struct {
	root      commonAppendRoot
	buckets   map[byte]map[string]json.RawMessage
	changed   map[byte]bool
	metadata  map[string]commonAppendRecord
	puts      map[string]CommonRecord
	relations []CommonRelationDelta
}

func commonAppendHash(raw []byte) string         { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func commonAppendKey(parts ...string) string     { raw, _ := json.Marshal(parts); return string(raw) }
func commonAppendBucket(key string) byte         { h := sha256.Sum256([]byte(key)); return h[0] }
func commonAppendJSON(value any) json.RawMessage { raw, _ := json.Marshal(value); return raw }
func commonAppendMetadata(r CommonRecord) commonAppendRecord {
	seq, hint, presence := commonVersionFields(r)
	r.Relations = map[string][]string{}
	wire, _ := json.Marshal(r)
	return commonAppendRecord{r.Kind, r.NativeID, seq, hint, presence, commonAppendHash(r.Attributes), len(wire)}
}
func commonAppendRecordWeight(id string, r commonAppendRecord) int {
	return r.Weight + len(commonAppendJSON(id)) + 4
}
func commonAppendRoleWeight(role string) int   { return len(commonAppendJSON(role)) + 8 }
func commonAppendEdgeWeight(target string) int { return len(commonAppendJSON(target)) + 2 }
func commonAppendFromDocument(doc CommonDocument) (commonAppendRoot, map[string][]byte, error) {
	root := commonAppendRoot{Format: commonAppendFormat, Profile: doc.Profile, Root: doc.Root, Records: len(doc.Records), Budget: 1024 + len(doc.Profile) + len(doc.Root)}
	buckets := map[byte]map[string]json.RawMessage{}
	put := func(key string, value any) {
		b := commonAppendBucket(key)
		if buckets[b] == nil {
			buckets[b] = map[string]json.RawMessage{}
		}
		buckets[b][key] = commonAppendJSON(value)
	}
	for id, r := range doc.Records {
		meta := commonAppendMetadata(r)
		put(commonAppendKey("record", id), meta)
		root.Budget += commonAppendRecordWeight(id, meta)
		for role, refs := range r.Relations {
			put(commonAppendKey("role", id, role), commonAppendRole{len(refs)})
			root.Budget += commonAppendRoleWeight(role)
			for ordinal, target := range refs {
				put(commonAppendKey("edge", id, role, strconv.Itoa(ordinal)), target)
				root.Edges++
				root.Budget += commonAppendEdgeWeight(target)
			}
		}
	}
	nodes := map[string][]byte{}
	for bucket, entries := range buckets {
		raw, e := json.Marshal(entries)
		if e != nil {
			return root, nil, e
		}
		if len(entries) > 8192 || len(raw) > 4<<20 {
			return root, nil, errCommonAppendBound
		}
		h := commonAppendHash(raw)
		root.Hashes[bucket] = h
		nodes[h] = raw
	}
	return root, nodes, nil
}
func commonAppendRootDigest(root commonAppendRoot) (string, []byte) {
	raw, _ := json.Marshal(root)
	return commonAppendDigestPrefix + commonAppendHash(raw), raw
}
func (s *Store) saveCommonAppend(ctx context.Context, tx pgx.Tx, thread, ns string, rev int64, root commonAppendRoot, nodes map[string][]byte) error {
	hashes, values := []string{}, [][]byte{}
	for h, v := range nodes {
		hashes = append(hashes, h)
		values = append(values, v)
	}
	if len(hashes) > 0 {
		tag, e := tx.Exec(ctx, `INSERT INTO ahp_state.common_append_nodes(thread_id,sha256,data) SELECT $1,h,v FROM unnest($2::text[],$3::bytea[]) x(h,v) ON CONFLICT(thread_id,sha256) DO UPDATE SET data=EXCLUDED.data WHERE common_append_nodes.data=EXCLUDED.data`, thread, hashes, values)
		if e != nil {
			return e
		}
		if tag.RowsAffected() != int64(len(hashes)) {
			return contracts.NewError(contracts.ErrUnavailable, "immutable common bucket collision/corruption")
		}
	}
	digest, raw := commonAppendRootDigest(root)
	if _, e := tx.Exec(ctx, `INSERT INTO ahp_state.common_append_roots VALUES($1,$2,$3,$4)`, thread, ns, rev, raw); e != nil {
		return e
	}
	_, e := tx.Exec(ctx, `INSERT INTO ahp_state.common_version_roots VALUES($1,$2,$3,$4)`, thread, ns, rev, digest)
	return e
}
func (s *Store) loadCommonAppend(ctx context.Context, tx pgx.Tx, thread, ns string, rev int64) (commonAppendRoot, bool, error) {
	var root commonAppendRoot
	var wanted string
	e := tx.QueryRow(ctx, `SELECT digest FROM ahp_state.common_version_roots WHERE thread_id=$1 AND namespace=$2 AND revision=$3`, thread, ns, rev).Scan(&wanted)
	if e != nil {
		return root, false, e
	}
	if !strings.HasPrefix(wanted, commonAppendDigestPrefix) {
		return root, false, nil
	}
	if !s.commonIncrementalAppend {
		return root, false, contracts.NewError(contracts.ErrUnavailable, "incremental common reader is not configured")
	}
	var raw []byte
	if e = tx.QueryRow(ctx, `SELECT data FROM ahp_state.common_append_roots WHERE thread_id=$1 AND namespace=$2 AND revision=$3`, thread, ns, rev).Scan(&raw); e != nil {
		return root, false, contracts.NewError(contracts.ErrUnavailable, "common bucket root missing")
	}
	if wanted != commonAppendDigestPrefix+commonAppendHash(raw) || json.Unmarshal(raw, &root) != nil || root.Format != commonAppendFormat || root.Records < 1 || root.Records > 50000 || root.Edges < 0 || root.Edges > 200000 || root.Budget < 1 {
		return root, false, contracts.NewError(contracts.ErrUnavailable, "common bucket root integrity failed")
	}
	for _, h := range root.Hashes {
		if h != "" && !deploymentSHA(h) {
			return root, false, contracts.NewError(contracts.ErrUnavailable, "invalid common bucket root hash")
		}
	}
	return root, true, nil
}
func (p *commonAppendPlan) get(ctx context.Context, tx pgx.Tx, thread, key string, out any) (bool, error) {
	b := commonAppendBucket(key)
	entries, ok := p.buckets[b]
	if !ok {
		entries = map[string]json.RawMessage{}
		if h := p.root.Hashes[b]; h != "" {
			var raw []byte
			if e := tx.QueryRow(ctx, `SELECT data FROM ahp_state.common_append_nodes WHERE thread_id=$1 AND sha256=$2`, thread, h).Scan(&raw); e != nil {
				return false, contracts.NewError(contracts.ErrUnavailable, "common bucket missing")
			}
			if len(raw) > 4<<20 || commonAppendHash(raw) != h || json.Unmarshal(raw, &entries) != nil || len(entries) > 8192 {
				return false, contracts.NewError(contracts.ErrUnavailable, "common bucket integrity failed")
			}
			for k := range entries {
				if commonAppendBucket(k) != b {
					return false, contracts.NewError(contracts.ErrUnavailable, "common bucket partition differs")
				}
			}
		}
		p.buckets[b] = entries
	}
	raw, ok := entries[key]
	if !ok {
		return false, nil
	}
	if e := json.Unmarshal(raw, out); e != nil {
		return false, contracts.NewError(contracts.ErrUnavailable, "invalid common bucket entry")
	}
	return true, nil
}
func (p *commonAppendPlan) set(key string, value any) {
	b := commonAppendBucket(key)
	if p.buckets[b] == nil {
		p.buckets[b] = map[string]json.RawMessage{}
	}
	p.buckets[b][key] = commonAppendJSON(value)
	p.changed[b] = true
}
func (s *Store) tryCommonAppend(ctx context.Context, tx pgx.Tx, thread, ns string, rev int64, delta *CommonDelta, refs []CommonArtifactReference) (*commonAppendPlan, error) {
	if !s.commonIncrementalAppend || rev == 0 || delta == nil || len(delta.Delete) > 0 || len(refs) > 0 || strings.HasSuffix(delta.Profile, CommonArtifactProfileSuffix) {
		return nil, nil
	}
	for _, edit := range delta.Relations {
		if edit.Mode != "append" {
			return nil, nil
		}
	}
	root, found, e := s.loadCommonAppend(ctx, tx, thread, ns, rev)
	if e != nil || !found {
		return nil, e
	}
	if root.Root != delta.Root || root.Profile != delta.Profile {
		return nil, invalid("incremental append requires the same native root/profile")
	}
	p := &commonAppendPlan{root: root, buckets: map[byte]map[string]json.RawMessage{}, changed: map[byte]bool{}, metadata: map[string]commonAppendRecord{}, puts: delta.Put, relations: delta.Relations}
	for id, row := range delta.Put {
		key := commonAppendKey("record", id)
		var old commonAppendRecord
		exists, e := p.get(ctx, tx, thread, key, &old)
		if e != nil {
			return nil, e
		}
		next := commonAppendMetadata(row)
		if exists {
			p.root.Budget -= commonAppendRecordWeight(id, old)
		} else {
			p.root.Records++
		}
		p.root.Budget += commonAppendRecordWeight(id, next)
		p.set(key, next)
		p.metadata[id] = next
	}
	for _, edit := range delta.Relations {
		var source commonAppendRecord
		exists, e := p.get(ctx, tx, thread, commonAppendKey("record", edit.Source), &source)
		if e != nil {
			return nil, e
		}
		if !exists {
			return nil, invalid("append source absent")
		}
		key := commonAppendKey("role", edit.Source, edit.Role)
		var role commonAppendRole
		exists, e = p.get(ctx, tx, thread, key, &role)
		if e != nil {
			return nil, e
		}
		if !exists || role.Length != *edit.ExpectedLength {
			return nil, contracts.NewError(contracts.ErrConflict, "delta append length differs")
		}
		for i, target := range edit.Targets {
			var record commonAppendRecord
			exists, e = p.get(ctx, tx, thread, commonAppendKey("record", target), &record)
			if e != nil {
				return nil, e
			}
			if !exists {
				return nil, invalid("dangling append target")
			}
			edgeKey := commonAppendKey("edge", edit.Source, edit.Role, strconv.Itoa(role.Length+i))
			var old string
			exists, e = p.get(ctx, tx, thread, edgeKey, &old)
			if e != nil {
				return nil, e
			}
			if exists {
				return nil, contracts.NewError(contracts.ErrUnavailable, "append ordinal already occupied")
			}
			p.set(edgeKey, target)
			p.root.Edges++
			p.root.Budget += commonAppendEdgeWeight(target)
		}
		p.set(key, commonAppendRole{role.Length + len(edit.Targets)})
	}
	if p.root.Records > 50000 || p.root.Edges > 200000 {
		return nil, invalid("common graph count outside limit")
	}
	// A conservative budget near the limit falls back to exact full validation.
	if p.root.Budget > MaxCommonDocumentBytes {
		return nil, nil
	}
	for b := range p.changed {
		raw, e := json.Marshal(p.buckets[b])
		if e != nil {
			return nil, e
		}
		if len(p.buckets[b]) > 8192 || len(raw) > 4<<20 {
			return nil, nil
		}
	}
	return p, nil
}
func (s *Store) putCommonAppend(ctx context.Context, tx pgx.Tx, thread, ns string, rev int64, p *commonAppendPlan) error {
	payloads := map[string][]byte{}
	records := [][]any{}
	ids := []string{}
	for id, row := range p.puts {
		meta := p.metadata[id]
		payloads[meta.AttributesHash] = row.Attributes
		ids = append(ids, id)
		records = append(records, []any{thread, ns, id, rev, meta.Kind, meta.NativeID, meta.Sequence, meta.OwnerHint, meta.Presence, meta.AttributesHash})
	}
	if e := s.putCommonPayloads(ctx, tx, thread, payloads); e != nil {
		return e
	}
	if len(ids) > 0 {
		if _, e := tx.Exec(ctx, `UPDATE ahp_state.common_record_versions SET to_revision=$3 WHERE thread_id=$1 AND namespace=$2 AND to_revision IS NULL AND id=ANY($4::text[])`, thread, ns, rev, ids); e != nil {
			return e
		}
	}
	if len(records) > 0 {
		if _, e := tx.CopyFrom(ctx, pgx.Identifier{"ahp_state", "common_record_versions"}, []string{"thread_id", "namespace", "id", "from_revision", "kind", "native_id", "sequence", "owner_hint", "presence", "attributes_hash"}, pgx.CopyFromRows(records)); e != nil {
			return e
		}
	}
	var root commonAppendRecord
	exists, e := p.get(ctx, tx, thread, commonAppendKey("record", p.root.Root), &root)
	if e != nil {
		return e
	}
	if !exists {
		return invalid("append root absent")
	}
	if _, e = tx.Exec(ctx, `INSERT INTO ahp_state.common_records(thread_id,namespace,revision,id,kind,native_id,sequence,owner_hint,presence,attributes,attributes_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,''::bytea,$10)`, thread, ns, rev, p.root.Root, root.Kind, root.NativeID, root.Sequence, root.OwnerHint, root.Presence, root.AttributesHash); e != nil {
		return e
	}
	edges := [][]any{}
	for _, edit := range p.relations {
		for i, target := range edit.Targets {
			edges = append(edges, []any{thread, ns, edit.Source, edit.Role, *edit.ExpectedLength + i, target, rev})
		}
	}
	if len(edges) > 0 {
		if _, e = tx.CopyFrom(ctx, pgx.Identifier{"ahp_state", "common_edge_versions"}, []string{"thread_id", "namespace", "source", "role", "ordinal", "target", "from_revision"}, pgx.CopyFromRows(edges)); e != nil {
			return e
		}
	}
	nodes := map[string][]byte{}
	for b := range p.changed {
		raw, e := json.Marshal(p.buckets[b])
		if e != nil {
			return e
		}
		if len(raw) > 4<<20 {
			return invalid("common append bucket exceeds bound")
		}
		h := commonAppendHash(raw)
		p.root.Hashes[b] = h
		nodes[h] = raw
	}
	return s.saveCommonAppend(ctx, tx, thread, ns, rev, p.root, nodes)
}
func (s *Store) verifyCommonAppendRead(ctx context.Context, tx pgx.Tx, thread, ns string, rev int64, doc CommonDocument) error {
	expected, ok, e := s.loadCommonAppend(ctx, tx, thread, ns, rev)
	if e != nil {
		return e
	}
	if !ok {
		return invalid("common append root not present")
	}
	got, nodes, e := commonAppendFromDocument(doc)
	if e != nil {
		return e
	}
	a, _ := commonAppendRootDigest(got)
	b, _ := commonAppendRootDigest(expected)
	if a != b {
		return contracts.NewError(contracts.ErrUnavailable, "versioned common bucket integrity failed")
	}

	hashes := []string{}
	for h := range nodes {
		hashes = append(hashes, h)
	}
	rows, e := tx.Query(ctx, `SELECT sha256,data FROM ahp_state.common_append_nodes WHERE thread_id=$1 AND sha256=ANY($2::text[])`, thread, hashes)
	if e != nil {
		return e
	}
	seen := 0
	for rows.Next() {
		var h string
		var raw []byte
		if e = rows.Scan(&h, &raw); e != nil {
			rows.Close()
			return e
		}
		if !bytes.Equal(raw, nodes[h]) {
			rows.Close()
			return contracts.NewError(contracts.ErrUnavailable, "stored common bucket differs from native state")
		}
		seen++
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	if seen != len(nodes) {
		return contracts.NewError(contracts.ErrUnavailable, "common bucket missing")
	}

	return nil
}
func (s *Store) pruneCommonAppend(ctx context.Context, tx pgx.Tx, thread, ns string, floor int64) error {
	if !s.commonIncrementalAppend {
		return nil
	}

	if _, e := tx.Exec(ctx, `DELETE FROM ahp_state.common_append_roots WHERE thread_id=$1 AND namespace=$2 AND revision<$3`, thread, ns, floor); e != nil {
		return e
	}
	// Mark every retained root in every namespace. Pins already lowered floor.
	rows, e := tx.Query(ctx, `SELECT r.data,v.digest FROM ahp_state.common_append_roots r JOIN ahp_state.common_version_roots v USING(thread_id,namespace,revision) WHERE r.thread_id=$1`, thread)
	if e != nil {
		return e
	}
	live := map[string]bool{}
	for rows.Next() {
		var raw []byte
		var root commonAppendRoot
		var digest string
		if e = rows.Scan(&raw, &digest); e != nil {
			rows.Close()
			return e
		}
		if e = json.Unmarshal(raw, &root); e != nil || digest != commonAppendDigestPrefix+commonAppendHash(raw) || root.Format != commonAppendFormat {
			rows.Close()
			return contracts.NewError(contracts.ErrUnavailable, "common retained root integrity failed")
		}
		for _, h := range root.Hashes {
			if h != "" {
				live[h] = true
			}
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	hashes := []string{}
	for h := range live {
		hashes = append(hashes, h)
	}
	if len(hashes) > 0 {
		rows, e := tx.Query(ctx, `SELECT sha256,data FROM ahp_state.common_append_nodes WHERE thread_id=$1 AND sha256=ANY($2::text[])`, thread, hashes)
		if e != nil {
			return e
		}
		count := 0
		for rows.Next() {
			var h string
			var raw []byte
			if e = rows.Scan(&h, &raw); e != nil {
				rows.Close()
				return e
			}
			if commonAppendHash(raw) != h {
				rows.Close()
				return contracts.NewError(contracts.ErrUnavailable, "retained common node corrupt")
			}
			count++
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		if count != len(hashes) {
			return contracts.NewError(contracts.ErrUnavailable, "retained common node missing")
		}
	}
	_, e = tx.Exec(ctx, `DELETE FROM ahp_state.common_append_nodes WHERE thread_id=$1 AND NOT(sha256=ANY($2::text[]))`, thread, hashes)
	return e
}
