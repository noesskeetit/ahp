package state

import (
	"bytes"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"reflect"
	"strings"
	"testing"
)

func TestCommonIncrementalAppendSkipsUntouchedPayloadAndStillDetectsReadCorruption(t *testing.T) {
	x := rev40Fixture(t)
	WithCommonIncrementalAppend()(x.s)
	if e := x.s.Migrate(x.ctx); e != nil {
		t.Fatal(e)
	}
	first := rev40DeltaRequest(t, "first", 0, rev40Initial)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, first); e != nil {
		t.Fatal(e)
	}
	original := []byte(`{"text":"alpha"}`)
	if _, e := x.s.pool.Exec(x.ctx, `UPDATE ahp_state.common_payloads SET attributes='corrupt'::bytea WHERE thread_id=$1 AND attributes=$2`, x.ta, original); e != nil {
		t.Fatal(e)
	}
	next := rev40DeltaRequest(t, "second", 1, `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"b":{"kind":"event","attributes":{"text":"beta"},"relations":{}}},"relations":[{"source":"s","role":"events","mode":"append","expected_length":1,"targets":["b"]}]}`)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, next); e != nil {
		t.Fatal("append read untouched native history", e)
	}
	if _, e := x.s.ReadRecords(x.ctx, x.a, x.ta, first.Namespace, 2); contracts.Code(e) != contracts.ErrUnavailable {
		t.Fatal("cold read failed to detect corrupted native bytes", e)
	}
	if _, e := x.s.pool.Exec(x.ctx, `UPDATE ahp_state.common_payloads SET attributes=$2 WHERE thread_id=$1 AND attributes='corrupt'::bytea`, x.ta, original); e != nil {
		t.Fatal(e)
	}
	for rev, want := range map[int64][]string{1: {"a"}, 2: {"a", "b"}} {
		got, e := x.s.ReadRecords(x.ctx, x.a, x.ta, first.Namespace, rev)
		if e != nil || !reflect.DeepEqual(got.Document.Records["s"].Relations["events"], want) {
			t.Fatal("incremental historical snapshot changed", rev, e)
		}
	}
	bad := next
	bad.Key = "bad-length"
	bad.ExpectedRevision = 2
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, bad); contracts.Code(e) != contracts.ErrConflict {
		t.Fatal("append accepted stale role cursor", e)
	}
	replay, e := x.s.CommitRecords(x.ctx, x.a, x.fa, next)
	if e != nil || replay.Revision != 2 {
		t.Fatal("append lost original receipt", e)
	}
}
func TestCommonIncrementalAppendMixedFullDeleteAndRetention(t *testing.T) {
	x := rev40Fixture(t)
	WithCommonIncrementalAppend()(x.s)
	o, e := WithCommonRetention([]string{"test/1"})
	if e != nil {
		t.Fatal(e)
	}
	o(x.s)
	if e = x.s.Migrate(x.ctx); e != nil {
		t.Fatal(e)
	}
	for _, r := range []CommonCommit{rev40DeltaRequest(t, "one", 0, rev40Initial), rev40DeltaRequest(t, "two", 1, `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","delete":["a"],"relations":[{"source":"s","role":"events","mode":"replace","targets":[]}]}`)} {
		if _, e = x.s.CommitRecords(x.ctx, x.a, x.fa, r); e != nil {
			t.Fatal(e)
		}
	}
	read, e := x.s.ReadRecords(x.ctx, x.a, x.ta, "common/research-2", 2)
	if e != nil {
		t.Fatal(e)
	}
	root := read.Document.Records["s"]
	root.Attributes = json.RawMessage(`{"full":true}`)
	read.Document.Records["s"] = root
	if _, e = x.s.CommitRecords(x.ctx, x.a, x.fa, CommonCommit{Namespace: "common/research-2", Key: "full", ExpectedRevision: 2, Document: read.Document}); e != nil {
		t.Fatal(e)
	}
	other := rev40DeltaRequest(t, "other", 0, rev40Initial)
	other.Namespace = "other"
	if _, e = x.s.CommitRecords(x.ctx, x.a, x.fa, other); e != nil {
		t.Fatal(e)
	}
	retentionRelease(t, x, x.fa)
	if _, e = x.s.PruneCommonHistory(x.ctx, x.a, CommonPruneRequest{ThreadID: x.ta, Namespace: "common/research-2", Key: "prune", ExpectedRevision: 3, KeepFromRevision: 3}); e != nil {
		t.Fatal(e)
	}
	if _, e = x.s.ReadRecords(x.ctx, x.a, x.ta, "common/research-2", 3); e != nil {
		t.Fatal("retained root lost", e)
	}
	if _, e = x.s.ReadRecords(x.ctx, x.a, x.ta, "other", 1); e != nil {
		t.Fatal("other namespace immutable nodes reclaimed", e)
	}
	var dead int
	if e = x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_append_nodes n WHERE thread_id=$1 AND NOT EXISTS(SELECT 1 FROM ahp_state.common_append_roots r WHERE r.thread_id=n.thread_id AND position(n.sha256 IN convert_from(r.data,'UTF8'))>0)`, x.ta).Scan(&dead); e != nil || dead != 0 {
		t.Fatal("unreachable append nodes retained", dead, e)
	}
}

func TestCommonIncrementalLargeNativeIDFallsBackWithoutRejectingValidState(t *testing.T) {
	x := rev40Fixture(t)
	WithCommonIncrementalAppend()(x.s)
	if e := x.s.Migrate(x.ctx); e != nil {
		t.Fatal(e)
	}
	native, _ := json.Marshal(strings.Repeat("n", 3300000))
	doc := CommonDocument{Format: CommonFormat, Profile: "test/1", Root: "s", Records: map[string]CommonRecord{"s": {Kind: "session", NativeID: native, Attributes: json.RawMessage(`{}`), Relations: map[string][]string{}}}}
	if e := doc.Validate(); e != nil {
		t.Fatal("valid baseline document refused", e)
	}
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, CommonCommit{Namespace: "large", Key: "large", Document: doc}); e != nil {
		t.Fatal("optional bucket layout rejected valid opaque native ID", e)
	}
	read, e := x.s.ReadRecords(x.ctx, x.a, x.ta, "large", 1)
	if e != nil || !bytes.Equal(read.Document.Records["s"].NativeID, native) {
		t.Fatal("fallback changed native bytes", e)
	}
}
func TestCommonIncrementalCorruptRetainedNodeRollsBackPruneAndDisabledReaderRefuses(t *testing.T) {
	x := rev40Fixture(t)
	WithCommonIncrementalAppend()(x.s)
	o, e := WithCommonRetention([]string{"test/1"})
	if e != nil {
		t.Fatal(e)
	}
	o(x.s)
	if e = x.s.Migrate(x.ctx); e != nil {
		t.Fatal(e)
	}
	for i, key := range []string{"one", "two"} {
		r := rev40DeltaRequest(t, key, int64(i), rev40Initial)
		if i == 1 {
			r = rev40DeltaRequest(t, key, 1, `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"s":{"kind":"session","attributes":{"v":2},"relations":{}}}}`)
		}
		if _, e = x.s.CommitRecords(x.ctx, x.a, x.fa, r); e != nil {
			t.Fatal(e)
		}
	}
	retentionRelease(t, x, x.fa)
	request := CommonPruneRequest{ThreadID: x.ta, Namespace: "common/research-2", Key: "prune", ExpectedRevision: 2, KeepFromRevision: 2}
	disabled := New(x.s.pool, WithCommonRecordVersions(), o)
	if _, e = disabled.PruneCommonHistory(x.ctx, x.a, request); contracts.Code(e) != contracts.ErrUnavailable {
		t.Fatal("disabled reader pruned incremental roots", e)
	}
	var h string
	var raw []byte
	if e = x.s.pool.QueryRow(x.ctx, `SELECT sha256,data FROM ahp_state.common_append_nodes WHERE thread_id=$1 LIMIT 1`, x.ta).Scan(&h, &raw); e != nil {
		t.Fatal(e)
	}
	if _, e = x.s.pool.Exec(x.ctx, `UPDATE ahp_state.common_append_nodes SET data='broken'::bytea WHERE thread_id=$1 AND sha256=$2`, x.ta, h); e != nil {
		t.Fatal(e)
	}
	if _, e = x.s.PruneCommonHistory(x.ctx, x.a, request); contracts.Code(e) != contracts.ErrUnavailable {
		t.Fatal("GC accepted corrupt retained node", e)
	}
	var archived int
	if e = x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_cuts WHERE thread_id=$1 AND format=$2`, x.ta, commonPrunedLayout).Scan(&archived); e != nil || archived != 0 {
		t.Fatal("failed GC published partial prune", e, archived)
	}
	if _, e = x.s.pool.Exec(x.ctx, `UPDATE ahp_state.common_append_nodes SET data=$3 WHERE thread_id=$1 AND sha256=$2`, x.ta, h, raw); e != nil {
		t.Fatal(e)
	}
	if _, e = x.s.PruneCommonHistory(x.ctx, x.a, request); e != nil {
		t.Fatal("same-key retry after rolled-back prune", e)
	}
}
