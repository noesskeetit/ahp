package state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"reflect"
	"sync"
	"testing"
)

func rev29Fixture(t *testing.T) *rev7Owners {
	t.Helper()
	x := rev27Fixture(t)
	WithCommonPayloadObjects()(x.s)
	if e := x.s.Migrate(x.ctx); e != nil {
		t.Fatal(e)
	}
	return x
}
func rev29Doc(t *testing.T, key string, rev int64, payload []byte) CommonCommit {
	t.Helper()
	r := rev27Request(t, key, rev, nil)
	v := r.Document.Records["s"]
	v.Attributes = json.RawMessage(payload)
	r.Document.Records["s"] = v
	return r
}
func TestRev29UnchangedPayloadRetainedOnce(t *testing.T) {
	x := rev29Fixture(t)
	payload := []byte(`{"v":"` + string(bytes.Repeat([]byte("abcdefgh"), 1024)) + `"}`)
	for i := int64(0); i < 3; i++ {
		r := rev29Doc(t, fmt.Sprint("cut", i), i, payload)
		if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, r); e != nil {
			t.Fatal(e)
		}
	}
	var rawBytes int64
	if e := x.s.pool.QueryRow(x.ctx, `SELECT coalesce(sum(octet_length(attributes)),0) FROM ahp_state.common_records WHERE thread_id=$1`, x.ta).Scan(&rawBytes); e != nil {
		t.Fatal(e)
	}
	var objectsExist bool
	if e := x.s.pool.QueryRow(x.ctx, `SELECT to_regclass('ahp_state.common_payloads') IS NOT NULL`).Scan(&objectsExist); e != nil {
		t.Fatal(e)
	}
	var objectBytes, count int64
	if objectsExist {
		if e := x.s.pool.QueryRow(x.ctx, `SELECT count(*),coalesce(sum(octet_length(attributes)),0) FROM ahp_state.common_payloads WHERE thread_id=$1`, x.ta).Scan(&count, &objectBytes); e != nil {
			t.Fatal(e)
		}
	}
	if rawBytes+objectBytes != int64(len(payload)) || count != 1 {
		t.Fatalf("AMPLIFICATION: 3 identical payloads retained %d bytes in record rows + %d in %d immutable objects; want exactly one %d-byte object", rawBytes, objectBytes, count, len(payload))
	}
	for rev := int64(1); rev <= 3; rev++ {
		got, e := x.s.ReadRecords(x.ctx, x.a, x.ta, "common/research-2", rev)
		if e != nil || !bytes.Equal(got.Document.Records["s"].Attributes, payload) {
			t.Fatal("history changed", rev, e)
		}
	}
}

func TestRev29MixedWriterModesPreserveOpaqueHistoryAndRelations(t *testing.T) {
	x := rev29Fixture(t)
	ns := "common/research-2"
	// Duplicate JSON keys, NUL/surrogate escape, explicit null and large ints are
	// native payloads, not normalized by the storage layout.
	payload := []byte(`{"v":1,"v":2,"nul":"\u0000","s":"\ud800","n":9007199254740993}`)
	want := []CommonDocument{}
	for i := int64(0); i < 3; i++ {
		x.s.commonPayloadObjects = i == 1
		req := rev29Doc(t, fmt.Sprint("mixed", i), i, payload)
		root := req.Document.Records["s"]
		root.NativeID = json.RawMessage(`"\u0000"`)
		root.Sequence = json.RawMessage(`null`)
		root.OwnerHint = json.RawMessage(`null`)
		root.Relations = map[string][]string{"ordered": {"b", "b"}, "empty": {}}
		req.Document.Records["s"] = root
		req.Document.Records["b"] = CommonRecord{Kind: "branch", NativeID: json.RawMessage(`9007199254740993`), Attributes: json.RawMessage(`{"cursor":3}`), Relations: map[string][]string{}}
		if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
			t.Fatal(e)
		}
		want = append(want, req.Document)
	}
	// Even inline-configured NEW readers must understand object-backed old cuts.
	x.s.commonPayloadObjects = false
	for i, d := range want {
		got, e := x.s.ReadRecords(x.ctx, x.a, x.ta, ns, int64(i+1))
		if e != nil || !reflect.DeepEqual(got.Document, d) {
			t.Fatal("mixed layout changed opaque native document", i, e)
		}
	}
	if e := x.s.Migrate(x.ctx); e != nil {
		t.Fatal("idempotent re-migration", e)
	}
}
func rev29ObjectCount(t *testing.T, x *rev7Owners) int64 {
	t.Helper()
	var n int64
	if e := x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_payloads WHERE thread_id=$1`, x.ta).Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func TestRev29ReplayRollbackAndStaleFenceLeaveNoObjects(t *testing.T) {
	x := rev29Fixture(t)
	req := rev29Doc(t, "first", 0, []byte(`{"v":"old"}`))
	a, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil {
		t.Fatal(e)
	}
	b, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil || !reflect.DeepEqual(a, b) || rev29ObjectCount(t, x) != 1 {
		t.Fatal("replay mutated state", e)
	}
	failed := rev29Doc(t, "foreign-ack", 1, []byte(`{"v":"must-rollback"}`))
	failed.AckInbox = []string{"missing-event"}
	if _, e = x.s.CommitRecords(x.ctx, x.a, x.fa, failed); e == nil {
		t.Fatal("missing ACK allowed")
	}
	if rev29ObjectCount(t, x) != 1 {
		t.Fatal("aborted transaction left immutable orphan")
	}
	stale := x.fa
	stale.Epoch++
	if _, e = x.s.CommitRecords(x.ctx, x.a, stale, rev29Doc(t, "stale", 1, []byte(`{"v":"stale"}`))); contracts.Code(e) != contracts.ErrStaleEpoch {
		t.Fatal("fence", e)
	}
	if rev29ObjectCount(t, x) != 1 {
		t.Fatal("stale writer created object")
	}
	got, e := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 0)
	if e != nil || got.Receipt.Revision != 1 {
		t.Fatal("aborted operation changed head", e)
	}
}
func TestRev29CorruptObjectRefusesReadAndReusedCommit(t *testing.T) {
	x := rev29Fixture(t)
	req := rev29Doc(t, "initial", 0, []byte(`{"v":42}`))
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal(e)
	}
	// Direct SQL is a corruption injection, not an API available to a worker.
	if _, e := x.s.pool.Exec(x.ctx, `UPDATE ahp_state.common_payloads SET attributes=$2 WHERE thread_id=$1`, x.ta, []byte(`{"v":99}`)); e != nil {
		t.Fatal(e)
	}
	if _, e := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 0); contracts.Code(e) != contracts.ErrUnavailable {
		t.Fatal("corruption was returned as valid state", e)
	}
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, rev29Doc(t, "must-not-ack", 1, []byte(`{"v":42}`))); contracts.Code(e) != contracts.ErrUnavailable {
		t.Fatal("corrupt existing object acknowledged", e)
	}
	var n int64
	if e := x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_cuts WHERE thread_id=$1`, x.ta).Scan(&n); e != nil || n != 1 {
		t.Fatal("corrupt write published cut", e)
	}
}
func TestRev29ThreadScopedReuseDeletionAndSubjectIsolation(t *testing.T) {
	x := rev29Fixture(t)
	req := rev29Doc(t, "same", 0, []byte(`{"v":"same bytes"}`))
	for _, v := range []struct {
		sc contracts.Scope
		f  Fence
	}{{x.a, x.fa}, {x.a, x.fb}, {x.b, x.fc}} {
		if _, e := x.s.CommitRecords(x.ctx, v.sc, v.f, req); e != nil {
			t.Fatal(e)
		}
	}
	var n int64
	if e := x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_payloads WHERE thread_id=ANY($1::text[])`, []string{x.ta, x.tb, x.tc}).Scan(&n); e != nil || n != 3 {
		t.Fatal("cross-thread payload reuse", e)
	}
	if _, e := x.s.ReadRecords(x.ctx, x.b, x.ta, req.Namespace, 0); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("subject access leak", e)
	}
	if e := x.s.DeleteThread(x.ctx, x.a, x.ta); e != nil {
		t.Fatal(e)
	}
	if rev29ObjectCount(t, x) != 0 {
		t.Fatal("deleted thread retained payloads")
	}
	if got, e := x.s.ReadRecords(x.ctx, x.b, x.tc, req.Namespace, 0); e != nil || !bytes.Equal(got.Document.Records["s"].Attributes, req.Document.Records["s"].Attributes) {
		t.Fatal("deletion affected another owner", e)
	}
	if _, e := x.s.DeleteSubject(x.ctx, x.a); e != nil {
		t.Fatal(e)
	}
	if e := x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_payloads WHERE thread_id=ANY($1::text[])`, []string{x.ta, x.tb}).Scan(&n); e != nil || n != 0 {
		t.Fatal("subject erase left payloads", e)
	}
	if _, e := x.s.ReadRecords(x.ctx, x.b, x.tc, req.Namespace, 0); e != nil {
		t.Fatal("erasure crossed subject", e)
	}
}
func TestRev29ConcurrentPublicationOneWinnerNoOrphan(t *testing.T) {
	x := rev29Fixture(t)
	var wg sync.WaitGroup
	out := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := x.s.CommitRecords(x.ctx, x.a, x.fa, rev29Doc(t, fmt.Sprint("race", i), 0, []byte(fmt.Sprintf(`{"winner":%d}`, i))))
			out <- e
		}(i)
	}
	wg.Wait()
	close(out)
	ok, conflict := 0, 0
	for e := range out {
		if e == nil {
			ok++
		} else if contracts.Code(e) == contracts.ErrConflict {
			conflict++
		} else {
			t.Fatal("unexpected race error", e)
		}
	}
	if ok != 1 || conflict != 1 || rev29ObjectCount(t, x) != 1 {
		t.Fatal("race lost atomicity", ok, conflict)
	}
}
