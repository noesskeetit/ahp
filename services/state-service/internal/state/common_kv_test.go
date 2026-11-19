package state

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func rev27Fixture(t *testing.T) *rev7Owners {
	t.Helper()
	x := rev7OwnerFixture(t)
	x.s.commonRecords = true
	if err := x.s.Migrate(x.ctx); err != nil {
		t.Fatal(err)
	}
	return x
}

// Wire decoding makes the missing feature observable without a compile-only
// failure. The old server drops unknown kv; the old HTTP decoder refuses it.
func rev27Request(t *testing.T, key string, rev int64, items []KVPut) CommonCommit {
	t.Helper()
	var d CommonDocument
	if e := json.Unmarshal([]byte(`{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","records":{"s":{"kind":"session","attributes":{"event":1},"relations":{}}}}`), &d); e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(map[string]any{"namespace": "common/research-2", "key": key, "expected_revision": rev, "document": d, "kv": items})
	if e != nil {
		t.Fatal(e)
	}
	var req CommonCommit
	if e = json.Unmarshal(raw, &req); e != nil {
		t.Fatal(e)
	}
	return req
}
func rev27Items(head string, n int, version int64) []KVPut {
	return []KVPut{
		{Namespace: []string{"head"}, Key: head, ExpectedOwner: "thread", Value: json.RawMessage(fmt.Sprintf(`{"revision":%d}`, n))},
		{Namespace: []string{"user"}, Key: "state", ExpectedOwner: "subject", ExpectedVersion: version, Value: json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))},
		{Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment", ExpectedVersion: version, Value: json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))},
	}
}
func rev27Read(t *testing.T, x *rev7Owners, scope contracts.Scope, head string) []*KVItem {
	t.Helper()
	r, e := x.s.GetKVBatch(x.ctx, scope, []KVRead{{Namespace: []string{"head"}, Key: head, ExpectedOwner: "thread"}, {Namespace: []string{"user"}, Key: "state", ExpectedOwner: "subject"}, {Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment"}})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestRev27CommonKVPublishesOneTransactionAndReplay(t *testing.T) {
	x := rev27Fixture(t)
	req := rev27Request(t, "publish", 0, rev27Items(x.ta, 1, 0))
	receipt, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil {
		t.Fatal(e)
	}
	rows := rev27Read(t, x, x.a, x.ta)
	if rows[0] == nil || rows[1] == nil || rows[2] == nil {
		t.Fatal("PARTIAL_PUBLICATION: common cut committed but KV head/user/app absent")
	}
	if rows[0].ThreadID != x.ta || rows[1].ThreadID != "" || rows[2].ThreadID != "" {
		t.Fatal("owner lifetime wrong")
	}
	before, _ := json.Marshal(rows)
	again, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil || !reflect.DeepEqual(again, receipt) {
		t.Fatal("common receipt replay", e)
	}
	after, _ := json.Marshal(rev27Read(t, x, x.a, x.ta))
	if string(before) != string(after) {
		t.Fatal("replay mutated KV")
	}
	bad := rev27Request(t, "publish", 0, rev27Items(x.ta, 2, 0))
	if _, e = x.s.CommitRecords(x.ctx, x.a, x.fa, bad); contracts.Code(e) != contracts.ErrKeyReuse {
		t.Fatal("KV must be part of request digest", e)
	}
}
func TestRev27CommonKVSharedCASRollsBackCutAndOtherOwners(t *testing.T) {
	x := rev27Fixture(t)
	rev7Put(t, x, x.a, x.fa, "app", "state", "seed", 0, `{"n":0}`)
	req := rev27Request(t, "loser", 0, rev27Items(x.ta, 1, 0))
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); contracts.Code(e) != contracts.ErrConflict {
		t.Fatal("shared CAS should fail", e)
	}
	if _, e := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 0); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("failed KV published common cut", e)
	}
	rows := rev27Read(t, x, x.a, x.ta)
	if rows[0] != nil || rows[1] != nil || string(rows[2].Value) != `{"n":0}` {
		t.Fatal("failed transaction partially mutated owners")
	}
}
func TestRev27CommonKVForeignAckRollsBackAll(t *testing.T) {
	x := rev27Fixture(t)
	req := rev27Request(t, "foreign", 0, rev27Items(x.ta, 1, 0))
	req.AckInbox = []string{"absent-foreign-input"}
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e == nil {
		t.Fatal("foreign ACK allowed")
	}
	for _, item := range rev27Read(t, x, x.a, x.ta) {
		if item != nil {
			t.Fatal("foreign ACK left owner data")
		}
	}
	if _, e := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 0); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("foreign ACK left common cut", e)
	}
}
func TestRev27CommonKVOwnerGuardAndDeletion(t *testing.T) {
	x := rev27Fixture(t)
	items := rev27Items(x.ta, 1, 0)
	wrong := append([]KVPut(nil), items...)
	wrong[1].ExpectedOwner = "thread"
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, rev27Request(t, "wrong", 0, wrong)); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("owner hint bypass", e)
	}
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, rev27Request(t, "ok", 0, items)); e != nil {
		t.Fatal(e)
	}
	bob := rev27Read(t, x, x.b, x.ta)
	if bob[0] != nil || bob[1] != nil || bob[2] == nil {
		t.Fatal("subject/app isolation")
	}
	if e := x.s.DeleteThread(x.ctx, x.a, x.ta); e != nil {
		t.Fatal(e)
	}
	alice := rev27Read(t, x, x.a, x.ta)
	if alice[0] != nil || alice[1] == nil || alice[2] == nil {
		t.Fatal("deleting session removed shared data")
	}
	if _, e := x.s.DeleteSubject(x.ctx, x.a); e != nil {
		t.Fatal(e)
	}
	bob = rev27Read(t, x, x.b, x.ta)
	if bob[2] == nil {
		t.Fatal("subject erasure removed app owner")
	}
}
func TestRev27CommonKVConcurrentWritersHaveOneWinner(t *testing.T) {
	x := rev27Fixture(t)
	rev7Put(t, x, x.a, x.fa, "app", "state", "seed", 0, `{"n":0}`)
	rev7Put(t, x, x.a, x.fa, "user", "state", "seed-user", 0, `{"n":0}`)
	reqs := []CommonCommit{rev27Request(t, "a", 0, rev27Items(x.ta, 1, 1)), rev27Request(t, "b", 0, rev27Items(x.tb, 2, 1))}
	fences := []Fence{x.fa, x.fb}
	threads := []string{x.ta, x.tb}
	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func(i int) { defer wg.Done(); <-start; _, errs[i] = x.s.CommitRecords(x.ctx, x.a, fences[i], reqs[i]) }(i)
	}
	close(start)
	wg.Wait()
	wins := 0
	for i, e := range errs {
		if e == nil {
			wins++
		} else if contracts.Code(e) != contracts.ErrConflict {
			t.Fatal(e)
		}
		_, readErr := x.s.ReadRecords(x.ctx, x.a, threads[i], reqs[i].Namespace, 0)
		if (e == nil) != (readErr == nil) {
			t.Fatal("losing CAS left a common cut")
		}
	}
	if wins != 1 {
		t.Fatalf("CAS winners %d", wins)
	}
}

func TestRev27CommonKVTerminalReplayDoesNotNeedOldLease(t *testing.T) {
	x := rev27Fixture(t)
	req := rev27Request(t, "terminal", 0, rev27Items(x.ta, 1, 0))
	req.RunStatus = &RunStatusOp{Status: RunDone, CloseReason: "model_finished"}
	first, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil {
		t.Fatal(e)
	}
	again, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil || !reflect.DeepEqual(first, again) {
		t.Fatal("terminal receipt replay changed owner values", e)
	}
	if len(rev27Read(t, x, x.a, x.ta)) != 3 {
		t.Fatal("missing final KV")
	}
}

func TestRev27CommonKVReadHeadAndOwnersOneSnapshot(t *testing.T) {
	x := rev27Fixture(t)
	done := make(chan error, 1)
	go func() {
		for n := 1; n <= 15; n++ {
			items := rev27Items(x.ta, n, int64(n-1))
			items[0].ExpectedVersion = int64(n - 1)
			req := rev27Request(t, fmt.Sprintf("sequence-%d", n), int64(n-1), items)
			row := req.Document.Records["s"]
			row.Attributes = json.RawMessage(fmt.Sprintf(`{"event":%d}`, n))
			req.Document.Records["s"] = row
			if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
				done <- e
				return
			}
		}
		done <- nil
	}()
	reads := 0
	for {
		rows := rev27Read(t, x, x.a, x.ta)
		if rows[0] != nil {
			var head struct {
				Revision int64 `json:"revision"`
			}
			var user, app struct {
				N int64 `json:"n"`
			}
			if e := json.Unmarshal(rows[0].Value, &head); e != nil {
				t.Fatal(e)
			}
			if rows[1] == nil || rows[2] == nil {
				t.Fatal("head visible without owners")
			}
			json.Unmarshal(rows[1].Value, &user)
			json.Unmarshal(rows[2].Value, &app)
			if user.N != head.Revision || app.N != head.Revision {
				t.Fatal("torn owner bundle")
			}
			cut, e := x.s.ReadRecords(x.ctx, x.a, x.ta, "common/research-2", head.Revision)
			if e != nil {
				t.Fatal(e)
			}
			var value struct {
				Event int64 `json:"event"`
			}
			json.Unmarshal(cut.Document.Records["s"].Attributes, &value)
			if value.Event != head.Revision {
				t.Fatal("native cut not pinned to read snapshot")
			}
			reads++
		}
		select {
		case e := <-done:
			if e != nil {
				t.Fatal(e)
			}
			if reads == 0 {
				t.Fatal("no native bundle observed")
			}
			return
		default:
		}
	}
}

func TestRev27CommonOpaqueNativeAttributesCommit(t *testing.T) {
	x := rev27Fixture(t)
	var doc CommonDocument
	if e := json.Unmarshal([]byte(commonWireFixture), &doc); e != nil {
		t.Fatal(e)
	}
	req := CommonCommit{Namespace: "opaque", Key: "native-opaque", Document: doc}
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal("OPAQUE_COMMIT_REFUSED", e)
	}
	read, e := x.s.ReadRecords(x.ctx, x.a, x.ta, "opaque", 0)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(string(read.Document.Records["s"].Attributes), `"x":1,"x":2`) {
		t.Fatal("native duplicate keys normalized away")
	}
	if _, e = x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal("opaque exact replay", e)
	}
}

func TestRev27CommonRegularDigestBackwardsCompatible(t *testing.T) {
	x := rev27Fixture(t)
	req := rev27Request(t, "compatible", 0, nil)
	expected, e := contracts.Digest(struct {
		RunID   string       `json:"run_id"`
		Request CommonCommit `json:"request"`
	}{x.fa.RunID, req})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal(e)
	}
	var actual string
	if e = x.s.pool.QueryRow(x.ctx, `SELECT digest FROM ahp_state.common_receipts WHERE thread_id=$1 AND namespace=$2 AND key=$3`, x.ta, req.Namespace, req.Key).Scan(&actual); e != nil {
		t.Fatal(e)
	}
	if actual != expected {
		t.Fatal("existing non-opaque receipt digest changed")
	}
}
