package state

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Database decode locations must not change the typed receipt returned after
// a lost ACK. Compare the live return with the durable replay, including time.
func TestCommonReceiptReplaySurvivesDatabaseDecodeLocation(t *testing.T) {
	for _, offset := range []int{0, 3 * 60 * 60} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			x := rev40Fixture(t)
			cfg := x.s.pool.Config()
			cfg.AfterConnect = func(_ context.Context, c *pgx.Conn) error {
				c.TypeMap().RegisterType(&pgtype.Type{Name: "timestamptz", OID: pgtype.TimestamptzOID,
					Codec: &pgtype.TimestamptzCodec{ScanLocation: time.FixedZone("database-decode", offset)}})
				return nil
			}
			pool, err := pgxpool.NewWithConfig(x.ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			x.s.pool = pool
			req := rev27Request(t, "location-replay", 0, nil)
			first, err := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
			if err != nil || !reflect.DeepEqual(first, replay) {
				t.Fatalf("receipt changed at the persistence boundary: initial=%#v replay=%#v err=%v", first, replay, err)
			}
		})
	}
}

// Go SDK-style marshaling must produce a usable delta request, not an invalid
// empty CommonDocument that the strict HTTP decoder rejects before execution.
func TestRev40DeltaMarshalsWithoutEmptyFullDocument(t *testing.T) {
	var req CommonCommit
	raw := []byte(`{"namespace":"n","key":"k","delta":{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"s":{"kind":"session","attributes":{},"relations":{}}}}}`)
	if e := json.Unmarshal(raw, &req); e != nil {
		t.Fatal(e)
	}
	b, e := json.Marshal(req)
	if e != nil {
		t.Fatal(e)
	}
	var body map[string]json.RawMessage
	if e = json.Unmarshal(b, &body); e != nil {
		t.Fatal(e)
	}
	if _, present := body["document"]; present {
		t.Fatal("DELTA_WIRE_CONTAINS_EMPTY_FULL_DOCUMENT")
	}
	var decoded CommonCommit
	if e = json.Unmarshal(b, &decoded); e != nil {
		t.Fatal("Go delta request cannot be decoded", e)
	}
}

func rev40DeltaRequest(t *testing.T, key string, revision int64, patch string) CommonCommit {
	t.Helper()
	var req CommonCommit
	raw := fmt.Sprintf(`{"namespace":"common/research-2","key":%q,"expected_revision":%d,"delta":%s}`, key, revision, patch)
	if e := json.Unmarshal([]byte(raw), &req); e != nil {
		t.Fatal(e)
	}
	return req
}

const rev40Initial = `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"s":{"kind":"session","attributes":{"v":1},"relations":{}},"a":{"kind":"event","attributes":{"text":"alpha"},"relations":{}}},"relations":[{"source":"s","role":"events","mode":"replace","targets":["a"]},{"source":"s","role":"empty","mode":"replace","targets":[]}]}`

func TestRev40AppendReplaceDeleteAndEveryHistoricalCut(t *testing.T) {
	x := rev40Fixture(t)
	initial := rev40DeltaRequest(t, "one", 0, rev40Initial)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, initial); e != nil {
		t.Fatal(e)
	}
	second := rev40DeltaRequest(t, "two", 1, `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"b":{"kind":"event","attributes":{"text":"beta"},"relations":{}}},"relations":[{"source":"s","role":"events","mode":"append","expected_length":1,"targets":["b","a"]}]}`)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, second); e != nil {
		t.Fatal(e)
	}
	third := rev40DeltaRequest(t, "three", 2, `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","delete":["b"],"relations":[{"source":"s","role":"events","mode":"replace","targets":["a"]},{"source":"s","role":"empty","mode":"delete"}]}`)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, third); e != nil {
		t.Fatal(e)
	}
	fourth := rev40DeltaRequest(t, "four", 3, `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"b":{"kind":"event","attributes":{"text":"new beta"},"relations":{}}},"relations":[{"source":"s","role":"events","mode":"append","expected_length":1,"targets":["b"]}]}`)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, fourth); e != nil {
		t.Fatal(e)
	}
	wants := [][]string{{"a"}, {"a", "b", "a"}, {"a"}, {"a", "b"}}
	for i, want := range wants {
		got, e := x.s.ReadRecords(x.ctx, x.a, x.ta, initial.Namespace, int64(i+1))
		if e != nil || !reflect.DeepEqual(got.Document.Records["s"].Relations["events"], want) {
			t.Fatal("history/order corrupted", i, e)
		}
		_, empty := got.Document.Records["s"].Relations["empty"]
		if empty != (i < 2) {
			t.Fatal("empty/absent role confused", i)
		}
		b, exists := got.Document.Records["b"]
		if exists != (i == 1 || i == 3) {
			t.Fatal("deleted/reinserted record history wrong", i)
		}
		if i == 3 && string(b.Attributes) != `{"text":"new beta"}` {
			t.Fatal("reinsertion did not create a new version")
		}
	}
}

func TestRev40DeltaReplayCASAndFence(t *testing.T) {
	x := rev40Fixture(t)
	req := rev40DeltaRequest(t, "one", 0, rev40Initial)
	first, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil {
		t.Fatal(e)
	}
	replay, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil || !reflect.DeepEqual(first, replay) {
		t.Fatal("delta replay", e)
	}
	req.Key = "stale"
	req.ExpectedRevision = 1
	bad := x.fa
	bad.Epoch++
	if _, e = x.s.CommitRecords(x.ctx, x.a, bad, req); contracts.Code(e) != contracts.ErrStaleEpoch {
		t.Fatal("stale writer", e)
	}
	req.ExpectedRevision = 0
	if _, e = x.s.CommitRecords(x.ctx, x.a, x.fa, req); contracts.Code(e) != contracts.ErrConflict {
		t.Fatal("CAS", e)
	}
	if _, e = x.s.CommitRecords(x.ctx, x.b, x.fa, req); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("foreign scope", e)
	}
	var cuts int
	if e = x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_cuts WHERE thread_id=$1`, x.ta).Scan(&cuts); e != nil || cuts != 1 {
		t.Fatal("rejected writer published metadata", e, cuts)
	}
}

func TestRev40DeltaCASRacePublishesOneWinner(t *testing.T) {
	x := rev40Fixture(t)
	initial := rev40DeltaRequest(t, "one", 0, rev40Initial)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, initial); e != nil {
		t.Fatal(e)
	}
	requests := []CommonCommit{}
	for i := 2; i <= 3; i++ {
		requests = append(requests, rev40DeltaRequest(t, fmt.Sprint("r", i), 1, fmt.Sprintf(`{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"s":{"kind":"session","attributes":{"v":%d},"relations":{}}}}`, i)))
	}
	var wg sync.WaitGroup
	codes := make(chan string, 2)
	for _, r := range requests {
		wg.Add(1)
		go func(req CommonCommit) {
			defer wg.Done()
			_, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
			codes <- contracts.Code(e)
		}(r)
	}
	wg.Wait()
	close(codes)
	successes, conflicts := 0, 0
	for c := range codes {
		if c == "" {
			successes++
		} else if c == contracts.ErrConflict {
			conflicts++
		} else {
			t.Fatal("unexpected race failure", c)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatal("CAS race", successes, conflicts)
	}
	got, e := x.s.ReadRecords(x.ctx, x.a, x.ta, initial.Namespace, 0)
	if e != nil || got.Receipt.Revision != 2 {
		t.Fatal("head", e)
	}
}

func TestRev40LateAckFailureRollsBackVersionsPayloadsAndKV(t *testing.T) {
	x := rev40Fixture(t)
	req := rev40DeltaRequest(t, "one", 0, rev40Initial)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal(e)
	}
	var before int
	if e := x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_payloads WHERE thread_id=$1`, x.ta).Scan(&before); e != nil {
		t.Fatal(e)
	}
	next := rev40DeltaRequest(t, "rollback", 1, `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"s":{"kind":"session","attributes":{"never":"publish"},"relations":{}}}}`)
	next.AckInbox = []string{"absent"}
	next.KV = rev27Items(x.ta, 2, 0)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, next); e == nil {
		t.Fatal("bad ACK accepted")
	}
	got, e := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 0)
	if e != nil || got.Receipt.Revision != 1 || string(got.Document.Records["s"].Attributes) != `{"v":1}` {
		t.Fatal("rollback changed native snapshot", e)
	}
	for _, row := range rev27Read(t, x, x.a, x.ta) {
		if row != nil {
			t.Fatal("rollback left shared KV")
		}
	}
	var after int
	if e = x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_payloads WHERE thread_id=$1`, x.ta).Scan(&after); e != nil || after != before {
		t.Fatal("rollback left payload", e)
	}
}

func TestRev40UpgradeAndDisabledReaderFailClosed(t *testing.T) {
	x := rev29Fixture(t)
	first := rev27Request(t, "legacy", 0, nil)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, first); e != nil {
		t.Fatal(e)
	}
	WithCommonRecordVersions()(x.s)
	if e := x.s.Migrate(x.ctx); e != nil {
		t.Fatal(e)
	}
	patch := rev40DeltaRequest(t, "upgraded", 1, `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"s":{"kind":"session","attributes":{"after":2},"relations":{}}}}`)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, patch); e != nil {
		t.Fatal(e)
	}
	old, e := x.s.ReadRecords(x.ctx, x.a, x.ta, first.Namespace, 1)
	if e != nil || !reflect.DeepEqual(old.Document, first.Document) {
		t.Fatal("old layout lost", e)
	}
	x.s.commonRecordVersions = false
	if _, e := x.s.ReadRecords(x.ctx, x.a, x.ta, first.Namespace, 0); contracts.Code(e) != contracts.ErrUnavailable {
		t.Fatal("disabled reader returned partial data", e)
	}
	first.Key = "disabled-writer"
	first.ExpectedRevision = 2
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, first); contracts.Code(e) != contracts.ErrUnavailable {
		t.Fatal("disabled writer changed versioned namespace", e)
	}
}

func TestRev40ThreadDeletionCascadesAllVersionMetadata(t *testing.T) {
	x := rev40Fixture(t)
	req := rev40DeltaRequest(t, "one", 0, rev40Initial)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal(e)
	}
	if e := x.s.DeleteThread(x.ctx, x.a, x.ta); e != nil {
		t.Fatal(e)
	}
	for _, table := range []string{"common_record_versions", "common_role_versions", "common_edge_versions", "common_version_roots", "common_records", "common_payloads", "common_cuts"} {
		var n int
		if e := x.s.pool.QueryRow(x.ctx, "SELECT count(*) FROM ahp_state."+table+" WHERE thread_id=$1", x.ta).Scan(&n); e != nil || n != 0 {
			t.Fatal("cascade left rows", table, n, e)
		}
	}
}

// Even a missing final edge leaves an otherwise valid graph. Its exact cut
// hash must expose loss rather than returning a silently shorter history.
func TestRev40MissingTailEdgeRefusesRead(t *testing.T) {
	x := rev40Fixture(t)
	r := rev27Request(t, "cut", 0, nil)
	s := r.Document.Records["s"]
	s.Relations = map[string][]string{"same": {"s", "s"}}
	r.Document.Records["s"] = s
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, r); e != nil {
		t.Fatal(e)
	}
	if _, e := x.s.pool.Exec(x.ctx, `DELETE FROM ahp_state.common_edge_versions WHERE thread_id=$1 AND ordinal=1`, x.ta); e != nil {
		t.Fatal(e)
	}
	if _, e := x.s.ReadRecords(x.ctx, x.a, x.ta, r.Namespace, 0); e == nil {
		t.Fatal("TRUNCATED_VALID_GRAPH_WAS_RETURNED")
	}
}

// The same key cannot certify different opaque payload bytes, including a
// duplicate-key native object whose whitespace the generic JSON writer drops.
func TestRev40DeltaDigestBindsExactOpaqueBytes(t *testing.T) {
	x := rev40Fixture(t)
	var req CommonCommit
	if e := json.Unmarshal([]byte(`{"namespace":"n","key":"opaque","delta":{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"s":{"kind":"session","attributes":{"x":1,"x":2},"relations":{}}}}}`), &req); e != nil {
		t.Fatal(e)
	}
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal(e)
	}
	got, e := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 0)
	if e != nil || !bytes.Equal(got.Document.Records["s"].Attributes, []byte(`{"x":1,"x":2}`)) {
		t.Fatal("opaque bytes changed", e)
	}
	row := req.Delta.Put["s"]
	row.Attributes = json.RawMessage(`{ "x":1,"x":2}`)
	req.Delta.Put["s"] = row
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e == nil {
		t.Fatal("OPAQUE_BYTE_CHANGE_REUSED_RECEIPT")
	}
}

func rev40Fixture(t *testing.T) *rev7Owners {
	t.Helper()
	x := rev29Fixture(t)
	WithCommonRecordVersions()(x.s)
	if e := x.s.Migrate(x.ctx); e != nil {
		t.Fatal(e)
	}
	return x
}

// Reintroducing full record/edge CopyFrom on each cut must fail this test.
func TestRev40UnchangedMetadataStoredOnceWithExactHistory(t *testing.T) {
	x := rev40Fixture(t)
	r := rev27Request(t, "first", 0, nil)
	root := r.Document.Records["s"]
	root.Relations = map[string][]string{"events": {"a", "b"}, "empty": {}}
	r.Document.Records["s"] = root
	r.Document.Records["a"] = CommonRecord{Kind: "event", Attributes: json.RawMessage(`{"text":"alpha"}`), Relations: map[string][]string{}}
	r.Document.Records["b"] = CommonRecord{Kind: "event", Attributes: json.RawMessage(`{"text":"beta"}`), Relations: map[string][]string{}}
	for i := int64(0); i < 3; i++ {
		r.Key = fmt.Sprint("same", i)
		r.ExpectedRevision = i
		if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, r); e != nil {
			t.Fatal(e)
		}
	}
	var anchors int
	if e := x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_records WHERE thread_id=$1`, x.ta).Scan(&anchors); e != nil {
		t.Fatal(e)
	}
	if anchors > 3 {
		t.Fatalf("METADATA_AMPLIFICATION:3 identical cuts rewrote%d record rows; only root anchors should repeat", anchors)
	}
	for revision := int64(1); revision <= 3; revision++ {
		got, e := x.s.ReadRecords(x.ctx, x.a, x.ta, r.Namespace, revision)
		if e != nil || !reflect.DeepEqual(got.Document, r.Document) {
			t.Fatal("historical native structure changed", revision, e)
		}
	}
	var records, edges int
	if e := x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_record_versions WHERE thread_id=$1`, x.ta).Scan(&records); e != nil {
		t.Fatal(e)
	}
	if e := x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_edge_versions WHERE thread_id=$1`, x.ta).Scan(&edges); e != nil {
		t.Fatal(e)
	}
	if records != 3 || edges != 2 {
		t.Fatalf("unchanged metadata duplicated:records=%d edges=%d", records, edges)
	}
}

// A server that ignores delta or accepts it as an empty document must fail.
func TestRev40DeltaWirePublishesAndReconstructs(t *testing.T) {
	x := rev40Fixture(t)
	var req CommonCommit
	raw := `{"namespace":"common/research-2","key":"delta-first","expected_revision":0,"delta":{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"s":{"kind":"session","attributes":{"v":1},"relations":{}},"a":{"kind":"event","attributes":{"text":"a"},"relations":{}}},"relations":[{"source":"s","role":"events","mode":"replace","targets":["a"]}]}}`
	if e := json.Unmarshal([]byte(raw), &req); e != nil {
		t.Fatal(e)
	}
	receipt, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil {
		t.Fatalf("DELTA_NOT_ACCEPTED:%v", e)
	}
	got, e := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 0)
	if e != nil || receipt.Revision != 1 || len(got.Document.Records) != 2 || !reflect.DeepEqual(got.Document.Records["s"].Relations["events"], []string{"a"}) {
		t.Fatal("delta native structure absent", e)
	}
}
