package state

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"reflect"
	"strings"
	"testing"
)

func rev47Owner(n int) json.RawMessage {
	native, _ := json.Marshal(map[string]string{"large": strings.Repeat("x", n)})
	b, _ := json.Marshal(map[string]string{"format": "adk-shared-owner/research-1", "app": "research-app", "owner": "deployment", "json_base64": base64.StdEncoding.EncodeToString(native)})
	return b
}

func TestRev47OversizedSharedOwnerRefusesEntireNativePublication(t *testing.T) {
	x := rev40Fixture(t)
	req := rev40DeltaRequest(t, "initial", 0, rev40Initial)
	req.KV = []KVPut{{Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment", Value: rev47Owner(750000)}}
	first, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil {
		t.Fatal(e)
	}
	before, e := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 0)
	if e != nil {
		t.Fatal(e)
	}
	var rootRows int
	if e := x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_version_roots WHERE thread_id=$1`, x.ta).Scan(&rootRows); e != nil {
		t.Fatal(e)
	}
	big := rev47Owner(800000)
	if len(big) <= kvMaxValueBytes {
		t.Fatal("test did not cross wire value limit")
	}
	next := rev40DeltaRequest(t, "refused", 1, `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"s":{"kind":"session","attributes":{"event":"must not publish"},"relations":{}}}}`)
	next.KV = []KVPut{{Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment", ExpectedVersion: 1, Value: big}}
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, next); contracts.Code(e) != contracts.ErrInvalid {
		t.Fatal("oversized app publication accepted", e)
	}
	after, e := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 0)
	if e != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("native event published without owner state", e)
	}
	app, e := x.s.GetKV(x.ctx, x.b, []string{"app"}, "state")
	if e != nil || app.Version != first.KV[0].Version || !bytes.Equal(app.Value, first.KV[0].Value) {
		t.Fatal("shared owner partially changed", e)
	}
	var afterRoots int
	if e := x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_version_roots WHERE thread_id=$1`, x.ta).Scan(&afterRoots); e != nil || afterRoots != rootRows {
		t.Fatal("failed commit left an integrity root", e)
	}
}

func TestRev47PerKeySharedLayoutExceedsOldAggregateLimitAtomically(t *testing.T) {
	x := rev40Fixture(t)
	req := rev40DeltaRequest(t, "per-key", 0, rev40Initial)
	// Demonstrates a viable State contract alternative, not an already shipped
	// ADK per-key adapter. Eight writes / total batch <=4MiB remain hard limits.
	for i := 0; i < 8; i++ {
		value, _ := json.Marshal(map[string]string{"value": strings.Repeat("x", 160000)})
		req.KV = append(req.KV, KVPut{Namespace: []string{"app"}, Key: fmt.Sprint("key-", i), ExpectedOwner: "deployment", Value: value})
	}
	receipt, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil || len(receipt.KV) != 8 {
		t.Fatal("per-key atomic candidate", e)
	}
	total := 0
	for i := 0; i < 8; i++ {
		row, e := x.s.GetKV(x.ctx, x.b, []string{"app"}, fmt.Sprint("key-", i))
		if e != nil || row.ThreadID != "" {
			t.Fatal("app key was attached to source session", e)
		}
		total += len(row.Value)
	}
	if total <= kvMaxValueBytes {
		t.Fatal("candidate did not exceed former owner blob limit")
	}
	if e := x.s.DeleteThread(x.ctx, x.a, x.ta); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 8; i++ {
		if _, e := x.s.GetKV(x.ctx, x.b, []string{"app"}, fmt.Sprint("key-", i)); e != nil {
			t.Fatal("session erasure removed app data", e)
		}
	}
}

func TestRev47LegacyPrunerMustRefuseCommonHistoryInsteadOfFalseSuccess(t *testing.T) {
	x := rev40Fixture(t)
	req := rev40DeltaRequest(t, "initial", 0, rev40Initial)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal(e)
	}
	for _, all := range []bool{false, true} {
		_, e := x.s.PruneCheckpointHistory(x.ctx, x.a, CheckpointPruneRequest{ThreadID: x.ta, Namespace: req.Namespace, AllNamespaces: all, Limit: 1})
		if contracts.Code(e) != contracts.ErrInvalid {
			t.Fatalf("COMMON_HISTORY_PRUNE_FALSE_SUCCESS: all=%v err=%v", all, e)
		}
	}
	if _, e := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 1); e != nil {
		t.Fatal("retention attempt lost a common cut", e)
	}
}
