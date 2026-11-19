package state

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
)

type rev42Reader interface {
	ReadRecordsBundleSince(context.Context, contracts.Scope, string, string, []KVRead, int64) (CommonBundle, error)
}

func rev42Conditional(t *testing.T, x *rev7Owners) rev42Reader {
	t.Helper()
	r, ok := any(x.s).(rev42Reader)
	if !ok {
		t.Fatal("CONDITIONAL_BUNDLE_NOT_IMPLEMENTED")
	}
	return r
}
func rev42Body(t *testing.T, b CommonBundle) map[string]any {
	t.Helper()
	raw, e := json.Marshal(b)
	if e != nil {
		t.Fatal(e)
	}
	var out map[string]any
	if e = json.Unmarshal(raw, &out); e != nil {
		t.Fatal(e)
	}
	return out
}

func TestRev42KnownCutOmitsHistoryButReadsCurrentSharedOwners(t *testing.T) {
	x := rev40Fixture(t)
	r := rev42Conditional(t, x)
	req := rev40DeltaRequest(t, "one", 0, rev40Initial)
	req.KV = rev27Items(x.ta, 1, 0)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal(e)
	}
	keys := []KVRead{{Namespace: []string{"user"}, Key: "state", ExpectedOwner: "subject"}, {Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment"}}
	rev7Put(t, x, x.a, x.fb, "user", "state", "shared-newer-than-cut", 1, `{"n":2}`)
	out, e := r.ReadRecordsBundleSince(x.ctx, x.a, x.ta, req.Namespace, keys, 1)
	if e != nil {
		t.Fatal(e)
	}
	body := rev42Body(t, out)
	if body["common"] != nil || body["common_unchanged"] != true {
		t.Fatal("matching cut retransmitted history", body["common_unchanged"])
	}
	receipt, ok := body["receipt"].(map[string]any)
	if !ok || receipt["revision"] != float64(1) {
		t.Fatal("unchanged receipt missing")
	}
	if len(out.KV) != 2 || out.KV[0] == nil || out.KV[0].Version != 2 || string(out.KV[0].Value) != `{"n": 2}` && string(out.KV[0].Value) != `{"n":2}` {
		t.Fatal("known cut returned stale shared owner")
	}
}
func TestRev42KnownRevisionMismatchReturnsFullCut(t *testing.T) {
	x := rev40Fixture(t)
	r := rev42Conditional(t, x)
	req := rev40DeltaRequest(t, "one", 0, rev40Initial)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal(e)
	}
	for _, known := range []int64{0, 2} {
		out, e := r.ReadRecordsBundleSince(x.ctx, x.a, x.ta, req.Namespace, []KVRead{{Namespace: []string{"user"}, Key: "state", ExpectedOwner: "subject"}}, known)
		if e != nil || out.Common == nil || len(out.Common.Document.Records) != 2 || rev42Body(t, out)["common_unchanged"] == true {
			t.Fatal("mismatch hid current native state", known, e)
		}
	}
	if _, e := r.ReadRecordsBundleSince(x.ctx, x.a, x.ta, req.Namespace, nil, -1); contracts.Code(e) != contracts.ErrInvalid {
		t.Fatal("negative known revision", e)
	}
}
func TestRev42KnownCutDoesNotBypassScopeOrErasure(t *testing.T) {
	x := rev40Fixture(t)
	r := rev42Conditional(t, x)
	req := rev40DeltaRequest(t, "one", 0, rev40Initial)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal(e)
	}
	if _, e := r.ReadRecordsBundleSince(x.ctx, x.b, x.ta, req.Namespace, []KVRead{{Namespace: []string{"user"}, Key: "state", ExpectedOwner: "subject"}}, 1); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("cached foreign cut leaked", e)
	}
	if _, e := x.s.DeleteSubject(x.ctx, x.a); e != nil {
		t.Fatal(e)
	}
	if _, e := r.ReadRecordsBundleSince(x.ctx, x.a, x.ta, req.Namespace, []KVRead{{Namespace: []string{"user"}, Key: "state", ExpectedOwner: "subject"}}, 1); e == nil {
		t.Fatal("cache hint bypassed erasure")
	}
}
