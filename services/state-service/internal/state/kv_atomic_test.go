package state

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

type rev7Owners struct {
	s          *Store
	ctx        context.Context
	a, b       contracts.Scope
	fa, fb, fc Fence
	ta, tb, tc string
}

func rev7OwnerFixture(t *testing.T) *rev7Owners {
	t.Helper()
	pool, _ := isolatedPool(t)
	if isolateName == "" {
		t.Fatal("fresh isolated test DB required")
	}
	a := contracts.Scope{Tenant: "owner-" + contracts.NewID(), Deployment: "adk", Subject: "alice"}
	b := a
	b.Subject = "bob"
	option, err := WithKVNamespacePolicies([]KVNamespacePolicy{
		{Tenant: a.Tenant, Deployment: a.Deployment, Namespace: []string{"user"}, Owner: "subject"},
		{Tenant: a.Tenant, Deployment: a.Deployment, Namespace: []string{"app"}, Owner: "deployment"},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := New(pool, option)
	ctx := context.Background()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	x := &rev7Owners{s: s, ctx: ctx, a: a, b: b}
	own := func(scope contracts.Scope, label string) (string, Fence) {
		th, e := s.CreateThread(ctx, scope, label, contracts.NewID())
		if e != nil {
			t.Fatal(e)
		}
		run, e := s.CreateRun(ctx, scope, th.ID, label, contracts.NewID(), json.RawMessage("{}"))
		if e != nil {
			t.Fatal(e)
		}
		lease, e := s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, label, run.ID, time.Minute)
		if e != nil || lease == nil {
			t.Fatal(e)
		}
		return th.ID, Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	}
	x.ta, x.fa = own(a, "alice-a")
	x.tb, x.fb = own(a, "alice-b")
	x.tc, x.fc = own(b, "bob-a")
	return x
}
func rev7Put(t *testing.T, x *rev7Owners, scope contracts.Scope, f Fence, ns, key, op string, v int64, body string) KVItem {
	t.Helper()
	got, e := x.s.PutKV(x.ctx, scope, f, []string{ns}, key, op, v, json.RawMessage(body))
	if e != nil {
		t.Fatal(e)
	}
	return got
}

// A second replica must not silently turn retained user state into session KV
// just because its startup configuration omitted the reviewed namespace.
func TestRev7OwnerPolicyOmissionCannotChangeLifetime(t *testing.T) {
	x := rev7OwnerFixture(t)
	u := rev7Put(t, x, x.a, x.fa, "user", "state", "retained-before-restart", 0, `{"favorite":"blue"}`)
	plain := New(x.s.pool)
	if err := plain.Migrate(x.ctx); err != nil {
		// Startup may reject the inconsistent profile before serving traffic.
		if contracts.Code(err) != contracts.ErrInvalid && contracts.Code(err) != contracts.ErrUnavailable {
			t.Fatal("unrelated startup error is not an ownership denial", err)
		}
		return
	}
	got, err := plain.PutKV(x.ctx, x.a, x.fb, []string{"user"}, "state", "policyless-replica", u.Version, json.RawMessage(`{"favorite":"green"}`))
	if err == nil {
		t.Fatalf("policyless replica reinterpreted retained state: now_thread_owned=%t", got.ThreadID == x.tb)
	}
	if contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatal("unrelated write failure is not a policy denial", err)
	}
	if _, err = plain.GetKV(x.ctx, x.a, []string{"user"}, "state"); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatal("policyless GetKV did not fail closed", err)
	}
	if _, err = plain.SearchKV(x.ctx, x.a, nil, "", nil, 10); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatal("policyless SearchKV did not fail closed", err)
	}
	if err = plain.DeleteKV(x.ctx, x.a, x.fb, []string{"user"}, "state", "policyless-delete", u.Version); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatal("policyless DeleteKV did not fail closed", err)
	}
	still, err := x.s.GetKV(x.ctx, x.a, []string{"user"}, "state")
	if err != nil || still.Version != u.Version || still.ThreadID != "" {
		t.Fatal("refused writer changed retained state", err)
	}
}

func TestRev7OwnerDeclarationSurvivesCompatibleReplicaRestart(t *testing.T) {
	x := rev7OwnerFixture(t)
	u := rev7Put(t, x, x.a, x.fa, "user", "state", "before-restart", 0, `{"favorite":"blue"}`)
	policies := []KVNamespacePolicy{}
	for _, p := range x.s.kvOwnerPolicies {
		policies = append(policies, p)
	}
	option, err := WithKVNamespacePolicies(policies)
	if err != nil {
		t.Fatal(err)
	}
	restarted := New(x.s.pool, option)
	if err = restarted.Migrate(x.ctx); err != nil {
		t.Fatal(err)
	}
	got, err := restarted.GetKV(x.ctx, x.a, []string{"user"}, "state")
	if err != nil || got.ThreadID != "" || got.Version != u.Version {
		t.Fatal("compatible restart changed retained data", err)
	}
}

func TestRev7OwnerRegistrationWaitsForNamespaceWriter(t *testing.T) {
	x := rev7OwnerFixture(t)
	ctx, cancel := context.WithTimeout(x.ctx, 5*time.Second)
	defer cancel()
	plain := New(x.s.pool)
	ns := []string{"late-owner"}
	tx, err := x.s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if err = plain.checkKVPolicies(ctx, tx, x.a, [][]string{ns}); err != nil {
		t.Fatal(err)
	}
	option, err := WithKVNamespacePolicies([]KVNamespacePolicy{{Tenant: x.a.Tenant, Deployment: x.a.Deployment, Namespace: ns, Owner: "subject"}})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- New(x.s.pool, option).Migrate(ctx) }()
	key := kvCatalogLockID(kvPolicyID(x.a, ns))
	waiting := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err = x.s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND mode='ExclusiveLock' AND classid::bigint=((hashtextextended($1,$2)>>32)&4294967295) AND objid::bigint=(hashtextextended($1,$2)&4294967295) AND objsubid=1)`, key, int64(kvPolicyLockSeed)).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case e := <-result:
			t.Fatal("registration did not wait for admitted writer", e)
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !waiting {
		t.Fatal("registration namespace lock wait not observed")
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	if _, err = plain.PutKV(ctx, x.a, x.fa, ns, "late", "old-writer-after-registration", 0, json.RawMessage(`{"v":1}`)); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatal("old writer ignored newly committed policy", err)
	}
}

func TestRev7OwnerPolicyAppliesBeforeFirstValue(t *testing.T) {
	x := rev7OwnerFixture(t)
	plain := New(x.s.pool)
	if err := plain.Migrate(x.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := plain.PutKV(x.ctx, x.a, x.fa, []string{"app"}, "first-value", "policyless-first", 0, json.RawMessage(`{"edition":1}`)); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatal("omitted policy admitted a private shadow before the first shared value", err)
	}
	if _, err := plain.GetKVBatch(x.ctx, x.a, []KVRead{{Namespace: []string{"user"}, Key: "absent", ExpectedOwner: "thread"}}); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatal("read accepted an incompatible namespace lifetime", err)
	}
}

func TestRev7OwnerPoliciesCannotChangeAcrossReplicas(t *testing.T) {
	x := rev7OwnerFixture(t)
	option, err := WithKVNamespacePolicies([]KVNamespacePolicy{{Tenant: x.a.Tenant, Deployment: x.a.Deployment, Namespace: []string{"app"}, Owner: "subject"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = New(x.s.pool, option).Migrate(x.ctx); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatal("replica silently changed a reviewed namespace owner", err)
	}
}
func TestRev7OwnerRetentionAndIsolation(t *testing.T) {
	x := rev7OwnerFixture(t)
	u := rev7Put(t, x, x.a, x.fa, "user", "state", "user1", 0, "{\"favorite\":\"blue\"}")
	if u.ThreadID != "" {
		t.Fatal("user state must not belong to a session")
	}
	u = rev7Put(t, x, x.a, x.fb, "user", "state", "user2", u.Version, "{\"favorite\":\"green\"}")
	if _, e := x.s.GetKV(x.ctx, x.b, []string{"user"}, "state"); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("foreign private user state visible", e)
	}
	app := rev7Put(t, x, x.a, x.fa, "app", "state", "app1", 0, "{\"theme\":\"dark\"}")
	if app.ThreadID != "" {
		t.Fatal("app state must not belong to a session")
	}
	if got, e := x.s.GetKV(x.ctx, x.b, []string{"app"}, "state"); e != nil || got.Version != app.Version {
		t.Fatal("declared app state not shared", e)
	}
	private := rev7Put(t, x, x.a, x.fa, "private", "state", "private1", 0, "{\"only\":\"alice\"}")
	if private.ThreadID != x.ta {
		t.Fatal("default retention changed")
	}
	if _, e := x.s.GetKV(x.ctx, x.b, []string{"private"}, "state"); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("default subject isolation lost")
	}
	other := x.b
	other.Deployment = "other"
	if _, e := x.s.GetKV(x.ctx, other, []string{"app"}, "state"); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("app crossed deployment")
	}
	for _, id := range []string{x.ta, x.tb} {
		if e := x.s.DeleteThread(x.ctx, x.a, id); e != nil {
			t.Fatal(e)
		}
	}
	if got, e := x.s.GetKV(x.ctx, x.a, []string{"user"}, "state"); e != nil || got.Version != u.Version {
		t.Fatal("deleting last session removed user state", e)
	}
	if _, e := x.s.GetKV(x.ctx, x.b, []string{"app"}, "state"); e != nil {
		t.Fatal("deleting session removed app state")
	}
	if _, e := x.s.DeleteSubject(x.ctx, x.a); e != nil {
		t.Fatal(e)
	}
	if _, e := x.s.GetKV(x.ctx, x.a, []string{"user"}, "state"); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("subject data survived deletion", e)
	}
	// Control-plane revocation/fencing is separate from deleting history.
	if e := x.s.FenceSubject(x.ctx, x.a); e != nil {
		t.Fatal(e)
	}
	if _, e := x.s.GetKV(x.ctx, x.a, []string{"app"}, "state"); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("erased actor reads app state", e)
	}
	if _, e := x.s.GetKV(x.ctx, x.b, []string{"app"}, "state"); e != nil {
		t.Fatal("subject deletion removed deployment-owned state")
	}
}
func TestRev7AtomicKVPublicationAndReplay(t *testing.T) {
	x := rev7OwnerFixture(t)
	req := KVBatchRequest{OperationKey: "publish", Items: []KVPut{
		{Namespace: []string{"head"}, Key: "a", ExpectedOwner: "thread", Value: json.RawMessage("{\"event\":1}")},
		{Namespace: []string{"user"}, Key: "state", ExpectedOwner: "subject", Value: json.RawMessage("{\"choice\":\"EUR\"}")},
		{Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment", Value: json.RawMessage("{\"edition\":1}")},
	}}
	first, e := x.s.PutKVBatch(x.ctx, x.a, x.fa, req)
	if e != nil || len(first.Items) != 3 {
		t.Fatal(e)
	}
	_ = rev7Put(t, x, x.b, x.fc, "app", "state", "other-update", 1, "{\"edition\":2}")
	replay, e := x.s.PutKVBatch(x.ctx, x.a, x.fa, req)
	if e != nil || replay.Items[2].Version != first.Items[2].Version {
		t.Fatal("lost ACK replay changed result", e)
	}
	for i := range first.Items {
		if !bytes.Equal(replay.Items[i].Value, first.Items[i].Value) {
			t.Fatal("replay changed canonical JSON bytes")
		}
	}
	req.Items[1].Value = json.RawMessage("{\"choice\":\"USD\"}")
	if _, e = x.s.PutKVBatch(x.ctx, x.a, x.fa, req); contracts.Code(e) != contracts.ErrKeyReuse {
		t.Fatal("key/body mismatch accepted", e)
	}
	req.OperationKey = "conflict"
	req.Items[0].ExpectedVersion = 1
	req.Items[1].ExpectedVersion = 1
	req.Items[2].ExpectedVersion = 1
	if _, e = x.s.PutKVBatch(x.ctx, x.a, x.fa, req); contracts.Code(e) != contracts.ErrConflict {
		t.Fatal("stale app version accepted", e)
	}
	reads := []KVRead{{Namespace: []string{"head"}, Key: "a", ExpectedOwner: "thread"}, {Namespace: []string{"user"}, Key: "state", ExpectedOwner: "subject"}, {Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment"}}
	values, e := x.s.GetKVBatch(x.ctx, x.a, reads)
	if e != nil || len(values) != 3 {
		t.Fatal(e)
	}
	if values[0].Version != 1 || values[1].Version != 1 || values[2].Version != 2 {
		t.Fatal("failed batch partially published")
	}
	bad := req
	bad.OperationKey = "owner-escalation"
	bad.Items = []KVPut{{Namespace: []string{"private"}, Key: "x", ExpectedOwner: "deployment", Value: json.RawMessage("{}")}}
	if _, e = x.s.PutKVBatch(x.ctx, x.a, x.fa, bad); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("caller chose owner", e)
	}
	bad.OperationKey = "duplicate"
	bad.Items = append([]KVPut{}, req.Items[0], req.Items[0])
	if _, e = x.s.PutKVBatch(x.ctx, x.a, x.fa, bad); contracts.Code(e) != contracts.ErrInvalid {
		t.Fatal("duplicate target accepted", e)
	}
	bad.OperationKey = "stale-epoch"
	bad.Items = req.Items[:1]
	f := x.fa
	f.Epoch++
	if _, e = x.s.PutKVBatch(x.ctx, x.a, f, bad); contracts.Code(e) != contracts.ErrStaleEpoch {
		t.Fatal("stale writer accepted", e)
	}
}
func TestRev7OwnerSearchAndBatchReceiptDeletion(t *testing.T) {
	x := rev7OwnerFixture(t)
	req := KVBatchRequest{OperationKey: "publish", Items: []KVPut{
		{Namespace: []string{"user"}, Key: "u", ExpectedOwner: "subject", Value: json.RawMessage("{\"private\":1}")},
		{Namespace: []string{"app"}, Key: "a", ExpectedOwner: "deployment", Value: json.RawMessage("{\"shared\":1}")},
	}}
	if _, e := x.s.PutKVBatch(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal(e)
	}
	got, e := x.s.SearchKV(x.ctx, x.b, nil, "", nil, 10)
	if e != nil || len(got) != 1 || got[0].Key != "a" {
		t.Fatal("search leaked user data or omitted app state", e)
	}
	if e := x.s.DeleteThread(x.ctx, x.a, x.ta); e != nil {
		t.Fatal(e)
	}
	var count int
	if e = x.s.pool.QueryRow(x.ctx, "SELECT count(*) FROM ahp_state.kv_ops WHERE tenant=$1 AND deployment=$2 AND subject=$3", x.a.Tenant, x.a.Deployment, x.a.Subject).Scan(&count); e != nil {
		t.Fatal(e)
	}
	if count != 0 {
		t.Fatal("deleted batch receipt kept a copy", count)
	}
	if _, e = x.s.GetKV(x.ctx, x.b, []string{"app"}, "a"); e != nil {
		t.Fatal("receipt cleanup deleted shared value")
	}
}

func TestRev7OwnerConcurrentCASAndConsistentRead(t *testing.T) {
	x := rev7OwnerFixture(t)
	_ = rev7Put(t, x, x.a, x.fa, "app", "counter", "init", 0, "{\"n\":0}")
	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)
	for i, f := range []Fence{x.fa, x.fb} {
		go func(i int, f Fence) {
			defer wg.Done()
			_, err := x.s.PutKVBatch(x.ctx, x.a, f, KVBatchRequest{OperationKey: fmt.Sprintf("race-%d", i), Items: []KVPut{
				{Namespace: []string{"head"}, Key: fmt.Sprintf("head-%d", i), ExpectedOwner: "thread", Value: json.RawMessage(fmt.Sprintf("{\"n\":%d}", i))},
				{Namespace: []string{"app"}, Key: "counter", ExpectedOwner: "deployment", ExpectedVersion: 1, Value: json.RawMessage(fmt.Sprintf("{\"n\":%d}", i+1))},
			}})
			results <- err
		}(i, f)
	}
	wg.Wait()
	close(results)
	success, conflict := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if contracts.Code(err) == contracts.ErrConflict {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal("both competing commits published", success, conflict)
	}
	heads, e := x.s.GetKVBatch(x.ctx, x.a, []KVRead{{Namespace: []string{"head"}, Key: "head-0", ExpectedOwner: "thread"}, {Namespace: []string{"head"}, Key: "head-1", ExpectedOwner: "thread"}})
	if e != nil || (heads[0] == nil) == (heads[1] == nil) {
		t.Fatal("losing batch left a head", e)
	}
}
func TestRev7MultiKeyReadersSeeWholePublication(t *testing.T) {
	x := rev7OwnerFixture(t)
	request := func(n int) KVBatchRequest {
		return KVBatchRequest{OperationKey: fmt.Sprintf("atomic-%d", n), Items: []KVPut{
			{Namespace: []string{"head"}, Key: "current", ExpectedOwner: "thread", ExpectedVersion: int64(n), Value: json.RawMessage(fmt.Sprintf("{\"n\":%d}", n))},
			{Namespace: []string{"app"}, Key: "current", ExpectedOwner: "deployment", ExpectedVersion: int64(n), Value: json.RawMessage(fmt.Sprintf("{\"n\":%d}", n))},
		}}
	}
	if _, e := x.s.PutKVBatch(x.ctx, x.a, x.fa, request(0)); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		for n := 1; n <= 25; n++ {
			if _, e := x.s.PutKVBatch(x.ctx, x.a, x.fa, request(n)); e != nil {
				done <- e
				return
			}
		}
		done <- nil
	}()
	defer func() {
		if e := <-done; e != nil {
			t.Error(e)
		}
	}()
	for n := 0; n < 50; n++ {
		values, e := x.s.GetKVBatch(x.ctx, x.a, []KVRead{{Namespace: []string{"head"}, Key: "current", ExpectedOwner: "thread"}, {Namespace: []string{"app"}, Key: "current", ExpectedOwner: "deployment"}})
		if e != nil || values[0] == nil || values[1] == nil {
			t.Fatal(e)
		}
		if !bytes.Equal(values[0].Value, values[1].Value) {
			t.Fatal("mixed snapshot of a single committed batch")
		}
	}
}
func TestRev7OwnerProfileExportRefusesIncompleteScope(t *testing.T) {
	x := rev7OwnerFixture(t)
	x.s.scopeTransfer = ScopeTransferConfig{NativeBuild: "shared-build", Offline: func(context.Context, contracts.Scope, string, string) error { return nil }}
	_, err := x.s.ExportScopeV2(x.ctx, x.a, "migration", "shared-build")
	if contracts.Code(err) != contracts.ErrInvalid || !strings.Contains(err.Error(), "owner-scoped KV") {
		t.Fatal("shared export must fail with explicit unsupported profile", err)
	}
}
func TestRev7OwnerPolicyDoesNotSilentlyReinterpretExistingRows(t *testing.T) {
	e := kvFixture(t)
	_ = kvPut(t, e, []string{"legacy"}, "key", "legacy", 0, "{\"n\":1}")
	option, err := WithKVNamespacePolicies([]KVNamespacePolicy{{Tenant: e.scope.Tenant, Deployment: e.scope.Deployment, Namespace: []string{"legacy"}, Owner: "deployment"}})
	if err != nil {
		t.Fatal(err)
	}
	changed := New(e.s.pool, option)
	if err = changed.Migrate(e.ctx); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatal("ownership change silently hid existing rows", err)
	}
	if _, err = e.s.GetKV(e.ctx, e.scope, []string{"legacy"}, "key"); err != nil {
		t.Fatal("failed migration changed existing data", err)
	}
}
