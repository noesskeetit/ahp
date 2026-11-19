package state

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func rev31Keys() []KVRead {
	return []KVRead{{Namespace: []string{"user"}, Key: "state", ExpectedOwner: "subject"}, {Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment"}}
}
func rev31Put(t *testing.T, n int, version int64) CommonCommit {
	t.Helper()
	r := rev27Request(t, fmt.Sprint("bundle-", n), version, nil)
	row := r.Document.Records["s"]
	row.Attributes = json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))
	r.Document.Records["s"] = row
	for _, v := range []struct{ ns, owner string }{{"user", "subject"}, {"app", "deployment"}} {
		r.KV = append(r.KV, KVPut{Namespace: []string{v.ns}, Key: "state", ExpectedOwner: v.owner, ExpectedVersion: version, Value: json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))})
	}
	return r
}
func rev31Store(t *testing.T, x *rev7Owners) (*Store, int32) {
	t.Helper()
	cfg := x.s.pool.Config().Copy()
	cfg.MaxConns = 1
	p, e := pgxpool.NewWithConfig(x.ctx, cfg)
	if e != nil {
		t.Fatal("one-connection pool")
	}
	t.Cleanup(p.Close)
	s := New(p, WithCommonPayloadObjects())
	s.kvOwnerPolicies = x.s.kvOwnerPolicies
	var pid int32
	if e = p.QueryRow(x.ctx, `SELECT pg_backend_pid()`).Scan(&pid); e != nil {
		t.Fatal(e)
	}
	return s, pid
}
func rev31Wait(t *testing.T, x *rev7Owners, pid int32, kind string) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		var waiting bool
		if e := x.s.pool.QueryRow(x.ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND wait_event=$2)`, pid, kind).Scan(&waiting); e != nil {
			t.Fatal(e)
		}
		if waiting {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected database lock wait not observed", kind)
}
func rev31N(t *testing.T, b CommonBundle) int {
	t.Helper()
	if b.Common == nil || len(b.KV) != 2 || b.KV[0] == nil || b.KV[1] == nil {
		t.Fatal("partial bundle")
	}
	var row struct {
		N int `json:"n"`
	}
	if e := json.Unmarshal(b.Common.Document.Records["s"].Attributes, &row); e != nil {
		t.Fatal(e)
	}
	for _, kv := range b.KV {
		var v struct {
			N int `json:"n"`
		}
		if e := json.Unmarshal(kv.Value, &v); e != nil || v.N != row.N || kv.Version != b.Common.Receipt.Revision {
			t.Fatal("mixed common/shared versions", row.N, kv.Version, b.Common.Receipt.Revision, e)
		}
	}
	return row.N
}
func TestRev31BundleDeterministicSnapshotAcrossConcurrentPublication(t *testing.T) {
	x := rev29Fixture(t)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, rev31Put(t, 0, 0)); e != nil {
		t.Fatal(e)
	}
	// Force writer A to wait for the KV table, then force reader B to queue
	// behind A. B reads the old common head before A commits, but reaches KV
	// only AFTER A committed. Two independent RC reads deterministically mix.
	fn := "rev31_gate_" + isolateSafeID(contracts.NewID())
	tr := fn + "_t"
	sql := `CREATE FUNCTION ahp_state.` + pgx.Identifier{fn}.Sanitize() + `() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.thread_id='` + x.ta + `' THEN LOCK TABLE ahp_state.kv IN ACCESS EXCLUSIVE MODE; END IF; RETURN NEW; END $$;
 CREATE TRIGGER ` + pgx.Identifier{tr}.Sanitize() + ` BEFORE UPDATE ON ahp_state.common_heads FOR EACH ROW EXECUTE FUNCTION ahp_state.` + pgx.Identifier{fn}.Sanitize() + `()`
	if _, e := x.s.pool.Exec(x.ctx, sql); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_, _ = x.s.pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS `+pgx.Identifier{tr}.Sanitize()+` ON ahp_state.common_heads;DROP FUNCTION IF EXISTS ahp_state.`+pgx.Identifier{fn}.Sanitize()+`()`)
	})
	blocker, e := x.s.pool.Begin(x.ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer blocker.Rollback(context.Background())
	if _, e = blocker.Exec(x.ctx, `LOCK TABLE ahp_state.kv IN ACCESS EXCLUSIVE MODE`); e != nil {
		t.Fatal(e)
	}
	writer, wpid := rev31Store(t, x)
	reader, rpid := rev31Store(t, x)
	ctx, stop := context.WithTimeout(x.ctx, 8*time.Second)
	defer stop()
	writes := make(chan error, 1)
	req := rev31Put(t, 1, 1)
	go func() { _, e := writer.CommitRecords(ctx, x.a, x.fa, req); writes <- e }()
	rev31Wait(t, x, wpid, "relation")
	type result struct {
		b CommonBundle
		e error
	}
	reads := make(chan result, 1)
	go func() {
		b, e := reader.ReadRecordsBundle(ctx, x.a, x.ta, req.Namespace, rev31Keys())
		reads <- result{b, e}
	}()
	rev31Wait(t, x, rpid, "relation")
	if e = blocker.Commit(x.ctx); e != nil {
		t.Fatal(e)
	}
	if e = <-writes; e != nil {
		t.Fatal("writer", e)
	}
	r := <-reads
	if r.e != nil {
		t.Fatal("reader", r.e)
	}
	if rev31N(t, r.b) != 0 {
		t.Fatal("bundle did not retain original transaction snapshot")
	}
	latest, e := reader.ReadRecordsBundle(x.ctx, x.a, x.ta, req.Namespace, rev31Keys())
	if e != nil || rev31N(t, latest) != 1 {
		t.Fatal("next snapshot did not see complete new publication", e)
	}
}
func TestRev31BundleLockWaitSeesErasureCommittedBeforeAdmission(t *testing.T) {
	x := rev29Fixture(t)
	s, pid := rev31Store(t, x)
	tx, e := x.s.pool.Begin(x.ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(x.ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, subjectFenceKey(x.a)); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := s.ReadRecordsBundle(x.ctx, x.a, x.ta, "common/research-2", rev31Keys()); done <- e }()
	rev31Wait(t, x, pid, "advisory")
	if _, e = tx.Exec(x.ctx, `INSERT INTO ahp_state.erased_subjects(tenant,subject) VALUES($1,$2)`, x.a.Tenant, x.a.Subject); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(x.ctx); e != nil {
		t.Fatal(e)
	}
	if e = <-done; contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("stale RR snapshot leaked erased user data", e)
	}
}
func TestRev31BundleCancellationReturnsSingleConnectionWithoutLocks(t *testing.T) {
	x := rev29Fixture(t)
	s, pid := rev31Store(t, x)
	tx, e := x.s.pool.Begin(x.ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(x.ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, subjectFenceKey(x.a)); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(x.ctx)
	done := make(chan error, 1)
	go func() { _, e := s.ReadRecordsBundle(ctx, x.a, x.ta, "common/research-2", rev31Keys()); done <- e }()
	rev31Wait(t, x, pid, "advisory")
	cancel()
	if e = <-done; e == nil {
		t.Fatal("cancelled reader succeeded")
	}
	if e = tx.Commit(x.ctx); e != nil {
		t.Fatal(e)
	}
	// Reuse the same one-connection pool; a broken/cancelled socket may be
	// replaced, but a live connection must never return holding session locks.
	var newPID int32
	if e = s.pool.QueryRow(x.ctx, `SELECT pg_backend_pid()`).Scan(&newPID); e != nil {
		t.Fatal(e)
	}
	var locks int
	if e = s.pool.QueryRow(x.ctx, `SELECT count(*) FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND granted`, newPID).Scan(&locks); e != nil || locks != 0 {
		t.Fatal("session advisory lock leaked into pool", locks, e)
	}
	b, e := s.ReadRecordsBundle(x.ctx, x.a, x.ta, "common/research-2", rev31Keys())
	if e != nil || b.Common != nil || s.pool.Stat().AcquiredConns() != 0 {
		t.Fatal("one-connection pool cannot serve next bundle", e)
	}
}
func TestRev31BundleSharedScopeIsolationAndNoClientPointer(t *testing.T) {
	x := rev29Fixture(t)
	r := rev31Put(t, 0, 0)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, r); e != nil {
		t.Fatal(e)
	}
	b, e := x.s.ReadRecordsBundle(x.ctx, x.a, x.ta, r.Namespace, rev31Keys())
	if e != nil || rev31N(t, b) != 0 {
		t.Fatal(e)
	}
	// A foreign thread can still write an unrelated legacy-shaped KV key; the
	// bundle ignores it because its head is an internal server table, not KV.
	if _, e = x.s.PutKV(x.ctx, x.a, x.fb, []string{"adk27-head"}, x.ta, "legacy-collision", 0, json.RawMessage(`{"revision":999}`)); e != nil {
		t.Fatal(e)
	}
	b, e = x.s.ReadRecordsBundle(x.ctx, x.a, x.ta, r.Namespace, rev31Keys())
	if e != nil || b.Common.Receipt.Revision != 1 {
		t.Fatal("legacy pointer affected server bundle", e)
	}
	foreign, e := x.s.ReadRecordsBundle(x.ctx, x.b, x.tc, r.Namespace, rev31Keys())
	if e != nil || foreign.Common != nil || foreign.KV[0] != nil || foreign.KV[1] == nil {
		t.Fatal("shared-owner scope isolation", e)
	}
	if _, e = x.s.ReadRecordsBundle(x.ctx, x.b, x.ta, r.Namespace, rev31Keys()); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("foreign session disclosure", e)
	}
	if e = x.s.DeleteThread(x.ctx, x.a, x.ta); e != nil {
		t.Fatal(e)
	}
	kept, e := x.s.ReadRecordsBundle(x.ctx, x.a, x.tb, r.Namespace, rev31Keys())
	if e != nil || kept.Common != nil || kept.KV[0] == nil || kept.KV[1] == nil {
		t.Fatal("deleting writer session removed owners", e)
	}
}
func TestRev31BundleOwnerAndInputValidation(t *testing.T) {
	x := rev29Fixture(t)
	bad := rev31Keys()
	bad[0].ExpectedOwner = "deployment"
	if _, e := x.s.ReadRecordsBundle(x.ctx, x.a, x.ta, "common/research-2", bad); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("owner precondition ignored", e)
	}
	if _, e := x.s.ReadRecordsBundle(x.ctx, x.a, x.ta, "common/research-2", nil); contracts.Code(e) != contracts.ErrInvalid {
		t.Fatal("unbounded/empty bundle accepted", e)
	}
	if _, e := x.s.ReadRecordsBundle(x.ctx, x.a, x.ta, "common/research-2", make([]KVRead, 9)); contracts.Code(e) != contracts.ErrInvalid {
		t.Fatal("unbounded bundle accepted", e)
	}
}
