package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DEF-STAND-1.
//
// The idempotency key of an upload is scoped to (tenant, deployment, subject,
// idem_key), and the contract says what a reused key means: a replay of the
// identical request is the same upload, a different request under the same key
// is KEY_REUSE_MISMATCH. A key whose artifact was deleted is released with it
// (DEF-H24-8): the tombstone is a record, not a reservation, and the next upload
// under that key is a new generation -- so what this file has to keep proving is
// the surviving invariant, at most one LIVE row per key even when the durable
// index is absent.
//
// The live database proved that the contract was enforced by ONE mechanism
// only: the unique index artifacts_idem_uidx. On a database whose artifacts
// table predates that index the index is simply absent - and there
// INSERT ... ON CONFLICT DO NOTHING has no arbiter to conflict on, so the
// second upload silently inserts a second row. Two rows then make the
// migration itself fail ("could not create unique index"), which is how the
// composed front lost its data planes on every restart.
//
// These tests reproduce the produced-database shape (index dropped) and demand
// that the arbitration be done by the database through the code path, not by
// the accidental presence of an index - without weakening idempotent replay.

// dropIdemIndex removes the unique index the way the produced database has it:
// absent. Restored only for hygiene; the run database is dropped at exit.
func dropIdemIndex(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := isolatedPool(t).Exec(ctx, `
DROP INDEX IF EXISTS ahp_artifacts.`+idempotencyIndexName+`;
DROP INDEX IF EXISTS ahp_artifacts.`+idemLiveIndexName); err != nil {
		t.Fatalf("drop idempotency index: %v", err)
	}
}

func idemIndexPresent(t *testing.T) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var found bool
	if err := isolatedPool(t).QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_indexes
		        WHERE schemaname = 'ahp_artifacts' AND indexname = '`+idemLiveIndexName+`')`).
		Scan(&found); err != nil {
		t.Fatalf("read index presence: %v", err)
	}
	return found
}

// keyRows counts the rows one idempotency key owns, out of band.
func keyRows(t *testing.T, e *env, key string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	var ids string
	if err := isolatedPool(t).QueryRow(ctx, `
SELECT count(*), COALESCE(string_agg(id, ',' ORDER BY created_at), '')
  FROM ahp_artifacts.artifacts
 WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND idem_key = $4`,
		e.scope.Tenant, e.scope.Deployment, e.scope.Subject, key).Scan(&n, &ids); err != nil {
		t.Fatalf("count key rows: %v", err)
	}
	return n, ids
}

func shaOf(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// liveKeyRows counts the rows a key owns that are still here -- the number the
// invariant is about, since a tombstone no longer reserves the key.
func liveKeyRows(t *testing.T, e *env, key string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	var ids string
	if err := isolatedPool(t).QueryRow(ctx, `
SELECT count(*), COALESCE(string_agg(id, ',' ORDER BY created_at), '')
  FROM ahp_artifacts.artifacts
 WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND idem_key = $4
   AND NOT deleted`,
		e.scope.Tenant, e.scope.Deployment, e.scope.Subject, key).Scan(&n, &ids); err != nil {
		t.Fatalf("count live key rows: %v", err)
	}
	return n, ids
}

// TestBeginUploadAfterDeleteStartsNewGenerationWithoutIndex is the exact live
// shape -- an upload sealed, deleted, then begun again under the same key --
// read against the invariant that survived the DEF-H24-8 contract change. Before
// it this leg expected a refusal, and the live database showed why that was the
// wrong contract (a deleted chat made the same file unsavable forever). What must
// NOT come back is the shape that duplicated (djourney,space,u-ahp-accept,
// 8bd2bd23...) and killed the migration: with the durable index absent, the
// arbitration still has to leave exactly one LIVE row for the key.
func TestBeginUploadAfterDeleteStartsNewGenerationWithoutIndex(t *testing.T) {
	e := setup(t)
	dropIdemIndex(t)
	if idemIndexPresent(t) {
		t.Fatal("fixture failed to reproduce the produced database: index still present")
	}
	key := "k-" + t.Name()
	body := "first generation bytes"
	ref, _ := e.uploadSeal(t, key, body)
	n, ids := keyRows(t, e, key)
	if n != 1 {
		t.Fatalf("after first upload: want 1 row for the key, got %d (%s)", n, ids)
	}
	id := ref.ID

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := e.store.Delete(ctx, e.scope, id, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	next := "second generation bytes, after the chat was deleted"
	up, err := e.store.BeginUpload(ctx, e.scope, key, UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream",
		SHA256: shaOf(next), Size: int64(len(next)),
	})
	if err != nil {
		t.Fatalf("a freed key must be re-usable with the index absent: %v", err)
	}
	if up.ID == id {
		t.Fatal("the new generation must not be the tombstone")
	}
	if up.Object == "" {
		t.Fatal("the new generation must carry its own object path")
	}
	live, ids := liveKeyRows(t, e, key)
	if live != 1 {
		t.Fatalf("arbitration without the index must leave one live row per key: live=%d (%s)", live, ids)
	}
	total, _ := keyRows(t, e, key)
	if total != 2 {
		t.Fatalf("the tombstone stays as a record: rows=%d, want 2 (deleted + live)", total)
	}
	// A second concurrent-shaped call under the freed key, replayed identically,
	// still names the same new generation rather than a third row.
	again, err := e.store.BeginUpload(ctx, e.scope, key, UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream",
		SHA256: shaOf(next), Size: int64(len(next)),
	})
	if err != nil {
		t.Fatalf("replay of the new generation: %v", err)
	}
	if again.ID != up.ID {
		t.Fatalf("replay must name the same generation: %s vs %s", again.ID, up.ID)
	}
	if live, ids = liveKeyRows(t, e, key); live != 1 {
		t.Fatalf("replay must not add a live row: live=%d (%s)", live, ids)
	}
}

// TestConcurrentBeginUploadSameKeyWithoutIndex runs the same contract against
// two writers at once: exactly one row may exist, and the loser of the race
// gets the replay answer rather than a second registration.
func TestConcurrentBeginUploadSameKeyWithoutIndex(t *testing.T) {
	e := setup(t)
	dropIdemIndex(t)
	key := "k-" + t.Name()
	body := "concurrent bytes"
	sum := shaOf(body)
	req := UploadRequest{Name: "a.bin", MIME: "application/octet-stream",
		SHA256: sum, Size: int64(len(body))}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const writers = 2
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		ids  []string
		errs []error
	)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			up, err := e.store.BeginUpload(ctx, e.scope, key, req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			ids = append(ids, up.ID)
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("concurrent identical replays must not error: %v", errs[0])
	}
	if len(ids) != writers {
		t.Fatalf("want %d replay answers, got %d", writers, len(ids))
	}
	for _, id := range ids[1:] {
		if id != ids[0] {
			t.Fatalf("replay returned a different upload: %s vs %s", ids[0], id)
		}
	}
	n, rowIDs := keyRows(t, e, key)
	if n != 1 {
		t.Fatalf("idempotency key must own exactly one row: rows=%d (%s)", n, rowIDs)
	}
}

// TestBeginUploadKeyReuseDifferentDigestWithoutIndex keeps KEY_REUSE_MISMATCH
// enforced when there is no index to do it.
func TestBeginUploadKeyReuseDifferentDigestWithoutIndex(t *testing.T) {
	e := setup(t)
	dropIdemIndex(t)
	key := "k-" + t.Name()
	body := "original body"
	e.uploadSeal(t, key, body)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	other := "different body entirely"
	otherSum := shaOf(other)
	_, err := e.store.BeginUpload(ctx, e.scope, key, UploadRequest{
		Name: "b.bin", MIME: "application/octet-stream",
		SHA256: otherSum, Size: int64(len(other)),
	})
	codeIs(t, err, contracts.ErrKeyReuse)
	if n, ids := keyRows(t, e, key); n != 1 {
		t.Fatalf("a mismatched reuse must not add a row: rows=%d (%s)", n, ids)
	}
}

// TestIdempotentReplayHoldsWithoutIndex is the anti-weakening leg: the fix must
// not trade the replay for a blanket refusal.
func TestIdempotentReplayHoldsWithoutIndex(t *testing.T) {
	e := setup(t)
	dropIdemIndex(t)
	key := "k-" + t.Name()
	body := "replayed bytes"
	sum := shaOf(body)
	req := UploadRequest{Name: "a.bin", MIME: "application/octet-stream",
		SHA256: sum, Size: int64(len(body))}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, err := e.store.BeginUpload(ctx, e.scope, key, req)
	if err != nil {
		t.Fatalf("first BeginUpload: %v", err)
	}
	second, err := e.store.BeginUpload(ctx, e.scope, key, req)
	if err != nil {
		t.Fatalf("replay BeginUpload: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("replay must return the same upload: %s vs %s", first.ID, second.ID)
	}
	if first.Object != second.Object {
		t.Fatal("replay must return the same object path")
	}
	if n, ids := keyRows(t, e, key); n != 1 {
		t.Fatalf("replay must not add a row: rows=%d (%s)", n, ids)
	}
}

// TestMigrateGreenOverProducedDuplicates is the stand-death leg, restated for the
// live-key contract of DEF-H24-8. A restart must never lose the data planes
// because of rows a pre-constraint binary produced, and green migration must not
// come from weakening the guarantee. Under the live-key index the two produced
// shapes mean different things and are asserted separately: a tombstone plus one
// live row is the NORMAL shape of "the chat was deleted and the file was saved
// again" and installs the index; two live rows on one key is still impossible, so
// the index cannot be built, the migration stays green anyway, and it says so.
// Nothing here deletes operator data.
func TestMigrateGreenOverProducedDuplicates(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := isolatedPool(t)

	dropIdemIndex(t)
	key := "k-" + t.Name()
	const ins = `
INSERT INTO ahp_artifacts.artifacts
    (id, tenant, deployment, subject, object, name, mime, expected_sha, expected_size,
     version, sha256, size, labels, idem_key, request_digest, deleted, sealed_at)
VALUES ($1, $2, $3, $4, $5, 'a.bin', 'application/octet-stream', $6, 10,
        'v1', $6, 10, '{"subjects":["alice"]}'::jsonb, $7, $8, $9, now())`
	dup := shaOf("first generation bytes")
	seedRow := func(id, digest string, deleted bool) {
		t.Helper()
		if _, err := pool.Exec(ctx, ins, id, e.scope.Tenant, e.scope.Deployment,
			e.scope.Subject, e.scope.Tenant+"/"+id, dup, key, digest, deleted); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	// Shape one: one tombstone, one live row, three seconds apart, different
	// request digests -- the row the live front actually produced on 2026-11-02.
	seedRow("dup-"+e.scope.Tenant+"-a", dup, true)
	seedRow("dup-"+e.scope.Tenant+"-b", shaOf("second generation bytes"), false)
	if n, ids := keyRows(t, e, key); n != 2 {
		t.Fatalf("fixture must produce the produced shape: rows=%d (%s)", n, ids)
	}

	if err := e.store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate over a tombstone plus a live row must be green, got: %v", err)
	}
	if !idemIndexPresent(t) {
		t.Fatal("a tombstone does not reserve a key: the live-key index must build over this shape")
	}
	if e.store.IdempotencyIndexPending() {
		t.Fatal("nothing is pending once the live-key index is in place")
	}
	if n, _ := keyRows(t, e, key); n != 2 {
		t.Fatalf("migration must not delete operator data: rows=%d", n)
	}
	if n, _ := liveKeyRows(t, e, key); n != 1 {
		t.Fatalf("exactly one row still owns the key: live=%d", n)
	}
	// The live row keeps the key: a different install request is refused, and it
	// adds no row. (The replay half of this contract is proven through the API in
	// TestLiveUploadKeyKeepsReplayAndMismatch.)
	_, err := e.store.BeginUpload(ctx, e.scope, key, UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream", SHA256: shaOf("a different request"), Size: 10,
	})
	// A different install request over a key a LIVE row owns is a mismatch, the
	// same answer the plane gives everywhere else.
	codeIs(t, err, contracts.ErrKeyReuse)
	if n, ids := keyRows(t, e, key); n != 2 {
		t.Fatalf("a refused reuse must not add a row: rows=%d (%s)", n, ids)
	}

	// Shape two: two LIVE rows on one key. That stays a duplicate, the index stays
	// unbuildable, the front still comes up, and the missing backstop is reported
	// instead of hidden.
	dropIdemIndex(t)
	seedRow("dup-"+e.scope.Tenant+"-c", shaOf("third generation bytes"), false)
	if n, _ := liveKeyRows(t, e, key); n != 2 {
		t.Fatalf("fixture must produce a live duplicate: live=%d", n)
	}
	if err := e.store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate over a live duplicate must still be green, got: %v", err)
	}
	if idemIndexPresent(t) {
		t.Fatal("the index cannot exist while two live rows share a key: the tolerance would be a silent weakening")
	}
	if !e.store.IdempotencyIndexPending() {
		t.Fatal("a migration that could not install the index must say so")
	}
	if _, err := e.store.BeginUpload(ctx, e.scope, key, UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream", SHA256: shaOf("yet another request"), Size: 10,
	}); err == nil {
		t.Fatal("reuse of a key that owns live rows must be refused without the index")
	}
	if n, ids := liveKeyRows(t, e, key); n != 2 {
		t.Fatalf("refused reuse must not add a live row: live=%d (%s)", n, ids)
	}

	// Operator-style read-out of the historical rows, then the next restart: the
	// durable guarantee comes back on its own.
	// The read-out that matters is the one the operator can actually do: two live
	// rows cannot both keep one key, so one of them goes and the other stays.
	if _, err := pool.Exec(ctx,
		`DELETE FROM ahp_artifacts.artifacts WHERE id = $1`, "dup-"+e.scope.Tenant+"-c"); err != nil {
		t.Fatalf("simulate operator read-out: %v", err)
	}
	if err := e.store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate after the read-out: %v", err)
	}
	if !idemIndexPresent(t) {
		t.Fatal("after the duplicates are gone the live-key index must be installed again")
	}
	if e.store.IdempotencyIndexPending() {
		t.Fatal("a migration that installed the index must not report it pending")
	}
	// And with the durable backstop in place, the surviving live row still owns
	// the key: nothing about this shape is allowed to become a free key.
	_, err = e.store.BeginUpload(ctx, e.scope, key, UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream", SHA256: shaOf("a fourth generation"), Size: 10,
	})
	codeIs(t, err, contracts.ErrKeyReuse)
}

// legacyIndexPresent reads the OLD full index out of band. It is the index the
// produced database has, and the one that makes a tombstone keep reserving its
// upload key -- so a migration that leaves it behind has not changed any user
// visible behaviour, however green its own tests are.
func legacyIndexPresent(t *testing.T) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var found bool
	if err := isolatedPool(t).QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM pg_indexes
        WHERE schemaname = 'ahp_artifacts' AND indexname = '`+idempotencyIndexName+`')`).
		Scan(&found); err != nil {
		t.Fatalf("read legacy index presence: %v", err)
	}
	return found
}

// TestMigrateDropsTheLegacyKeyReservation is the leg the deployed stand taught the
// hard way: the front came up with BOTH indexes, so a deleted key was still
// reserved, while the migration reported nothing pending. The cause was a DROP
// INDEX that did not name its schema -- resolved against the connection's
// search_path, finding nothing in "public", succeeding because of IF EXISTS.
//
// The legacy index here is put on a probe table in the artifact schema rather than
// on ahp_artifacts.artifacts itself. The name is what the DROP addresses, so the
// assertion is identical, while a table-wide UNIQUE index over a shared test
// database would fail on another lane's in-flight upload -- and refuse it while it
// existed. That is not a fixture, it is a hazard.
func TestMigrateDropsTheLegacyKeyReservation(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := isolatedPool(t)
	probe := "h23_legacy_probe_" + e.scope.Tenant[len(e.scope.Tenant)-12:]

	dropIdemIndex(t)
	if _, err := pool.Exec(ctx, `CREATE TABLE ahp_artifacts.`+probe+` AS
SELECT tenant, deployment, subject, idem_key FROM ahp_artifacts.artifacts WHERE false`); err != nil {
		t.Fatalf("probe table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS ahp_artifacts.`+probe)
	})
	if _, err := pool.Exec(ctx, `
CREATE UNIQUE INDEX `+idempotencyIndexName+` ON ahp_artifacts.`+probe+` (tenant, deployment, subject, idem_key)`); err != nil {
		t.Fatalf("create legacy index: %v", err)
	}
	if !legacyIndexPresent(t) {
		t.Fatal("fixture must produce the legacy full index in the artifact schema")
	}
	if idemIndexPresent(t) {
		t.Fatal("the live-key index must not exist before the migration runs")
	}

	if err := e.store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate must be green over an existing legacy index, got: %v", err)
	}
	if !idemIndexPresent(t) {
		t.Fatal("the live-key index must be installed")
	}
	if legacyIndexPresent(t) {
		t.Fatal("DEF-H24-8 is inactive while the legacy full index exists: a tombstone still reserves its key")
	}
}

// TestLiveKeyIndexIsTheOnlyReservation is the behaviour the two indexes decide:
// with the live-key constraint alone, a tombstone leaves its key free and two live
// registrations of one key stay impossible -- at the database, not in the code.
func TestLiveKeyIndexIsTheOnlyReservation(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := isolatedPool(t)

	// The migration in setup() leaves exactly one constraint: the partial one.
	if !idemIndexPresent(t) {
		t.Fatal("the live-key index is the durable backstop")
	}
	if legacyIndexPresent(t) {
		t.Fatal("the legacy full index must not come back up")
	}

	key := "k-only-" + e.scope.Tenant
	const ins = `
INSERT INTO ahp_artifacts.artifacts
    (id, tenant, deployment, subject, object, name, mime, expected_sha, expected_size,
     version, sha256, size, labels, idem_key, request_digest, deleted, sealed_at)
VALUES ($1, $2, $3, $4, $5, 'a.bin', 'application/octet-stream', $6, 10,
        'v1', $6, 10, '{"subjects":["alice"]}'::jsonb, $7, $8, $9, now())`
	dup := shaOf("reservation generation bytes")
	if _, err := pool.Exec(ctx, ins, "only-"+e.scope.Tenant+"-a", e.scope.Tenant, e.scope.Deployment,
		e.scope.Subject, e.scope.Tenant+"/only-a", dup, key, dup, true); err != nil {
		t.Fatalf("seed tombstoned row: %v", err)
	}
	if _, err := pool.Exec(ctx, ins, "only-"+e.scope.Tenant+"-b", e.scope.Tenant, e.scope.Deployment,
		e.scope.Subject, e.scope.Tenant+"/only-b", dup, key, shaOf("next generation bytes"), false); err != nil {
		t.Fatalf("a tombstone must not block the next generation: %v", err)
	}
	_, err := pool.Exec(ctx, ins, "only-"+e.scope.Tenant+"-c", e.scope.Tenant, e.scope.Deployment,
		e.scope.Subject, e.scope.Tenant+"/only-c", dup, key, shaOf("third generation bytes"), false)
	if err == nil {
		t.Fatal("two live rows on one key must stay impossible at the database level")
	}
	if !strings.Contains(err.Error(), idemLiveIndexName) {
		t.Fatalf("the live-key index must be the one that refuses: %v", err)
	}
}
