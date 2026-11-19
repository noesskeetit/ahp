//go:build ahp_data

package main

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// This test never starts a collector and never calls RunOnce. Only the already
// running, operator-configured backend process can collect these new fixtures.
func TestDeployedStandingGCScheduler(t *testing.T) {
	if os.Getenv("AHP_DEPLOYED_GC_TEST") != "1" {
		t.Skip("requires an explicitly owned deployed backend with its standing GC enabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 170*time.Second)
	defer cancel()
	tenant := os.Getenv("AHP_DEPLOYED_GC_TENANT")
	database := os.Getenv("AHP_DEPLOYED_GC_DATABASE")
	if !strings.HasPrefix(tenant, "dot-e2e-") || !strings.HasPrefix(database, "ahp_hive_closure_") {
		t.Fatal("deployed fixture scope must identify the isolated acceptance stand")
	}
	pool, err := pgxpool.New(ctx, os.Getenv("AHP_DEPLOYED_GC_DATABASE_URL"))
	if err != nil {
		t.Fatal("deployed GC database configuration failed")
	}
	defer pool.Close()
	var actual string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&actual); err != nil || actual != database {
		t.Fatal("deployed GC database does not match the operator receipt")
	}
	u, err := url.Parse(os.Getenv("AHP_S3_ENDPOINT"))
	if err != nil || u.Host == "" {
		t.Fatal("owned object endpoint required")
	}
	objects, err := minio.New(u.Host, &minio.Options{Secure: u.Scheme == "https",
		Creds: credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), "")})
	if err != nil {
		t.Fatal("owned object client configuration failed")
	}
	nonce := contracts.NewID()
	scope := contracts.Scope{Tenant: tenant, Deployment: "gc-fixture-" + nonce, Subject: "gc-fixture-" + nonce}
	foreign := scope
	foreign.Tenant += "-gc-outside-" + nonce
	seed, err := artifacts.New(pool, objects, os.Getenv("AHP_S3_BUCKET"),
		func(_ context.Context, got contracts.Scope, _, _ string) error {
			if got != scope && got != foreign {
				return contracts.NewError(contracts.ErrForbidden, "fixture may touch only its newly allocated scopes")
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	e := &gcSchedEnv{pool: pool, client: objects, bucket: os.Getenv("AHP_S3_BUCKET"), scope: scope, seed: seed}
	if e.bucket == "" {
		t.Fatal("owned acceptance bucket required")
	}
	garbage, garbageObject := e.tombstoneExpired(t, ctx, scope, "standing-gc-expired-"+nonce)
	held, heldObject := e.tombstoneExpired(t, ctx, scope, "standing-gc-held-"+nonce)
	if err := seed.SetLegalHold(ctx, scope, held, true); err != nil {
		t.Fatal(err)
	}
	retained, retainedObject := e.tombstoneExpired(t, ctx, scope, "standing-gc-retained-"+nonce)
	future := time.Now().Add(time.Hour)
	if err := seed.SetRetention(ctx, scope, retained, &future); err != nil {
		t.Fatal(err)
	}
	outside, outsideObject := e.tombstoneExpired(t, ctx, foreign, "standing-gc-outside-"+nonce)
	st := state.New(pool)
	contextID := "gc-live-" + nonce
	thread, err := st.CreateThread(ctx, scope, contextID, nonce)
	if err != nil {
		t.Fatal(err)
	}
	run, err := st.CreateRun(ctx, scope, thread.ID, "gc-task-"+nonce, nonce, json.RawMessage(`{"type":"gc-probe"}`))
	if err != nil {
		t.Fatal(err)
	}
	live := sealArtifact(t, ctx, seed, scope, artifacts.Binding{ContextID: contextID}, "standing-gc-live-"+nonce)
	liveObject := e.objectOf(t, live)
	if err := seed.Delete(ctx, scope, live, "gc-delete-live-"+nonce); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour)
	if err := seed.SetRetention(ctx, scope, live, &past); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 80*time.Second, func() bool { return e.rowGone(t, garbage) && e.versions(t, garbageObject) == 0 }) {
		t.Fatal("standing backend did not remove expired metadata and exact S3 versions")
	}
	for id, object := range map[string]string{held: heldObject, retained: retainedObject, outside: outsideObject, live: liveObject} {
		if e.rowGone(t, id) || e.versions(t, object) == 0 {
			t.Fatalf("standing GC removed a protected fixture: %s", id)
		}
	}
	// Closing this test's own live run releases the only remaining liveness
	// reference. The next scheduled pass, not the test, must collect it.
	lease, err := st.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "gc-fixture", run.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("fixture run cannot be closed: %v", err)
	}
	if err := st.Release(ctx, scope, run.ID, lease.Worker, lease.Epoch, state.RunDone); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 80*time.Second, func() bool { return e.rowGone(t, live) && e.versions(t, liveObject) == 0 }) {
		t.Fatal("scheduled GC did not collect the artifact after its live reference ended")
	}
	t.Log("deployed standing GC: expired metadata+S3 versions purged; hold, retention, foreign tenant and live run protected; released run collected on a later tick")
}
