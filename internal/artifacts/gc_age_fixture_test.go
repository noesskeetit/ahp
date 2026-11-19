package artifacts

import (
	"context"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// Only fixtures intended to be old garbage call these helpers. The timestamps
// come from the actual database/S3 object, which can have a different clock
// from this test process. No retention value or production clock is changed.
func waitFixtureDeadline(t *testing.T, notBefore time.Time) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !time.Now().After(notBefore) {
		if time.Now().After(deadline) {
			t.Fatal("fixture did not reach its actual timestamp-based GC age")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (e *env) waitTombstoneAge(t *testing.T, ids ...string) {
	t.Helper()
	for _, id := range ids {
		var deleted time.Time
		var retain *time.Time
		if err := e.store.pool.QueryRow(context.Background(), `
SELECT deleted_at, retain_until FROM ahp_artifacts.artifacts
 WHERE tenant=$1 AND id=$2 AND deleted`, e.scope.Tenant, id).Scan(&deleted, &retain); err != nil {
			t.Fatalf("read tombstone fixture timestamp: %v", err)
		}
		eligible := deleted.Add(fastPolicy().TombstoneRetention)
		if retain != nil {
			eligible = *retain
		}
		waitFixtureDeadline(t, eligible)
	}
}

func (e *env) waitStagedAge(t *testing.T, ids ...string) {
	t.Helper()
	for _, id := range ids {
		var created time.Time
		if err := e.store.pool.QueryRow(context.Background(), `
SELECT created_at FROM ahp_artifacts.artifacts
 WHERE tenant=$1 AND id=$2 AND NOT deleted AND version IS NULL`, e.scope.Tenant, id).Scan(&created); err != nil {
			t.Fatalf("read staged fixture timestamp: %v", err)
		}
		waitFixtureDeadline(t, created.Add(fastPolicy().StagedGrace))
	}
}

func (e *env) waitObjectGrace(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var latest time.Time
	for obj := range e.client.ListObjects(ctx, e.bucket, minio.ListObjectsOptions{
		Prefix: e.scope.Tenant + "/", Recursive: true, WithVersions: true,
	}) {
		if obj.Err != nil {
			t.Fatalf("read object fixture timestamp: %v", obj.Err)
		}
		if obj.LastModified.After(latest) {
			latest = obj.LastModified
		}
	}
	if latest.IsZero() {
		t.Fatal("object age fixture has no actual versions")
	}
	waitFixtureDeadline(t, latest.Add(fastPolicy().OrphanGrace))
}
