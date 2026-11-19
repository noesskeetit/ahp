package statejournal

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestDirectS3JournalBlobRetention(t *testing.T) {
	if os.Getenv("AHP_JOURNAL_TEST_S3") == "" {
		t.Skip("requires disposable MinIO")
	}
	ctx, pool, plain := journalTestStore(t)
	bucket := "ahp-journal-" + strings.ToLower(rand.Text()[:12])
	_, s3 := blobTestOwner(t, pool, pool, bucket)
	if err := s3.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	store, err := NewWithS3(ctx, pool, s3, bucket)
	if err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "d", Subject: "owner"}
	lease, err := store.AcquireOrCreateSession(ctx, scope, "session", "custom", 1, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("A"), maxInlineBytes+19)
	ref, err := store.StageBlob(ctx, scope, "session", lease, data)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := store.StageBlob(ctx, scope, "session", lease, data)
	if err != nil || ref != repeated {
		t.Fatalf("immutable content upload changed receipt: %v", err)
	}
	input := AppendRequest{Key: "commit", Events: []EventInput{{
		ID: "event", Type: "output", Role: "assistant", Codec: "raw", Payload: Payload{Blob: &ref},
	}}}
	shared := input
	shared.KV = []KVMutation{{ScopeType: "user", Namespace: "test", Key: "blob", Payload: Payload{Blob: &ref}}}
	if _, err := store.AppendEvents(ctx, scope, "session", lease, true, shared); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("session blob accepted in shared KV: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO ahp_journal.scoped_kv
		    (tenant_id,deployment_id,scope_type,scope_owner,namespace,key,version,blob_ref)
		VALUES ($1,$2,'user',$3,'test','blob',1,'{}')`, scope.Tenant, scope.Deployment, scope.Subject)
	var dbErr *pgconn.PgError
	if !errors.As(err, &dbErr) || dbErr.ConstraintName != "scoped_kv_blob_session_only" {
		t.Fatalf("shared blob was not rejected by the ownership constraint: %v", err)
	}
	if _, err := store.AppendEvents(ctx, scope, "session", lease, false, input); err != nil {
		t.Fatal(err)
	}
	foreign := scope
	foreign.Subject = "different"
	if _, err := store.ReadBlob(ctx, foreign, "session", ref); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("other subject could inspect blob: %v", err)
	}
	orphan, err := store.StageBlob(ctx, scope, "session", lease, bytes.Repeat([]byte("B"), maxInlineBytes+19))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseLease(ctx, scope, "session", lease); err != nil {
		t.Fatal(err)
	}
	if err := store.SweepOrphanBlobs(ctx, scope.Tenant, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadBlob(ctx, scope, "session", orphan); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("orphan remains: %v", err)
	}
	coldStore, err := NewWithS3(ctx, pool, s3, bucket)
	if err != nil {
		t.Fatal(err)
	}
	page, err := plain.GetEvents(ctx, scope, "session", 0, 1)
	if err != nil || len(page.Events) != 1 || page.Events[0].Payload.Blob == nil {
		t.Fatalf("lost publication: %v", err)
	}
	loaded, err := coldStore.ReadBlob(ctx, scope, "session", *page.Events[0].Payload.Blob)
	if err != nil || !bytes.Equal(loaded, data) {
		t.Fatalf("committed blob changed: %s", contracts.Code(err))
	}
	if err := coldStore.SetLegalHold(ctx, scope, "session", true); err != nil {
		t.Fatal(err)
	}
	if err := coldStore.DeleteSession(ctx, scope, "session"); err != nil {
		t.Fatal(err)
	}
	if err := coldStore.SweepDeletedSessions(ctx, scope.Tenant, time.Now().Add(time.Minute), 64); err != nil {
		t.Fatal(err)
	}
	if _, err := s3.StatObject(ctx, bucket, s3BlobPrefix(scope.Tenant, "session")+ref.ArtifactID, minio.StatObjectOptions{}); err != nil {
		t.Fatalf("legal hold failed to retain blob: %v", err)
	}
	if err := coldStore.SetLegalHold(ctx, scope, "session", false); err != nil {
		t.Fatal(err)
	}
	if err := coldStore.SweepDeletedSessions(ctx, scope.Tenant, time.Now().Add(time.Minute), 64); err != nil {
		t.Fatal(err)
	}
	_, err = s3.StatObject(ctx, bucket, s3BlobPrefix(scope.Tenant, "session")+ref.ArtifactID, minio.StatObjectOptions{})
	if minio.ToErrorResponse(err).Code != "NoSuchKey" {
		t.Fatalf("deleted session still has blob: %v", err)
	}
	late := s3BlobPrefix(scope.Tenant, "session") + ref.ArtifactID
	if _, err := s3.PutObject(ctx, bucket, late, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := store.SweepOrphanBlobs(ctx, scope.Tenant, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s3.StatObject(ctx, bucket, late, minio.StatObjectOptions{}); minio.ToErrorResponse(err).Code != "NoSuchKey" {
		t.Fatalf("late stage leaked after session was swept: %v", err)
	}
}

type pausingUpload struct {
	base    http.RoundTripper
	entered chan struct{}
	release chan struct{}
}

func (t *pausingUpload) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodPut && strings.Contains(r.URL.Path, s3BlobRoot) {
		select {
		case t.entered <- struct{}{}:
		default:
		}
		select {
		case <-t.release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}
	return t.base.RoundTrip(r)
}

func TestStageLeavesLeaseAvailableAndRetainsReusedBlob(t *testing.T) {
	endpoint := os.Getenv("AHP_JOURNAL_TEST_S3")
	if endpoint == "" {
		t.Skip("requires disposable MinIO")
	}
	ctx, pool, _ := journalTestStore(t)
	bucket := "ahp-journal-" + strings.ToLower(rand.Text()[:12])
	_, normal := blobTestOwner(t, pool, pool, bucket)
	if err := normal.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	upload := &pausingUpload{base: http.DefaultTransport, entered: make(chan struct{}, 1), release: make(chan struct{})}
	defer func() {
		select {
		case <-upload.release:
		default:
			close(upload.release)
		}
	}()
	s3, err := minio.New(endpoint, &minio.Options{Transport: upload,
		Creds: credentials.NewStaticV4(os.Getenv("AHP_JOURNAL_TEST_S3_ACCESS"), os.Getenv("AHP_JOURNAL_TEST_S3_SECRET"), "")})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewWithS3(ctx, pool, s3, bucket)
	if err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "stage-" + rand.Text(), Deployment: "agent", Subject: "owner"}
	lease, err := store.AcquireOrCreateSession(ctx, scope, "session", "custom", 1, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		ref BlobRef
		err error
	}
	result := make(chan outcome, 1)
	data := bytes.Repeat([]byte("Z"), maxInlineBytes+19)
	go func(active Lease) {
		ref, err := store.StageBlob(ctx, scope, "session", active, data)
		result <- outcome{ref, err}
	}(lease)
	select {
	case <-upload.entered:
	case failed := <-result:
		t.Fatalf("upload didn't reach the paused PUT: %v", failed.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	heartbeat, cancel := context.WithTimeout(ctx, time.Second)
	lease, err = store.HeartbeatLease(heartbeat, scope, "session", lease, time.Minute)
	cancel()
	if err != nil {
		t.Fatalf("S3 upload held the session row: %v", err)
	}
	close(upload.release)
	created := <-result
	if created.err != nil {
		t.Fatal(created.err)
	}
	cutoff := time.Now()
	ref, err := store.StageBlob(ctx, scope, "session", lease, data)
	if err != nil || ref != created.ref {
		t.Fatalf("reuse changed published descriptor: %+v, %v", ref, err)
	}
	if err := store.ReleaseLease(ctx, scope, "session", lease); err != nil {
		t.Fatal(err)
	}
	if err := store.SweepOrphanBlobs(ctx, scope.Tenant, cutoff); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.ReadBlob(ctx, scope, "session", ref)
	if err != nil || !bytes.Equal(loaded, data) {
		t.Fatalf("new staging mark did not protect older object: %v", err)
	}
}
