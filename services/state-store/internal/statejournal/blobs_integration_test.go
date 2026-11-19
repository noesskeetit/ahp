package statejournal

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func blobTestOwner(t *testing.T, pool, reader *pgxpool.Pool, bucket string) (*artifacts.Store, *minio.Client) {
	t.Helper()
	endpoint := os.Getenv("AHP_JOURNAL_TEST_S3")
	if endpoint == "" {
		t.Skip("requires the disposable local journal artifact store")
	}
	host, _, err := net.SplitHostPort(endpoint)
	if host != "127.0.0.1" || err != nil || !strings.HasPrefix(bucket, "ahp-journal-") {
		t.Fatal("use a dedicated local MinIO fixture")
	}
	s3, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(
		os.Getenv("AHP_JOURNAL_TEST_S3_ACCESS"), os.Getenv("AHP_JOURNAL_TEST_S3_SECRET"), ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	checks := New(reader)
	owner, err := artifacts.New(pool, s3, bucket, func(context.Context, contracts.Scope, string, string) error {
		return nil
	}, artifacts.WithReferenceKeeper(artifacts.ReferenceScanFunc(checks.ArtifactsReferenced)),
		artifacts.WithCheckpointSettlement(checks.BlobPublicationSettled))
	if err != nil {
		t.Fatal(err)
	}
	return owner, s3
}

func putArtifactFixture(t *testing.T, ctx context.Context, owner *artifacts.Store, scope contracts.Scope, session, task string, payload []byte, staged bool) artifacts.Upload {
	t.Helper()
	digest := sha256.Sum256(payload)
	upload, err := owner.BeginUploadWithBinding(ctx, scope, artifacts.Binding{ContextID: session, TaskID: task},
		"input-"+session, artifacts.UploadRequest{
			Name: "receipt.bin", MIME: "application/octet-stream",
			SHA256: hex.EncodeToString(digest[:]), Size: int64(len(payload)), JournalStage: staged,
		})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, upload.PresignedURL, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("construct artifact PUT: %T", err)
	}
	for key, value := range upload.Headers {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("artifact PUT: %T", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode/100 != 2 {
		t.Fatalf("artifact PUT returned HTTP %d", response.StatusCode)
	}
	return upload
}

func TestJournalArtifactOwnershipAndGCPostgresMinIO(t *testing.T) {
	if os.Getenv("AHP_JOURNAL_TEST_S3") == "" {
		t.Skip("requires the disposable local journal artifact store")
	}
	ctx, pool, plain := journalTestStore(t)
	reader, err := pgxpool.NewWithConfig(ctx, pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "hermes", Subject: "user"}
	bucket := "ahp-journal-" + strings.ToLower(rand.Text()[:12])
	artifact, s3 := blobTestOwner(t, pool, reader, bucket)
	if err := artifact.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	store := NewWithArtifacts(pool, artifact)
	wantBytes := bytes.Repeat([]byte("x"), 307246)
	const session = "committed"
	lease, err := store.AcquireOrCreateSession(ctx, scope, session, "hermes", 1, "first", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	upload := putArtifactFixture(t, ctx, artifact, scope, session, lease.Worker, wantBytes, true)
	ref, err := artifact.Seal(ctx, scope, upload.ID)
	if err != nil {
		t.Fatal(err)
	}
	blob := &BlobRef{ArtifactID: ref.ID, Version: ref.Version, SHA256: ref.SHA256, SizeBytes: ref.Size}
	row := AppendRequest{Key: "large-receipt", Events: []EventInput{
		{ID: "result", Type: "tool.result", Role: "tool", Codec: "json", Payload: Payload{Blob: blob}},
	}}
	other, err := store.AcquireOrCreateSession(ctx, scope, "neighbor", "hermes", 1, "neighbor", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendEvents(ctx, scope, "neighbor", other, false, row); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("reference crossed a session owner: %v", err)
	}
	if _, err := plain.AppendEvents(ctx, scope, session, lease, false, row); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("reference was accepted without Artifacts owner: %v", err)
	}
	changed := *blob
	changed.Version = "different-version"
	row.Events[0].Payload.Blob = &changed
	if _, err := store.AppendEvents(ctx, scope, session, lease, false, row); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("mismatched artifact version was accepted: %v", err)
	}
	row.Events[0].Payload.Blob = blob
	if _, err := store.AppendEvents(ctx, scope, session, lease, false, row); err != nil {
		t.Fatal(err)
	}
	page, err := store.GetEvents(ctx, scope, session, 0, 10)
	if err != nil || len(page.Events) != 1 || page.Events[0].Payload.Blob == nil || *page.Events[0].Payload.Blob != *blob {
		t.Fatalf("lost reference: %+v (%v)", page.Events, err)
	}

	if err := store.ReleaseLease(ctx, scope, session, lease); err != nil {
		t.Fatal(err)
	}
	normal := putArtifactFixture(t, ctx, artifact, scope, "ordinary", "ordinary-task", wantBytes, false)
	if _, err := artifact.Seal(ctx, scope, normal.ID); err != nil {
		t.Fatal(err)
	}
	orphans := []artifacts.Upload{
		killAfterArtifactPut(t, ctx, scope.Tenant, bucket, "after-put"),
		killAfterArtifactPut(t, ctx, scope.Tenant, bucket, "after-seal"),
	}
	for _, orphan := range orphans {
		count := 0
		for item := range s3.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: orphan.Object, Recursive: true, WithVersions: true}) {
			if item.Err != nil {
				t.Fatal(item.Err)
			}
			count++
		}
		if count != 1 {
			t.Fatalf("expected one version in staging, got %d", count)
		}
	}
	timer := time.NewTimer(2200 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	gc, err := artifacts.NewGC(artifact, artifacts.WithLiveness(artifacts.LivenessFunc(New(reader).ArtifactLiveness)))
	if err != nil {
		t.Fatal(err)
	}
	report, err := gc.RunOnce(ctx, scope, artifacts.GCPolicy{StagedGrace: 2 * time.Second})
	if err != nil || report.CollectedStaged != 2 || report.Errors != 0 {
		t.Fatalf("orphan sweep: %+v (%v)", report, err)
	}
	for _, orphan := range orphans {
		for item := range s3.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: orphan.Object, Recursive: true, WithVersions: true}) {
			if item.Err != nil {
				t.Fatal(item.Err)
			}
			t.Fatalf("staged object version survived GC: %s", item.VersionID)
		}
	}
	if _, err := artifact.Get(artifacts.WithCallerContext(ctx, "ordinary"), scope, normal.ID); err != nil {
		t.Fatalf("collector changed an ordinary sealed artifact: %v", err)
	}
	readerBody, observed, err := artifact.Open(artifacts.WithCallerContext(ctx, session), scope, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer readerBody.Close()
	gotBytes, err := io.ReadAll(readerBody)
	if err != nil || observed.Version != ref.Version || !bytes.Equal(gotBytes, wantBytes) {
		t.Fatalf("neighbor lost committed blob: version=%s size=%d (%v)", observed.Version, len(gotBytes), err)
	}
	indexed, err := store.ArtifactsReferenced(ctx, scope.Tenant, []artifacts.ArtifactQuery{
		{ID: ref.ID}, {ID: orphans[0].ID}, {ID: orphans[1].ID},
	})
	if err != nil || !indexed[ref.ID] || indexed[orphans[0].ID] || indexed[orphans[1].ID] {
		t.Fatalf("unexpected live reference materialization: %+v (%v)", indexed, err)
	}
	t.Logf("committed object %d bytes retained; SIGKILL after PUT and after Seal each left one version before GC and zero after; staged collected %d", len(gotBytes), report.CollectedStaged)
}

func killAfterArtifactPut(t *testing.T, ctx context.Context, tenant, bucket, phase string) artifacts.Upload {
	t.Helper()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestJournalStageObjectChild$")
	command.Env = append(os.Environ(), "AHP_JOURNAL_STAGE_CRASH_TENANT="+tenant,
		"AHP_JOURNAL_STAGE_CRASH_BUCKET="+bucket, "AHP_JOURNAL_STAGE_CRASH_PHASE="+phase)
	command.Stderr = os.Stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	scanner := bufio.NewScanner(stdout)
	var uploaded artifacts.Upload
	if !scanner.Scan() || json.Unmarshal(scanner.Bytes(), &uploaded) != nil || uploaded.ID == "" || uploaded.Object == "" {
		t.Fatalf("worker did not record its staged object: %q (%v)", scanner.Text(), scanner.Err())
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil || command.ProcessState.Success() {
		t.Fatalf("worker did not stop at SIGKILL: %v", err)
	}
	return uploaded
}

func TestJournalStageObjectChild(t *testing.T) {
	tenant, bucket := os.Getenv("AHP_JOURNAL_STAGE_CRASH_TENANT"), os.Getenv("AHP_JOURNAL_STAGE_CRASH_BUCKET")
	phase := os.Getenv("AHP_JOURNAL_STAGE_CRASH_PHASE")
	if tenant == "" || bucket == "" {
		t.Skip("helper for staged upload process death")
	}
	ctx, pool, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: tenant, Deployment: "hermes", Subject: "user"}
	if phase != "after-put" && phase != "after-seal" {
		t.Fatal("missing crash phase")
	}
	session := "abandoned-" + phase
	lease, err := store.AcquireOrCreateSession(ctx, scope, session, "hermes", 1, "crashed-worker", 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	artifact, _ := blobTestOwner(t, pool, pool, bucket)
	upload := putArtifactFixture(t, ctx, artifact, scope, session, lease.Worker, bytes.Repeat([]byte("z"), 307246), true)
	if phase == "after-seal" {
		if _, err := artifact.Seal(ctx, scope, upload.ID); err != nil {
			t.Fatal(err)
		}
	}
	metadata, err := json.Marshal(artifacts.Upload{ID: upload.ID, Object: upload.Object})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(metadata))
	<-ctx.Done()
}
