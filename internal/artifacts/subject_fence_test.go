package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pauseUploadInsert struct{ entered, release chan struct{} }

func (p *pauseUploadInsert) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "INSERT INTO ahp_artifacts.artifacts") {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
		}
	}
	return ctx
}
func (*pauseUploadInsert) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestSubjectFenceDrainsUncommittedUploadRegistration(t *testing.T) {
	testFenceDrainsUncommittedUploadRegistration(t, false)
}
func TestTenantFenceDrainsUncommittedUploadRegistration(t *testing.T) {
	testFenceDrainsUncommittedUploadRegistration(t, true)
}
func testFenceDrainsUncommittedUploadRegistration(t *testing.T, tenant bool) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pause := &pauseUploadInsert{make(chan struct{}), make(chan struct{})}
	cfg := e.store.pool.Config()
	cfg.MaxConns = 1
	cfg.ConnConfig.Tracer = pause
	writerPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("writer pool: %T", err)
	}
	defer writerPool.Close()
	eraseCfg := e.store.pool.Config()
	eraseCfg.MaxConns = 1
	eraserPool, err := pgxpool.NewWithConfig(ctx, eraseCfg)
	if err != nil {
		t.Fatalf("eraser pool: %T", err)
	}
	defer eraserPool.Close()
	writer, err := New(writerPool, e.store.client, e.store.bucket, e.store.authorize)
	if err != nil {
		t.Fatal(err)
	}
	eraser, err := New(eraserPool, e.store.client, e.store.bucket, e.store.authorize)
	if err != nil {
		t.Fatal(err)
	}
	var pid int64
	if err = eraserPool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() {
		_, err := writer.BeginUpload(ctx, e.scope, "inflight", UploadRequest{Name: "inflight.bin", MIME: "application/octet-stream", SHA256: strings.Repeat("a", 64), Size: 4})
		written <- err
	}()
	<-pause.entered
	fenced := make(chan error, 1)
	go func() {
		if tenant {
			fenced <- eraser.FenceTenant(ctx, e.scope)
		} else {
			fenced <- eraser.FenceSubject(ctx, e.scope)
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	waiting := false
	for time.Now().Before(deadline) {
		select {
		case err := <-fenced:
			close(pause.release)
			<-written
			t.Fatalf("fence acknowledged before upload registration committed: %v", err)
		default:
		}
		if err = e.store.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, pid).Scan(&waiting); err != nil {
			close(pause.release)
			t.Fatal(err)
		}
		if waiting {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(pause.release)
	if !waiting {
		t.Fatal("eraser did not wait for upload registration")
	}
	if err = <-written; err != nil {
		t.Fatal(err)
	}
	if err = <-fenced; err != nil {
		t.Fatal(err)
	}
	scopes, err := e.store.SubjectScopes(ctx, e.scope)
	if err != nil || len(scopes) != 1 {
		t.Fatalf("post-fence discovery missed upload metadata: %v %v", scopes, err)
	}
	deleted, err := e.store.DeleteBySubject(ctx, e.scope)
	if err != nil || deleted.Tombstoned != 1 {
		t.Fatalf("drained upload not deleted: %+v %v", deleted, err)
	}
	late := e.scope
	late.Deployment = "new-deployment"
	_, err = e.store.BeginUpload(ctx, late, "late", UploadRequest{Name: "late.bin", MIME: "application/octet-stream", SHA256: strings.Repeat("a", 64), Size: 4})
	codeIs(t, err, contracts.ErrForbidden)
}

func TestSubjectFenceRejectsInflightSealOfHeldUpload(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	body := "held upload"
	sum := sha256.Sum256([]byte(body))
	up, err := e.store.BeginUpload(ctx, e.scope, "held-upload", UploadRequest{Name: "held.bin", MIME: "application/octet-stream", SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body))})
	if err != nil {
		t.Fatal(err)
	}
	put(t, up.PresignedURL, body)
	if err = e.store.SetLegalHold(ctx, e.scope, up.ID, true); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	original := e.store.authorize
	e.store.authorize = func(ctx context.Context, scope contracts.Scope, action, resource string) error {
		if err := original(ctx, scope, action, resource); err != nil {
			return err
		}
		if action == "seal" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	result := make(chan error, 1)
	go func() { _, err := e.store.Seal(ctx, e.scope, up.ID); result <- err }()
	<-entered
	if err = e.store.FenceSubject(ctx, e.scope); err != nil {
		close(release)
		t.Fatal(err)
	}
	res, err := e.store.DeleteBySubject(ctx, e.scope)
	if err != nil || res.Held != 1 {
		close(release)
		t.Fatalf("legal hold not retained: %+v %v", res, err)
	}
	close(release)
	codeIs(t, <-result, contracts.ErrForbidden)
	var version *string
	if err = e.store.pool.QueryRow(ctx, `SELECT version FROM ahp_artifacts.artifacts WHERE id=$1`, up.ID).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != nil {
		t.Fatal("late seal attached a version after subject fence")
	}
}

func TestSubjectFenceStopsDerivativePublicationPinsAndGrants(t *testing.T) {
	checks := map[string]func(context.Context, *env, ArtifactRef) error{
		"derivation": func(ctx context.Context, e *env, ref ArtifactRef) error {
			return e.store.MarkDerivation(ctx, e.scope, ref.ID, Derivation{Kind: "summary", Status: "ready", Detail: "late derived content"})
		},
		"publication": func(ctx context.Context, e *env, ref ArtifactRef) error {
			return e.store.PublishToWorkspace(ctx, e.scope, ref.ID, "workspace-a")
		},
		"pin": func(ctx context.Context, e *env, ref ArtifactRef) error {
			return e.store.PinRef(ctx, e.scope, "context", "late-context")
		},
		"grant": func(ctx context.Context, e *env, ref ArtifactRef) error {
			_, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID, GrantRequest{Version: ref.Version})
			return err
		},
	}
	for name, run := range checks {
		t.Run(name, func(t *testing.T) {
			e := setup(t)
			ctx := context.Background()
			ref, _ := e.uploadSeal(t, "existing", "subject material")
			if err := e.store.FenceSubject(ctx, e.scope); err != nil {
				t.Fatal(err)
			}
			codeIs(t, run(ctx, e, ref), contracts.ErrForbidden)
		})
	}
}
