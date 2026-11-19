package execution_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// compModel is the deterministic client-conformance model used to prove the
// gateway -> executor -> durable ledger -> artifact path end to end. The LIVE
// model leg is proven separately in executor_live_test.go against real Qwen.
type compModel struct{ body string }

func (m compModel) Generate(_ context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	if req.Step == 0 {
		args, _ := json.Marshal(map[string]string{"name": "answer.txt", "mime": "text/plain", "content": m.body})
		return &core.ModelResponse{Content: json.RawMessage(`""`),
			ToolCalls: []core.ToolCall{{ID: "c1", Name: "write_artifact", Args: args}}}, nil
	}
	return &core.ModelResponse{Content: json.RawMessage(`"done"`)}, nil
}

func gwSetup(t *testing.T) (*pgxpool.Pool, *gateway.Store, *operations.Store, string) {
	t.Helper()
	// Fail-closed: this is the shared harness of the whole execution suite, so a
	// missing real PostgreSQL must stop the run with a reason instead of letting
	// the suite report green over a dependency that was never exercised.
	dsn := requireRealDB(t)
	// The setup budget covers DDL only (CREATE SCHEMA + this suite's tables).
	// 30s looked fine on an idle instance and produced false FAILs the moment the
	// shared PostgreSQL was busy with other lanes' -race runs: the migration was
	// still running, not broken. A real migration failure still fails with its
	// own SQL error; only the "we gave up too early" mode is removed.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var b [8]byte
	_, _ = rand.Read(b[:])
	sfx := hex.EncodeToString(b[:])
	store, err := gateway.NewStore(pool, "ahp_gw_"+sfx)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ledger, err := operations.New(pool, "ahp_gw_"+sfx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return pool, store, ledger, sfx
}

func newExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sfx string, model core.ModelClient) *executor.Executor {
	t.Helper()
	requireArtifactPlane(t)
	ex, err := executor.New(ctx, pool, executor.Config{
		ParserCommand: artifacts.ParserCommand{Path: os.Getenv("AHP_ARTIFACT_PARSER")},
		S3Endpoint:    os.Getenv("AHP_S3_ENDPOINT"),
		S3Region:      os.Getenv("AHP_S3_REGION"),
		S3AccessKey:   os.Getenv("AHP_MINIO_ROOT_USER"),
		S3SecretKey:   os.Getenv("AHP_MINIO_ROOT_PASSWORD"),
		Bucket:        "ahp-exec-artifacts-" + sfx,
		Model:         model,
	})
	if err != nil {
		t.Fatal(err)
	}
	// clean the scoped bucket on exit
	_ = url.URL{}
	return ex
}

func TestCompositionGatewayPublicFaceExecutor(t *testing.T) {
	pool, store, ledger, sfx := gwSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	body := "gateway-composed-payload-" + sfx[:8]
	ex := newExec(t, ctx, pool, sfx, compModel{body: body})

	scope := contracts.Scope{Tenant: "t-" + sfx[:12], Deployment: "dep-1", Subject: "alice"}
	foreign := contracts.Scope{Tenant: "t-foreign-" + sfx[:8], Deployment: "dep-1", Subject: "bob"}

	svc, err := gateway.NewService(store, ledger, ex, gateway.NullNotifier{}, gateway.Config{
		WorkerID: "w-" + sfx[:8], Lease: 5 * time.Second, Poll: 10 * time.Millisecond,
		MaxBlock: 3 * time.Second, MaxAttempts: 2, ParkLease: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Public face: accept a task (return immediately), then let the durable
	// worker drive it through the real executor.
	task, err := svc.SendMessage(ctx, scope, "caller-"+sfx[:6], &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("produce the artifact")),
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go svc.Run(runCtx)

	deadline := time.Now().Add(25 * time.Second)
	var row gateway.TaskRow
	for time.Now().Before(deadline) {
		row, err = store.Task(ctx, scope, string(task.ID))
		if err == nil && row.Terminal() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || !row.Terminal() {
		t.Fatalf("task not terminal: err=%v row=%+v", err, row)
	}
	if row.State != a2a.TaskStateCompleted {
		t.Fatalf("state=%q want completed", row.State)
	}
	if len(row.Artifacts) != 1 {
		t.Fatalf("artifacts=%d want 1", len(row.Artifacts))
	}
	aid := string(row.Artifacts[0].ID)

	// Durable artifact: read the sealed bytes back (PUT == GET) and confirm the
	// sha metadata the gateway journaled.
	data, ref, err := ex.OpenArtifact(ctx, scope, aid)
	if err != nil {
		t.Fatalf("OpenArtifact owner: %v", err)
	}
	if string(data) != body {
		t.Fatalf("artifact bytes=%q want %q", string(data), body)
	}
	if ref.SHA256 == "" {
		t.Fatal("sealed artifact missing sha256")
	}

	// Tenant isolation on the public face: a foreign tenant cannot see the task.
	if _, err := store.Task(ctx, foreign, string(task.ID)); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("foreign task read: err=%v want ErrTaskNotFound", err)
	}
	// ...nor the underlying artifact.
	if _, _, err := ex.OpenArtifact(ctx, foreign, aid); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign artifact read: code=%q err=%v want NOT_FOUND", contracts.Code(err), err)
	}

	// Restart the gateway: a fresh service over the same durable store keeps the
	// completed task and its artifact.
	stop()
	svc2, err := gateway.NewService(store, ledger, ex, gateway.NullNotifier{}, gateway.Config{
		WorkerID: "restarted-" + sfx[:8], Lease: 5 * time.Second, Poll: 10 * time.Millisecond, MaxBlock: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	row2, err := svc2.Store().Task(ctx, scope, string(task.ID))
	if err != nil {
		t.Fatalf("restart Task read: %v", err)
	}
	if row2.State != a2a.TaskStateCompleted {
		t.Fatalf("after restart state=%q want completed", row2.State)
	}
	if len(row2.Artifacts) != 1 || string(row2.Artifacts[0].ID) != aid {
		t.Fatalf("after restart artifacts not preserved: %+v", row2.Artifacts)
	}
}
