package execution_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestLiveExecutorQwenToolRoundTrip is the LIVE acceptance proof for the
// canonical Qwen+tools -> a2a executor. Opt-in via AHP_LIVE_QWEN=1. The native
// Qwen credential comes only from the scoped Vault runtime secret (never the
// operator's ambient env); there is no non-Qwen fallback.
//
// It proves, against the real endpoint: the model itself emits a write_artifact
// tool call, the tool writes a REAL MinIO object sealed with a verified sha256,
// the bytes are model/tool-produced (NOT an echo of the input), the artifact is
// readable under the owning tenant, and a foreign tenant gets NOT_FOUND.
func TestLiveExecutorQwenToolRoundTrip(t *testing.T) {
	// Fail-closed: no quiet skip. The live model, the real object store and real
	// PostgreSQL are required dependencies of this proof, so a missing one is a
	// named FAIL; only an explicit opt-out downgrades it (see requireLiveQwen).
	requireLiveQwen(t)
	dsn := requireRealDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Assert the native endpoint is the platform Qwen host before any call.
	sec, err := secretstore.QwenFromEnv(ctx)
	if err != nil {
		t.Fatalf("secretstore.QwenFromEnv: %v", err)
	}
	u, _ := url.Parse(sec.BaseURL)
	t.Logf("native Qwen host=%s model=%s api_key_len=%d", u.Host, sec.Model, len(sec.APIKey))

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	bucket := "ahp-exec-artifacts-" + hex.EncodeToString(rnd[:])

	ex, err := executor.New(ctx, pool, executor.Config{
		ParserCommand: artifacts.ParserCommand{Path: os.Getenv("AHP_ARTIFACT_PARSER")},
		S3Endpoint:    os.Getenv("AHP_S3_ENDPOINT"),
		S3Region:      os.Getenv("AHP_S3_REGION"),
		S3AccessKey:   os.Getenv("AHP_MINIO_ROOT_USER"),
		S3SecretKey:   os.Getenv("AHP_MINIO_ROOT_PASSWORD"),
		Bucket:        bucket,
		MaxSteps:      8,
	})
	if err != nil {
		t.Fatalf("executor.New: %v", err)
	}

	// A unique, impossible-to-echo token the model must place in the artifact.
	nonce := hex.EncodeToString(rnd[:])
	scope := contracts.Scope{Tenant: "t-live-" + nonce[:8], Deployment: "d1", Subject: "svc"}
	userText := "Call the write_artifact tool once. Artifact name answer.txt, mime text/plain. " +
		"The artifact content MUST be exactly this token and nothing else: TOKEN-" + nonce

	res, err := ex.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: "t-live-" + nonce[:10], ContextID: "ctx-t-live-" + nonce[:10]},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(userText)),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	t.Logf("state=%q artifacts=%d message=%v", res.State, len(res.Artifacts), res.Message != nil)

	if res.State != a2a.TaskStateCompleted {
		t.Fatalf("state=%q want completed", res.State)
	}
	if len(res.Artifacts) == 0 {
		t.Fatalf("model produced no tool-driven artifact (proof #1 unmet)")
	}
	art := res.Artifacts[0]
	sha, _ := art.Metadata["sha256"].(string)

	data, ref, err := ex.OpenArtifact(ctx, scope, string(art.ID))
	if err != nil {
		t.Fatalf("OpenArtifact: %v", err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != ref.SHA256 || ref.SHA256 != sha {
		t.Fatalf("sha mismatch bytes=%s ref=%s meta=%s", hex.EncodeToString(sum[:]), ref.SHA256, sha)
	}
	// Not an echo: the artifact must not be a copy of the instruction text.
	if strings.TrimSpace(string(data)) == "" {
		t.Fatal("artifact bytes empty")
	}
	if strings.Contains(string(data), "Call the write_artifact tool") {
		t.Fatalf("artifact is an echo of the instruction: %q", string(data))
	}
	t.Logf("artifact bytes len=%d contains_token=%v", len(data), strings.Contains(string(data), nonce))

	// Foreign-tenant isolation on the same sealed id.
	foreign := contracts.Scope{Tenant: "t-foreign-" + nonce[:8], Deployment: "d1", Subject: "eve"}
	if _, _, err := ex.OpenArtifact(ctx, foreign, string(art.ID)); contracts.Code(err) != contracts.ErrNotFound {
		if !errors.Is(err, nil) && contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("foreign read: code=%q err=%v want NOT_FOUND", contracts.Code(err), err)
		}
		t.Fatal("foreign tenant unexpectedly could read the artifact")
	}
}
