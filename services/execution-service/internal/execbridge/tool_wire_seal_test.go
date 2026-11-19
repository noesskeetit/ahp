package execbridge

// tool_wire_seal_test.go -- the wiring gap that made the DEPLOYED face seal the
// model's CHAT REPLY instead of the file the model asked write_artifact to
// write: cmd/ahp/role_execution.go took the artifact plane, used it only as the
// export door and dropped plane.Tools, so every provider request left this face
// with no tool definitions at all. Two facts are proved here, both against real
// PostgreSQL and real object storage:
//
//  1. FromEnvWithArtifactsAndTools mounts the plane's registry, while
//     FromEnvWithArtifacts stays the old model-only mount (tools=nil) -- the
//     backward-compatible behaviour is unchanged, not reinterpreted.
//  2. With the registry mounted, the artifact a COMPLETED task carries IS the
//     model's file: its bytes are the write_artifact arguments (marker included,
//     byte for byte), exactly one artifact rides the task, the plane's own result
//     export adds no second object, steps_consumed >= 1, close_reason is
//     model_finished, and the turn is still a durable ahp_model.generations row
//     billed to the task's own budget root a2a-task:<id>. Without the registry
//     the fallback export still seals the answer text, so the old mode is intact.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/minio/minio-go/v7"
)

// sealScriptInference replays provider turns and RECORDS each request, so the
// test can tell "definitions went out" from "they never did" -- which is exactly
// the difference between the fixed and the broken mount.
type sealScriptInference struct {
	mu       sync.Mutex
	calls    int
	reqs     []model.ChatRequest
	texts    []string
	withTool bool
	marker   string
}

func (s *sealScriptInference) Model() string { return "scripted-seal-model" }

func (s *sealScriptInference) Stream(_ context.Context, req model.ChatRequest) (model.Stream, error) {
	s.mu.Lock()
	i := s.calls
	s.calls++
	s.reqs = append(s.reqs, req)
	s.mu.Unlock()
	if i == 0 && s.withTool {
		if len(req.Tools) == 0 {
			// The defect, reproduced honestly: with no definitions on the wire the
			// model cannot ask for a file, so it answers in prose.
			return &scriptedDeltaStream{payloads: []model.Delta{{Text: "ok", FinishReason: "stop"}}}, nil
		}
		args, err := json.Marshal(map[string]string{"name": "tool-proof.txt", "mime": "text/plain", "content": s.marker})
		if err != nil {
			return nil, err
		}
		return &scriptedDeltaStream{payloads: []model.Delta{{FinishReason: "tool_calls",
			ToolCalls: []model.ToolCall{{ID: "call_seal_1", Name: "write_artifact", Arguments: args}}}}}, nil
	}
	idx := i
	if idx >= len(s.texts) {
		idx = len(s.texts) - 1
	}
	return &scriptedDeltaStream{payloads: []model.Delta{{Text: s.texts[idx], FinishReason: "stop"}}}, nil
}

func TestFromEnvMountsToolRegistryOnlyWhenGivenOne(t *testing.T) {
	// Deterministic provider state: no Vault address means the bridge mounts on
	// UnconfiguredInference, which is the honest no-provider mount and costs
	// nothing. What this test measures is the TOOL mount, not the provider.
	t.Setenv("AHP_VAULT_ADDR", "")
	pool := dbPool(t)
	client, bucket := minioFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	plane, err := executor.NewArtifactPlane(ctx, pool, executor.ArtifactPlaneConfig{
		S3Endpoint:  os.Getenv("AHP_S3_ENDPOINT"),
		S3AccessKey: os.Getenv("AHP_MINIO_ROOT_USER"), S3SecretKey: os.Getenv("AHP_MINIO_ROOT_PASSWORD"),
		Bucket: bucket, ParserCommand: artifacts.ParserCommand{Path: trustedArtifactParser(t)},
	})
	if err != nil {
		t.Fatalf("artifact plane: %v", err)
	}
	t.Cleanup(func() {
		c, cn := context.WithTimeout(context.Background(), 20*time.Second)
		defer cn()
		for o := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			_ = client.RemoveObject(c, bucket, o.Key, minio.RemoveObjectOptions{VersionID: o.VersionID})
		}
		_ = client.RemoveBucket(c, bucket)
	})
	old, err := FromEnvWithArtifacts(ctx, pool, plane)
	if err != nil {
		t.Fatalf("FromEnvWithArtifacts: %v", err)
	}
	if old.ToolsMounted() {
		t.Fatal("FromEnvWithArtifacts must stay the model-only mount (backward compatibility)")
	}
	fixed, err := FromEnvWithArtifactsAndTools(ctx, pool, plane, plane.Tools)
	if err != nil {
		t.Fatalf("FromEnvWithArtifactsAndTools: %v", err)
	}
	if !fixed.ToolsMounted() {
		t.Fatal("FromEnvWithArtifactsAndTools must mount the plane's tool registry: without it the " +
			"model on this face can never call write_artifact")
	}
}

func TestBridgedToolSealIsTheTaskArtifact(t *testing.T) {
	// The prerequisite belongs to the whole test, so the skip belongs to the whole
	// test too. Skipping only inside the subtests left a parent reporting PASS while
	// nothing inside it ran, which is a counter-reading trap: a run that greps
	// top-level lines would score two unproven legs as passed.
	if reason := artifactPlaneSkipReason(); reason != "" {
		t.Log(reason)
		t.Skip(reason)
	}
	for _, mode := range []struct {
		name      string
		withTools bool
	}{
		{"with_tool_registry", true},
		{"without_tool_registry", false},
	} {
		t.Run(mode.name, func(t *testing.T) {
			pool := dbPool(t)
			client, bucket := minioFromEnv(t)
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			plane, err := executor.NewArtifactPlane(ctx, pool, executor.ArtifactPlaneConfig{
				S3Endpoint:  os.Getenv("AHP_S3_ENDPOINT"),
				S3AccessKey: os.Getenv("AHP_MINIO_ROOT_USER"), S3SecretKey: os.Getenv("AHP_MINIO_ROOT_PASSWORD"),
				Bucket: bucket, ParserCommand: artifacts.ParserCommand{Path: trustedArtifactParser(t)},
			})
			if err != nil {
				t.Fatalf("artifact plane: %v", err)
			}
			t.Cleanup(func() {
				c, cn := context.WithTimeout(context.Background(), 20*time.Second)
				defer cn()
				for o := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
					_ = client.RemoveObject(c, bucket, o.Key, minio.RemoveObjectOptions{VersionID: o.VersionID})
				}
				_ = client.RemoveBucket(c, bucket)
			})

			var rnd [6]byte
			_, _ = rand.Read(rnd[:])
			marker := "AHP-TOOLWIRE-" + hex.EncodeToString(rnd[:])
			answer := "seal-script-answer-" + hex.EncodeToString(rnd[:])
			inf := &sealScriptInference{texts: []string{answer, answer}, withTool: mode.withTools, marker: marker}
			var tools *core.ToolRegistry
			if mode.withTools {
				tools = plane.Tools
			}
			b, err := New(ctx, pool, Config{
				Inference: inf, MaxSteps: 4, TaskTokenLimit: 20000, StepReserve: 4000,
				Artifacts: plane, Tools: tools,
			})
			if err != nil {
				t.Fatalf("new bridge: %v", err)
			}
			scope := contracts.Scope{Tenant: "t-sealwire", Deployment: "d1", Subject: "svc"}
			var tid [6]byte
			_, _ = rand.Read(tid[:])
			taskID := "sealwire-" + hex.EncodeToString(tid[:])
			res, err := b.Execute(ctx, gateway.ExecInput{
				Scope: scope,
				Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
				Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("запиши маркер в файл")),
			})
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if res.State != a2a.TaskStateCompleted {
				t.Fatalf("state = %s (%s), want COMPLETED", res.State, messageText(res.Message))
			}
			// NEW ASSERT (the defect's signature): exactly ONE artifact rides the
			// task -- the model's file when it wrote one, the plane's export
			// otherwise. Never both, never zero.
			if len(res.Artifacts) != 1 {
				t.Fatalf("artifacts = %d, want exactly 1 (one artifact per task)", len(res.Artifacts))
			}
			art := res.Artifacts[0]
			// Read the sealed content. Where the sandboxed parser exists the
			// authorized face must serve it; where none can exist (a non-Linux
			// host cannot install the seccomp filter) the leg asserts the GATE --
			// unextracted text is refused -- and still proves WHAT was sealed by
			// reading the object out of band, so the claim about content survives
			// the platform boundary without weakening the gate.
			rc, ref, openErr := plane.Store.Open(ctx, scope, string(art.ID))
			var data []byte
			if openErr == nil {
				data, err = io.ReadAll(rc)
				rc.Close()
				if err != nil {
					t.Fatalf("read artifact bytes: %v", err)
				}
			} else {
				if trustedArtifactParser(t) != "" {
					t.Fatalf("artifact %s not downloadable with a trusted parser mounted: %v", art.ID, openErr)
				}
				if !strings.Contains(openErr.Error(), "quarantined or awaiting extraction") {
					t.Fatalf("artifact %s refused for the wrong reason: %v", art.ID, openErr)
				}
				t.Logf("extraction gate holds on this host (no Linux sandbox available): %v", openErr)
				var object string
				if err := pool.QueryRow(ctx, `SELECT object FROM ahp_artifacts.artifacts WHERE id=$1`, string(art.ID)).Scan(&object); err != nil {
					t.Fatalf("read the sealed object path: %v", err)
				}
				obj, err := client.GetObject(ctx, bucket, object, minio.GetObjectOptions{})
				if err != nil {
					t.Fatalf("open the sealed object: %v", err)
				}
				data, err = io.ReadAll(obj)
				obj.Close()
				if err != nil {
					t.Fatalf("read the sealed object: %v", err)
				}
				// The seal the store vouches for, read through the metadata face
				// (not content-gated), and the digest recomputed from the bytes.
				refs, lerr := plane.Store.ListByContext(ctx, scope, "ctx-"+taskID)
				if lerr != nil {
					t.Fatalf("list the sealed object: %v", lerr)
				}
				if len(refs) != 1 {
					t.Fatalf("objects bound to the task context = %d, want exactly 1 to read out of band", len(refs))
				}
				ref = refs[0]
				sum := sha256.Sum256(data)
				if hex.EncodeToString(sum[:]) != ref.SHA256 {
					t.Fatalf("the sealed object's bytes do not match the digest the store vouches for: sha256=%s ref=%s",
						hex.EncodeToString(sum[:]), ref.SHA256)
				}
			}
			want := answer
			if mode.withTools {
				// NEW ASSERT: the bytes ARE the write_artifact arguments, not the
				// reply prose. This is what the deployed face could not do.
				want = marker
				if art.Name != "tool-proof.txt" {
					t.Fatalf("artifact name = %q, want the model's own file name tool-proof.txt", art.Name)
				}
			}
			origin := "exported answer"
			if mode.withTools {
				origin = "tool argument content"
			}
			if string(data) != want {
				t.Fatalf("sealed bytes = %q, want the %s %q", data, origin, want)
			}
			if ref.SHA256 == "" {
				t.Fatal("store returned no verified seal")
			}
			// NEW ASSERT: no second object sneaked in through the fallback export.
			listed, err := plane.Store.ListByContext(ctx, scope, "ctx-"+taskID)
			if err != nil {
				t.Fatalf("list by context: %v", err)
			}
			if len(listed) != 1 {
				t.Fatalf("objects bound to the task context = %d, want exactly 1 (%v)", len(listed), names(listed))
			}
			if res.Stats.StepsConsumed < 1 {
				t.Fatalf("steps_consumed = %d, want >= 1", res.Stats.StepsConsumed)
			}
			if res.Stats.CloseReason != gateway.CloseReasonModelFinished {
				t.Fatalf("close_reason = %q, want %q", res.Stats.CloseReason, gateway.CloseReasonModelFinished)
			}

			var genStatus string
			var usage int
			if err := pool.QueryRow(ctx, `SELECT status, usage_total FROM ahp_model.generations
				WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task=$4
				ORDER BY created_at DESC LIMIT 1`,
				scope.Tenant, scope.Deployment, scope.Subject, taskID).Scan(&genStatus, &usage); err != nil {
				t.Fatalf("no ahp_model.generations row for task %s: %v", taskID, err)
			}
			if genStatus == "FAILED" || usage <= 0 {
				t.Fatalf("generation status=%s usage=%d: a real gateway turn must settle with usage", genStatus, usage)
			}
			var spent int64
			if err := pool.QueryRow(ctx, `SELECT spent FROM ahp_model.budgets
				WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task=$4 AND key=$5`,
				scope.Tenant, scope.Deployment, scope.Subject, "a2a-task:"+taskID, RootBudgetKey).
				Scan(&spent); err != nil {
				t.Fatalf("no budget charge under a2a-task:%s: %v", taskID, err)
			}
			if spent <= 0 {
				t.Fatalf("budget root a2a-task:%s spent=%d, want a real charge", taskID, spent)
			}

			inf.mu.Lock()
			defer inf.mu.Unlock()
			defsSent := len(inf.reqs) > 0 && len(inf.reqs[0].Tools) > 0
			if defsSent != mode.withTools {
				t.Fatalf("tool definitions on the step-0 request = %v, want %v", defsSent, mode.withTools)
			}
		})
	}
}

func names(as []artifacts.ArtifactRef) []string {
	out := make([]string, 0, len(as))
	for _, a := range as {
		out = append(out, a.Name)
	}
	return out
}
