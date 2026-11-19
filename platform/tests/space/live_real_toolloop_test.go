//go:build live_real

package space_test

// live_real_toolloop_test.go — the MANDATORY model-generated tool-execution proof.
//
// The real native-Qwen model, driven by the PRODUCTION core.Runtime.Run loop,
// must GENERATE a save_artifact tool_call that the loop EXECUTES against the
// AHP product tool over REAL MinIO, sealing a durable versioned artifact whose
// bytes round-trip (sha256 PUT == GET). This is NOT a manual seal leg: the model
// decides the tool call, the runtime executes it, and the model is then fed the
// tool result and must reach StepDone (full model->tool->model loop).
//
// No manual product-tool seal, no httptest, no newMockState, no :19081 stand-in
// on this path. Missing/unreachable native-Qwen or MinIO FAILS (fail-closed),
// never skips. See decisions/2026-10-30-emil-image-scope-correction.md (Emil is
// not a gate) and gap-doc docs/emil-workflow-state-gap.md (stateless engine; the
// tool loop is core-owned and must be proven with a real model-generated call).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestLiveRealModelGeneratedToolExecution proves Runtime.Run drives a real,
// model-generated save_artifact tool execution against real MinIO with durable
// round-trip. Fail-closed across bounded retries: if the real model never calls
// the tool, the test FAILS (it does not skip).
func TestLiveRealModelGeneratedToolExecution(t *testing.T) {
	l := ljRequire(t) // fail-closed: real PG DSN + native-Qwen gate + MinIO health
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	qwen := ljRealQwen(t, l) // constructs the real native-Qwen client (no fallback)

	const attempts = 4
	var lastDiag string
	for attempt := 0; attempt < attempts; attempt++ {
		sfx := ljRand(t, 6)
		bucket := "ahp-space-loop-" + sfx
		scope := contracts.Scope{Tenant: "t-" + sfx, Deployment: "d-" + sfx, Subject: ljSubject}
		sb := ljBootStore(t, ctx, l, bucket, scope, true) // real PG + real MinIO store
		nonce := ljRand(t, 8)
		wantContent := "AHP-LIVE-MODEL-TOOLLOOP " + nonce + " hash functions are collision-resistant and preimage-hard."

		saTool := core.NewSaveArtifactTool(&ljStoreSink{b: sb})
		mx := core.NewMultiExecutor()
		mx.Register(core.ToolSaveArtifact, saTool)
		reg := core.NewToolRegistry(mx)
		reg.Register(saTool.Def())
		dsink := space.NewMemDurabilitySink()
		rt := core.NewRuntime(core.RuntimeConfig{Model: qwen, Tools: reg, Durable: dsink})

		prompt := "You MUST call the save_artifact tool exactly once to save a file, then reply with the single word DONE. " +
			"Use these exact JSON arguments: " +
			`{"name":"live-loop-` + nonce + `.txt","mime":"text/plain","content":"` + wantContent + `"}. ` +
			"Do not answer in prose before the tool returns."

		results := make(chan space.StepResult, 128)
		runErr := rt.Run(ctx, &space.CoreRequest{
			TurnID: "loop-turn-" + ljRand(t, 4), Input: json.RawMessage(`"` + prompt + `"`),
			MaxSteps: 4, Scope: scope,
		}, results)
		if runErr != nil && ctx.Err() != nil {
			t.Fatalf("real model/tool loop context error: %v", ctx.Err())
		}

		var (
			handle      core.ArtifactHandle
			sawToolCall bool
			sawDone     bool
			sawErr      string
			modelSteps  int
		)
	drain:
		for {
			select {
			case s, ok := <-results:
				if !ok {
					break drain
				}
				switch s.StepType {
				case space.StepModel:
					modelSteps++
				case space.StepDone:
					sawDone = true
				case space.StepError:
					sawErr = string(s.Content)
				case space.StepTool:
					if s.ToolCall != nil && s.ToolCall.Name == core.ToolSaveArtifact {
						sawToolCall = true
						var h core.ArtifactHandle
						if err := json.Unmarshal(s.Content, &h); err == nil && h.ID != "" {
							handle = h
						}
					}
				}
			default:
				break drain
			}
		}

		if !sawToolCall || handle.ID == "" {
			lastDiag = "no model-generated save_artifact call (modelSteps=" + itoa(modelSteps) +
				" runErr=" + errStr(runErr) + " stepErr=" + sawErr + ")"
			t.Logf("attempt %d: %s", attempt+1, lastDiag)
			continue
		}

		// The loop executed the model's tool call and sealed a REAL versioned
		// artifact. Prove the bytes round-trip from MinIO (sha256 PUT == GET).
		rc, ref, oerr := sb.store.Open(ctx, scope, handle.ID)
		if oerr != nil {
			t.Fatalf("GET sealed model-generated artifact id=%s: %v", handle.ID, oerr)
		}
		got, rerr := io.ReadAll(rc)
		rc.Close()
		if rerr != nil {
			t.Fatalf("read model-generated artifact: %v", rerr)
		}
		sum := sha256.Sum256(got)
		getSHA := hex.EncodeToString(sum[:])
		if getSHA != handle.SHA256 {
			t.Fatalf("sha256 PUT != GET on model-generated artifact (get=%s handle=%s)", getSHA, handle.SHA256)
		}
		if len(got) == 0 || handle.Size <= 0 {
			t.Fatalf("model-generated artifact trivial: bytes=%d size=%d", len(got), handle.Size)
		}
		// Tool semantic correctness for a LIVE (non-deterministic) model: the
		// sealed artifact must carry this run's unique model-generated payload
		// marker (the nonce embedded in the content the model was told to save),
		// proving the runtime persisted the MODEL-GENERATED bytes through the
		// tool over real MinIO -- not a canned fixture and not a truncation.
		// Byte-exact equality is deliberately NOT asserted: a real LLM does not
		// reproduce a payload byte-for-byte (pinning exact bytes would be a mock,
		// not a model-generated journey).
		nonceStr := nonce
		if !strings.Contains(string(got), nonceStr) || !strings.Contains(string(got), "AHP-LIVE-MODEL-TOOLLOOP") {
			t.Fatalf("model-generated artifact does not carry this run's model payload marker (nonce=%q): got %d bytes %q -- save_artifact did not persist the model-generated content", nonceStr, len(got), string(got))
		}
		if len(got) < 64 {
			t.Fatalf("model-generated artifact too small for a substantive seal (bytes=%d): artifact-volume gate (iv finding #2) not met", len(got))
		}
		if !sawDone {
			t.Fatalf("loop executed the tool but never reached StepDone (model did not consume the tool result)")
		}
		if dsink.Len() < 1 {
			t.Fatalf("durable sink empty after model-generated tool loop: durable emission not proven")
		}

		t.Logf("MODEL-GENERATED tool execution OK: attempt=%d bucket=%s id=%s version=%s sha=%s PUT==GET bytes=%d model_steps=%d durable=%d ref.version=%s",
			attempt+1, bucket, handle.ID, handle.Version, handle.SHA256[:16], len(got), modelSteps, dsink.Len(), ref.Version)
		return
	}
	t.Fatalf("native-Qwen never produced a model-generated save_artifact tool_call within %d attempts: %s (fail-closed, not skip)", attempts, lastDiag)
}

func errStr(e error) string {
	if e == nil {
		return "<nil>"
	}
	return e.Error()
}
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}
