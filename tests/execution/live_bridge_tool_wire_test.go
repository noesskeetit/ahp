package execution_test

// live_bridge_tool_wire_test.go -- the LIVE proof of the OTHER door, the one the
// deployed composed front was missing: the execution role hands the artifact
// plane's tool registry to the bridge, so a real Qwen turn on the Model Gateway
// face receives the write_artifact definition, calls it, and the file the MODEL
// wrote is the artifact the task carries.
//
// Before the wiring fix this face could only ever seal its own answer text, which
// is what an independent journey run caught end-to-end: the request asked for a
// marker line and the downloaded bytes were the model's prose reply ("\n\nok").
// Opt-in with AHP_LIVE_QWEN=1; without it the test fails with a reason, never
// silently.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestLiveBridgedToolWireSealsModelArtifact(t *testing.T) {
	requireLiveQwen(t)
	if strings.TrimSpace(os.Getenv("AHP_S3_ENDPOINT")) == "" {
		t.Fatalf("AHP_S3_ENDPOINT is required: the tool effect has to land in real object storage")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 330*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	// Built the way the execution role builds it now: export door AND tool
	// registry, both resolved from the environment.
	plane, err := executor.ArtifactPlaneFromEnv(ctx, pool)
	if err != nil {
		t.Fatalf("artifact plane: %v", err)
	}
	bridge, err := execbridge.FromEnvWithArtifactsAndTools(ctx, pool, plane, plane.Tools)
	if err != nil {
		t.Fatalf("bridge: %v", err)
	}
	if !bridge.ProviderConfigured() {
		t.Fatal("AHP_LIVE_QWEN=1 but the bridge has no provider handle -- no real generation is possible")
	}
	if !bridge.ToolsMounted() {
		t.Fatal("the mount this test exists to prove is absent: no tool registry on the bridge")
	}

	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	nonce := hex.EncodeToString(rnd[:])
	marker := "AHP-TOOLWIRE-LIVE-" + nonce
	scope := contracts.Scope{Tenant: "t-livebrtw-" + nonce[:8], Deployment: "d-livebrtw", Subject: "a2a-agent"}
	taskID := "livebrtw-" + nonce

	prompt := "You must use a tool. Call write_artifact exactly once with name=\"live-marker.txt\", " +
		"mime=\"text/plain\" and content set EXACTLY to this single line (do not invent, do not add text):\n" +
		marker + "\nThen reply with the word ok."
	res, err := bridge.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(prompt)),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != a2a.TaskStateCompleted {
		t.Fatalf("state = %s (%s): the live turn on the tool-mounted face must complete",
			res.State, a2aTextOf(res.Message))
	}
	if len(res.Artifacts) == 0 {
		t.Fatal("artifacts=0: a tool-mounted face must carry the file the model wrote")
	}
	if res.Stats.StepsConsumed < 1 {
		t.Fatalf("steps_consumed = %d, want >= 1", res.Stats.StepsConsumed)
	}
	if res.Stats.CloseReason != gateway.CloseReasonModelFinished {
		t.Fatalf("close_reason = %q, want %q", res.Stats.CloseReason, gateway.CloseReasonModelFinished)
	}

	// The decisive fact: some artifact of this task carries the marker the MODEL
	// was told to write. Prose-only replies (the pre-fix behaviour) cannot satisfy
	// this, because the marker never appears in a reply the model was told to keep
	// to the single word ok.
	var sealed, sealedName string
	var sawMarker bool
	for _, art := range res.Artifacts {
		rc, ref, oerr := plane.Store.Open(ctx, scope, string(art.ID))
		if oerr != nil {
			t.Fatalf("artifact %s not downloadable: %v", art.ID, oerr)
		}
		data, rerr := io.ReadAll(rc)
		rc.Close()
		if rerr != nil {
			t.Fatalf("read artifact bytes: %v", rerr)
		}
		if !sawMarker && strings.Contains(string(data), marker) {
			sawMarker = true
			sealed, sealedName = string(data), art.Name
			if ref.SHA256 == "" {
				t.Fatal("store returned no verified seal for the model's file")
			}
		}
	}
	if !sawMarker {
		t.Fatalf("no artifact carries the model-written marker %q: this face sealed prose, not the tool effect "+
			"(artifacts=%d)", marker, len(res.Artifacts))
	}
	if strings.TrimSpace(sealed) != marker {
		t.Fatalf("sealed bytes = %q, want exactly the marker %q", sealed, marker)
	}

	// The turn is still a durable Model Gateway generation billed to the task's
	// own root -- mounting tools must not bypass admission or accounting.
	var genStatus string
	var usage int
	if err := pool.QueryRow(ctx, `SELECT status, usage_total FROM ahp_model.generations
		WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task=$4
		ORDER BY created_at DESC LIMIT 1`,
		scope.Tenant, scope.Deployment, scope.Subject, taskID).Scan(&genStatus, &usage); err != nil {
		t.Fatalf("no ahp_model.generations row for task %s: %v", taskID, err)
	}
	if genStatus == "FAILED" || usage <= 0 {
		t.Fatalf("generation status=%s usage=%d: a real provider turn must settle with usage", genStatus, usage)
	}
	var spent int64
	if err := pool.QueryRow(ctx, `SELECT spent FROM ahp_model.budgets
		WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task=$4 AND key=$5`,
		scope.Tenant, scope.Deployment, scope.Subject, "a2a-task:"+taskID, execbridge.RootBudgetKey).
		Scan(&spent); err != nil {
		t.Fatalf("no budget charge under a2a-task:%s: %v", taskID, err)
	}
	if spent <= 0 {
		t.Fatalf("budget root a2a-task:%s spent=%d, want a real charge", taskID, spent)
	}
	t.Logf("A2A_LIVE_TOOLWIRE task=%s artifact=%s bytes=%d model=%s steps=%d spent=%d",
		taskID, sealedName, len(sealed), bridge.Model(), res.Stats.StepsConsumed, spent)
}

func a2aTextOf(m *a2a.Message) string {
	if m == nil {
		return ""
	}
	var b strings.Builder
	for _, part := range m.Parts {
		b.WriteString(part.Text())
	}
	return b.String()
}
