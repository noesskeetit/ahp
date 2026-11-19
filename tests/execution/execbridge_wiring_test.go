package execution_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// This test pins the composed-runtime wiring contract that the delivery role is
// accountable for. It reads source text on purpose: the guarantee it protects is
// structural (WHICH executor is mounted into the composed cmd/ahp binary, and
// with which credential), not behavioural, and it must hold in a build that
// cannot run the plane.
//
// Gate G1 (whole-runtime compose): the execution plane mounted in the composed
// "all" role is the Model Gateway bridge (internal/execbridge) -- every model
// turn is a durable generation admitted against a per-task token budget -- and
// NOT the direct provider-key executor. The bridge holds no provider credential;
// the direct wiring exists only behind the explicit AHP_EXEC_MODEL_VIA_GATEWAY=0
// opt-out.

const (
	execRoleFile = "../../cmd/ahp/role_execution.go"
	dataRoleFile = "../../cmd/ahp/role_data.go"
)

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// requireOrder asserts every needle occurs exactly want times and appears in the
// given relative order, so the assertions cannot be satisfied by an unrelated
// mention elsewhere in the file.
func requireOrder(t *testing.T, src string, want int, needles ...string) {
	t.Helper()
	prev := -1
	for _, n := range needles {
		got := strings.Count(src, n)
		if got != want {
			t.Fatalf("occurrences of %q = %d, want %d", n, got, want)
		}
		idx := strings.Index(src, n)
		if idx <= prev {
			t.Fatalf("%q must come after the previous needle (index %d <= %d)", n, idx, prev)
		}
		prev = idx
	}
}

func TestComposedExecutionRoleMountsGatewayBridge(t *testing.T) {
	src := readFile(t, execRoleFile)

	// The real mount is build-tag gated; the untagged build keeps the stub.
	if first := strings.SplitN(src, "\n", 2)[0]; first != "//go:build ahp_exec" {
		t.Fatalf("role_execution.go build tag = %q, want %q", first, "//go:build ahp_exec")
	}
	stub := readFile(t, "../../cmd/ahp/role_execution_stub.go")
	if first := strings.SplitN(stub, "\n", 2)[0]; first != "//go:build !ahp_exec" {
		t.Fatalf("stub build tag = %q, want %q", first, "//go:build !ahp_exec")
	}
	if !strings.Contains(stub, "errFeatureNotBuilt") {
		t.Fatal("stub must fail closed with errFeatureNotBuilt, not a fake plane")
	}

	// Gateway + task budgets, in the order the wiring executes them: the bridge
	// is constructed, injected into gateway.NewService and then driven by the
	// durable worker. A missing StartWorker strands tasks at SUBMITTED (iv-119).
	// The constructor is the ARTIFACT-AWARE, TOOL-AWARE one: a bridge built
	// without the exporter completes A2A tasks with artifacts=[], which PDF
	// rev3.2 forbids ("результат команды экспортируется в Artifacts"), and a
	// bridge built without the plane's tool registry never lets the model write
	// the file it was asked for -- the deployed face then seals the chat reply
	// instead of the artifact. Naming both mount points here is what keeps either
	// door from being dropped silently again.
	requireOrder(t, src, 1,
		"execbridge.FromEnvWithArtifactsAndTools(",
		"gateway.NewService(gstore, ledger, exec",
		"executor.StartWorker(ctx, svc)",
	)
	// The tool registry the artifact plane builds must actually reach the bridge.
	// Reading the plane and discarding plane.Tools was the exact defect.
	if !strings.Contains(src, "tools = plane.Tools") {
		t.Fatal(`role_execution.go must hand plane.Tools to the bridge: without it the model on ` +
			"this face can never call write_artifact and the artifact is only ever the exported reply text")
	}
	// The mount is stated in the startup log, so the log itself is the evidence.
	if !strings.Contains(src, `"tool_wire", bridge.ToolsMounted()`) {
		t.Fatal(`missing log field "tool_wire": a face running model-only must be visible in the log`)
	}
	// The bridge is logged as gateway-routed, so the composed startup log is
	// itself evidence of the wiring.
	if !strings.Contains(src, `"via_gateway", true`) {
		t.Fatal(`missing log field "via_gateway", true on the bridge branch`)
	}
	// The credential-less plain constructor must not be the composed mount:
	// "execbridge.FromEnv(" is not a substring of the artifact-aware call, so a
	// regression that drops the exporter fails here with a reason.
	if strings.Contains(src, "execbridge.FromEnv(") {
		t.Fatal(`role_execution.go mounts execbridge.FromEnv( without an ArtifactExporter: ` +
			"A2A tasks would finish TASK_STATE_COMPLETED with artifacts=[]")
	}

	// The direct provider-key executor exists only as the explicit opt-out and
	// is unreachable by default: the opt-out test must precede it, and the
	// default (else) branch must construct the bridge.
	// A globally counted "} else {" stopped being an invariant the moment the
	// default branch grew a nested else, so the claim is scoped to the branches
	// instead: the direct constructor must sit inside the opt-out block, the
	// bridge in the default block, and the provider-key executor must have
	// exactly one construction site.
	optOut := strings.Index(src, `envOr("AHP_EXEC_MODEL_VIA_GATEWAY", "1") == "0"`)
	if optOut < 0 {
		t.Fatal(`missing the AHP_EXEC_MODEL_VIA_GATEWAY == "0" opt-out test: the direct executor must be opt-in`)
	}
	optOutElse := optOut + strings.Index(src[optOut:], "} else {")
	if optOutElse <= optOut {
		t.Fatal("the opt-out test must have an else branch carrying the default (gateway) wiring")
	}
	direct := strings.Index(src, "executor.NewFromEnv")
	if direct <= optOut || direct >= optOutElse {
		t.Fatalf("executor.NewFromEnv must be constructed inside the opt-out branch (opt-out at %d, else at %d, direct at %d)", optOut, optOutElse, direct)
	}
	if second := strings.Index(src[direct+len("executor.NewFromEnv"):], "executor.NewFromEnv"); second >= 0 {
		t.Fatal("the direct provider executor must have exactly one construction site, the opt-out branch")
	}
	if bridge := strings.Index(src, "execbridge.FromEnvWithArtifactsAndTools("); bridge <= optOutElse {
		t.Fatal("the default (else) branch must construct the Model Gateway bridge, not a stand-in")
	}
	// No provider credential is reachable from this file at all: the key stays
	// behind the Model Gateway's own inference handle.
	for _, forbidden := range []string{"secretstore", "AHP_QWEN", "api_key", "ApiKey"} {
		if strings.Contains(src, forbidden) {
			t.Fatalf("role_execution.go must not reference %q: the execution plane holds no provider credential", forbidden)
		}
	}
}

// TestExecbridgeMountedOnlyInComposedBinary pins the production composition
// boundary. The composed server owns the bridge, Identity labels and budgets; the
// AS runner uses the remote Model Gateway client. Test consumers are excluded.
func TestExecbridgeMountedOnlyInComposedBinary(t *testing.T) {
	root := "../.."
	want := []string{"cmd/ahp/model_budget.go", "cmd/ahp/model_identity.go", "cmd/ahp/mount_model.go", "cmd/ahp/mount_model_files.go", "cmd/ahp/mount_model_files_stub.go", "cmd/ahp/role_execution.go", "cmd/ahp/runtime_budget.go", "cmd/as-runner/profile_runtime.go", "cmd/as-runner/wiring.go"}
	var hits []string
	scanned := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "node_modules", "web", "docs", "acceptance":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "internal/execbridge/") {
			return nil
		}
		// Mounting is a property of the SHIPPED binary. A _test.go file cannot put an
		// executor into cmd/ahp, and this lane's own live legs must construct the
		// bridge to measure it at all (tests/execution/live_bridge_artifact_test.go
		// drives the real bridged send that proves generation+budget+artifact).
		// Exempting only the gate's own file turned every new leg into a false
		// "the bridge is mounted somewhere else" RED -- measured on 2ba2232, where
		// the only extra hit was exactly that live leg.
		if strings.HasSuffix(rel, "_test.go") {
			return nil
		}
		scanned++
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		if strings.Contains(string(b), "execbridge.") ||
			strings.Contains(string(b), `"github.com/ahp-platform/ahp/internal/execbridge"`) {
			hits = append(hits, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	// Teeth for the scan itself: a misrooted or over-broad walk would otherwise
	// "pass" by finding only the one file it wants to find.
	if scanned < 100 {
		t.Fatalf("the production scan covered %d Go files, want >=100: the walk does not really cover the runtime tree",
			scanned)
	}
	if !slices.Equal(hits, want) {
		t.Fatalf("execbridge referenced outside its package by %v, want exactly %v (scanned %d production files)",
			hits, want, scanned)
	}
}

// TestComposedDataRoleMountsArtifactFace pins the data-plane half of the composed
// binary: an authenticated task-token face with the "artifacts" capability, an
// allowlist authorizer, and the pinned region that makes an offline-legal presign
// possible (so the data face mounts in a PG-only compose instead of being warn-
// skipped). Historical acceptance: IV127/128 @986ac0a -- this must not regress.
func TestComposedDataRoleMountsArtifactFace(t *testing.T) {
	src := readFile(t, dataRoleFile)
	if first := strings.SplitN(src, "\n", 2)[0]; first != "//go:build ahp_data" {
		t.Fatalf("role_data.go build tag = %q, want %q", first, "//go:build ahp_data")
	}
	for _, needle := range []string{
		"auth.KindTask",
		`"artifacts"`,
		`"upload": true`,
		`Region: envOr("AHP_S3_REGION", "")`,
	} {
		if !strings.Contains(src, needle) {
			t.Fatalf("role_data.go missing %q", needle)
		}
	}
	stub := readFile(t, "../../cmd/ahp/role_data_stub.go")
	if first := strings.SplitN(stub, "\n", 2)[0]; first != "//go:build !ahp_data" {
		t.Fatalf("data stub build tag = %q, want %q", first, "//go:build !ahp_data")
	}
}
