package execbridge

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func testScope() contracts.Scope {
	return contracts.Scope{Tenant: "acme.tenant", Deployment: "dep-1", Subject: "user:alice"}
}

// The bridge must satisfy the gateway executor contract: that is what makes the
// composed cmd/ahp plane route model turns through the Model Gateway.
func TestBridgeIsGatewayExecutor(t *testing.T) {
	var exec gateway.Executor = (*Bridge)(nil)
	if exec == nil {
		t.Fatal("nil executor")
	}
}

func TestUnconfiguredInferenceIsDeterministicFailure(t *testing.T) {
	var inf model.Inference = UnconfiguredInference{}
	if inf.Model() != "unconfigured" {
		t.Fatalf("model = %q", inf.Model())
	}
	stream, err := inf.Stream(context.Background(), model.ChatRequest{Model: "unconfigured"})
	if stream != nil {
		t.Fatal("Stream must never fabricate a stream")
	}
	if err == nil {
		t.Fatal("Stream must fail without a provider runtime secret")
	}
	if !errors.Is(err, ErrProviderUnconfigured) {
		t.Fatalf("error must be ErrProviderUnconfigured, got %v", err)
	}
	// The whole no-provider evidence argument depends on this: a non-transport
	// error is classified by the Model Gateway as a committed failure, so the
	// generation reaches GenFailed and the budget reservation is released.
	if model.IsTransportError(err) {
		t.Fatal("no-provider failure must NOT be a transport error (it would park the task)")
	}
}

func TestNewFailsClosed(t *testing.T) {
	ctx := context.Background()
	if _, err := New(ctx, nil, Config{Inference: UnconfiguredInference{}}); err == nil {
		t.Fatal("nil pool must be rejected")
	}
	// pgxpool.New is lazy: no connection is attempted, so this exercises the
	// fail-closed inference guard without a database.
	pool, err := pgxpool.New(ctx, "postgres://nobody@127.0.0.1:1/none")
	if err != nil {
		t.Fatalf("lazy pool: %v", err)
	}
	defer pool.Close()
	if _, err := New(ctx, pool, Config{}); err == nil {
		t.Fatal("a missing model.Inference handle must be rejected")
	} else if !strings.Contains(err.Error(), "model.Inference") {
		t.Fatalf("error should name the missing handle, got %v", err)
	}
}

func TestFromEnvWithoutPoolFailsClosed(t *testing.T) {
	t.Setenv("AHP_VAULT_ADDR", "")
	t.Setenv("AHP_QWEN_RUNTIME_SECRET_PATH", "")
	if _, err := FromEnv(context.Background(), nil); err == nil {
		t.Fatal("FromEnv must not build a bridge without a pool")
	}
}

func TestGatewayMessagesCarryNativeToolTurns(t *testing.T) {
	msgs := []core.ChatMessage{
		{Role: "system", Content: jsonString("be terse")},
		{Role: "user", Content: jsonString("hello")},
		{Role: "tool", ToolCallID: "call-9", Content: jsonString(`{"ok":true}`)},
		{Role: "assistant", Content: jsonString("done")},
		{Role: "user", Content: json.RawMessage("   ")},
		{Role: "user", Content: nil},
	}
	got := gatewayMessages(msgs)
	if len(got) != 4 {
		t.Fatalf("empty turns must be dropped, got %+v", got)
	}
	if got[0].Role != "system" || got[0].Content != "be terse" {
		t.Fatalf("system turn: %+v", got[0])
	}
	if got[1].Role != "user" || got[1].Content != "hello" {
		t.Fatalf("user turn: %+v", got[1])
	}
	// Since T1 the boundary is native: a tool result travels as role=tool with
	// the call id it answers. Folding it into user prose is what made a
	// provider answer a call it never issued; prose folding survives ONLY for
	// legacy rows with no call id (asserted below).
	if got[2].Role != "tool" || got[2].ToolCallID != "call-9" {
		t.Fatalf("tool turn must travel natively with its call id, got %+v", got[2])
	}
	if got[3].Role != "assistant" || got[3].Content != "done" {
		t.Fatalf("assistant turn: %+v", got[3])
	}

	legacy := gatewayMessages([]core.ChatMessage{
		{Role: "tool", Content: jsonString(`{"ok":true}`)},
	})
	if len(legacy) != 1 || legacy[0].Role != "user" || !strings.HasPrefix(legacy[0].Content, "tool result: ") {
		t.Fatalf("a call-less legacy tool row must fold, got %+v", legacy)
	}

	native := gatewayMessages([]core.ChatMessage{
		{Role: "assistant", Content: jsonString(""), ToolCalls: json.RawMessage(`[{"id":"c1","name":"write_artifact","args":{"name":"a"}}]`)},
	})
	if len(native) != 1 || native[0].Role != "assistant" || len(native[0].ToolCalls) != 1 ||
		native[0].ToolCalls[0].Function.Name != "write_artifact" || native[0].ToolCalls[0].Function.Arguments != `{"name":"a"}` {
		t.Fatalf("assistant tool_calls must lower to the wire shape, got %+v", native)
	}

	fallback := gatewayMessages(nil)
	if len(fallback) != 1 || fallback[0].Role != "user" || fallback[0].Content == "" {
		t.Fatalf("empty conversation must still carry one user turn, got %+v", fallback)
	}
}

func TestKeyNamespacing(t *testing.T) {
	if got, want := stepKey("task-7", 3, 0), "execbridge:task-7:step:3"; got != want {
		t.Fatalf("stepKey = %q, want %q", got, want)
	}
	if got, want := stepKey("task-7", 3, 0)+":reserve", "execbridge:task-7:step:3:reserve"; got != want {
		t.Fatalf("reserve key = %q, want %q", got, want)
	}
	if stepKey("task-7", 3, 0) == stepKey("task-7", 4, 0) {
		t.Fatal("steps must not share an idempotency key")
	}
	// A blank model turn seals its attempt, so a retry that reuses the same key
	// is answered as a replayed effect and never reaches the provider. Each
	// physical attempt must therefore own its key...
	if stepKey("task-7", 3, 0) == stepKey("task-7", 3, 1) {
		t.Fatal("a retry attempt must not reuse the sealed key of the attempt it retries")
	}
	if got, want := stepKey("task-7", 3, 1), "execbridge:task-7:step:3:r1"; got != want {
		t.Fatalf("retry key = %q, want %q", got, want)
	}
	// ...while the first attempt keeps the historical key, so generations
	// persisted before this change stay replayable after a restart.
	if got, want := stepKey("task-7", 3, 0), "execbridge:task-7:step:3"; got != want {
		t.Fatalf("first-attempt key = %q, want the pre-retry key unchanged", got)
	}
	// The budget task is namespaced so a bridge budget can never collide with a
	// budget some other plane opened for the same bare id.
	if got, want := taskBudgetTask("task-7"), "a2a-task:task-7"; got != want {
		t.Fatalf("budget task = %q, want %q", got, want)
	}
	if taskBudgetTask("task-7") == "task-7" {
		t.Fatal("budget task must be namespaced")
	}
	if RootBudgetKey == "" {
		t.Fatal("root budget key must be stable")
	}
}

// Execute must reject an unusable attempt BEFORE touching any store, so a
// zero-value bridge is safe to exercise here: no pool, no gateway, no panic.
func TestExecuteRejectsUnusableAttemptWithoutTouchingStores(t *testing.T) {
	ctx := context.Background()
	b := &Bridge{}

	res, err := b.Execute(ctx, gateway.ExecInput{Scope: testScope(), Task: gateway.TaskRow{ID: "task-1"}})
	if err != nil {
		t.Fatalf("empty input is a deterministic failure, not an error: %v", err)
	}
	if res.State != a2a.TaskStateFailed {
		t.Fatalf("state = %v, want FAILED", res.State)
	}
	// A nil input is a missing inbound message, not an empty one: naming the
	// difference is what keeps an operator from hunting a wire bug that is not
	// there. Both must still be a deterministic FAILED, never an error.
	if txt := messageText(res.Message); !strings.Contains(txt, "no inbound user message") {
		t.Fatalf("a nil input must be named as such: %q", txt)
	}

	textless, err := b.Execute(ctx, gateway.ExecInput{Scope: testScope(), Task: gateway.TaskRow{ID: "task-1b"},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"k": "v"}))})
	if err != nil {
		t.Fatalf("a textless message is a deterministic failure, not an error: %v", err)
	}
	if textless.State != a2a.TaskStateFailed {
		t.Fatalf("state = %v, want FAILED", textless.State)
	}
	if txt := messageText(textless.Message); !strings.Contains(txt, "no text part") {
		t.Fatalf("a textless message must be named as such: %q", txt)
	}

	if _, err := b.Execute(ctx, gateway.ExecInput{Scope: contracts.Scope{Tenant: "t"}, Task: gateway.TaskRow{ID: "task-1"}}); err == nil {
		t.Fatal("an incomplete scope must be rejected")
	}
	if _, err := b.Execute(ctx, gateway.ExecInput{Scope: testScope(), Task: gateway.TaskRow{}}); err == nil {
		t.Fatal("a task without an id must be rejected")
	}
}

func TestBridgeAccessors(t *testing.T) {
	b := &Bridge{modelName: "qwen-test", unconfigured: true, taskTokenLimit: 20000}
	if b.ProviderConfigured() {
		t.Fatal("unconfigured bridge must report no provider")
	}
	if b.Model() != "qwen-test" {
		t.Fatalf("model = %q", b.Model())
	}
	if b.TaskTokenLimit() != 20000 {
		t.Fatalf("task token limit = %d", b.TaskTokenLimit())
	}
	configured := &Bridge{modelName: "m"}
	if !configured.ProviderConfigured() {
		t.Fatal("a bridge with a real handle must report a provider")
	}
}

func TestMessageAndContentText(t *testing.T) {
	if got := messageText(nil); got != "" {
		t.Fatalf("nil message: %q", got)
	}
	m := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("line one"), a2a.NewTextPart("line two"))
	if got := messageText(m); got != "line one\nline two" {
		t.Fatalf("messageText = %q", got)
	}
	if got := contentText(jsonString("hi")); got != "hi" {
		t.Fatalf("contentText(string) = %q", got)
	}
	if got := contentText(json.RawMessage(`{"a":1}`)); got != `{"a":1}` {
		t.Fatalf("contentText(object) = %q", got)
	}
	if got := contentText(nil); got != "" {
		t.Fatalf("contentText(nil) = %q", got)
	}
}

func TestHelpers(t *testing.T) {
	if truncate("abcdef", 3) != "abc" || truncate("ab", 3) != "ab" {
		t.Fatal("truncate")
	}
	if errorText("  ", "fallback") != "fallback" || errorText(" real ", "fallback") != "real" {
		t.Fatal("errorText")
	}
	if positiveOr(0, 5) != 5 || positiveOr(-1, 5) != 5 || positiveOr(2, 5) != 2 {
		t.Fatal("positiveOr")
	}
	if positiveInt64Or(0, 5) != 5 || positiveInt64Or(3, 5) != 3 {
		t.Fatal("positiveInt64Or")
	}
	t.Setenv("AHP_TEST_ENV_OR", "")
	if envOr("AHP_TEST_ENV_OR", "fb") != "fb" {
		t.Fatal("envOr empty")
	}
	t.Setenv("AHP_TEST_ENV_OR", "x")
	if envOr("AHP_TEST_ENV_OR", "fb") != "x" {
		t.Fatal("envOr set")
	}
	t.Setenv("AHP_TEST_ENV_INT", "nope")
	if envInt("AHP_TEST_ENV_INT", 7) != 7 {
		t.Fatal("envInt invalid")
	}
	t.Setenv("AHP_TEST_ENV_INT", "9")
	if envInt("AHP_TEST_ENV_INT", 7) != 9 {
		t.Fatal("envInt valid")
	}
	if itoa(12) != "12" {
		t.Fatal("itoa")
	}
	// This boundary is text inference only: the prompt must not promise a tool or
	// a file the transport cannot carry, or the plane is advertising an effect it
	// can only fake.
	if p := turnPrompt("do it"); !strings.Contains(p, "Model Gateway") || !strings.Contains(p, "do it") {
		t.Fatalf("turnPrompt = %q", p)
	}
	if p := turnPrompt("do it"); strings.Contains(p, executor.ArtifactToolName) {
		t.Fatalf("prompt must not offer the model a tool this face cannot serve: %q", p)
	}
	if string(jsonString("a\"b")) != `"a\"b"` {
		t.Fatal("jsonString")
	}
}

// The exported result of a task is offered ON that task, so its name has to carry
// the task id: a consumer must not be able to mistake one task's answer for
// another's. And the rendering keeps the store's sha256, because that seal is the
// only proof the bytes left this process intact.
func TestResultArtifactNamingAndRendering(t *testing.T) {
	name := resultArtifactName("01a190a1-c834-7a80-beff-7064f4173f9e")
	if !strings.HasPrefix(name, "a2a-task-") || !strings.HasSuffix(name, "-result.txt") {
		t.Fatalf("unexpected result artifact name %q", name)
	}
	if !strings.Contains(name, "7064f4173f9e") {
		t.Fatalf("name %q does not identify the task it belongs to", name)
	}
	if len(resultArtifactName(strings.Repeat("x", 400))) > 64 {
		t.Fatal("result artifact name must stay bounded")
	}

	seal := executor.SealedArtifact{ArtifactID: "art-1", SHA256: "ab12", Size: 7, MIME: "text/plain", Name: name}
	if arts := executor.ArtifactsFromSeals(nil); arts != nil {
		t.Fatal("no seal must produce no artifact")
	}
	arts := executor.ArtifactsFromSeals([]executor.SealedArtifact{seal})
	if len(arts) != 1 || string(arts[0].ID) != "art-1" || arts[0].Metadata["sha256"] != "ab12" {
		t.Fatalf("artifact rendering lost the seal: %+v", arts)
	}
}

// A task whose row carries no context id cannot bind the artifacts its run
// will produce, so the attempt must be refused deterministically BEFORE any
// spend: a zero-value bridge has no budget store, and reaching one would panic
// or bill. This is the ingress half of the artifact-binding fix; the export
// half (Seal refusing an unbound binding) lives in internal/executor.
func TestExecuteRefusesUnbindableTaskBeforeSpend(t *testing.T) {
	b := &Bridge{}
	res, err := b.Execute(context.Background(), gateway.ExecInput{
		Scope: testScope(),
		Task:  gateway.TaskRow{ID: "task-ctx-1"},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("write something")),
	})
	if err != nil {
		t.Fatalf("an unbindable task is a deterministic failure, not an error: %v", err)
	}
	if res.State != a2a.TaskStateFailed {
		t.Fatalf("state = %v, want FAILED", res.State)
	}
	if txt := messageText(res.Message); !strings.Contains(txt, "no context id") {
		t.Fatalf("the refusal must name the missing context: %q", txt)
	}
}

// Every write the exec planes perform must go through the bound upload door.
// A regression to BeginUpload (no binding) would silently put artifacts
// outside the deletion cascade, so the gate is on the source itself.
func TestExecPlaneSealPathsAreBound(t *testing.T) {
	for _, f := range []string{"../executor/artifact_plane.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if !strings.Contains(string(src), "BeginUploadWithBinding(") {
			t.Fatalf("%s must seal through BeginUploadWithBinding", f)
		}
		if strings.Contains(string(src), "Store.BeginUpload(") {
			t.Fatalf("%s still calls the unbound BeginUpload", f)
		}
	}
	src, err := os.ReadFile("bridge.go")
	if err != nil {
		t.Fatalf("read bridge.go: %v", err)
	}
	if !strings.Contains(string(src), "artifacts.Binding{ContextID: in.Task.ContextID") {
		t.Fatal("the bridge must bind the exported result to the task's own context")
	}
}
