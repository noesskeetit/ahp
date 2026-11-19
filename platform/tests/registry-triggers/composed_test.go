package registrytriggers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/triggers"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// pinningDispatcher is the composed Registry+Triggers behaviour: a fire posts an
// A2A message to the agent, and the task the platform started is pinned to the
// version Registry just resolved. PDF rev 3.2, c. 29 allows these two services
// to be one deployable ("На старте сервисы можно собрать в меньшее число
// deployables: Registry + Triggers"), and c. 42 requires the started task to be
// bound to its version.
type pinningDispatcher struct {
	inner *triggers.A2ADispatcher
	store *registry.Store
	// resolve is what the composed block resolved for this installation before
	// firing; the pin must land on exactly this version.
	mu       sync.Mutex
	pinned   map[string]string
	installs map[string]string
}

func (d *pinningDispatcher) Fire(ctx context.Context, scope contracts.Scope, req triggers.FireRequest) (triggers.FireResult, error) {
	res, err := d.inner.Fire(ctx, scope, req)
	if err != nil {
		return res, err
	}
	if res.TaskID == "" {
		return res, fmt.Errorf("composed block: the platform started no task")
	}
	d.mu.Lock()
	installation := d.installs[req.Target]
	d.mu.Unlock()
	if installation == "" {
		return res, fmt.Errorf("composed block: no installation for target %s", req.Target)
	}
	version, err := d.store.Pin(ctx, scope.Tenant, installation, res.TaskID)
	if err != nil {
		return res, fmt.Errorf("composed block: pin started task: %w", err)
	}
	d.mu.Lock()
	d.pinned[res.TaskID] = version.ID
	d.mu.Unlock()
	return res, nil
}

// harnessIssuer stands in for Identity & Policy: it only answers for the
// delegation reference it was issued, and it mints a FRESH token per call.
type harnessIssuer struct {
	ref    string
	signer *auth.Signer
	scope  contracts.Scope
	t      *testing.T
	mu     sync.Mutex
	tokens []string
}

// IssueToken mints a fresh task credential, the way Identity & Policy does at
// fire time (PDF rev 3.2, c. 48). A reference it never issued is NOT_FOUND,
// which is the authority verdict that must stop the trigger.
func (i *harnessIssuer) IssueToken(_ context.Context, _ contracts.Scope, delegationRef string, ttl time.Duration) (string, time.Time, error) {
	if delegationRef != i.ref {
		return "", time.Time{}, contracts.NewError(contracts.ErrNotFound, "harness identity: unknown delegation reference")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	task := fmt.Sprintf("trigger-token-%s-%d", i.scope.Tenant, len(i.tokens)+1)
	token := mintTask(i.t, i.signer, i.scope.Tenant, i.scope.Deployment, i.scope.Subject, task, "ctx-"+task)
	i.tokens = append(i.tokens, token)
	return token, time.Now().Add(ttl), nil
}

type staticTasks struct{}

func (staticTasks) IsTaskActive(context.Context, contracts.Scope, string) (bool, error) {
	return true, nil
}

type staticSecrets struct{ secret []byte }

func (s staticSecrets) Resolve(_ context.Context, _ contracts.Scope, ref string) ([]byte, error) {
	if ref == "sec-1" {
		return s.secret, nil
	}
	return nil, fmt.Errorf("unknown secret ref")
}

// TestComposedTriggerFirePinsTheStartedTask is the Registry+Triggers composed
// proof on real PostgreSQL: a due schedule fires exactly once, the fire reaches
// the agent as an A2A message carrying a freshly minted token, the started task
// is pinned to the resolved version, and a later rollout does not move it.
func TestComposedTriggerFirePinsTheStartedTask(t *testing.T) {
	dbURL := requireEnv(t, "AHP_TEST_DATABASE_URL")
	ctx := context.Background()
	pool := newPool(t, dbURL)

	suffix := uniqueSuffix()
	scope := contracts.Scope{Tenant: "h15c" + suffix, Deployment: "d" + suffix, Subject: "s" + suffix}

	store := registry.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("registry migrate: %v", err)
	}
	// "Оценки перед публикацией": the composed Registry+Triggers block publishes
	// through the same mandatory evals gate the product mount wires, so build the
	// store over the real internal/evals plane and record a passing verdict for the
	// exact digest before publishing. No stub, no ungated bypass.
	evalStore := evals.NewStore(pool)
	if err := evalStore.Migrate(ctx); err != nil {
		t.Fatalf("evals migrate: %v", err)
	}
	store = registry.New(pool, registry.WithEvalGate(evalStore))

	// A published artifact, installed for the tenant: this is what the trigger
	// is allowed to start work on.
	publisher := "pub-h15-" + suffix
	agentID := "agent-h15-" + suffix
	if _, err := store.RegisterAgent(ctx, publisher, agentID); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	m := registry.Manifest{
		Kind:       registry.KindAgent,
		System:     "h15.composed." + suffix,
		Visibility: registry.VisibilityTenant,
		Audience:   registry.AudienceShared,
		Class:      registry.ClassC,
		Components: []registry.Component{{Name: "entry", A2A: true, MinReplicas: 1, MaxReplicas: 2}},
		State:      registry.StateConfig{Partition: "subject", Compat: "v1"},
		Capabilities: registry.Capabilities{
			Models: []string{"default-chat"}, SelfTasks: 1,
		},
		Limits: registry.Limits{Depth: 2, ActiveTasks: 8, TaskTimeout: "15m"},
	}
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evalStore.SetPolicy(ctx, evals.Policy{
		ID: "h15-composed-policy", TaskSetID: "reference-set-h15",
		RequiredTaskIDs: []string{"reference-task"}, MaxAgeSeconds: 3600,
	}); err != nil {
		t.Fatalf("declare eval policy: %v", err)
	}
	if _, err := evalStore.Record(ctx, evals.RecordInput{
		AgentDigest: digest, TaskSetID: "reference-set-h15",
		Setup: map[string]any{"suite": "composed"}, Outcome: evals.OutcomePass,
		Tasks:    []evals.TaskResult{{TaskID: "reference-task", Passed: true, TerminalState: evals.TerminalCompleted}},
		PolicyID: "h15-composed-policy", TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("record passing verdict: %v", err)
	}
	v1, err := store.PutVersion(ctx, publisher, agentID, digest, m)
	if err != nil {
		t.Fatalf("PutVersion: %v", err)
	}
	granted := registry.Capabilities{Models: []string{"default-chat"}, SelfTasks: 1}
	inst, err := store.Install(ctx, scope.Tenant, agentID, v1.ID, registry.EnvironmentProd, "k-h15", granted)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	// The fake platform A2A entry records what each fire actually sent.
	type observed struct {
		messageID string
		bearer    string
		text      string
	}
	var mu sync.Mutex
	var seen []observed
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
			Params struct {
				Message struct {
					MessageID string        `json:"messageId"`
					Parts     []messagePart `json:"parts"`
				} `json:"message"`
			} `json:"params"`
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req)
		mu.Lock()
		seen = append(seen, observed{
			messageID: req.Params.Message.MessageID,
			bearer:    r.Header.Get("authorization"),
			text:      first(req.Params.Message.Parts),
		})
		mu.Unlock()
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"task":{"id":"task-` + suffix + `-1","status":{"state":"TASK_STATE_WORKING"}}}}`))
	}))
	defer gateway.Close()

	signer := devSigner(t)
	issuer := &harnessIssuer{ref: "del-h15-" + suffix, signer: signer, scope: scope, t: t}
	inner, err := triggers.NewA2ADispatcher(triggers.A2AConfig{URL: gateway.URL + "/a2a/public"})
	if err != nil {
		t.Fatalf("dispatcher: %v", err)
	}
	dispatcher := &pinningDispatcher{inner: inner, store: store,
		pinned: map[string]string{}, installs: map[string]string{"agent:" + agentID: inst.ID}}

	schema := "ahp_trg_c" + suffix
	svc, err := triggers.New(ctx, pool, schema, triggers.Config{
		Issuer:     issuer,
		Dispatcher: dispatcher,
		Secrets:    staticSecrets{secret: []byte("h15-webhook-secret")},
		Tasks:      staticTasks{},
		FireTTL:    time.Minute,
	})
	if err != nil {
		t.Fatalf("triggers.New: %v", err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(cctx, `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
	})

	now := time.Now().UTC()
	trigger, err := svc.CreateSchedule(ctx, scope, triggers.CreateScheduleSpec{
		ParentTask: "parent-h15", DelegationRef: issuer.ref, Target: "agent:" + agentID,
		Payload:  json.RawMessage(`{"text":"run the scheduled digest"}`),
		Interval: time.Hour, MaxFires: 2, Delegated: true,
		FirstFireAt: &now,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	fired, err := svc.ProcessDue(ctx, now.Add(time.Second), 0)
	if err != nil || fired != 1 {
		t.Fatalf("ProcessDue = %d, %v", fired, err)
	}

	// 1. The fire reached the agent as one A2A message with a deterministic id.
	mu.Lock()
	got := append([]observed(nil), seen...)
	mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("gateway saw %d messages, want exactly 1", len(got))
	}
	if want := triggers.FireMessageID(trigger.TriggerID, 1); got[0].messageID != want {
		t.Fatalf("messageId = %q, want the deterministic %q", got[0].messageID, want)
	}
	if got[0].text != "run the scheduled digest" {
		t.Fatalf("the agent got %q", got[0].text)
	}
	if !strings.HasPrefix(got[0].bearer, "Bearer ") || len(got[0].bearer) < 40 {
		t.Fatal("the fire carried no bearer token")
	}

	// 2. The started task is pinned to the version Registry resolved for the
	//    installation, and ResolveConfig keeps answering with it after a rollout.
	started := "task-" + suffix + "-1"
	pinned, err := store.PinnedVersion(ctx, scope.Tenant, started)
	if err != nil || pinned.ID != v1.ID {
		t.Fatalf("started task pin = %+v, %v (want %s)", pinned, err, v1.ID)
	}
	eff, err := store.ResolveConfig(ctx, scope.Tenant, inst.ID, started)
	if err != nil || eff.VersionID != v1.ID || eff.Task != started {
		t.Fatalf("ResolveConfig for the started task = %+v, %v", eff, err)
	}

	// A new version is rolled out; the running task stays where it started.
	m2 := m
	m2.Limits.ActiveTasks = 9
	digest2, err := contracts.Digest(m2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evalStore.Record(ctx, evals.RecordInput{
		AgentDigest: digest2, TaskSetID: "reference-set-h15",
		Setup: map[string]any{"suite": "composed", "rev": 2}, Outcome: evals.OutcomePass,
		Tasks:    []evals.TaskResult{{TaskID: "reference-task", Passed: true, TerminalState: evals.TerminalCompleted}},
		PolicyID: "h15-composed-policy", TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("record passing verdict v2: %v", err)
	}
	v2, err := store.PutVersion(ctx, publisher, agentID, digest2, m2)
	if err != nil {
		t.Fatalf("PutVersion v2: %v", err)
	}
	if _, err := store.Promote(ctx, scope.Tenant, inst.ID, v2.ID); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	afterRollout, err := store.ResolveConfig(ctx, scope.Tenant, inst.ID, started)
	if err != nil {
		t.Fatalf("ResolveConfig after rollout: %v", err)
	}
	if afterRollout.VersionID != v1.ID {
		t.Fatalf("a rollout moved the trigger-started task: %s (installed now %s)", afterRollout.VersionID, v2.ID)
	}
	fresh, err := store.ResolveConfig(ctx, scope.Tenant, inst.ID, "")
	if err != nil || fresh.VersionID != v2.ID {
		t.Fatalf("post-rollout resolution = %+v, %v", fresh, err)
	}

	// 3. The trigger row stores the delegation reference and never a token.
	var row string
	if err := pool.QueryRow(ctx, `SELECT payload::text || '|' || delegation_ref FROM "`+schema+`".triggers WHERE trigger_id = $1`, trigger.TriggerID).Scan(&row); err != nil {
		t.Fatalf("read trigger row: %v", err)
	}
	issuer.mu.Lock()
	tokens := append([]string(nil), issuer.tokens...)
	issuer.mu.Unlock()
	if len(tokens) != 1 {
		t.Fatalf("expected exactly one issued token, got %d", len(tokens))
	}
	if strings.Contains(row, tokens[0]) {
		t.Fatal("the trigger row stored a bearer token (PDF: it must store only a delegation reference)")
	}
	if !strings.Contains(row, issuer.ref) {
		t.Fatalf("the trigger row does not reference its delegation: %s", row)
	}
	if strings.Contains(row, "Bearer") {
		t.Fatalf("token material in the trigger row: %s", row)
	}

	// 4. The fire is journalled with the task it started.
	var ledgerDetail string
	if err := pool.QueryRow(ctx, `SELECT result::text FROM "`+schema+`".operations ORDER BY created_at DESC LIMIT 1`).Scan(&ledgerDetail); err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if !strings.Contains(ledgerDetail, started) {
		t.Fatalf("ledger result does not name the started task: %s", redact(ledgerDetail))
	}
}

// messagePart is one A2A message part as the gateway receives it.
type messagePart struct {
	Text string `json:"text"`
}

func first(parts []messagePart) string {
	if len(parts) == 0 {
		return ""
	}
	return parts[0].Text
}
