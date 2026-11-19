package registrytriggers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/triggers"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testEnvOr returns the environment value for key or fallback when unset/blank.
func testEnvOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// mountGate is the honest gate for the mounted-front leg: it needs the live
// native-Qwen stand AND the mount flag, and a second front started WITHOUT the
// mount flags so the default-off behavior is measured, not assumed.
func mountGate(t *testing.T) (front, offFront, schema string) {
	t.Helper()
	if strings.TrimSpace(os.Getenv("AHP_MOUNT_W2A")) != "1" {
		t.Skip("SKIPPED by design: mounted-front catalog/trigger leg; set AHP_MOUNT_W2A=1 plus the " +
			"live gate vars (see /tmp/h23/live-env.sh) to run it")
	}
	front = liveGate(t) // also asserts AHP_LIVE_QWEN=1 and an https front URL
	offFront = strings.TrimSuffix(strings.TrimSpace(os.Getenv("AHP_OFF_FRONT_URL")), "/")
	if offFront == "" {
		t.Fatal("AHP_MOUNT_W2A=1 but AHP_OFF_FRONT_URL is empty: start the no-flag front (deploy-h23.sh)")
	}
	schema = strings.TrimSpace(os.Getenv("AHP_TRIGGERS_SCHEMA"))
	if schema == "" {
		schema = "ahp_triggers_all"
	}
	return front, offFront, schema
}

// pollJournal waits for the fire's ledger row the FRONT wrote and returns it.
func pollJournal(t *testing.T, pool *pgxpool.Pool, schema, key string, wait time.Duration) journal {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		var (
			status string
			raw    string
		)
		err := pool.QueryRow(context.Background(),
			`SELECT status, COALESCE(result, 'null'::jsonb)::text FROM "`+schema+`".operations WHERE key = $1`, key).
			Scan(&status, &raw)
		if err == nil {
			var j journal
			_ = json.Unmarshal([]byte(raw), &j)
			j.Status = status
			j.Raw = raw
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("no ledger row for %s within %s: the mounted front never journalled the fire (%v)", key, wait, err)
		}
		time.Sleep(2 * time.Second)
	}
}

// TestMountedFrontServesCatalogAndFiresTriggerThroughFront is the W2-A product
// gap closed: the catalog role-list and the trigger create+fire run against the
// MOUNTED composed front over mTLS+Bearer (not in-process). The front's own
// dispatcher starts a real native-Qwen run, which seals an artifact into the real
// object store; the assertions are journey-strength and the artifact sha is
// verified out-of-band.
func TestMountedFrontServesCatalogAndFiresTriggerThroughFront(t *testing.T) {
	front, offFront, schema := mountGate(t)
	dbURL := requireEnv(t, "AHP_TEST_DATABASE_URL")
	ctx := context.Background()
	pool := newPool(t, dbURL)
	signer := devSigner(t)

	suffix := uniqueSuffix()
	// Reproducibility: every PERSISTED, collision-prone identifier carries the
	// per-run nonce (install idempotency key, agent, publisher, subject, parent
	// task, delegation ref, marker), so the leg passes repeatedly on one stand with
	// no state cleanup. tenant/deployment are the ONE exception and stay a stand
	// constant: /a2a/public is transport.Task-guarded and binds the presenting
	// client-certificate workload to the fire token's scope, and the front's
	// dispatcher presents a single clientAuth certificate minted at stand start for
	// exactly this workload -- so the trigger's tenant/deployment cannot be random
	// per run. They are configurable so the leg ports to any stand that mints a
	// dispatcher certificate for a matching workload (defaults = helper23 stand).
	tenant := testEnvOr("AHP_MOUNT_TENANT", "h23m")
	deployment := testEnvOr("AHP_MOUNT_DEPLOYMENT", "dmount")
	scope := contracts.Scope{Tenant: tenant, Deployment: deployment, Subject: "s" + suffix}

	// --- default-off proof: a front WITHOUT the flags does not serve these faces
	off := newFrontClient(t, offFront, scope.Tenant, scope.Deployment)
	offTok := mintTask(t, signer, scope.Tenant, scope.Deployment, scope.Subject, "off-probe", "ctx-off")
	if st, body, err := off.post("/v1/registry/CatalogList", offTok, map[string]any{}, 30*time.Second); err != nil || st != 404 {
		t.Fatalf("no-flag front served /v1/registry/CatalogList = http %d (want 404): %s", st, redact(string(body)))
	}
	if st, body, err := off.post("/v1/triggers/List", offTok, map[string]any{}, 30*time.Second); err != nil || st != 404 {
		t.Fatalf("no-flag front served /v1/triggers/List = http %d (want 404): %s", st, redact(string(body)))
	}
	t.Logf("EVIDENCE default-off front: registry+triggers both 404")

	// --- seed the accepted Registry package in-process (setup only): a published,
	//     installed prod version is what a trigger and an employee catalog need.
	store := registry.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("registry migrate: %v", err)
	}
	// "Оценки перед публикацией": wire the real evals gate so the in-process seed
	// publication stays valid, matching how the product mounts the plane.
	evalStore := evals.NewStore(pool)
	if err := evalStore.Migrate(ctx); err != nil {
		t.Fatalf("evals migrate: %v", err)
	}
	store = registry.New(pool, registry.WithEvalGate(evalStore))
	publisher := "h23mp" + suffix
	agentID := "h23ma" + suffix
	if _, err := store.RegisterAgent(ctx, publisher, agentID); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	manifest := registry.Manifest{
		Kind: registry.KindAgent, System: "h23.mount." + suffix,
		Visibility: registry.VisibilityTenant, Audience: registry.AudienceShared, Class: registry.ClassC,
		Components:   []registry.Component{{Name: "entry", A2A: true, MinReplicas: 1, MaxReplicas: 2}},
		State:        registry.StateConfig{Partition: "subject", Compat: "v1"},
		Capabilities: registry.Capabilities{Models: []string{"default-chat"}, SelfTasks: 1},
		Limits:       registry.Limits{Depth: 2, ActiveTasks: 8, TaskTimeout: "15m"},
	}
	digest, err := contracts.Digest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evalStore.SetPolicy(ctx, evals.Policy{
		ID: "h23-mount-front-policy", TaskSetID: "reference-set-h23",
		RequiredTaskIDs: []string{"reference-task"}, MaxAgeSeconds: 3600,
	}); err != nil {
		t.Fatalf("declare eval policy: %v", err)
	}
	if _, err := evalStore.Record(ctx, evals.RecordInput{
		AgentDigest: digest, TaskSetID: "reference-set-h23",
		Setup: map[string]any{"suite": "mount-front"}, Outcome: evals.OutcomePass,
		Tasks:    []evals.TaskResult{{TaskID: "reference-task", Passed: true, TerminalState: evals.TerminalCompleted}},
		PolicyID: "h23-mount-front-policy", TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("record passing verdict: %v", err)
	}
	version, err := store.PutVersion(ctx, publisher, agentID, digest, manifest)
	if err != nil {
		t.Fatalf("PutVersion: %v", err)
	}
	inst, err := store.Install(ctx, scope.Tenant, agentID, version.ID, registry.EnvironmentProd, "k-h23-"+suffix,
		registry.Capabilities{Models: []string{"default-chat"}, SelfTasks: 1})
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	fc := newFrontClient(t, front, scope.Tenant, scope.Deployment)
	tok := func(name string) string {
		return mintTask(t, signer, scope.Tenant, scope.Deployment, scope.Subject, name, "ctx-"+name)
	}

	// --- publish THROUGH the mounted front (admin action; tenant from claims) ---
	if st, body, err := fc.post("/v1/registry/Publish", tok("publish"),
		map[string]any{"installation_id": inst.ID, "roles": []string{"employee"}}, 30*time.Second); err != nil || st != 200 {
		t.Fatalf("front Publish = http %d, %v: %s", st, err, redact(string(body)))
	}

	// --- catalog role-list THROUGH the front: employee sees it, a stranger role
	//     does not, and a foreign tenant never sees another tenant's card.
	var list struct {
		Cards []registry.CatalogEntry `json:"cards"`
	}
	st, body, err := fc.post("/v1/registry/CatalogList", tok("catalog-emp"), map[string]any{"roles": []string{"employee"}}, 30*time.Second)
	if err != nil || st != 200 {
		t.Fatalf("front CatalogList(employee) = http %d, %v: %s", st, err, redact(string(body)))
	}
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("decode catalog: %v", err)
	}
	seenEmployee := false
	for _, c := range list.Cards {
		if c.AgentID == agentID {
			seenEmployee = true
		}
	}
	if !seenEmployee {
		t.Fatalf("employee-role catalog through the front does not list the published agent (cards=%d)", len(list.Cards))
	}
	st, body, _ = fc.post("/v1/registry/CatalogList", tok("catalog-stranger"), map[string]any{"roles": []string{"nobody-" + suffix}}, 30*time.Second)
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("decode stranger catalog: %v", err)
	}
	for _, c := range list.Cards {
		if c.AgentID == agentID {
			t.Fatalf("a role outside the audience saw the card through the front")
		}
	}
	foreign := newFrontClient(t, front, "h23x"+suffix, "dx"+suffix)
	st, body, err = foreign.post("/v1/registry/CatalogList",
		mintTask(t, signer, "h23x"+suffix, "dx"+suffix, "sx"+suffix, "foreign-cat", "ctx-foreign"),
		map[string]any{"roles": []string{"employee"}}, 30*time.Second)
	if err != nil || st != 200 {
		t.Fatalf("foreign CatalogList = http %d, %v", st, err)
	}
	var foreignList struct {
		Cards []registry.CatalogEntry `json:"cards"`
	}
	_ = json.Unmarshal(body, &foreignList)
	for _, c := range foreignList.Cards {
		if c.AgentID == agentID {
			t.Fatalf("a foreign tenant listed another tenant's catalog card through the front")
		}
	}
	t.Logf("EVIDENCE catalog through mounted front: employee=%t stranger_hidden foreign_hidden", seenEmployee)

	// --- create the trigger THROUGH the front; stored with a delegation ref only.
	nonce := "h23" + suffix
	marker := "AHP-H23-LIVE-" + nonce
	prompt := "You must use a tool. Call write_artifact exactly once with name=\"live-h23.txt\", mime=\"text/plain\" " +
		"and content set EXACTLY to this single line (do not invent, do not add text):\n" + marker + "\nThen reply with the word ok."
	payload, _ := json.Marshal(map[string]string{"text": prompt})
	now := time.Now().UTC()
	var trig triggers.Trigger
	st, body, err = fc.post("/v1/triggers/CreateSchedule", tok("create-trig"), map[string]any{
		"parent_task": "h23-mount-parent-" + suffix, "delegation_ref": "h23mdel" + suffix, "target": "agent:" + agentID,
		"payload": json.RawMessage(payload), "interval": int64(time.Minute), "max_fires": 1,
		"delegated": true, "first_fire_at": now,
	}, 60*time.Second)
	if err != nil || st != 200 {
		t.Fatalf("front CreateSchedule = http %d, %v: %s", st, err, redact(string(body)))
	}
	if err := json.Unmarshal(body, &trig); err != nil {
		t.Fatalf("decode trigger: %v: %s", err, redact(string(body)))
	}
	if trig.TriggerID == "" {
		t.Fatal("front returned a trigger with no id")
	}

	// --- the trigger is listable THROUGH the front ---
	st, body, err = fc.post("/v1/triggers/List", tok("list-trig"), map[string]any{}, 30*time.Second)
	if err != nil || st != 200 || !strings.Contains(string(body), trig.TriggerID) {
		t.Fatalf("front List did not return the created trigger: http %d, %v", st, err)
	}

	// --- fire THROUGH the front: nudge its dispatcher, then read the ledger the
	//     FRONT wrote (the ticker may fire first; max_fires=1 guarantees one).
	fc.post("/v1/triggers/FireNow", tok("fire-now"), map[string]any{"limit": 0}, 60*time.Second)
	row1 := pollJournal(t, pool, schema, fmt.Sprintf("trigger:%s:fire:1", trig.TriggerID), 60*time.Second)
	if row1.Status != "SUCCEEDED" {
		t.Fatalf("front-ledger fire status = %s, want SUCCEEDED (raw %s)", row1.Status, redact(row1.Raw))
	}
	if row1.TaskID == "" {
		t.Fatalf("front fire started no task; existence-only: %s", redact(row1.Raw))
	}
	if want := triggers.FireMessageID(trig.TriggerID, 1); row1.Detail.MessageID != want {
		t.Fatalf("fire messageId = %q, want %q", row1.Detail.MessageID, want)
	}

	// --- journey-strength evidence, then out-of-band artifact verification ---
	doc := pollTask(t, fc, signer, scope, row1.TaskID, 240*time.Second)
	h := harvest(doc)
	t.Logf("EVIDENCE mounted-front task=%s state=%s steps=%d close=%s artifacts=%d sha=%s",
		h.TaskID, h.State, h.Steps, h.CloseReason, h.ArtifactRefs, h.RecordedSHA)
	if got := strings.ToLower(strings.TrimPrefix(h.State, "TASK_STATE_")); got != "completed" {
		t.Fatalf("mounted-front fire task state = %q, want completed", h.State)
	}
	if h.Steps < 1 {
		t.Fatalf("steps_consumed = %d, want >= 1", h.Steps)
	}
	if h.CloseReason != "model_finished" {
		t.Fatalf("close_reason = %q, want model_finished", h.CloseReason)
	}
	if h.ArtifactID == "" || h.ArtifactRefs < 1 {
		t.Fatalf("no sealed artifact: id=%q refs=%d", h.ArtifactID, h.ArtifactRefs)
	}
	if h.RecordedSHA == "" {
		t.Fatal("task carried no recorded sha256")
	}
	dlTok := mintTask(t, signer, scope.Tenant, scope.Deployment, scope.Subject, "dl-"+h.TaskID, h.ContextID)
	st, artifact, err := fc.post("/v1/artifacts/Download", dlTok, map[string]string{"id": h.ArtifactID}, 90*time.Second)
	if err != nil || st != 200 {
		t.Fatalf("Download = http %d, %v", st, err)
	}
	if !strings.Contains(string(artifact), marker) {
		t.Fatalf("downloaded bytes are not the model's nonce output (marker missing in %d bytes)", len(artifact))
	}
	if got := sha256Hex(artifact); got != h.RecordedSHA {
		t.Fatalf("out-of-band sha256(GET)=%s != recorded=%s", got, h.RecordedSHA)
	}

	// --- a foreign tenant cannot read the run the front started ---
	foreignCtx := mintTask(t, signer, "h23x"+suffix, "dx"+suffix, "sx"+suffix, "foreign-read", h.ContextID)
	_, raw, err := foreign.a2a("GetTask", map[string]any{"id": row1.TaskID}, foreignCtx, 30*time.Second)
	if err != nil {
		t.Fatalf("foreign GetTask: %v", err)
	}
	if !strings.Contains(string(raw), "NOT_FOUND") && !strings.Contains(string(raw), "not found") {
		t.Fatalf("foreign tenant read the front-started task: %s", redact(string(raw)))
	}
	foreignDL := mintTask(t, signer, "h23x"+suffix, "dx"+suffix, "sx"+suffix, "foreign-dl", h.ContextID)
	st, body, err = foreign.post("/v1/artifacts/Download", foreignDL, map[string]string{"id": h.ArtifactID}, 30*time.Second)
	if err != nil || (st != 404 && st != 403) {
		t.Fatalf("foreign Download = http %d, %v, want 403/404", st, err)
	}
	t.Logf("EVIDENCE foreign tenant denied on GetTask + Download=%d", st)
}
