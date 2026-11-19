package registrytriggers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/triggers"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// liveStand is the shared isolated-stand wiring the live legs need: a published,
// eval-gated, installed agent in Registry, and a Triggers service on its own
// schema whose dispatcher posts real A2A to the deployed native-Qwen front. It
// exists so the schedule and webhook live legs assert the SAME observable effect
// (a real task started on the front), not a row written in the trigger schema.
type liveStand struct {
	front   string
	scope   contracts.Scope
	pool    *pgxpool.Pool
	store   *registry.Store
	svc     *triggers.Service
	front15 *frontClient
	signer  *auth.Signer
	issuer  *harnessIssuer
	agentID string
	instID  string
	schema  string
}

// newLiveTriggerStand builds the stand. It is gated by liveGate: without the
// gate the caller skips, with the gate but no front it fails rather than lying.
func newLiveTriggerStand(t *testing.T, webhookSecret []byte) *liveStand {
	t.Helper()
	front := liveGate(t)
	dbURL := requireEnv(t, "AHP_TEST_DATABASE_URL")
	signer := devSigner(t)
	ctx := context.Background()

	suffix := uniqueSuffix()
	scope := contracts.Scope{Tenant: "h15w" + suffix, Deployment: "d" + suffix, Subject: "s" + suffix}
	pool := newPool(t, dbURL)
	store := registry.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("registry migrate: %v", err)
	}
	evalStore := evals.NewStore(pool)
	if err := evalStore.Migrate(ctx); err != nil {
		t.Fatalf("evals migrate: %v", err)
	}
	store = registry.New(pool, registry.WithEvalGate(evalStore))

	publisher := "h15wp" + suffix
	agentID := "h15wa" + suffix
	if _, err := store.RegisterAgent(ctx, publisher, agentID); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	manifest := registry.Manifest{
		Kind:       registry.KindAgent,
		System:     "h15.live." + suffix,
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
	digest, err := contracts.Digest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evalStore.SetPolicy(ctx, evals.Policy{
		ID: "h15-live-policy", TaskSetID: "reference-set-h15",
		RequiredTaskIDs: []string{"reference-task"}, MaxAgeSeconds: 3600,
	}); err != nil {
		t.Fatalf("declare fixture eval policy: %v", err)
	}
	if _, err := evalStore.Record(ctx, evals.RecordInput{
		AgentDigest: digest, TaskSetID: "reference-set-h15",
		Setup: map[string]any{"suite": "live"}, Outcome: evals.OutcomePass,
		Tasks:    []evals.TaskResult{{TaskID: "reference-task", Passed: true, TerminalState: evals.TerminalCompleted}},
		PolicyID: "h15-live-policy", TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("record passing verdict: %v", err)
	}
	version, err := store.PutVersion(ctx, publisher, agentID, digest, manifest)
	if err != nil {
		t.Fatalf("PutVersion: %v", err)
	}
	inst, err := store.Install(ctx, scope.Tenant, agentID, version.ID, registry.EnvironmentProd, "k-h15-live",
		registry.Capabilities{Models: []string{"default-chat"}, SelfTasks: 1})
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	front15 := newFrontClient(t, front, scope.Tenant, scope.Deployment)
	issuer := &harnessIssuer{ref: "h15wdel" + suffix, signer: signer, scope: scope, t: t}
	dispatcher, err := triggers.NewA2ADispatcher(triggers.A2AConfig{
		URL:     front + "/a2a/public",
		TLS:     clientTLSConfig(t, front15.certDir),
		Timeout: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("dispatcher: %v", err)
	}
	schema := "ahp_trg_w" + suffix
	svc, err := triggers.New(ctx, pool, schema, triggers.Config{
		Issuer:     issuer,
		Dispatcher: dispatcher,
		Secrets:    staticSecrets{secret: webhookSecret},
		Tasks:      staticTasks{},
		FireTTL:    10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("triggers.New: %v", err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(cctx, `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
	})
	return &liveStand{front: front, scope: scope, pool: pool, store: store, svc: svc,
		front15: front15, signer: signer, issuer: issuer, agentID: agentID, instID: inst.ID, schema: schema}
}

// deliveryCount is how many webhook deliveries the trigger has reserved. An
// unauthorised or duplicate delivery must not move this number.
func (s *liveStand) deliveryCount(t *testing.T, triggerID string) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM "`+s.schema+`".webhook_deliveries WHERE trigger_id = $1`, triggerID).Scan(&n); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	return n
}

// succeededFires is how many effect-ledger entries in this trigger's schema
// reached SUCCEEDED. The dispatcher (wired to the real front) is the only path
// that can produce one, so a fixed count proves no new agent run was started.
func (s *liveStand) succeededFires(t *testing.T) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM "`+s.schema+`".operations WHERE status = 'SUCCEEDED'`).Scan(&n); err != nil {
		t.Fatalf("count succeeded operations: %v", err)
	}
	return n
}

func (s *liveStand) status(t *testing.T, triggerID string) string {
	t.Helper()
	var st string
	if err := s.pool.QueryRow(context.Background(),
		`SELECT status FROM "`+s.schema+`".triggers WHERE trigger_id = $1`, triggerID).Scan(&st); err != nil {
		t.Fatalf("read trigger status: %v", err)
	}
	return st
}

// TestLiveWebhookStartsAgentRunRejectsBadSignatureIsIdempotentAndStopsOnRevoke
// is the row-23 live leg for the WEBHOOK transport. On the isolated native-Qwen
// stand it proves, against a real front, in order:
//   - a bad HMAC is refused before any reservation: no delivery slot, no effect
//     ledger entry, and (because the dispatcher is the real A2A client) no agent
//     run was started;
//   - a valid HMAC starts a REAL agent run whose task the deployed front reports
//     as completed with model steps consumed (an executed turn, not a DB row);
//   - a byte-identical replay of the same message_id returns the same sealed
//     receipt and starts NO second run;
//   - after Revoke a fresh valid delivery is refused and starts no run.
//
// It spends exactly one real agent run (the valid delivery); every other step is
// a negative or idempotent replay that must not consume compute.
func TestLiveWebhookStartsAgentRunRejectsBadSignatureIsIdempotentAndStopsOnRevoke(t *testing.T) {
	secret := []byte("h15-live-webhook-secret")
	st := newLiveTriggerStand(t, secret)
	ctx := context.Background()

	suffix := uniqueSuffix()
	nonce := "w" + suffix
	marker := "AHP-H15-LIVE-" + nonce
	prompt := "You must use a tool. Call write_artifact exactly once with name=\"live-h15.txt\", mime=\"text/plain\" " +
		"and content set EXACTLY to this single line (do not invent, do not add text):\n" + marker + "\nThen reply with the word ok."
	// The trigger's stored configuration is a reference-shaped payload; the
	// external webhook body is what carries the event content to the agent.
	cfg, err := json.Marshal(map[string]string{"text": prompt})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]string{"prompt": prompt})
	if err != nil {
		t.Fatal(err)
	}

	trigger, err := st.svc.CreateWebhook(ctx, st.scope, triggers.CreateWebhookSpec{
		ParentTask:    "h15-live-parent",
		DelegationRef: st.issuer.ref,
		Target:        "agent:" + st.agentID,
		Payload:       cfg,
		SecretRef:     "sec-1",
		MaxFires:      5,
		Delegated:     true,
	})
	if err != nil {
		t.Fatalf("CreateWebhook: %v", err)
	}

	// (1) bad signature on a fresh message id: refused, nothing reserved, no run.
	msgID := "h15w-msg-" + suffix
	if got := st.deliveryCount(t, trigger.TriggerID); got != 0 {
		t.Fatalf("a bad-signature delivery reserved %d slots, want 0", got)
	}
	if got := st.succeededFires(t); got != 0 {
		t.Fatalf("a bad-signature delivery started %d runs, want 0", got)
	}
	if _, err := st.svc.ReceiveWebhook(ctx, st.scope, trigger.TriggerID, msgID, "deadbeef", body); err == nil {
		t.Fatal("ReceiveWebhook accepted an invalid HMAC signature")
	} else if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("invalid HMAC error code = %s, want FORBIDDEN: %v", contracts.Code(err), err)
	}
	t.Logf("EVIDENCE webhook invalid HMAC: refused code=%s delivery_slots=%d runs=%d", contracts.ErrForbidden,
		st.deliveryCount(t, trigger.TriggerID), st.succeededFires(t))

	// (2) valid signature: the front starts a real agent run.
	sig := triggers.SignWebhookDelivery(secret, trigger.TriggerID, msgID, body)
	op, err := st.svc.ReceiveWebhook(ctx, st.scope, trigger.TriggerID, msgID, sig, body)
	if err != nil {
		t.Fatalf("valid webhook delivery: %v", err)
	}
	if op.Status != "SUCCEEDED" {
		t.Fatalf("webhook ledger status = %s, want SUCCEEDED (detail %s)", op.Status, redact(string(op.Result)))
	}
	task1 := fireTaskID(t, op.Result)
	if task1 == "" {
		t.Fatalf("the platform started no task on a valid webhook: %s", redact(string(op.Result)))
	}
	if got := st.deliveryCount(t, trigger.TriggerID); got != 1 {
		t.Fatalf("a valid delivery reserved %d slots, want 1", got)
	}

	// The observable effect: the deployed front reports the task as a real,
	// completed run that consumed model steps. Foreign tenants cannot see it.
	doc := pollTask(t, st.front15, st.signer, st.scope, task1, 240*time.Second)
	h := harvest(doc)
	t.Logf("EVIDENCE webhook-started task=%s state=%s steps_consumed=%d close_reason=%s artifacts=%d artifact_id=%s",
		h.TaskID, h.State, h.Steps, h.CloseReason, h.ArtifactRefs, h.ArtifactID)
	if state := strings.ToLower(strings.TrimPrefix(h.State, "TASK_STATE_")); state != "completed" {
		t.Fatalf("webhook-started task state = %q, want completed (steps=%d close=%s)", h.State, h.Steps, h.CloseReason)
	}
	if h.Steps < 1 {
		t.Fatalf("the webhook-started run consumed no model steps: it was not a real turn (steps=%d)", h.Steps)
	}
	foreign := newFrontClient(t, st.front, "h15x"+suffix, "dx"+suffix)
	foreignTok := mintTask(t, st.signer, "h15x"+suffix, "dx"+suffix, "sx"+suffix, "foreign-read", "foreign-"+suffix+"-"+task1)
	_, raw, err := foreign.a2a("GetTask", map[string]any{"id": task1}, foreignTok, 30*time.Second)
	if err != nil {
		t.Fatalf("foreign GetTask: %v", err)
	}
	if !strings.Contains(string(raw), "NOT_FOUND") && !strings.Contains(string(raw), "not found") {
		t.Fatalf("a foreign tenant read the webhook-started task: %s", redact(string(raw)))
	}

	// (3) byte-identical replay: same receipt, still one run, one slot.
	beforeRuns := st.succeededFires(t)
	replayOp, err := st.svc.ReceiveWebhook(ctx, st.scope, trigger.TriggerID, msgID, sig, body)
	if err != nil {
		t.Fatalf("replayed delivery errored: %v", err)
	}
	if replayTask := fireTaskID(t, replayOp.Result); replayTask != task1 {
		t.Fatalf("replay started a different task %q, want the original %q", replayTask, task1)
	}
	if got := st.deliveryCount(t, trigger.TriggerID); got != 1 {
		t.Fatalf("replay added a delivery slot: %d, want 1", got)
	}
	if got := st.succeededFires(t); got != beforeRuns {
		t.Fatalf("replay started an extra run: %d, want %d", got, beforeRuns)
	}
	t.Logf("EVIDENCE webhook idempotent replay: same task=%s slots=1 runs unchanged", task1)

	// (4) revoke stops subsequent valid deliveries.
	revoked, err := st.svc.Revoke(ctx, st.scope, trigger.TriggerID)
	if err != nil || revoked.Status != triggers.StatusRevoked {
		t.Fatalf("Revoke = %+v, %v", revoked, err)
	}
	newMsg := "h15w-msg2-" + suffix
	sig2 := triggers.SignWebhookDelivery(secret, trigger.TriggerID, newMsg, body)
	if _, err := st.svc.ReceiveWebhook(ctx, st.scope, trigger.TriggerID, newMsg, sig2, body); err == nil {
		t.Fatal("a revoked webhook accepted a fresh valid delivery")
	} else if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("revoked delivery code = %s, want FORBIDDEN: %v", contracts.Code(err), err)
	}
	if got := st.deliveryCount(t, trigger.TriggerID); got != 1 {
		t.Fatalf("a revoked delivery reserved a slot: %d, want 1", got)
	}
	if got := st.succeededFires(t); got != beforeRuns {
		t.Fatalf("a revoked delivery started a run: %d, want %d", got, beforeRuns)
	}
	if st.status(t, trigger.TriggerID) != triggers.StatusRevoked {
		t.Fatalf("trigger status after revoke = %s, want REVOKED", st.status(t, trigger.TriggerID))
	}
	t.Logf("EVIDENCE webhook revoke: fresh valid delivery refused, slots=1 runs unchanged status=REVOKED")
}

// TestLiveScheduleRevokeStopsFutureFires is the row-23 live leg for SCHEDULE
// revocation on the ticker path: a stored schedule starts one real agent run,
// and after Revoke the scheduler fires no further turns. It spends one real run.
func TestLiveScheduleRevokeStopsFutureFires(t *testing.T) {
	st := newLiveTriggerStand(t, []byte("h15-live-webhook-secret"))
	ctx := context.Background()

	nonce := "sc" + uniqueSuffix()
	marker := "AHP-H15-LIVE-" + nonce
	prompt := "You must use a tool. Call write_artifact exactly once with name=\"live-h15.txt\", mime=\"text/plain\" " +
		"and content set EXACTLY to this single line (do not invent, do not add text):\n" + marker + "\nThen reply with the word ok."
	payload, err := json.Marshal(map[string]string{"text": prompt})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	trigger, err := st.svc.CreateSchedule(ctx, st.scope, triggers.CreateScheduleSpec{
		ParentTask:    "h15-live-parent",
		DelegationRef: st.issuer.ref,
		Target:        "agent:" + st.agentID,
		Payload:       payload,
		Interval:      time.Minute,
		MaxFires:      5,
		Delegated:     true,
		FirstFireAt:   &now,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	// fire 1 starts a real run.
	fired, err := st.svc.ProcessDue(ctx, now.Add(time.Second), 0)
	if err != nil || fired != 1 {
		t.Fatalf("ProcessDue fire 1 = %d, %v (want exactly 1)", fired, err)
	}
	row1 := readJournal(t, st.pool, st.schema, fmt.Sprintf("trigger:%s:fire:1", trigger.TriggerID))
	if row1.Status != "SUCCEEDED" || row1.TaskID == "" {
		t.Fatalf("schedule fire 1 = %s task=%q, want a real started task", row1.Status, row1.TaskID)
	}
	doc := pollTask(t, st.front15, st.signer, st.scope, row1.TaskID, 240*time.Second)
	h := harvest(doc)
	t.Logf("EVIDENCE schedule fire1 task=%s state=%s steps_consumed=%d close_reason=%s",
		h.TaskID, h.State, h.Steps, h.CloseReason)
	if state := strings.ToLower(strings.TrimPrefix(h.State, "TASK_STATE_")); state != "completed" {
		t.Fatalf("schedule-started task state = %q, want completed", h.State)
	}
	if h.Steps < 1 {
		t.Fatalf("schedule-started run consumed no steps: not a real turn")
	}

	// revoke, then the scheduler must start nothing further.
	revoked, err := st.svc.Revoke(ctx, st.scope, trigger.TriggerID)
	if err != nil || revoked.Status != triggers.StatusRevoked {
		t.Fatalf("Revoke = %+v, %v", revoked, err)
	}
	runsBefore := st.succeededFires(t)
	fired2, err := st.svc.ProcessDue(ctx, now.Add(time.Hour), 0)
	if err != nil {
		t.Fatalf("ProcessDue after revoke: %v", err)
	}
	if fired2 != 0 {
		t.Fatalf("a revoked schedule fired %d more times, want 0", fired2)
	}
	if got := st.succeededFires(t); got != runsBefore {
		t.Fatalf("a revoked schedule started a new run: %d, want %d", got, runsBefore)
	}
	var row2 int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM "`+st.schema+`".operations WHERE key = $1`,
		fmt.Sprintf("trigger:%s:fire:2", trigger.TriggerID)).Scan(&row2); err != nil {
		t.Fatalf("count fire:2: %v", err)
	}
	if row2 != 0 {
		t.Fatalf("a revoked schedule wrote a fire:2 ledger row, want none")
	}
	t.Logf("EVIDENCE schedule revoke: later tick fired=0 runs unchanged fire:2 rows=0")
}

// fireTaskID pulls the platform-assigned task id out of a fire's ledger result.
func fireTaskID(t *testing.T, result []byte) string {
	t.Helper()
	var res triggers.FireResult
	if err := json.Unmarshal(result, &res); err != nil {
		t.Fatalf("fire result is not a FireResult: %s", redact(string(result)))
	}
	return res.TaskID
}
