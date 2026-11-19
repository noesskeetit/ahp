package registrytriggers_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

// liveGate is the honest gate for the one leg that spends real native-Qwen
// (G0) compute against the isolated stand. Without the gate the leg is SKIPPED
// with a message that says exactly how to run it; with the gate but without a
// stand it FAILS rather than pretending a live run happened.
func liveGate(t *testing.T) string {
	t.Helper()
	if strings.TrimSpace(os.Getenv("AHP_LIVE_QWEN")) != "1" {
		t.Skip("SKIPPED by design: this leg starts REAL agent runs on the native-Qwen (G0) stand; " +
			"set AHP_LIVE_QWEN=1 and AHP_DEPLOYED_URL=https://127.0.0.1:<own-port> (see /tmp/h15/deploy-h15.sh) to run it")
	}
	front := strings.TrimSuffix(strings.TrimSpace(os.Getenv("AHP_DEPLOYED_URL")), "/")
	if front == "" {
		t.Fatal("AHP_LIVE_QWEN=1 but AHP_DEPLOYED_URL is empty: start the isolated stand first; refusing to claim a live run")
	}
	if !strings.HasPrefix(front, "https://") {
		t.Fatalf("AHP_DEPLOYED_URL must be an mTLS https front, got %q", front)
	}
	// The full fixture suite may use another database than the deployed front.
	// Only this live case must bootstrap Registry and Identity in the front's
	// own DB. t.Setenv restores the fixture DSN before the next in-process test.
	if dsn := strings.TrimSpace(os.Getenv("AHP_DEPLOYED_DATABASE_URL")); dsn != "" {
		t.Setenv("AHP_TEST_DATABASE_URL", dsn)
	}
	if os.Getenv("AHP_DEPLOYED_IAM_PROFILE") == "synthetic" {
		t.Setenv("AHP_TEST_RESOURCE_IAM", "synthetic")
	}
	// A live fixture declares its own reference policy below. Restore the
	// operator's policy even after a failed assertion, before closing this pool.
	// Otherwise a passing test invalidates unrelated admitted deployments.
	pool := newPool(t, requireEnv(t, "AHP_TEST_DATABASE_URL"))
	policies := evals.NewStore(pool)
	before, err := policies.CurrentPolicy(context.Background())
	if err != nil {
		t.Fatalf("live stand must have an operator evaluation policy: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		after, err := policies.CurrentPolicy(ctx)
		if err != nil {
			t.Errorf("read policy after live fixture: %v", err)
			return
		}
		if after.ID == before.ID && after.Digest == before.Digest {
			return
		}
		if after.ID != "h15-live-policy" {
			t.Errorf("another operator changed the evaluation policy; refusing to overwrite %s", after.ID)
			return
		}
		if _, err := policies.SetPolicy(ctx, before); err != nil {
			t.Errorf("restore operator evaluation policy: %v", err)
		}
	})
	return front
}

// clientTLSConfig rebuilds the mTLS material a frontClient uses, so a component
// that owns its own http.Client (the trigger dispatcher) can present it too.
func clientTLSConfig(t *testing.T, certDir string) *tls.Config {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(filepath.Join(certDir, "client.crt"), filepath.Join(certDir, "client.key"))
	if err != nil {
		t.Fatalf("load client cert: %v", err)
	}
	caPEM, err := os.ReadFile(deployCAFile(t))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("stand CA did not parse")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: pool, ServerName: "localhost"}
}

// pollTask waits for a platform task to reach a terminal state and returns the
// task document exactly as the deployed front returned it.
func pollTask(t *testing.T, c *frontClient, signer *auth.Signer, scope contracts.Scope, taskID string, wait time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(wait)
	last := "not polled"
	for attempt := 0; ; attempt++ {
		token := mintTask(t, signer, scope.Tenant, scope.Deployment, scope.Subject,
			fmt.Sprintf("poll-%s-%d", taskID, attempt), "ctx-poll-"+taskID)
		status, raw, err := c.a2a("GetTask", map[string]any{"id": taskID}, token, 60*time.Second)
		if err != nil {
			last = "transport: " + redact(err.Error())
		} else if status != 200 {
			last = fmt.Sprintf("http %d %s", status, redact(string(raw)))
		} else {
			var env struct {
				Result map[string]any `json:"result"`
				Error  *struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			switch {
			case json.Unmarshal(raw, &env) != nil:
				last = "unparseable reply: " + redact(string(raw))
			case env.Error != nil:
				last = fmt.Sprintf("rpc error code=%d message=%s", env.Error.Code, redact(env.Error.Message))
			default:
				// DEF-H9-1: the result is the A2A oneof envelope {task|message}.
				doc := env.Result
				if inner, ok := doc["task"].(map[string]any); ok {
					doc = inner
				}
				h := harvest(doc)
				if isDone(h.State) {
					return doc
				}
				last = fmt.Sprintf("state=%s steps=%d artifacts=%d", h.State, h.Steps, h.ArtifactRefs)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s did not reach a terminal state within %s: last observation: %s", taskID, wait, last)
		}
		time.Sleep(4 * time.Second)
	}
}

// journal is the trigger's own ledger entry for one fire.
type journal struct {
	Status string `json:"-"`
	TaskID string `json:"task_id"`
	Raw    string `json:"-"`
	Detail struct {
		MessageID string `json:"message_id"`
		State     string `json:"state"`
	} `json:"detail"`
}

// Dispatch errors deliberately omit bearer tokens and response bodies. Keep
// their safe status/code visible when a schedule is refused before a task exists.
type liveDispatchDiagnostic struct {
	t     *testing.T
	inner triggers.Dispatcher
}

func (d liveDispatchDiagnostic) Fire(ctx context.Context, scope contracts.Scope, req triggers.FireRequest) (triggers.FireResult, error) {
	result, err := d.inner.Fire(ctx, scope, req)
	if err != nil {
		d.t.Logf("live dispatch refused or unknown: %v", err)
	}
	return result, err
}

func readJournal(t *testing.T, pool *pgxpool.Pool, schema, key string) journal {
	t.Helper()
	var q journal
	var raw string
	err := pool.QueryRow(context.Background(),
		`SELECT status, COALESCE(result, 'null'::jsonb)::text FROM "`+schema+`".operations WHERE key = $1`, key).Scan(&q.Status, &raw)
	if err != nil {
		t.Fatalf("ledger row for %s: %v", key, err)
	}
	q.Raw = raw
	if err := json.Unmarshal([]byte(raw), &q); err != nil {
		t.Fatalf("ledger result for %s is not the fire result: %s", key, redact(raw))
	}
	return q
}

// TestLiveTriggerStartsAgentRunAndSealsArtifact is the W2-A end-to-end leg on the
// isolated stand: a stored trigger holds ONLY a delegation reference, at fire
// time a token is re-issued, the fire leaves as an A2A message, the platform
// starts a REAL native-Qwen agent run, that run seals an artifact into the real
// object store, and the evidence is the canonical journey fields (state,
// artifact, steps_consumed, close_reason, sha256 of the bytes) plus the pinning
// Registry owes the started task. Nothing here is existence-only.
func TestLiveTriggerStartsAgentRunAndSealsArtifact(t *testing.T) {
	front := liveGate(t)
	dbURL := requireEnv(t, "AHP_TEST_DATABASE_URL")
	signer := devSigner(t)
	ctx := context.Background()

	suffix := uniqueSuffix()
	scope := contracts.Scope{Tenant: "h15l" + suffix, Deployment: "d" + suffix, Subject: "s" + suffix}
	pool := newPool(t, dbURL)
	store := registry.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("registry migrate: %v", err)
	}
	// "Оценки перед публикацией": wire the real evals gate so the fixture's
	// publication stays valid (the product mounts it this way); the live-stand leg
	// that follows is unchanged.
	evalStore := evals.NewStore(pool)
	if err := evalStore.Migrate(ctx); err != nil {
		t.Fatalf("evals migrate: %v", err)
	}
	store = registry.New(pool, registry.WithEvalGate(evalStore))

	// Registry side: a published agent version installed for this tenant in prod.
	publisher := "h15lp" + suffix
	agentID := "h15la" + suffix
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
		t.Fatalf("declare eval policy: %v", err)
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

	// Triggers side, on its own schema in the same database (PDF c. 29 allows
	// Registry + Triggers as one deployable with a local transaction inside).
	front15 := newFrontClient(t, front, scope.Tenant, scope.Deployment)
	issuer := &harnessIssuer{ref: "h15ldel" + suffix, signer: signer, scope: scope, t: t}
	dispatcher, err := triggers.NewA2ADispatcher(triggers.A2AConfig{
		URL:     front + "/a2a/public",
		TLS:     clientTLSConfig(t, front15.certDir),
		Timeout: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("dispatcher: %v", err)
	}
	schema := "ahp_trg_l" + suffix
	svc, err := triggers.New(ctx, pool, schema, triggers.Config{
		Issuer:     issuer,
		Dispatcher: liveDispatchDiagnostic{t: t, inner: dispatcher},
		Secrets:    staticSecrets{secret: []byte("h15-live-webhook-secret")},
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

	// The prompt is the canonical journey one: the model must call
	// write_artifact with a nonce nobody but this run could have produced.
	nonce := "h15" + suffix
	marker := "AHP-H15-LIVE-" + nonce
	prompt := "You must use a tool. Call write_artifact exactly once with name=\"live-h15.txt\", mime=\"text/plain\" " +
		"and content set EXACTLY to this single line (do not invent, do not add text):\n" + marker + "\nThen reply with the word ok."
	payload, err := json.Marshal(map[string]string{"text": prompt})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	trigger, err := svc.CreateSchedule(ctx, scope, triggers.CreateScheduleSpec{
		ParentTask:    "h15-live-parent",
		DelegationRef: issuer.ref,
		Target:        "agent:" + agentID,
		Payload:       payload,
		Interval:      time.Minute,
		MaxFires:      2,
		Delegated:     true,
		FirstFireAt:   &now,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	// --- fire 1: the stored schedule must start a real agent run -------------
	fired, err := svc.ProcessDue(ctx, now.Add(time.Second), 0)
	if err != nil || fired != 1 {
		t.Fatalf("ProcessDue fire 1 = %d, %v (want exactly 1)", fired, err)
	}
	row1 := readJournal(t, pool, schema, fmt.Sprintf("trigger:%s:fire:1", trigger.TriggerID))
	if row1.Status != "SUCCEEDED" {
		t.Fatalf("ledger fire 1 status = %s, want SUCCEEDED (detail %s)", row1.Status, redact(row1.TaskID))
	}
	task1 := row1.TaskID
	if task1 == "" {
		t.Fatalf("the platform started no task: the fire is existence-only; ledger detail: %s", redact(row1.Raw))
	}
	if want := triggers.FireMessageID(trigger.TriggerID, 1); row1.Detail.MessageID != want {
		t.Fatalf("fire 1 messageId = %q, want the deterministic %q", row1.Detail.MessageID, want)
	}

	taskDoc := pollTask(t, front15, signer, scope, task1, 240*time.Second)
	h := harvest(taskDoc)
	t.Logf("EVIDENCE task1=%s state=%s artifacts=%d steps_consumed=%d close_reason=%s artifact_id=%s",
		h.TaskID, h.State, h.ArtifactRefs, h.Steps, h.CloseReason, h.ArtifactID)
	if state := strings.ToLower(strings.TrimPrefix(h.State, "TASK_STATE_")); state != "completed" {
		t.Logf("trigger task terminal status: %s", redact(fmt.Sprint(taskDoc["status"])))
		t.Fatalf("trigger-started task state = %q, want completed (steps=%d close=%s)", h.State, h.Steps, h.CloseReason)
	}
	if h.ArtifactID == "" || h.ArtifactRefs < 1 {
		t.Fatalf("the model emitted no sealed artifact: artifact_id=%q refs=%d", h.ArtifactID, h.ArtifactRefs)
	}
	if h.Steps < 1 {
		t.Fatalf("the executor recorded no step accounting: steps_consumed=%d", h.Steps)
	}
	if h.CloseReason != "model_finished" {
		t.Fatalf("close_reason = %q, want model_finished", h.CloseReason)
	}

	// The artifact bytes must be the model's own output, sealed in the real
	// object store: sha256(GET) == sha256 recorded by the executor.
	ctxID := h.ContextID
	dlToken := mintTask(t, signer, scope.Tenant, scope.Deployment, scope.Subject, "dl-"+task1, ctxID)
	status, body, err := front15.post("/v1/artifacts/Download", dlToken, map[string]string{"id": h.ArtifactID}, 90*time.Second)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if status != 200 {
		t.Fatalf("Download of the sealed artifact = http %d: %s", status, redact(string(body)))
	}
	gotSHA := sha256Hex(body)
	t.Logf("EVIDENCE artifact bytes=%d sha256=%s recorded=%s marker_present=%t",
		len(body), gotSHA, h.RecordedSHA, strings.Contains(string(body), marker))
	if !strings.Contains(string(body), marker) {
		t.Fatalf("the downloaded bytes are not the model's nonce-bearing output (marker missing in %d bytes)", len(body))
	}
	if h.RecordedSHA != "" && gotSHA != h.RecordedSHA {
		t.Fatalf("sha256 of the fetched bytes %s != sha256 recorded by the executor %s", gotSHA, h.RecordedSHA)
	}
	if h.RecordedSHA == "" {
		t.Fatal("the task carried no recorded sha256 to compare the sealed bytes against")
	}

	// Registry owes the started task the version it resolved at start (c. 42).
	pinned, err := store.Pin(ctx, scope.Tenant, inst.ID, task1)
	if err != nil {
		t.Fatalf("Pin the started task: %v", err)
	}
	if pinned.ID != version.ID {
		t.Fatalf("pin = %s, want the installed %s", pinned.ID, version.ID)
	}
	eff, err := store.ResolveConfig(ctx, scope.Tenant, inst.ID, task1)
	if err != nil || eff.VersionID != version.ID || eff.Task != task1 {
		t.Fatalf("ResolveConfig of the trigger-started task = %+v, %v", eff, err)
	}

	// --- fire 2: a new token, a new messageId, a new task --------------------
	fired, err = svc.ProcessDue(ctx, now.Add(2*time.Minute), 0)
	if err != nil || fired != 1 {
		t.Fatalf("ProcessDue fire 2 = %d, %v (want exactly 1)", fired, err)
	}
	row2 := readJournal(t, pool, schema, fmt.Sprintf("trigger:%s:fire:2", trigger.TriggerID))
	if row2.Status != "SUCCEEDED" {
		t.Fatalf("ledger fire 2 status = %s, want SUCCEEDED", row2.Status)
	}
	if row2.TaskID == "" || row2.TaskID == task1 {
		t.Fatalf("fire 2 task = %q, want a distinct second task (fire 1 was %s)", row2.TaskID, task1)
	}
	if want := triggers.FireMessageID(trigger.TriggerID, 2); row2.Detail.MessageID != want {
		t.Fatalf("fire 2 messageId = %q, want %q", row2.Detail.MessageID, want)
	}
	if row2.Detail.MessageID == row1.Detail.MessageID {
		t.Fatal("two fires shared one messageId: the platform could dedup the second run away")
	}
	secondDoc := pollTask(t, front15, signer, scope, row2.TaskID, 240*time.Second)
	h2 := harvest(secondDoc)
	t.Logf("EVIDENCE task2=%s state=%s steps_consumed=%d close_reason=%s artifacts=%d",
		h2.TaskID, h2.State, h2.Steps, h2.CloseReason, h2.ArtifactRefs)
	if state := strings.ToLower(strings.TrimPrefix(h2.State, "TASK_STATE_")); state != "completed" {
		t.Fatalf("second trigger-started task state = %q, want completed", h2.State)
	}
	if h2.Steps < 1 {
		t.Fatalf("second run recorded no steps: steps_consumed=%d", h2.Steps)
	}

	issuer.mu.Lock()
	tokens := append([]string(nil), issuer.tokens...)
	issuer.mu.Unlock()
	if len(tokens) != 2 {
		t.Fatalf("issuer minted %d tokens for two fires, want one per fire", len(tokens))
	}
	if tokens[0] == tokens[1] {
		t.Fatal("both fires reused one credential: Identity must re-issue per fire (PDF c. 48)")
	}

	// --- what the trigger row stores: a reference, never a credential --------
	var stored string
	if err := pool.QueryRow(ctx,
		`SELECT payload::text || '|' || delegation_ref || '|' || target FROM "`+schema+`".triggers WHERE trigger_id = $1`,
		trigger.TriggerID).Scan(&stored); err != nil {
		t.Fatalf("read trigger row: %v", err)
	}
	for i, tok := range tokens {
		if strings.Contains(stored, tok) {
			t.Fatalf("fire %d bearer token is persisted in the trigger row (PDF c. 48: reference only)", i+1)
		}
	}
	if strings.Contains(stored, "Bearer") || strings.Count(stored, ".") > 8 {
		t.Fatalf("credential-shaped material in the trigger row: %s", redact(stored))
	}
	if !strings.Contains(stored, issuer.ref) {
		t.Fatalf("the trigger row lost its delegation reference: %s", redact(stored))
	}

	// --- a foreign tenant cannot see the run it did not start ----------------
	foreign := newFrontClient(t, front, "h15x"+suffix, "dx"+suffix)
	// Give the foreign principal its own valid context; it cannot adopt the
	// owner's context merely to exercise a read-denial assertion.
	foreignContext := "foreign-" + suffix + "-" + task1
	foreignCtx := mintTask(t, signer, "h15x"+suffix, "dx"+suffix, "sx"+suffix, "foreign-read", foreignContext)
	_, raw, err := foreign.a2a("GetTask", map[string]any{"id": task1}, foreignCtx, 30*time.Second)
	if err != nil {
		t.Fatalf("foreign GetTask: %v", err)
	}
	if !strings.Contains(string(raw), "NOT_FOUND") && !strings.Contains(string(raw), "not found") {
		t.Fatalf("a foreign tenant read the trigger-started task: %s", redact(string(raw)))
	}
	foreignDL := mintTask(t, signer, "h15x"+suffix, "dx"+suffix, "sx"+suffix, "foreign-dl", foreignContext)
	status, body, err = foreign.post("/v1/artifacts/Download", foreignDL, map[string]string{"id": h.ArtifactID}, 30*time.Second)
	if err != nil {
		t.Fatalf("foreign Download: %v", err)
	}
	if status != 404 && status != 403 {
		t.Fatalf("foreign tenant Download = http %d, want 404/403: %s", status, redact(string(body)))
	}
	t.Logf("EVIDENCE foreign tenant: GetTask denied, Download=http %d", status)
}
