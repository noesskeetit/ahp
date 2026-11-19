package dote2e_test

// Actual second runtime, same employee and database. Only the physical Computer
// substrate is the permitted mock; Identity, State, Compute and Qwen are real.
import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestTwoDotDeploymentsKeepPersonalComputersAndTurnsSeparate(t *testing.T) {
	stand := loadStand(t)
	manifest := os.Getenv("AHP_DOT_E2E_SECOND_DOT")
	if manifest == "" {
		t.Skip("SKIP_ENV[tag=dot_e2e_layout] reason=second_actual_dot_runtime_not_supplied missing=AHP_DOT_E2E_SECOND_DOT")
	}
	var second struct {
		Status, Deployment, Tenant, Binary, SHA256, Revision, Origin, Certificate, Key, CA, Workload string
		PID                                                                                          int
	}
	data, err := os.ReadFile(manifest)
	if err != nil || json.Unmarshal(data, &second) != nil {
		t.Fatal("read second actual Dot manifest")
	}
	if second.Status != "ready" || second.Deployment == "" || second.Deployment == stand.Scope["deployment"] || second.Tenant != stand.Scope["tenant"] || second.Revision != stand.Revision {
		t.Fatal("second Dot manifest does not name a separate own deployment of the recorded build")
	}
	if second.Binary != filepath.Join(stand.BinDir, "as-runner") || second.SHA256 != stand.Binaries["as-runner"] {
		t.Fatal("second runtime binary is not the frozen acceptance runtime")
	}
	// A health response alone must not certify a fabricated second deployment.
	exe := filepath.Join("/proc", strconv.Itoa(second.PID), "exe")
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil || resolved != second.Binary {
		t.Fatal("second actual runtime process identity does not match")
	}
	bytes, err := os.ReadFile(exe)
	digest := sha256.Sum256(bytes)
	if err != nil || hex.EncodeToString(digest[:]) != second.SHA256 {
		t.Fatal("second actual runtime digest differs")
	}
	ca, err := os.ReadFile(second.CA)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		t.Fatal("second Dot CA missing")
	}
	cert, err := tls.LoadX509KeyPair(second.Certificate, second.Key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil || len(leaf.URIs) != 1 || leaf.URIs[0].String() != second.Workload || second.Workload != "spiffe://ahp/tenant/"+second.Tenant+"/deployment/"+second.Deployment {
		t.Fatal("second Dot certificate is not bound to the claimed deployment")
	}
	other := *stand
	other.HTTP = newLaneClient(cert, pool)
	other.Scope = map[string]any{}
	for key, value := range stand.Scope {
		other.Scope[key] = value
	}
	other.Scope["deployment"] = second.Deployment
	checks := map[string]any{"actual_second_runtime": map[string]any{"status": "PASS", "pid": second.PID, "deployment": second.Deployment, "sha256": second.SHA256, "workload": second.Workload}}
	defer func() {
		verdict := "PASS"
		if t.Failed() {
			verdict = "FAIL"
			checks["test_failure"] = map[string]any{"status": "FAIL"}
		}
		stand.record(t, "two-personal-dots", verdict, checks)
	}()
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := strconv.FormatInt(time.Now().UnixNano(), 10)
	key, realm := "two-dots-"+stamp, "two-dots-realm-"+stamp
	tokens := make([]string, 2)
	computers := make([]computerReply, 2)
	for index, s := range []*instance{stand, &other} {
		// Identity context IDs have one owner scope. The same employee's second
		// Dot gets a separate context; sharing the browser turn key is harmless.
		tokens[index], _ = s.taskGrantContext(t, s.HTTP, human, key, key+"-"+s.Scope["deployment"].(string), []string{"compute", "state"})
		computers[index], err = createComputer(t, s, tokens[index], key, realm, "S")
		if err != nil {
			t.Fatalf("Dot %d CreateComputer: %v", index, err)
		}
		if computers[index].Deployment != s.Scope["deployment"] || computers[index].Subject != stand.Scope["subject"] || computers[index].Realm != realm {
			t.Fatalf("unexpected personal Computer scope: %+v", computers[index])
		}
		replay, replayErr := createComputer(t, s, tokens[index], key, realm, "S")
		if replayErr != nil || replay.ID != computers[index].ID {
			t.Fatalf("Dot %d idempotent replay failed", index)
		}
	}
	err = other.call(other.HTTP, "/v1/identity/control/exchange", map[string]any{
		"human_token": human, "task": key + "-foreign-context", "context": key + "-" + stand.Scope["deployment"].(string), "capabilities": []string{"state"}}, human, nil)
	if status(err) != 404 {
		t.Fatalf("second Dot re-used the first Dot's context: %v", err)
	}
	checks["cross_dot_identity_context_refused"] = map[string]any{"status": "PASS", "http": 404}
	if computers[0].ID == computers[1].ID {
		t.Fatal("two Dot deployments share one personal Computer")
	}
	checks["same_user_realm_key_two_computers"] = map[string]any{"status": "PASS", "first": computers[0], "second": computers[1], "physical_substrate": "permitted mock"}
	for index, s := range []*instance{stand, &other} {
		err = s.call(s.HTTP, "/v1/compute/GetComputer", map[string]any{"computer_id": computers[1-index].ID}, tokens[index], nil)
		if status(err) != 404 && status(err) != 403 {
			t.Fatalf("Dot %d cross-deployment Computer read not denied: %v", index, err)
		}
	}
	checks["cross_dot_computer_read_denied"] = map[string]any{"status": "PASS"}
	var count int
	if err = stand.Pool.QueryRow(context.Background(), `SELECT count(DISTINCT deployment) FROM ahp_compute.computers WHERE tenant=$1 AND subject=$2 AND realm=$3 AND profile='personal'`, stand.Scope["tenant"], stand.Scope["subject"], realm).Scan(&count); err != nil || count != 2 {
		t.Fatalf("persisted deployment dimension: count=%d err=%v", count, err)
	}
	checks["two_persisted_deployment_keys"] = map[string]any{"status": "PASS", "count": count}
	// Both runtimes receive the same turn key in their distinct owned contexts.
	// Each executes its own scoped State run using the real Model Gateway.
	publishDotAgentConfig(t, stand, "lane-qwen", []string{"save_artifact", "emil.delegate"}, "Answer in one short sentence.", 6)
	edges := []instance{*stand, other}
	edges[1].Origin = second.Origin
	accepted := make([]struct {
		TurnID   string `json:"turn_id"`
		ThreadID string `json:"thread_id"`
	}, 2)
	for index := range edges {
		if err = edges[index].call(edges[index].HTTP, "/v1/space/AcceptTurn", map[string]any{"key": key, "context_id": "turn-" + key + "-" + edges[index].Scope["deployment"].(string), "input": map[string]any{"parts": []any{map[string]any{"kind": "text", "text": "Reply with exactly TWO-DOTS-OK"}}}, "max_steps": 6}, human, &accepted[index]); err != nil {
			t.Fatalf("Dot %d AcceptTurn: %v", index, err)
		}
		if accepted[index].TurnID == "" || accepted[index].ThreadID == "" {
			t.Fatalf("Dot %d empty accepted turn", index)
		}
	}
	if accepted[0] == accepted[1] || accepted[0].ThreadID == accepted[1].ThreadID {
		t.Fatal("two actual Dot queues deduplicate across deployment")
	}
	deadline := time.Now().Add(240 * time.Second)
	for index := range edges {
		var turn struct {
			Status        string `json:"status"`
			FailureReason string `json:"failure_reason"`
		}
		for {
			err = edges[index].call(edges[index].HTTP, "/v1/space/GetTurn", map[string]any{"turn_id": accepted[index].TurnID}, human, &turn)
			if err != nil {
				t.Fatalf("Dot %d GetTurn: %v", index, err)
			}
			if turn.Status == "done" {
				break
			}
			if turn.Status == "failed" || turn.Status == "canceled" || time.Now().After(deadline) {
				t.Fatalf("Dot %d turn status=%s reason=%s", index, turn.Status, turn.FailureReason)
			}
			time.Sleep(time.Second)
		}
		var deployment, subject string
		var generations, usage int
		err = stand.Pool.QueryRow(context.Background(), `SELECT t.deployment,t.subject,count(g.id),coalesce(sum(g.usage_total),0) FROM ahp_state.threads t JOIN ahp_state.runs r ON r.thread_id=t.id LEFT JOIN ahp_model.generations g ON g.tenant=t.tenant AND g.task=r.task_id WHERE t.id=$1 AND t.tenant=$2 GROUP BY t.deployment,t.subject`, accepted[index].ThreadID, stand.Scope["tenant"]).Scan(&deployment, &subject, &generations, &usage)
		if err != nil || deployment != edges[index].Scope["deployment"] || subject != stand.Scope["subject"] || generations < 1 || usage < 1 {
			t.Fatalf("Dot %d actual run/model proof invalid: dep=%s generations=%d usage=%d err=%v", index, deployment, generations, usage, err)
		}
		checks["turn_"+deployment] = map[string]any{"status": "PASS", "accepted": accepted[index], "deployment": deployment, "model_generations": generations, "usage_total": usage}
		for _, path := range []string{"GetTurn", "ReadStream"} {
			body := map[string]any{"turn_id": accepted[1-index].TurnID}
			if path == "ReadStream" {
				body = map[string]any{"thread_id": accepted[1-index].ThreadID, "after": 0}
			}
			err = edges[index].call(edges[index].HTTP, "/v1/space/"+path, body, human, nil)
			if status(err) != 404 && status(err) != 403 {
				t.Fatalf("Dot %d cross-dot %s not refused: %v", index, path, err)
			}
		}
	}
	checks["cross_dot_turn_and_stream_read_denied"] = map[string]any{"status": "PASS"}
}
