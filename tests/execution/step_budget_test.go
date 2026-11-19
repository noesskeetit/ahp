package execution_test

// step_budget_test.go -- the step budget is a contract, not an accident.
//
// Two defects lived on this boundary and both were invisible from the outside:
//
//   - The caller's max_steps never reached the executor. gateway.ExecInput
//     carried no budget, so internal/execbridge substituted its deployment
//     default (AHP_EXEC_MAX_STEPS, default 4). A caller that asked for 8 steps
//     silently got 4, and nothing on the task said so.
//   - Why the loop stopped was not recorded. "The model finished" and "the step
//     budget ran out" both looked like a finished run, so a run that stopped
//     without the artifact the operator explicitly asked for could not be told
//     apart from one that ran out of room -- and an acceptance rule that says
//     "done without the requested artifact is a FAIL" had nothing to read.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
)

type sbRecord struct {
	mu       sync.Mutex
	maxSteps []int
}

func (r *sbRecord) note(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxSteps = append(r.maxSteps, n)
}

func (r *sbRecord) seen() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.maxSteps...)
}

// sbExecutor reports what it was given and what it did, exactly as the real
// bridge does: the bound in force plus the reason the loop closed.
type sbExecutor struct {
	sbRecord
	consumed int
	reason   string
	state    a2a.TaskState
}

func (e *sbExecutor) Execute(_ context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
	budget := in.MaxSteps
	if budget == 0 {
		budget = 4 // the deployment default, applied only because none was stated
	}
	e.note(budget)
	return gateway.ExecResult{
		State:   e.state,
		Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("turn done")),
		Stats: gateway.ExecStats{
			StepsConsumed: e.consumed,
			StepBudget:    budget,
			CloseReason:   e.reason,
		},
	}, nil
}

// sbRawSend posts a hand-built SendMessage body with caller metadata, so the
// budget arrives the way a client sends it -- on the wire, not through a Go
// struct the gateway itself defined.
func sbRawSend(t *testing.T, srv *http.Client, url, token, msgID, text string, metadata map[string]any) (int, *a2a.Task, string, int) {
	t.Helper()
	msg := map[string]any{
		"kind":      "message",
		"role":      "user",
		"messageId": msgID,
		"parts":     []any{map[string]any{"kind": "text", "text": text}},
	}
	params := map[string]any{"message": msg}
	if metadata != nil {
		params["metadata"] = metadata
	}
	rawParams, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: "SendMessage", Params: rawParams})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := srv.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcErr         `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("body is not a JSON-RPC response: %v (%s)", err, raw)
	}
	if out.Error != nil {
		return resp.StatusCode, nil, out.Error.Message, out.Error.Code
	}
	var sr sendResult
	_ = json.Unmarshal(out.Result, &sr)
	return resp.StatusCode, sr.Task, "", 0
}

// sbGetTask reads the task back over the same face: what the operator can see is
// what the journal actually holds.
func sbGetTask(t *testing.T, client *http.Client, url, token, taskID string) map[string]any {
	t.Helper()
	params, err := json.Marshal(map[string]any{"id": taskID})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 2, Method: "GetTask", Params: params})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Result struct {
			Metadata map[string]any `json:"metadata"`
			Status   struct {
				State string `json:"state"`
			} `json:"status"`
		} `json:"result"`
		Error *rpcErr `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("GetTask is not a JSON-RPC response: %v (%s)", err, raw)
	}
	if out.Error != nil {
		t.Fatalf("GetTask refused: code=%d message=%q", out.Error.Code, out.Error.Message)
	}
	return out.Result.Metadata
}

// A caller that states max_steps must have that exact number reach the
// executor, and the task must say which bound was in force and what it consumed.
func TestCallerStepBudgetReachesExecutorAndIsJournalled(t *testing.T) {
	exec := &sbExecutor{consumed: 2, reason: gateway.CloseReasonModelFinished, state: a2a.TaskStateCompleted}
	srv, signer, tenant, dep := vrFaceCeil(t, exec, 8)
	token := ptTaskToken(t, signer, tenant, dep, "a2a-agent", "sb-caller")

	status, task, detail, code := sbRawSend(t, srv.Client(), srv.URL+"/a2a/public", token,
		"sb-msg-"+randSuffix(), "Один абзац про снимки.", map[string]any{"max_steps": float64(6)})
	if detail != "" {
		t.Fatalf("a stated budget inside the ceiling was refused: %q code=%d", detail, code)
	}
	if status != http.StatusOK || task == nil {
		t.Fatalf("send must give 200 + a task, got status=%d task=%v", status, task)
	}
	seen := exec.seen()
	if len(seen) != 1 || seen[0] != 6 {
		t.Fatalf("executor must run on the caller's 6 steps, saw %v (substituting the deployment default is the defect)", seen)
	}
	if task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("task state = %s, want COMPLETED", task.Status.State)
	}
	md := sbGetTask(t, srv.Client(), srv.URL+"/a2a/public", token, string(task.ID))
	for key, want := range map[string]any{
		// what the caller asked for, and what actually bounded the run: two keys,
		// so a substitution can never hide inside one number.
		"max_steps":             float64(6),
		"step_budget_effective": float64(6),
		"step_budget_source":    "caller",
		"steps_consumed":        float64(2),
		"close_reason":          gateway.CloseReasonModelFinished,
		"attempt":               float64(1),
	} {
		if got := md[key]; got != want {
			t.Fatalf("task metadata %s = %v, want %v (full: %v)", key, got, want, md)
		}
	}
}

// A budget above the operator's ceiling is refused before a task exists. The
// forbidden behaviour is a silent clamp: the caller would believe it asked for 12.
func TestStepBudgetAboveCeilingRefusedAtIngress(t *testing.T) {
	exec := &sbExecutor{consumed: 1, reason: gateway.CloseReasonModelFinished, state: a2a.TaskStateCompleted}
	srv, signer, tenant, dep := vrFaceCeil(t, exec, 4)
	token := ptTaskToken(t, signer, tenant, dep, "a2a-agent", "sb-ceiling")

	status, task, detail, code := sbRawSend(t, srv.Client(), srv.URL+"/a2a/public", token,
		"sb-msg-"+randSuffix(), "Один абзац про снимки.", map[string]any{"max_steps": float64(12)})
	if task != nil {
		t.Fatalf("an over-ceiling budget must not create a task, got %s", task.ID)
	}
	if code != -32602 || status != http.StatusOK && status != http.StatusBadRequest {
		t.Fatalf("over-ceiling budget = code %d status %d, want JSON-RPC -32602", code, status)
	}
	if detail == "" {
		t.Fatal("the refusal must explain itself")
	}
	if got := exec.seen(); len(got) != 0 {
		t.Fatalf("nothing may be executed after an ingress refusal, saw %v", got)
	}
}

// A non-numeric budget is a client error, not a value to reinterpret.
func TestNonNumericStepBudgetRefused(t *testing.T) {
	exec := &sbExecutor{consumed: 1, reason: gateway.CloseReasonModelFinished, state: a2a.TaskStateCompleted}
	srv, signer, tenant, dep := vrFaceCeil(t, exec, 8)
	token := ptTaskToken(t, signer, tenant, dep, "a2a-agent", "sb-bad")

	for _, bad := range []any{"8", float64(0), float64(-2), true, float64(2.5)} {
		task, detail, code := func() (*a2a.Task, string, int) {
			s, tk, d, c := sbRawSend(t, srv.Client(), srv.URL+"/a2a/public", token,
				"sb-msg-"+randSuffix(), "Один абзац.", map[string]any{"max_steps": bad})
			_ = s
			return tk, d, c
		}()
		if task != nil {
			t.Fatalf("max_steps=%v created a task %s", bad, task.ID)
		}
		if code != -32602 {
			t.Fatalf("max_steps=%v = code %d (%q), want -32602", bad, code, detail)
		}
	}
	if got := exec.seen(); len(got) != 0 {
		t.Fatalf("no execution after ingress refusals, saw %v", got)
	}
}

// The whole point of the stats: a run that stopped on the budget is visible as
// such, so "finished without the requested artifact" can be attributed honestly.
func TestStepLimitCloseReasonIsVisibleOnTask(t *testing.T) {
	exec := &sbExecutor{consumed: 3, reason: gateway.CloseReasonStepLimit, state: a2a.TaskStateFailed}
	srv, signer, tenant, dep := vrFaceCeil(t, exec, 4)
	token := ptTaskToken(t, signer, tenant, dep, "a2a-agent", "sb-limit")

	_, task, detail, code := sbRawSend(t, srv.Client(), srv.URL+"/a2a/public", token,
		"sb-msg-"+randSuffix(), "Один абзац.", map[string]any{"max_steps": float64(3)})
	if task == nil {
		t.Fatalf("a legal budget must be accepted, got %q/%d", detail, code)
	}
	md := sbGetTask(t, srv.Client(), srv.URL+"/a2a/public", token, string(task.ID))
	if md["close_reason"] != gateway.CloseReasonStepLimit {
		t.Fatalf("close_reason = %v, want %q (metadata %v)", md["close_reason"], gateway.CloseReasonStepLimit, md)
	}
	if md["steps_consumed"] != float64(3) || md["max_steps"] != float64(3) || md["step_budget_effective"] != float64(3) {
		t.Fatalf("the exhausted budget must be visible: %v", md)
	}
}
