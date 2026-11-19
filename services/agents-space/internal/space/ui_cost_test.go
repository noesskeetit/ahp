package space

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// costUpstream records what the Model Gateway actually saw. Every assertion below is about
// the request the gateway receives, because that -- not the browser response -- is where a
// proxy leaks a human credential or invents a task.
type costUpstream struct {
	calls   int
	path    string
	method  string
	auth    string
	body    string
	status  int
	payload string
}

func (u *costUpstream) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u.calls++
		u.path = r.URL.Path
		u.method = r.Method
		u.auth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		u.body = string(b)
		if u.status != 0 {
			w.WriteHeader(u.status)
		}
		if u.payload != "" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, u.payload)
		}
	}
}

func costProxyFor(t *testing.T, upstream *httptest.Server, tasks RunTaskResolver) http.Handler {
	t.Helper()
	h, err := NewCostProxy(CostProxyConfig{
		ModelCostURL: upstream.URL,
		Client:       upstream.Client(),
		Tasks:        tasks,
	})
	if err != nil {
		t.Fatalf("NewCostProxy: %v", err)
	}
	return h
}

func fixedTask(task string) RunTaskResolver {
	return func(context.Context, string, string) (string, error) { return task, nil }
}

const costAnswer = `{"trees":[{"tree_root":"rtask-u-ahp-accept:run-1","unit":"steps","limit":50,"spent":7,"committed":7,"remaining":43,"available_to_root":43,"exhausted":false,"generations":1}]}`

// TestCostProxyCarriesTheExchangedTaskTokenAndNeverAnythingElse is the authority test: the
// gateway must see the SCOPED TASK token this process exchanged, on its own route, with a
// task the resolver named -- and nothing that smells like the browser's session.
func TestCostProxyCarriesTheExchangedTaskTokenAndNeverAnythingElse(t *testing.T) {
	up := &costUpstream{status: http.StatusOK, payload: costAnswer}
	server := httptest.NewServer(up.handler())
	defer server.Close()

	var gotThread, gotToken string
	proxy := costProxyFor(t, server, func(_ context.Context, token, threadID string) (string, error) {
		gotToken, gotThread = token, threadID
		return "rtask-u-ahp-accept:run-1", nil
	})
	srv := httptest.NewServer(withTask(proxy, "task-token-abc"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + UICostPath + "?thread_id=thread-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if up.calls != 1 {
		t.Fatalf("gateway calls = %d, want 1", up.calls)
	}
	if up.path != "/v1/model/TreeCost" || up.method != http.MethodPost {
		t.Fatalf("gateway saw %s %s, want POST /v1/model/TreeCost", up.method, up.path)
	}
	if up.auth != "Bearer task-token-abc" {
		t.Fatalf("gateway authorization = %q, want the exchanged task token only", up.auth)
	}
	if !strings.Contains(up.body, `"task_id":"rtask-u-ahp-accept:run-1"`) {
		t.Fatalf("gateway body = %q, want the task the resolver named", up.body)
	}
	if gotToken != "task-token-abc" || gotThread != "thread-1" {
		t.Fatalf("resolver saw token %q thread %q: it must receive the exchanged token and the requested thread", gotToken, gotThread)
	}
	if resp.StatusCode != http.StatusOK || string(body) != costAnswer {
		t.Fatalf("response = %d %q, want the gateway answer verbatim", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
}

// TestCostProxyTakesNoTaskFromTheRequest: a caller may name a thread it owns and nothing
// else. A task_id in the query is ignored, not trusted.
func TestCostProxyTakesNoTaskFromTheRequest(t *testing.T) {
	up := &costUpstream{status: http.StatusOK, payload: costAnswer}
	server := httptest.NewServer(up.handler())
	defer server.Close()

	proxy := costProxyFor(t, server, func(_ context.Context, _, threadID string) (string, error) {
		if threadID != "thread-9" {
			t.Fatalf("resolver thread = %q, want the requested thread", threadID)
		}
		return "the-threads-own-task", nil
	})
	srv := httptest.NewServer(withTask(proxy, "task-token-abc"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + UICostPath + "?thread_id=thread-9&task_id=someone-elses-task")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if strings.Contains(up.body, "someone-elses-task") {
		t.Fatalf("a task named by the browser reached the gateway: %q", up.body)
	}
	if !strings.Contains(up.body, `"task_id":"the-threads-own-task"`) {
		t.Fatalf("gateway body = %q", up.body)
	}
}

// TestCostProxyDoesNotSmoothOverAGatewayRefusal: a refusal stays a refusal. The screen must
// show no number rather than a number this process invented.
func TestCostProxyDoesNotSmoothOverAGatewayRefusal(t *testing.T) {
	denied := `{"code":"FORBIDDEN","message":"FORBIDDEN: model: task token lacks the state capability"}`
	up := &costUpstream{status: http.StatusForbidden, payload: denied}
	server := httptest.NewServer(up.handler())
	defer server.Close()

	srv := httptest.NewServer(withTask(costProxyFor(t, server, fixedTask("rtask-x")), "task-token-abc"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + UICostPath + "?thread_id=thread-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want the upstream refusal preserved", resp.StatusCode)
	}
	if string(body) != denied {
		t.Fatalf("body = %q, want the gateway error verbatim", body)
	}
	if strings.Contains(string(body), "0") && strings.Contains(string(body), "spent") {
		t.Fatalf("a synthetic cost number appeared: %q", body)
	}
}

// TestCostProxyRefusesWithoutAnExchangedToken: the human JWT alone never reaches the
// gateway, so a request that arrived without the exchange is refused here.
func TestCostProxyRefusesWithoutAnExchangedToken(t *testing.T) {
	up := &costUpstream{status: http.StatusOK, payload: costAnswer}
	server := httptest.NewServer(up.handler())
	defer server.Close()

	srv := httptest.NewServer(costProxyFor(t, server, fixedTask("rtask-x")))
	defer srv.Close()

	resp, err := http.Get(srv.URL + UICostPath + "?thread_id=thread-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 without an exchanged task token", resp.StatusCode)
	}
	if up.calls != 0 {
		t.Fatalf("the gateway was called %d times without an exchanged token", up.calls)
	}
}

// TestCostProxyRefusesAnQuestionWithNoThread: the route reads one thread's consumption and
// cannot answer "everything".
func TestCostProxyRefusesAQuestionWithNoThread(t *testing.T) {
	up := &costUpstream{status: http.StatusOK, payload: costAnswer}
	server := httptest.NewServer(up.handler())
	defer server.Close()

	srv := httptest.NewServer(withTask(costProxyFor(t, server, fixedTask("rtask-x")), "task-token-abc"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + UICostPath)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), ReasonThreadRequired) {
		t.Fatalf("status %d body %q, want 400 naming %q", resp.StatusCode, body, ReasonThreadRequired)
	}
}

// TestCostProxySaysWhenNothingCharged: a thread whose runs never touched a budget tree has
// no cost to read, and that is stated instead of calling the gateway with an empty task.
func TestCostProxySaysWhenNothingCharged(t *testing.T) {
	up := &costUpstream{status: http.StatusOK, payload: costAnswer}
	server := httptest.NewServer(up.handler())
	defer server.Close()

	srv := httptest.NewServer(withTask(costProxyFor(t, server, func(context.Context, string, string) (string, error) {
		return "", nil
	}), "task-token-abc"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + UICostPath + "?thread_id=thread-empty")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound || !strings.Contains(string(body), ReasonNoRunForThread) {
		t.Fatalf("status %d body %q, want 404 naming %q", resp.StatusCode, body, ReasonNoRunForThread)
	}
	if up.calls != 0 {
		t.Fatalf("gateway called %d times with no task to name", up.calls)
	}
}

// TestCostProxyRoutes: only a GET/HEAD is a cost read; anything else is not a route of ours.
func TestCostProxyRoutes(t *testing.T) {
	up := &costUpstream{status: http.StatusOK, payload: costAnswer}
	server := httptest.NewServer(up.handler())
	defer server.Close()

	srv := httptest.NewServer(withTask(costProxyFor(t, server, fixedTask("rtask-x")), "task-token-abc"))
	defer srv.Close()

	resp, err := http.Post(srv.URL+UICostPath+"?thread_id=thread-1", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST status = %d, want 404 (the route is a read)", resp.StatusCode)
	}
}

// TestNewCostProxyRefusesAnOriginCarryingTheAPIPath keeps DEF-H24-12's rule on the new
// surface: an operator who writes the API path into the origin is refused at startup
// instead of discovering doubled paths in production.
func TestNewCostProxyRefusesAnOriginCarryingTheAPIPath(t *testing.T) {
	if _, err := NewCostProxy(CostProxyConfig{
		ModelCostURL: "https://front.example/v1/model",
		Client:       http.DefaultClient,
		Tasks:        fixedTask("rtask-x"),
	}); err == nil {
		t.Fatal("an origin that carries the API path must be refused")
	}
	if _, err := NewCostProxy(CostProxyConfig{ModelCostURL: "https://front.example", Client: nil, Tasks: fixedTask("rtask-x")}); err == nil {
		t.Fatal("the cost read must not run without Space's workload client")
	}
	if _, err := NewCostProxy(CostProxyConfig{ModelCostURL: "https://front.example", Client: http.DefaultClient, Tasks: nil}); err == nil {
		t.Fatal("the cost read must not run without a task resolver")
	}
}

// TestCostAnswerIsTheGatewaysOwnShape stops this package from re-describing consumption:
// the fields a browser renders are the ledger's fields.
func TestCostAnswerIsTheGatewaysOwnShape(t *testing.T) {
	var decoded map[string]any
	if err := json.Unmarshal([]byte(costAnswer), &decoded); err != nil {
		t.Fatalf("the sample answer must be the gateway JSON: %v", err)
	}
	trees, ok := decoded["trees"].([]any)
	if !ok || len(trees) != 1 {
		t.Fatalf("decoded = %v", decoded)
	}
	root := trees[0].(map[string]any)
	for _, field := range []string{"tree_root", "unit", "limit", "spent", "remaining", "exhausted"} {
		if _, ok := root[field]; !ok {
			t.Fatalf("the pass-through dropped %q", field)
		}
	}
}
