package execbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestModelHTTPUsesDurableGenerationAndTaskIsolation(t *testing.T) {
	pool := dbPool(t)
	inf := &fakeInference{text: "durable model answer", usage: modelUsage()}
	b, err := New(context.Background(), pool, Config{Inference: inf, TaskTokenLimit: 20000, StepReserve: 4000})
	if err != nil {
		t.Fatal(err)
	}
	scope := uniqueScope()
	call := func(task, op, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/model/"+op, bytes.NewBufferString(body))
		claims := auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: task, Context: "ctx-model-http"}
		r = r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope, Claims: claims}))
		w := httptest.NewRecorder()
		b.serveModel(w, r)
		return w
	}
	body := `{"key":"answer-1","messages":[{"role":"user","content":"answer"}]}`
	w := call("task-a", "SubmitModel", body)
	if w.Code != 200 {
		t.Fatalf("submit=%d %s", w.Code, w.Body.String())
	}
	var gen model.Generation
	if err := json.Unmarshal(w.Body.Bytes(), &gen); err != nil {
		t.Fatal(err)
	}
	if gen.Status != model.GenCompleted || gen.Output != "durable model answer" {
		t.Fatalf("unexpected generation %+v", gen)
	}
	if w = call("task-a", "SubmitModel", body); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if inf.count() != 1 {
		t.Fatalf("replay called model %d times", inf.count())
	}
	for _, op := range []string{"GetModel", "CancelModel"} {
		w = call("task-b", op, `{"id":"`+gen.ID+`"}`)
		if w.Code != 404 {
			t.Fatalf("foreign task %s got%d", op, w.Code)
		}
	}
	w = call("task-a", "SubmitModel", `{"key":"answer-1","messages":[{"role":"user","content":"changed"}]}`)
	if w.Code != 400 {
		t.Fatalf("key mismatch got%d %s", w.Code, w.Body.String())
	}
	w = call("task-a", "SubmitModel", `{"key":"answer-2","provider":"untrusted","messages":[{"role":"user","content":"answer"}]}`)
	if w.Code != 400 {
		t.Fatalf("caller supplied provider accepted: %d", w.Code)
	}
	if inf.count() != 1 {
		t.Fatal("refused request reached provider")
	}
}
