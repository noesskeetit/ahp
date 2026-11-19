package memoryapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These are owner/HTTP dispatch tests over real PostgreSQL. Only the already
// verified transport principal is supplied here; the deployed test covers mTLS.
func operationEnv(t *testing.T, options ...memory.Option) (*memory.Store, *Handler, contracts.Scope) {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL is required")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	allow := func(context.Context, contracts.Scope, string, string) error { return nil }
	s, err := memory.New(pool, nil, allow, nil, options...)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP SCHEMA ahp_memory CASCADE") })
	h, err := New(s, auth.NewVerifier("test", nil), func(context.Context, auth.Claims, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "operation-" + contracts.NewID(), Deployment: "dot", Subject: "alice"}
	return s, h.(*Handler), scope
}

func operationCall(t *testing.T, h *Handler, scope contracts.Scope, op, body string, want int) map[string]any {
	t.Helper()
	h.task = func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope})))
		})
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/memory/"+op, strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != want {
		t.Fatalf("%s status=%d want=%d body=%s", op, w.Code, want, w.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestFenceClosesOneKeyWithoutErasingSubject(t *testing.T) {
	s, h, scope := operationEnv(t)
	old, err := s.Propose(t.Context(), scope, memory.ProposeRequest{Kind: memory.KindUser, Text: "keep this note", OpKey: "existing-note"})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		v := operationCall(t, h, scope, "Fence", `{"op_key":"cancel-one"}`, 200)
		if v["status"] != "FENCED" || v["op_key"] != "cancel-one" {
			t.Fatalf("fence receipt: %v", v)
		}
	}
	v := operationCall(t, h, scope, "Get", `{"op_key":"cancel-one"}`, 200)
	if v["status"] != "FENCED" {
		t.Fatalf("get fenced receipt: %v", v)
	}
	_, err = s.Propose(t.Context(), scope, memory.ProposeRequest{Kind: memory.KindUser, Text: "must never appear", OpKey: "cancel-one"})
	if contracts.Code(err) != contracts.ErrConflict || contracts.ReasonOf(err) != "FENCED" {
		t.Fatalf("fenced mutation: %v", err)
	}
	got, err := s.Get(t.Context(), scope, old.Memory.ID)
	if err != nil || got.Text != "keep this note" {
		t.Fatalf("Fence erased a note: %v", err)
	}
	_, err = s.Propose(t.Context(), scope, memory.ProposeRequest{Kind: memory.KindUser, Text: "later unrelated note", OpKey: "unrelated"})
	if err != nil {
		t.Fatalf("Fence erased the subject: %v", err)
	}
}

func TestGetAndFenceReturnCommittedReceiptNotRecord(t *testing.T) {
	s, h, scope := operationEnv(t)
	req := memory.ProposeRequest{Kind: memory.KindUser, Text: "receipt must not contain this", OpKey: "already-committed"}
	p, err := s.Propose(t.Context(), scope, req)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"Get", "Fence", "Get"} {
		v := operationCall(t, h, scope, op, `{"op_key":"already-committed"}`, 200)
		if v["status"] != "SUCCEEDED" || v["result_id"] != p.Memory.ID || v["text"] != nil {
			t.Fatalf("%s receipt: %v", op, v)
		}
	}
	replayed, err := s.Propose(t.Context(), scope, req)
	if err != nil || !replayed.Replay || replayed.Memory.ID != p.Memory.ID {
		t.Fatalf("committed fence changed outcome: %v", err)
	}
	for _, field := range []string{"Subject", "Deployment", "Tenant"} {
		other := scope
		switch field {
		case "Subject":
			other.Subject = "bob"
		case "Deployment":
			other.Deployment = "other"
		case "Tenant":
			other.Tenant = "other"
		}
		operationCall(t, h, other, "Get", `{"op_key":"already-committed"}`, 404)
		if field == "Subject" {
			operationCall(t, h, other, "Fence", `{"op_key":"already-committed"}`, 404)
		}
	}
}

func TestMemoryOperationKeyIsDeploymentWide(t *testing.T) {
	s, _, scope := operationEnv(t)
	req := memory.ProposeRequest{Kind: memory.KindUser, Text: "first owner", OpKey: "same-key"}
	_, err := s.Propose(t.Context(), scope, req)
	if err != nil {
		t.Fatal(err)
	}
	other := scope
	other.Subject = "bob"
	_, err = s.Propose(t.Context(), other, req)
	if contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("key reused by another subject inside deployment: %v", err)
	}
	other.Deployment = "independent-deployment"
	if _, err = s.Propose(t.Context(), other, req); err != nil {
		t.Fatalf("independent deployment key: %v", err)
	}
}
