package journal_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	wire "github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/sdk/go/state/journal"
)

func TestScopedSessionIDIsStableAcrossCallers(t *testing.T) {
	alice := contracts.Scope{Tenant: "tenant-a", Deployment: "agent-1", Subject: "alice"}
	got := wire.ScopedSessionID(alice, "chat-1")
	if got != "vms-9dc9193afb1bd8fb986c262b50787ad2" {
		t.Fatalf("unexpected canonical ID: %q", got)
	}
	for _, scope := range []contracts.Scope{
		{Tenant: "tenant-b", Deployment: "agent-1", Subject: "alice"},
		{Tenant: "tenant-a", Deployment: "agent-2", Subject: "alice"},
		{Tenant: "tenant-a", Deployment: "agent-1", Subject: "bob"},
	} {
		if other := wire.ScopedSessionID(scope, "chat-1"); other == got {
			t.Fatalf("conversation ID collided across scopes: %+v", scope)
		}
	}
	if wire.ScopedSessionID(alice, "chat-2") == got {
		t.Fatal("different conversation IDs collided")
	}
}

func TestCoordinatorSDKAddressAndCursor(t *testing.T) {
	const base = "/internal/v1/state/coordinator"
	const target = base + "/scopes/tenant-b/agent-2/alice@team/sessions"
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+target+":enqueue", func(w http.ResponseWriter, r *http.Request) {
		var input journal.EnqueueRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.SessionID != "managed" || input.ExecutionMode != wire.ExecutionVMManaged {
			t.Errorf("unexpected enqueue input: %+v, %v", input, err)
		}
		fmt.Fprint(w, `{"session_id":"managed","head_seq":1}`)
	})
	mux.HandleFunc("GET "+target+"/managed", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("view") == "resume" {
			fmt.Fprint(w, `{"session_id":"managed","harness_type":"go","head_seq":1}`)
			return
		}
		fmt.Fprint(w, `{"status":"QUEUED","head_seq":1}`)
	})
	mux.HandleFunc("GET "+target+"/managed/events", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("after_seq") != "1" || q.Get("limit") != "7" || q.Get("state_namespace") != "ctx" {
			t.Errorf("unexpected event query: %s", r.URL.RawQuery)
		}
		fmt.Fprint(w, `{"head_seq":1,"events":[]}`)
	})
	mux.HandleFunc("POST "+target+"/managed/vm:cancel", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Turn int64 `json:"turn_seq"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Turn != 1 {
			t.Errorf("unexpected cancel body: %+v, %v", body, err)
		}
		fmt.Fprint(w, `{"status":"IDLE"}`)
	})
	mux.HandleFunc("POST "+target+"/managed/vm:requeue-fenced", func(w http.ResponseWriter, r *http.Request) {
		var body journal.FenceRequeueRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.TurnSeq != 1 || body.Epoch != 2 || body.ExecutionID != "exec-test" {
			t.Errorf("unexpected requeue body: %+v, %v", body, err)
		}
		fmt.Fprint(w, `{}`)
	})
	mux.HandleFunc("GET "+base+"/vm-reconcile", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("status") != "ACTIVE" || q.Get("after") != "2026-11-23T12:01:02.123456Z" ||
			q.Get("after_tenant") != "tenant-a" || q.Get("after_session") != "prior" {
			t.Errorf("unexpected reconcile query: %s", r.URL.RawQuery)
		}
		fmt.Fprint(w, `{"sessions":[{"scope":{"tenant":"tenant-b","deployment":"agent-2","subject":"alice@team"},"session_id":"managed","status":"ACTIVE"}],"next":{"at":"2026-11-23T12:01:03Z","tenant_id":"tenant-b","session_id":"managed"}}`)
	})
	var received atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get("Authorization"), fmt.Sprintf("Bearer task-%d", received.Add(1)); got != want {
			t.Errorf("authorization = %q, want %q", got, want)
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	var issued atomic.Int32
	client, err := journal.New(journal.Config{
		BaseURL: server.URL, HTTPClient: server.Client(),
		TokenSource: func(context.Context) (string, error) {
			return fmt.Sprintf("task-%d", issued.Add(1)), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	coordinator := client.Coordinator()
	scope := contracts.Scope{Tenant: "tenant-b", Deployment: "agent-2", Subject: "alice@team"}
	scoped := coordinator.ForScope(scope)
	receipt, err := scoped.EnqueueTurn(ctx, journal.EnqueueRequest{SessionID: "managed", ExecutionMode: wire.ExecutionVMManaged})
	if err != nil || receipt.SessionID != "managed" || receipt.HeadSeq != 1 {
		t.Fatalf("enqueue receipt: %+v, %v", receipt, err)
	}
	status, err := scoped.GetTurnStatus(ctx, "managed")
	if err != nil || status.Status != "QUEUED" {
		t.Fatalf("status: %+v, %v", status, err)
	}
	state, err := scoped.InspectSession(ctx, "managed")
	if err != nil || state.Harness != "go" {
		t.Fatalf("resume: %+v, %v", state, err)
	}
	events, err := scoped.GetEvents(ctx, "managed", 1, 7, "ctx")
	if err != nil || events.HeadSeq != 1 {
		t.Fatalf("events: %+v, %v", events, err)
	}
	status, err = scoped.CancelSession(ctx, "managed", 1)
	if err != nil || status.Status != "IDLE" {
		t.Fatalf("cancel status: %+v, %v", status, err)
	}
	if err := scoped.RequeueFencedVMTurn(ctx, "managed", journal.FenceRequeueRequest{TurnSeq: 1, Epoch: 2, ExecutionID: "exec-test"}); err != nil {
		t.Fatal(err)
	}
	cursor := journal.VMReconcileCursor{
		At:       time.Date(2026, time.November, 23, 12, 1, 2, 123456000, time.UTC),
		TenantID: "tenant-a", SessionID: "prior",
	}
	page, err := coordinator.ListGlobalRecoverableVMPage(ctx, "ACTIVE", &cursor)
	if err != nil || len(page.Sessions) != 1 || page.Sessions[0].Scope != scope ||
		page.Next == nil || page.Next.TenantID != "tenant-b" || page.Next.SessionID != "managed" {
		t.Fatalf("reconcile page: %+v, %v", page, err)
	}
}
