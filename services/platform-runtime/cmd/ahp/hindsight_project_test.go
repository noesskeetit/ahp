//go:build ahp_data

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	memsdk "github.com/ahp-platform/ahp/sdk/go/memory"
	statesdk "github.com/ahp-platform/ahp/sdk/go/state"
)

// runNativeProjectCompletion covers the supported automatic contract: a
// payload.type=turn State run, released through the real task/mTLS API. A generic
// custom SDK run is not automatically a Memory source merely by being terminal.
func runNativeProjectCompletion(t *testing.T, ctx context.Context, s *server, ids *identity.Store, verifier *auth.Verifier, signer *auth.Signer, mux *http.ServeMux, origin string, httpClient *http.Client, scope contracts.Scope) {
	t.Helper()
	_, ws, _, err := s.newDataPlanes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	project := "orion-" + contracts.NewID()
	if _, err = ws.Create(ctx, scope, project); err != nil {
		t.Fatal(err)
	}
	contextID := scope.Tenant + ":project-context"
	const task = "project-completion-task"
	// Short grant intentionally selects the documented immediate-flush path;
	// it does not change the default five-turn/two-minute project buffer.
	_, err = ids.CreateRoot(ctx, scope, identity.RootRequest{Task: task, Context: contextID, Deployment: scope.Deployment, Capabilities: []string{"state", "memory", "memory:review", "memory:delete"}, TTL: 3 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	token, err := ids.MintTask(ctx, scope, task, signer, "ahp", 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := ids.MintAuthorityTask(ctx, scope, task, signer, "ahp", 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bind, _ := json.Marshal(map[string]string{"context_id": contextID, "project_id": project})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+"/v1/memory/bind", bytes.NewReader(bind))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+authority)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "project-binding")
	response, err := httpClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("ingress project bind: %d", response.StatusCode)
	}
	store := state.New(s.pool, state.WithRunCompletionOutbox())
	if err = migrateStateAll(ctx, store); err != nil {
		t.Fatal(err)
	}
	handler, err := stateapi.New(store, verifier, ids.Authorize, func(ctx context.Context, scope contracts.Scope, task string) (string, error) {
		return ids.MintTask(ctx, scope, task, signer, "ahp", 2*time.Minute)
	})
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle("/v1/state/", handler)
	stateClient, err := statesdk.New(statesdk.Config{BaseURL: origin, HTTPClient: httpClient, TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := stateClient.CreateThread(ctx, contextID, "project-thread")
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"type": "turn", "project_id": project, "input": "Project Orion has adopted PostgreSQL for durable operation receipts. Alice prefers concise Russian explanations."})
	run, err := stateClient.CreateRun(ctx, thread.ID, "project-run", payload)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := stateClient.ClaimRunFor(ctx, "memory-project-worker", 60000, run.ID)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v", err)
	}
	token = claimed.TaskToken
	fence := statesdk.Fence{RunID: run.ID, Worker: claimed.Lease.Worker, Epoch: claimed.Lease.Epoch}
	_, err = stateClient.AppendStream(ctx, fence, run.ID+":model:0", "model", map[string]string{"content": "Project Orion decision confirmed: store durable operation receipts in PostgreSQL, with idempotency keys for mutations."})
	if err != nil {
		t.Fatal(err)
	}
	if err = stateClient.Release(ctx, fence, statesdk.RunDone); err != nil {
		t.Fatal(err)
	}
	client, err := memsdk.New(origin, func(context.Context) (string, error) { return token, nil }, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	query := memsdk.SearchRequest{ProjectID: project, Scope: "project", Query: "What database did Project Orion choose for durable operation receipts?"}
	var facts memsdk.SearchResult
	deadline := time.Now().Add(75 * time.Second)
	for {
		facts, err = client.Search(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		if len(facts.Facts) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("State completion did not produce native project facts")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	found := false
	for _, fact := range facts.Facts {
		if strings.Contains(strings.ToLower(fact.Text), "postgres") {
			found = true
		}
	}
	if !found || len(facts.Degraded) > 0 {
		t.Fatal("native project recall did not recover the recorded database decision")
	}
	// Adding a participant immediately closes access to private project memory.
	if err = ws.AddMember(ctx, scope, project, "bob"); err != nil {
		t.Fatal(err)
	}
	_, err = client.Search(ctx, query)
	var denied *memsdk.Error
	if !errors.As(err, &denied) || denied.Status != 403 {
		t.Fatalf("shared Workspace retained private Memory access: %v", err)
	}
	admin, err := memsdk.New(origin, func(context.Context) (string, error) { return authority, nil }, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Management().Clear(ctx, "project-clear", memsdk.ClearRequest{ProjectID: project, Scope: memsdk.Project}); err != nil {
		t.Fatal(err)
	}
	t.Logf("native project completion PASS: State SDK run=%s release=done -> trusted outbox -> Hindsight facts=%d; shared Workspace403; owner clear accepted", run.ID, len(facts.Facts))
}
