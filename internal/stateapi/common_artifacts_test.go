package stateapi

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"net/http"
	"testing"
	"time"
)

// Real mTLS/PG RED: new explicit reference metadata is absent from the API.
// Desired refusal is UNAVAILABLE until a configured State/Artifact reference
// owner exists; silently accepting a descriptor as opaque is not integration.
func TestRev34ExplicitCommonArtifactRefRequiresConfiguredOwnerOverMTLS(t *testing.T) {
	e := rev7APIEnv(t)
	state.WithCommonPayloadObjects()(e.store)
	if x := e.store.Migrate(e.ctx); x != nil {
		t.Fatal(x)
	}
	original := e.handler.(*Handler)
	h, handlerErr := New(e.store, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, original.issue)
	if handlerErr != nil {
		t.Fatal(handlerErr)
	}
	e.srv.Config.Handler = h
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject}
	th, x := e.store.CreateThread(e.ctx, scope, "ctx-1", "artifact-thread")
	if x != nil {
		t.Fatal(x)
	}
	run, x := e.store.CreateRun(e.ctx, scope, th.ID, "task", "artifact-run", json.RawMessage(`{}`))
	if x != nil {
		t.Fatal(x)
	}
	lease, x := e.store.ClaimRunFor(e.ctx, e.tenant, e.deployment, "worker", run.ID, time.Minute)
	if x != nil || lease == nil {
		t.Fatal(x)
	}
	ref := map[string]any{"id": "artifact-1", "tenant": e.tenant, "deployment": e.deployment, "subject": testSubject, "context_id": "ctx-1", "task_id": "task", "version": "version-1", "sha256": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "size": 1048577, "labels": map[string]any{}}
	marker := map[string]any{"encoding": state.CheckpointArtifactEncoding, "ref": ref}
	request := map[string]any{"fence": state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, "namespace": "common/research-2", "key": "common-artifact", "expected_revision": 0, "document": map[string]any{"format": "ahp-common-semantic-records/research-2", "profile": "native-explicit-artifact/research-1", "root": "root", "records": map[string]any{"root": map[string]any{"kind": "session", "attributes": marker, "relations": map[string]any{}}}}, "artifact_references": []any{map[string]any{"record_id": "root", "artifact": ref}}}
	body, x := json.Marshal(request)
	if x != nil {
		t.Fatal(x)
	}
	status, response := e.call(t, "CommitRecords", e.signTask(t, "task"), string(body))
	if status != http.StatusServiceUnavailable || response["code"] != "UNAVAILABLE" {
		t.Fatalf("MISSING_COMMON_ARTIFACT_OWNER_CONTRACT: actual status=%d code=%v message=%v; want explicit unavailable until safe owner configured", status, response["code"], response["message"])
	}
	var cuts int
	if x = e.store.Pool().QueryRow(e.ctx, `SELECT count(*) FROM ahp_state.common_cuts`).Scan(&cuts); x != nil {
		t.Fatal(x)
	}
	if cuts != 0 {
		t.Fatal(fmt.Sprint("unprotected cut published ", cuts))
	}
}
