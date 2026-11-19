package stateapi

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
)

func TestRev31BundleIsTaskBoundAndReturnsFreshEmptyCut(t *testing.T) {
	e := rev7APIEnv(t)
	state.WithCommonRecords()(e.store)
	option, err := state.WithKVNamespacePolicies([]state.KVNamespacePolicy{{Tenant: e.tenant, Deployment: e.deployment, Namespace: []string{"app"}, Owner: "deployment"}})
	if err != nil {
		t.Fatal(err)
	}
	option(e.store)
	if err = e.store.Migrate(e.ctx); err != nil {
		t.Fatal(err)
	}
	original := e.handler.(*Handler)
	h, err := New(e.store, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, original.issue)
	if err != nil {
		t.Fatal(err)
	}
	e.srv.Config.Handler = h
	token := e.signTask(t, "bundle-task")
	status, thread := e.call(t, "CreateThread", token, `{"context_id":"ctx-1","key":"bundle-thread"}`)
	mustStatus(t, status, http.StatusOK, thread)
	body, _ := json.Marshal(map[string]any{"thread_id": thread["id"], "namespace": "common/research-2", "keys": []any{map[string]any{"namespace": []string{"app"}, "key": "state", "expected_owner": "deployment"}}})
	status, bundle := e.call(t, "ReadRecordsBundle", token, string(body))
	if status != http.StatusOK {
		t.Fatalf("MISSING_ATOMIC_READ_BUNDLE: actual mTLS endpoint returned %d code=%v, want 200 with explicit empty common and shared values", status, bundle["code"])
	}
	if bundle["common"] != nil || len(bundle["kv"].([]any)) != 1 || bundle["kv"].([]any)[0] != nil {
		t.Fatal("fresh thread not empty bundle", bundle)
	}
	foreign, x := e.signer.Sign(auth.Claims{Audience: testAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject, Task: "another-task", Root: "root", Context: "different-context", Grant: "grant", Depth: 0}, time.Minute)
	if x != nil {
		t.Fatal(x)
	}
	status, denied := e.call(t, "ReadRecordsBundle", foreign, string(body))
	mustStatus(t, status, http.StatusNotFound, denied)
	var tampered map[string]any
	_ = json.Unmarshal(body, &tampered)
	tampered["revision"] = 1
	b, _ := json.Marshal(tampered)
	status, denied = e.call(t, "ReadRecordsBundle", token, string(b))
	mustStatus(t, status, http.StatusBadRequest, denied)
	tampered["keys"].([]any)[0].(map[string]any)["expected_owner"] = "subject"
	delete(tampered, "revision")
	b, _ = json.Marshal(tampered)
	status, denied = e.call(t, "ReadRecordsBundle", token, string(b))
	mustStatus(t, status, http.StatusForbidden, denied)

	status, bundle = e.call(t, "ReadRecordsBundle", "", string(body))
	mustStatus(t, status, http.StatusForbidden, bundle)
}
