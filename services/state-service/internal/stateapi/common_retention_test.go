package stateapi

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/state"
	"net/http"
	"testing"
	"time"
)

func TestCommonRetentionMTLSRootTaskBindingAndHumanPrune(t *testing.T) {
	e := rev7APIEnv(t)
	state.WithCommonRecordVersions()(e.store)
	option, err := state.WithCommonRetention([]string{"test/1"})
	if err != nil {
		t.Fatal(err)
	}
	option(e.store)
	if err := e.store.Migrate(e.ctx); err != nil {
		t.Fatal(err)
	}
	old := e.handler.(*Handler)
	h, err := NewWithAdmin(e.store, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, old.issue, AdminConfig{Audience: testAudience, Authorize: func(context.Context, auth.Claims, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	e.srv.Config.Handler = h
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject}
	thread, err := e.store.CreateThread(e.ctx, sc, "ctx-1", "retention-thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.store.CreateRun(e.ctx, sc, thread.ID, "retention-run", "retention-task", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := e.store.ClaimRunFor(e.ctx, sc.Tenant, sc.Deployment, "writer", run.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatal(err)
	}
	fence := state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	for i := int64(1); i <= 3; i++ {
		attrs, _ := json.Marshal(map[string]int64{"number": i})
		_, err := e.store.CommitRecords(e.ctx, sc, fence, state.CommonCommit{Namespace: "native", Key: "cut-" + string(rune('0'+i)), ExpectedRevision: i - 1, Document: state.CommonDocument{Format: state.CommonFormat, Profile: "test/1", Root: "s", Records: map[string]state.CommonRecord{"s": {Kind: "session", Attributes: attrs, Relations: map[string][]string{}}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	body, _ := json.Marshal(map[string]any{"fence": fence, "root": map[string]any{"namespace": "native", "key": "branch", "operation_key": "pin", "revision": 2, "expected_version": 0}})
	token := e.signTask(t, run.TaskID)
	status, result := e.call(t, "PutCommonRoot", token, string(body))
	if status != http.StatusOK {
		t.Fatalf("ROOT_RETENTION_ROUTE_MISSING: status=%d code=%v", status, result["code"])
	}
	client, err := sdk.New(sdk.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	revision := int64(2)
	pin, err := client.PutCommonRoot(e.ctx, sdk.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, sdk.CommonRootRequest{Namespace: "native", Key: "branch", OperationKey: "pin", Revision: &revision})
	if err != nil || pin.Version != 1 || pin.Revision == nil || *pin.Revision != 2 {
		t.Fatal("SDK pin lost identical replay", err, pin)
	}
	wrong, err := e.signer.Sign(auth.Claims{Audience: testAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject, Task: run.TaskID, Context: "wrong-context", Grant: "grant", Root: "root"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	status, denied := e.call(t, "PutCommonRoot", wrong, string(body))
	mustStatus(t, status, http.StatusNotFound, denied)
	if err := e.store.Release(e.ctx, sc, run.ID, lease.Worker, lease.Epoch, state.RunDone); err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(state.CommonPruneRequest{ThreadID: thread.ID, Namespace: "native", Key: "prune", ExpectedRevision: 3, KeepFromRevision: 3})
	adminBody, _ := json.Marshal(map[string]any{"target": map[string]any{"deployment": e.deployment, "subject": testSubject}, "input": json.RawMessage(input)})
	status, denied = doCall(t, e.srv.Client(), e.srv.URL+"/v1/state-admin/PruneCommonHistory", http.MethodPost, token, string(adminBody))
	mustStatus(t, status, http.StatusForbidden, denied)
	status, report := adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject, "PruneCommonHistory", string(input), "state-admin")
	if status != http.StatusOK || report["archived_cuts"] != float64(1) || report["readable_from"] != float64(2) {
		t.Fatal("human retention did not respect root", status, report["code"])
	}
	if _, err := e.store.ReadRecords(e.ctx, sc, thread.ID, "native", 1); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("archived cut still readable", err)
	}
	if _, err := e.store.ReadRecords(e.ctx, sc, thread.ID, "native", 2); err != nil {
		t.Fatal("root was not protected", err)
	}
}
