package stateapi

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRev32DeploymentTransferRequiresCoordinatorOnRealMTLS(t *testing.T) {
	e := rev7APIEnv(t)
	old := e.handler.(*Handler)
	h, err := NewWithAdmin(e.store, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, old.issue, AdminConfig{Audience: testAudience, Authorize: func(_ context.Context, c auth.Claims, cap string) error {
		if c.Subject != "state-admin" || (cap != AdminCapability && cap != DeploymentTransferCapability) {
			return contracts.NewError(contracts.ErrForbidden, "not transfer admin")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	e.srv.Config.Handler = h
	status, body := adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, "", "ExportDeploymentV3", `{"migration_id":"migration","native_build":"common-owner/research-3"}`, "state-admin")
	if status != http.StatusForbidden || body["code"] != "FORBIDDEN" {
		t.Fatalf("MISSING_DEPLOYMENT_TRANSFER_GATE: actual mTLS route status=%d code=%v; want explicit forbidden without trusted offline coordinator", status, body["code"])
	}
}

func TestRev32DeploymentAdminMTLSPositiveAndBoundaries(t *testing.T) {
	e := rev7APIEnv(t)
	state.WithCommonPayloadObjects()(e.store)
	unit := state.DeploymentUnit{Tenant: e.tenant, Deployment: e.deployment}
	frozen, allowTransfer := false, false
	state.WithDeploymentTransfer(state.DeploymentTransferConfig{NativeBuild: "common-owner/research-3", Authorize: func(_ context.Context, c state.DeploymentCustody) error {
		if !frozen || c.Unit != unit || c.MigrationID != "move" {
			return contracts.NewError(contracts.ErrForbidden, "no frozen custody for unit")
		}
		return nil
	}})(e.store)
	if err := e.store.Migrate(e.ctx); err != nil {
		t.Fatal(err)
	}
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	th, err := e.store.CreateThread(e.ctx, sc, "private-alice", "private-alice")
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.store.CreateRun(e.ctx, sc, th.ID, "native", "native-alice", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := e.store.ClaimRunFor(e.ctx, e.tenant, e.deployment, "native-worker", run.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatal("lease", err)
	}
	var doc state.CommonDocument
	if err = json.Unmarshal([]byte(`{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","records":{"s":{"kind":"session","attributes":{"v":42},"relations":{}}}}`), &doc); err != nil {
		t.Fatal(err)
	}
	if _, err = e.store.CommitRecords(e.ctx, sc, state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, state.CommonCommit{Namespace: "native", Key: "native", Document: doc}); err != nil {
		t.Fatal(err)
	}
	original := e.handler.(*Handler)
	h, err := NewWithAdmin(e.store, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, original.issue, AdminConfig{Audience: testAudience, Authorize: func(_ context.Context, c auth.Claims, cap string) error {
		if c.Subject != "state-admin" || cap != AdminCapability && (cap != DeploymentTransferCapability || !allowTransfer) {
			return contracts.NewError(contracts.ErrForbidden, "no deployment-transfer permission")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	e.srv.Config.Handler = h
	input := `{"migration_id":"move","native_build":"common-owner/research-3"}`
	status, body := adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, "", "ExportDeploymentV3", input, "state-admin")
	mustStatus(t, status, http.StatusForbidden, body)
	allowTransfer = true
	status, body = adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, "", "ExportDeploymentV3", input, "state-admin")
	mustStatus(t, status, http.StatusForbidden, body)
	frozen = true
	status, body = adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, "", "ExportDeploymentV3", input, "ordinary-human")
	mustStatus(t, status, http.StatusForbidden, body)
	token := e.signTask(t, "task")
	status, body = doCall(t, e.srv.Client(), e.srv.URL+"/v1/state-admin/ExportDeploymentV3", http.MethodPost, token, `{"target":{"deployment":"`+e.deployment+`"},"input":`+input+`}`)
	mustStatus(t, status, http.StatusForbidden, body)
	status, body = adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, "alice", "ExportDeploymentV3", input, "state-admin")
	mustStatus(t, status, http.StatusBadRequest, body)
	status, body = adminCall(t, e, e.srv.Client(), e.tenant, "another-deployment", "", "ExportDeploymentV3", input, "state-admin")
	mustStatus(t, status, http.StatusForbidden, body)
	status, body = adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, "", "ExportDeploymentV3", input, "state-admin")
	mustStatus(t, status, http.StatusOK, body)
	raw, _ := json.Marshal(body)
	var image state.DeploymentV3
	if err = json.Unmarshal(raw, &image); err != nil {
		t.Fatal(err)
	}
	if image.Manifest.Unit != unit || !reflect.DeepEqual(image.Manifest.Subjects, []string{"alice"}) || image.Manifest.Counts["common_cuts"] != 1 {
		t.Fatal("admin identity was confused with data ownership")
	}
	if err = state.ValidateDeploymentV3(image, unit, "common-owner/research-3"); err != nil {
		t.Fatal(err)
	}
	bad := strings.Replace(string(raw), `"format":"ahp-state-deployment/3"`, `"format":"x","format":"ahp-state-deployment/3"`, 1)
	human, err := e.signer.Sign(auth.Claims{Kind: auth.KindHuman, Tenant: e.tenant, Subject: "state-admin", Audience: testAudience}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	status, body = doCall(t, e.srv.Client(), e.srv.URL+"/v1/state-admin/ImportDeploymentV3", http.MethodPost, human, `{"target":{"deployment":"`+e.deployment+`"},"input":{"native_build":"common-owner/research-3","archive":`+bad+`}}`)
	mustStatus(t, status, http.StatusBadRequest, body)
}
