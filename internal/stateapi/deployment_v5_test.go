package stateapi

import (
	"context"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"net/http"
	"testing"
	"time"
)

func TestRev46V5AdministrativeMTLSRouteFailsExplicitlyWithoutOwner(t *testing.T) {
	e := rev7APIEnv(t)
	state.WithCommonRecordVersions()(e.store)
	state.WithDeploymentTransfer(state.DeploymentTransferConfig{NativeBuild: "common-owner/research-3", Authorize: func(context.Context, state.DeploymentCustody) error { return nil }})(e.store)
	if err := e.store.Migrate(e.ctx); err != nil {
		t.Fatal(err)
	}
	original := e.handler.(*Handler)
	h, err := NewWithAdmin(e.store, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, original.issue, AdminConfig{Audience: testAudience, Authorize: func(context.Context, auth.Claims, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	e.srv.Config.Handler = h
	token, err := e.signer.Sign(auth.Claims{Kind: auth.KindHuman, Tenant: e.tenant, Subject: "migration-admin", Audience: testAudience}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	status, body := doCall(t, e.srv.Client(), e.srv.URL+"/v1/state-admin/ExportDeploymentV5", http.MethodPost, token, `{"target":{"deployment":"`+e.deployment+`"},"input":{"migration_id":"interval-move","native_build":"common-owner/research-3"}}`)
	if status != 503 || body["code"] != "UNAVAILABLE" {
		t.Fatalf("MISSING_INTERVAL_TRANSFER_ROUTE: status=%d code=%v", status, body["code"])
	}
}
