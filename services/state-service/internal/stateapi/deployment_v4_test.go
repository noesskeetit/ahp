package stateapi

import (
	"context"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"net/http"
	"testing"
	"time"
)

// New method must be a real admin route with explicit dependency refusal,
// never a client offline boolean or an archive that silently omits roots.
func TestRev36ArtifactDependentDeploymentRequiresConfiguredOwnerOverMTLS(t *testing.T) {
	e := rev7APIEnv(t)
	state.WithCommonPayloadObjects()(e.store)
	state.WithCommonArtifactReferences()(e.store)
	state.WithDeploymentTransfer(state.DeploymentTransferConfig{NativeBuild: "common-owner/research-3", Authorize: func(context.Context, state.DeploymentCustody) error { return nil }})(e.store)
	if err := e.store.Migrate(e.ctx); err != nil {
		t.Fatal(err)
	}
	original := e.handler.(*Handler)
	handler, err := NewWithAdmin(e.store, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, original.issue, AdminConfig{Audience: testAudience, Authorize: func(context.Context, auth.Claims, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	e.srv.Config.Handler = handler
	human, err := e.signer.Sign(auth.Claims{Kind: auth.KindHuman, Tenant: e.tenant, Subject: "migration-admin", Audience: testAudience}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	status, body := doCall(t, e.srv.Client(), e.srv.URL+"/v1/state-admin/ExportDeploymentV4", http.MethodPost, human, `{"target":{"deployment":"`+e.deployment+`"},"input":{"migration_id":"artifact-dependent-move","native_build":"common-owner/research-3"}}`)
	if status != http.StatusServiceUnavailable || body["code"] != "UNAVAILABLE" {
		t.Fatalf("MISSING_ARTIFACT_DEPENDENCY_TRANSFER_CONTRACT: status=%d code=%v message=%v; want explicit unavailable without a configured shared Artifact owner", status, body["code"], body["message"])
	}
}
