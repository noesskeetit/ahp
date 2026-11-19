package workspaceapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func TestHumanCannotSubmitTrustedCheckVerdict(t *testing.T) {
	// A legacy sanctioned user edge can admit a verified human to the workspace
	// API. The verdict writer must refuse before it can consult any store.
	h := &Handler{}
	req := httptest.NewRequest(http.MethodPost, "/v1/workspace/SetCheck", strings.NewReader(`{"candidate_id":"draft","name":"ci","status":"passed"}`))
	req = req.WithContext(transport.WithPrincipal(req.Context(), transport.Principal{Scope: contracts.Scope{Tenant: "t", Subject: "owner", Deployment: "edge"}, Claims: auth.Claims{Kind: auth.KindHuman}}))
	rec := httptest.NewRecorder()
	h.setCheck(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("human verdict accepted: %d", rec.Code)
	}
}
