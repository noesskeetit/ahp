package space

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func TestHumanToolsProxyPreservesHumanDecisionAuthority(t *testing.T) {
	up := &planeUpstream{status: http.StatusForbidden, payload: `{"code":"FORBIDDEN"}`}
	srv := httptest.NewServer(up.handler())
	defer srv.Close()
	h, err := NewHumanToolsProxy(srv.URL, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{auth.KindTask, auth.KindHuman} {
		req := httptest.NewRequest(http.MethodPost, UIToolsPath+"Approve", strings.NewReader(`{"request_id":"req-1","digest":"digest","tool":"tool"}`))
		req.Header.Set("Authorization", "Bearer original-human-token")
		principal := transport.Principal{}
		principal.Claims.Kind = kind
		req = req.WithContext(transport.WithPrincipal(WithScopedTask(req.Context(), "must-not-forward-task"), principal))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status %d", rec.Code)
		}
		if kind == auth.KindTask && up.calls != 0 {
			t.Fatal("task authority reached approval API")
		}
	}
	if up.calls != 1 || up.auth != "Bearer original-human-token" || up.path != "/v1/tools/Approve" {
		t.Fatalf("wrong human forwarding: calls=%d path=%q", up.calls, up.path)
	}
	rec := doPlaneRequest(t, h, http.MethodPost, UIToolsPath+"Call", `{}`, "task-token")
	if rec.Code != 404 || up.calls != 1 {
		t.Fatal("tool execution escaped approval-only allowlist")
	}
}
