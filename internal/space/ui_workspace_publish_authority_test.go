package space

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func TestWorkspaceExplicitPublishPreservesVerifiedHumanAuthority(t *testing.T) {
	for _, op := range []string{"Publish", "GetWorkspace"} {
		t.Run(op, func(t *testing.T) {
			up := &planeUpstream{payload: `{}`}
			srv := httptest.NewServer(up.handler())
			defer srv.Close()
			h := planeHandlerFor(t, WorkspacePlane, srv)
			r := httptest.NewRequest(http.MethodPost, UIWorkspacePath+op, strings.NewReader(`{}`))
			r.Header.Set("Authorization", "Bearer verified-human-session")
			ctx := WithScopedTask(r.Context(), "private-agent-task")
			ctx = transport.WithPrincipal(ctx, transport.Principal{Claims: auth.Claims{Kind: auth.KindHuman}})
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r.WithContext(ctx))
			want := "Bearer private-agent-task"
			if op == "Publish" {
				want = "Bearer verified-human-session"
			}
			if w.Code != http.StatusOK || up.auth != want {
				t.Fatalf("%s forwarded wrong authority: status=%d", op, w.Code)
			}
		})
	}
}

func TestWorkspaceExplicitPublishRefusesUnverifiedOrTaskIdentity(t *testing.T) {
	up := &planeUpstream{payload: `{}`}
	srv := httptest.NewServer(up.handler())
	defer srv.Close()
	h := planeHandlerFor(t, WorkspacePlane, srv)
	for _, kind := range []string{"", auth.KindTask} {
		r := httptest.NewRequest(http.MethodPost, UIWorkspacePath+"Publish", strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer presented-credential")
		ctx := WithScopedTask(r.Context(), "private-agent-task")
		if kind != "" {
			ctx = transport.WithPrincipal(ctx, transport.Principal{Claims: auth.Claims{Kind: kind}})
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r.WithContext(ctx))
		if w.Code != http.StatusForbidden || up.calls != 0 {
			t.Fatal("non-human identity reached explicit workspace publication")
		}
	}
}
