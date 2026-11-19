//go:build state_integration

package space_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// realTaskExchanger is a TaskExchanger backed by the REAL Identity signer of the
// env: it mints a genuine KindTask token for the verified subject + server-fixed
// scope. It proves that the scoped token the handler forwards to State is one the
// real State server actually authorizes for that subject.
type realTaskExchanger struct{ e *cEnv }

func (r realTaskExchanger) Exchange(_ context.Context, h space.HumanPrincipal, task string) (string, string, error) {
	if task == "" {
		task = "task-" + h.Subject
	}
	tok, err := r.e.signer.Sign(auth.Claims{
		Audience: cAudience, Kind: auth.KindTask,
		Tenant: r.e.tenant, Deployment: r.e.deployment,
		Subject: h.Subject, Task: task, Root: "run-" + task,
		Context: "ctx-" + task, Grant: "grant-" + h.Subject, Depth: 0,
	}, 5*time.Minute)
	return tok, task, err
}

// authByHeader injects a verified principal derived from the X-Test-Subject header
// (stands in for the OIDC human verifier): the subject is server-verified, the
// browser cannot choose tenant/deployment.
func authByHeader(tenant, deployment string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s := r.Header.Get("X-Test-Subject")
			if s == "" {
				next.ServeHTTP(w, r)
				return
			}
			ctx := transport.WithPrincipal(r.Context(), transport.Principal{
				Scope: contracts.Scope{Tenant: tenant, Deployment: deployment, Subject: s},
			})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// TestSpaceHandlerScopedTaskRealState drives the production Space request path —
// RequireScopedTask + human->task exchange — over the REAL State server on
// PostgreSQL. It proves three things no offline test can:
//  1. A browser human bearer ("humanjwt-*") is NOT what reaches State; the turn
//     only succeeds because ExchangeMiddleware minted a REAL scoped Task token that
//     the real State server authorizes for that subject.
//  2. The scoped Task token reaches State for every handler call (AcceptTurn and
//     the durable ListRunHistory), and the run is durably persisted.
//  3. Subject isolation holds at the State boundary: a second subject's exchanged
//     token cannot read the first subject's thread (404, no leak).
func TestSpaceHandlerScopedTaskRealState(t *testing.T) {
	e := cNewEnv(t)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: "w-hsr"})
	h := space.NewHandler(space.HandlerConfig{
		Coordinator:       coord,
		Auth:              space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
		RequireScopedTask: true,
	})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)

	// Accept a turn as "ada" carrying only a human JWT bearer (NOT a State token).
	code, body := postSpaceAs(t, hs.URL, "AcceptTurn", map[string]any{
		"key": "hsr-ada", "context_id": "ctx-hsr-ada",
		"input": map[string]any{"role": "user", "content": "hi"}, "max_steps": 1,
	}, "ada")
	if code != 200 {
		t.Fatalf("AcceptTurn(ada) via human bearer + exchange = %d, want 200 (body=%s)", code, body)
	}
	threadID, _ := decodeMap(t, body)["thread_id"].(string)
	if threadID == "" {
		t.Fatalf("missing thread_id: %s", body)
	}

	// Durable run list for ada's thread also flows the exchanged scoped token.
	if c, b := postSpaceAs(t, hs.URL, "ListRunHistory", map[string]any{"thread_id": threadID}, "ada"); c != 200 {
		t.Fatalf("ListRunHistory(ada) = %d, want 200 (body=%s)", c, b)
	}

	// Foreign subject cannot read ada's thread (its own exchanged token is scoped
	// to itself, so State answers not-found).
	if c, b := postSpaceAs(t, hs.URL, "ListRunHistory", map[string]any{"thread_id": threadID}, "bob"); c != 404 {
		t.Fatalf("ListRunHistory(bob,ada-thread) = %d, want 404 (body=%s)", c, b)
	}
}
