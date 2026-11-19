package identity_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Real-PostgreSQL tests for the Tier-1 ingress authority model (frozen contract
// items A and the reuse/attenuation ruling). They prove, not merely that the
// exchange returns 200, but that the MINTED grant is what it must be:
//
//   - re-exchanging a reused authority with narrower requested capabilities, or
//     after the human lost a permission, mints an ATTENUATED grant; the dropped
//     capability is denied by Store.Evaluate on the minted task, while a retained
//     capability still works;
//   - the response names the task the returned token is actually bound to;
//   - retiring one session does not break another session or the next login:
//     only a subject revocation blocks future logins.
//
// These skip on a machine without PostgreSQL and are executed by integration
// validation against the isolated AHP test database.

func exSeed(t *testing.T, h *exchangeHarness, perms []string) (identity.BrowserUser, string, string, string) {
	t.Helper()
	tag := sanitizeID(contracts.NewID())
	tenant := "acme-" + tag
	subject := "human-" + tag
	username := subject + "@acme.test"
	password := "correct-horse-battery-staple"
	u := identity.BrowserUser{UserID: "u-" + tag, Tenant: tenant, Subject: subject, DisplayName: "H", Email: username}
	if err := h.store.SeedBrowserUser(testContext(t), u, username, password, []string{"user"}, perms); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return u, username, password, tenant
}

// exLogin logs a user in on a fresh browser client and returns its human token.
func exLogin(t *testing.T, h *exchangeHarness, username, password string) (string, *http.Client) {
	t.Helper()
	bc := h.browserClient(t)
	if res := h.post(t, bc, browserPrefix+"/auth/login", map[string]string{"username": username, "password": password}, true); res.status != http.StatusOK {
		t.Fatalf("login: %d %s", res.status, res.body)
	}
	res := h.post(t, bc, browserPrefix+"/auth/token", map[string]string{}, true)
	if res.status != http.StatusOK {
		t.Fatalf("token: %d %s", res.status, res.body)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(res.body, &tok); err != nil || tok.AccessToken == "" {
		t.Fatalf("decode token: %v %s", err, res.body)
	}
	return tok.AccessToken, bc
}

type exResult struct {
	status int
	token  string
	task   string
}

func exExchange(t *testing.T, h *exchangeHarness, c *http.Client, humanToken, task string, caps []string) exResult {
	t.Helper()
	body := map[string]any{"human_token": humanToken, "task": task}
	if caps != nil {
		body["capabilities"] = caps
	}
	res := h.post(t, c, "/v1/identity/control/exchange", body, false)
	out := exResult{status: res.status}
	var v struct {
		Token string `json:"token"`
		Task  string `json:"task"`
	}
	_ = json.Unmarshal(res.body, &v)
	out.token, out.task = v.Token, v.Task
	return out
}

func exClaims(t *testing.T, h *exchangeHarness, tok, tenant, deployment string) auth.Claims {
	t.Helper()
	claims, err := h.verif.Verify(tok, testAudience, auth.KindTask,
		auth.WorkloadID(contracts.Scope{Tenant: tenant, Deployment: deployment}))
	if err != nil {
		t.Fatalf("verify minted token: %v", err)
	}
	return claims
}

func exEvaluateDenied(t *testing.T, ctx context.Context, h *exchangeHarness, scope contracts.Scope, task, cap string) {
	t.Helper()
	_, err := h.store.Evaluate(ctx, scope, task, cap)
	if err == nil {
		t.Fatalf("capability %q on task %s must be DENIED but Evaluate succeeded", cap, task)
	}
	if code := contracts.Code(err); code != contracts.ErrForbidden {
		t.Fatalf("capability %q denial code=%s body=%v", cap, code, err)
	}
}

func exEvaluateOK(t *testing.T, ctx context.Context, h *exchangeHarness, scope contracts.Scope, task, cap string) {
	t.Helper()
	if _, err := h.store.Evaluate(ctx, scope, task, cap); err != nil {
		t.Fatalf("capability %q on task %s must be allowed: %v", cap, task, err)
	}
}

func TestExchangeAttenuationOnReuse(t *testing.T) {
	h := newExchangeHarness(t, []string{exchangerDeployment}, "")
	ctx := testContext(t)
	u, username, password, tenant := exSeed(t, h, []string{"space:access", "space:write"})
	humanTok, _ := exLogin(t, h, username, password)

	workload := contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment}
	c := h.exchangerClient(t, workload)
	granted := contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment, Subject: u.Subject}
	authority := "space-authority:" + u.Subject + ":" + exchangerDeployment + ":sess-1"

	// (1) Full exchange: effective set == the human's permissions == the root
	// ceiling, so the minted task IS the authority task and carries both caps.
	full := exExchange(t, h, c, humanTok, authority, nil)
	if full.status != http.StatusOK {
		t.Fatalf("full exchange: %d", full.status)
	}
	cFull := exClaims(t, h, full.token, tenant, exchangerDeployment)
	if cFull.Task != authority {
		t.Fatalf("full exchange task should be the authority, got %q", cFull.Task)
	}
	if full.task != cFull.Task {
		t.Fatalf("response task %q must equal the token's bound task %q", full.task, cFull.Task)
	}
	exEvaluateOK(t, ctx, h, granted, cFull.Task, "space:write")

	// (2) Re-exchange the SAME authority with NARROWER requested capabilities.
	narrow := exExchange(t, h, c, humanTok, authority, []string{"space:access"})
	if narrow.status != http.StatusOK {
		t.Fatalf("narrow exchange: %d", narrow.status)
	}
	cNarrow := exClaims(t, h, narrow.token, tenant, exchangerDeployment)
	if cNarrow.Task == authority {
		t.Fatal("a narrowed exchange must NOT mint the broad authority task")
	}
	if narrow.task != cNarrow.Task {
		t.Fatalf("response must name the attenuated task %q, got %q", cNarrow.Task, narrow.task)
	}
	exEvaluateOK(t, ctx, h, granted, cNarrow.Task, "space:access")
	exEvaluateDenied(t, ctx, h, granted, cNarrow.Task, "space:write")

	// The broad token still works for what it legitimately holds: attenuation did
	// not mutate the durable root, it minted a separate narrower grant.
	exEvaluateOK(t, ctx, h, granted, cFull.Task, "space:write")

	// (3) DROP space:write from the human (keep space:access), same account, and
	// re-use the same session/authority asking for space:write. The minted grant
	// must not carry the removed permission even though it was requested.
	if err := h.store.SeedBrowserUser(ctx, u, username, password, []string{"user"}, []string{"space:access"}); err != nil {
		t.Fatalf("reseed narrower: %v", err)
	}
	dropped := exExchange(t, h, c, humanTok, authority, []string{"space:access", "space:write"})
	if dropped.status != http.StatusOK {
		t.Fatalf("dropped-perm exchange: %d %s", dropped.status, "")
	}
	cDropped := exClaims(t, h, dropped.token, tenant, exchangerDeployment)
	if cDropped.Task == authority {
		t.Fatal("with a removed permission the minted task must be attenuated, not the broad root")
	}
	exEvaluateDenied(t, ctx, h, granted, cDropped.Task, "space:write")
	exEvaluateOK(t, ctx, h, granted, cDropped.Task, "space:access")

	// And an escalation the human never held is refused outright.
	if got := exExchange(t, h, c, humanTok, "space-authority:esc", []string{"space:admin"}); got.status != http.StatusForbidden {
		t.Fatalf("escalating capability: want 403 got %d", got.status)
	}
}

func TestExchangeSessionRevocationIsolation(t *testing.T) {
	h := newExchangeHarness(t, []string{exchangerDeployment}, "")
	u, username, password, tenant := exSeed(t, h, []string{"space:access"})
	workload := contracts.Scope{Tenant: tenant, Deployment: exchangerDeployment}
	c := h.exchangerClient(t, workload)

	tok1, bc1 := exLogin(t, h, username, password) // session 1
	tok2, _ := exLogin(t, h, username, password)    // session 2, same subject

	// Two independent chats on two sessions both work.
	if r := exExchange(t, h, c, tok1, "chat-A", nil); r.status != http.StatusOK {
		t.Fatalf("session1 chat-A: %d", r.status)
	}
	if r := exExchange(t, h, c, tok2, "chat-B", nil); r.status != http.StatusOK {
		t.Fatalf("session2 chat-B: %d", r.status)
	}

	// Logging out session 1 retires only that session.
	if res := h.post(t, bc1, browserPrefix+"/auth/logout", map[string]string{}, true); res.status != http.StatusOK {
		t.Fatalf("logout: %d %s", res.status, res.body)
	}
	if r := exExchange(t, h, c, tok1, "chat-A2", nil); r.status != http.StatusForbidden {
		t.Fatalf("retired session must be denied: %d", r.status)
	}
	// The other session and its other chat are unaffected.
	if r := exExchange(t, h, c, tok2, "chat-B2", nil); r.status != http.StatusOK {
		t.Fatalf("other session must survive a different logout: %d", r.status)
	}
	// And a fresh login (new session) is unaffected.
	tok3, _ := exLogin(t, h, username, password)
	if r := exExchange(t, h, c, tok3, "chat-C", nil); r.status != http.StatusOK {
		t.Fatalf("fresh login must be unaffected by a prior logout: %d", r.status)
	}

	// Only an explicit SUBJECT revocation blocks future logins: it is enforced at
	// the session boundary, so a revoked subject cannot even start a new session.
	if err := h.store.RevokeSubject(testContext(t), tenant, u.Subject); err != nil {
		t.Fatalf("revoke subject: %v", err)
	}
	bc4 := h.browserClient(t)
	if res := h.post(t, bc4, browserPrefix+"/auth/login",
		map[string]string{"username": username, "password": password}, true); res.status == http.StatusOK {
		t.Fatal("subject revocation must block a future login, but login succeeded")
	}
}
