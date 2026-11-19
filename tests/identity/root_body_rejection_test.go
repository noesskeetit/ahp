// This file pins the rejection contract of the Identity task/human HTTP face: a body
// the handler cannot read is a 400 carrying a reason, never a 200 carrying nothing.
//
// Why it exists. transport.ReadJSON decodes strictly (unknown fields, duplicate keys,
// trailing data) and WRITES NOTHING - it only returns the contract error. Every
// handler in internal/identityapi/handlers.go answered it with
//
//	if err := transport.ReadJSON(w, r, &body, 0); err != nil { return }
//
// so net/http completed the exchange as 200 with an empty body: the write the client
// believed it made never happened, and nothing on the wire says so. Measured by
// helper10 on canon 74d9f1b5 with the exact shape reported from a live Space journey:
//
//	POST /v1/identity/roots/{task}/delegate  {"ttl_out":"30m"}  -> 200 ""   (before fix)
//
// Space's client tests `status != 201`, finds no grant, and can only report
// "delegation did not happen" - the reason was discarded twice.
// decisions/2026-10-31-http-surface-no-false-200.md forbids this answer shape for
// unrouted paths; an unreadable body on a routed path is the same lie.
package identity_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// postRaw sends a byte-exact body. It exists because the cases below are bodies that
// cannot be represented as a Go value, which is precisely what is under test.
func postRaw(t *testing.T, h *harness, c *http.Client, method, path, bearer, raw string) response {
	t.Helper()
	req, err := http.NewRequestWithContext(testContext(t), method, h.baseURL+path, bytes.NewReader([]byte(raw)))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return response{status: res.StatusCode, body: body}
}

// requireRejected asserts the answer a client can act on: the 400 class, a contract
// envelope, and a message/reason that names what was wrong.
func requireRejected(t *testing.T, res response, wantFragment string) {
	t.Helper()
	// A mint answer carries a credential: keep it out of test logs and reports.
	shown := res.body
	if bytes.Contains(shown, []byte(`"token"`)) {
		shown = []byte(`{"token":"<redacted>"}`)
	}
	if res.status == http.StatusOK && len(bytes.TrimSpace(res.body)) == 0 {
		t.Fatalf("false 200: status=200 with an empty body - a client cannot distinguish " +
			"an accepted request from a rejected one")
	}
	if res.status != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s, want 400", res.status, shown)
	}
	var envelope struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal(res.body, &envelope); err != nil {
		t.Fatalf("400 without a contract envelope: body=%s err=%v", shown, err)
	}
	if envelope.Code != string(contracts.ErrInvalid) {
		t.Errorf("code=%q, want %q", envelope.Code, contracts.ErrInvalid)
	}
	combined := strings.ToLower(envelope.Message + " " + envelope.Reason)
	if !strings.Contains(combined, strings.ToLower(wantFragment)) {
		t.Errorf("rejection does not name the problem: message=%q reason=%q want fragment %q",
			envelope.Message, envelope.Reason, wantFragment)
	}
}

func TestDelegateRejectsUnusableBodyAndWritesNothing(t *testing.T) {
	h := newHarness(t)
	scope := uniqueScope(t)
	h.createRootStore(t, scope, "root-rj", true, nil)
	tok := h.mint(t, scope, "root-rj")
	c := h.taskClient(t, scope)

	for _, tc := range []struct{ name, raw, fragment string }{
		{"wrong field name", `{"task":"child-rj","context":"ctx-rj","ttl_out":"30m"}`, "ttl_out"},
		{"malformed json", `{"task":"child-rj",`, "json"},
		{"empty body", ``, "empty"},
		{"value of wrong type", `{"task":42,"context":"ctx-rj","ttl":"30m"}`, "task"},
		{"missing ttl", `{"task":"child-rj","context":"ctx-rj"}`, "ttl"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireRejected(t, postRaw(t, h, c, http.MethodPost,
				"/v1/identity/roots/root-rj/delegate", tok, tc.raw), tc.fragment)
			// Rejected means not written: the child the body named must not exist.
			got := h.do(t, c, http.MethodGet, "/v1/identity/roots/child-rj", tok, nil)
			if got.status != http.StatusNotFound {
				t.Errorf("child-rj exists after a rejected delegate: status=%d body=%s",
					got.status, got.body)
			}
		})
	}
}

func TestSiblingIdentityRoutesRejectUnusableBodies(t *testing.T) {
	h := newHarness(t)
	scope := uniqueScope(t)
	h.createRootStore(t, scope, "root-sb", true, nil)
	tok := h.mint(t, scope, "root-sb")
	task := h.taskClient(t, scope)
	control := h.controlClient(t, scope.Tenant)

	t.Run("create-root", func(t *testing.T) {
		raw := `{"task":"root-sb2","context":"` + ctxID(scope, "r2") +
			`","capabilities":["identity:read"],"ttl":"1h","deployment":"` + scope.Deployment +
			`","ttl_out":"1h"}`
		requireRejected(t, postRaw(t, h, h.human, http.MethodPost,
			"/v1/identity/roots", h.signHuman(scope), raw), "ttl_out")
		if _, err := h.store.Get(testContext(t), scope, "root-sb2"); err == nil {
			t.Error("root-sb2 was created by a request the server could not read")
		}
	})

	t.Run("evaluate", func(t *testing.T) {
		requireRejected(t, postRaw(t, h, task, http.MethodPost,
			"/v1/identity/roots/root-sb/evaluate", tok, `{"cap":"identity:read"}`), "cap")
	})

	t.Run("labels", func(t *testing.T) {
		requireRejected(t, postRaw(t, h, task, http.MethodPost,
			"/v1/identity/roots/root-sb/labels", tok, `{"subject":["s"]}`), "subject")
	})

	t.Run("control-cancel-subtree", func(t *testing.T) {
		requireRejected(t, postRaw(t, h, control, http.MethodPost,
			"/v1/identity/control/cancel-subtree", "", `{"tenent":"t"}`), "tenent")
	})
}

// TestMintToleratesAbsentBodyButRejectsUnusableBody pins both halves of the mint
// contract: an absent body is the documented way to mint with defaults, while a body
// that is present and unreadable must not be answered with a token - the route used to
// discard the decode error and sign a credential for ttl/audience it never parsed.
func TestMintToleratesAbsentBodyButRejectsUnusableBody(t *testing.T) {
	h := newHarness(t)
	scope := uniqueScope(t)
	h.createRootStore(t, scope, "root-mj", true, nil)
	tok := h.mint(t, scope, "root-mj")
	c := h.taskClient(t, scope)

	requireRejected(t, postRaw(t, h, c, http.MethodPost,
		"/v1/identity/roots/root-mj/mint", tok, `{"ttl_out":"5m"}`), "ttl_out")

	got := h.do(t, c, http.MethodPost, "/v1/identity/roots/root-mj/mint", tok, nil)
	if got.status != http.StatusOK {
		t.Fatalf("mint without a body: status=%d want 200 with defaults", got.status)
	}
	var out map[string]string
	if err := json.Unmarshal(got.body, &out); err != nil || out["token"] == "" {
		t.Fatalf("mint without a body returned no token: body=%s err=%v", got.body, err)
	}
}
