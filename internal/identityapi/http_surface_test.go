// This file pins the HTTP surface contract of the Identity handler itself: a path
// inside a namespace this process owns, but with no route behind it, is a wrong API
// call and must be answered as one.
//
// Why it exists (measured on the live human edge :18081 at 2026-11-01T09:20Z, pin
// debf8b1, read-only curls):
//
//	/v1/identity/nope      -> 404 text/plain  "404 page not found"
//	/u-ahp/v1/nope         -> 404 text/plain  "404 page not found"
//	/u-ahp/v1/auth/nope    -> 404 text/plain  "404 page not found"
//
// while every other reserved namespace on the same origin answers the contract
// envelope ({"code":"NOT_FOUND"}) from web/uiHandler and the edge catch-all. So an
// "endpoint answered" smoke had to special-case two prefixes for no reason, and the
// browser prefix - the one a login bug lands on - answered in a format no client
// parses. decisions/2026-10-31-http-surface-no-false-200.md: a reserved unknown path
// is 404 with {"code":"NOT_FOUND"}.
//
// The second half matters as much as the first: the naive fix (a bare {rest...}
// catch-all) also eats Go's honest method-mismatch answer, which was measured before
// writing this (probe run/hive-director-vm-RESUME-20261101T045812Z/probe-mux-20261101T0920Z:
// GET on a POST-only route returned 404 "route not found" instead of 405 + Allow).
// A client that is one verb away from working must be told so, and
// tools/stand-html-render-check.sh reads that 405 as the signal that a shell is
// served while the login route is dead. So the 405 keeps its verb, its Allow header
// and its bytes.
package identityapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/identityapi"
)

func surfaceHandler() http.Handler {
	// No store, verifier or signer: every assertion here is about routing, and a
	// routing answer must not need a database to be provable.
	return identityapi.Handler(identityapi.Config{
		Browser: &identityapi.BrowserConfig{FeatureFlags: map[string]bool{"space_chat_ui": true}},
	})
}

func TestUnroutedPathInOwnedNamespaceAnswersContractJSON(t *testing.T) {
	h := surfaceHandler()

	for _, path := range []string{
		"/u-ahp/v1/nope",
		"/u-ahp/v1/",
		"/u-ahp/v1/auth/nope",
		"/v1/identity/nope",
		"/v1/identity/roots", // registered as POST only, so for GET see the 405 leg below
	} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			if path == "/v1/identity/roots" {
				continue // method-mismatch case, covered by the next test
			}

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))

			if rec.Code != http.StatusNotFound {
				t.Errorf("%s %s: status=%d, want 404 (body %q)", method, path, rec.Code, rec.Body.String())
				continue
			}

			var body struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Errorf("%s %s: body is not JSON: %v (%q)", method, path, err, rec.Body.String())
				continue
			}
			// Exact case: "not_found" was measured on the live edge on another
			// reserved branch and rejected (decision 2026-10-31-http-surface-no-false-200).
			if body.Code != "NOT_FOUND" {
				t.Errorf("%s %s: code=%q, want exactly NOT_FOUND", method, path, body.Code)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" && ct != "application/json; charset=utf-8" {
				t.Errorf("%s %s: content-type=%q, want application/json", method, path, ct)
			}
		}
	}
}

// TestKnownPathWrongMethodKeepsMethodNotAllowed is the positive control: the
// contract catch-all must refuse routes that do not exist, not hide the ones that do.
func TestKnownPathWrongMethodKeepsMethodNotAllowed(t *testing.T) {
	h := surfaceHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/u-ahp/v1/auth/login", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /u-ahp/v1/auth/login: status=%d, want 405 (body %q)", rec.Code, rec.Body.String())
	}
	if allow := rec.Header().Get("Allow"); allow != "POST" {
		t.Errorf("Allow=%q, want POST", allow)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("content-type=%q, want the same text answer Go gave before this change", ct)
	}
}

// TestMountedRoutesStillResolve catches a catch-all that swallows real routes.
// /healthz needs no store, no tokens and no database.
func TestMountedRoutesStillResolve(t *testing.T) {
	h := surfaceHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz: status=%d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
}
