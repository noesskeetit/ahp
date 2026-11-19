package identityapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/identityapi"
)

// The Human -> authorized Space -> scoped task exchange is the only product path
// that turns a live human principal into a task token whose budget is no wider
// than its parent (PDF rev 3.2 requirement 6: "the subject is the same, rights
// and budget are no wider than the parent"). These tests pin WHEN the route
// exists; the semantics of the token it mints are pinned by the exchange suite.
// Routing is provable without a database, so none is used here.

func postControlExchange(h http.Handler) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/identity/control/exchange", nil))
	return rec
}

// TestControlExchangeAbsentWithoutBrowserOrFlag is the off leg: a plain control
// plane still answers the contract 404, exactly as before the flag existed.
func TestControlExchangeAbsentWithoutBrowserOrFlag(t *testing.T) {
	rec := postControlExchange(identityapi.Handler(identityapi.Config{}))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("no flags: status=%d, want 404 (body %q)", rec.Code, rec.Body.String())
	}
}

// TestControlExchangeMountedByExplicitFlag is the on leg: the explicit flag
// mounts the route with no browser surface present, so the request reaches the
// workload transport (which refuses an unverified workload certificate) instead
// of being told the route does not exist.
func TestControlExchangeMountedByExplicitFlag(t *testing.T) {
	rec := postControlExchange(identityapi.Handler(identityapi.Config{ControlExchange: true}))
	if rec.Code == http.StatusNotFound {
		t.Fatalf("ControlExchange=true: route still 404, not mounted (body %q)", rec.Body.String())
	}
	if rec.Code != http.StatusUnauthorized && rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d, want 401/403 from the workload transport (body %q)", rec.Code, rec.Body.String())
	}
}

// TestBrowserCompositionStillMountsControl is the regression leg: the browser
// composition keeps the route it has always had.
func TestBrowserCompositionStillMountsControl(t *testing.T) {
	rec := postControlExchange(identityapi.Handler(identityapi.Config{Browser: &identityapi.BrowserConfig{}}))
	if rec.Code == http.StatusNotFound {
		t.Fatalf("browser composition lost /v1/identity/control/exchange (body %q)", rec.Body.String())
	}
	if rec.Code != http.StatusUnauthorized && rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d, want 401/403 (body %q)", rec.Code, rec.Body.String())
	}
}
