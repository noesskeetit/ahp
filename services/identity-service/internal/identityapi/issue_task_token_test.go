package identityapi_test

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/identityapi"
)

func TestIssueTaskTokenWorkloadBoundary(t *testing.T) {
	h := identityapi.Handler(identityapi.Config{Audience: "ahp", Store: identity.New(nil)})
	u, _ := url.Parse("spiffe://ahp/tenant/tenant/deployment/worker")
	leaf := &x509.Certificate{URIs: []*url.URL{u}}
	verified := &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	for _, tc := range []struct {
		name, body string
		tls        *tls.ConnectionState
		auth       string
		want       int
	}{
		{"no_peer", `{}`, nil, "", 403},
		{"employee_is_not_workload", `{}`, nil, "Bearer employee-jwt", 403},
		{"unverified_peer", `{}`, &tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}, "", 403},
		{"subject_selector", `{"subject":"other"}`, verified, "", 400},
		{"tenant_selector", `{"tenant":"other"}`, verified, "", 400},
		{"deployment_selector", `{"deployment":"other"}`, verified, "", 400},
		{"arbitrary_audience", `{"audience":"external"}`, verified, "", 400},
		{"malformed_ttl", `{"ttl":"garbage"}`, verified, "", 400},
		{"negative_ttl", `{"ttl":"-1s"}`, verified, "", 400},
		{"zero_explicit_ttl", `{"ttl":"0s"}`, verified, "", 400},
		{"excessive_ttl", `{"ttl":"16m"}`, verified, "", 400},
		{"null_body", `null`, verified, "", 400},
		{"duplicate_fields", `{"ttl":"1s","ttl":"2s"}`, verified, "", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/v1/identity/tasks/task/issue-token", strings.NewReader(tc.body))
			r.TLS = tc.tls
			r.Header.Set("Authorization", tc.auth)
			r.Header.Set("X-Forwarded-Client-Cert", "spoof")
			r.Header.Set("X-AHP-Tenant", "tenant")
			r.Header.Set("X-AHP-Deployment", "worker")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d want=%d", w.Code, tc.want)
			}
		})
	}
}

func TestIssueTaskTokenMalformedTask(t *testing.T) {
	h := identityapi.Handler(identityapi.Config{Audience: "ahp", Store: identity.New(nil)})
	u, _ := url.Parse("spiffe://ahp/tenant/tenant/deployment/worker")
	r := httptest.NewRequest(http.MethodPost, "/v1/identity/tasks/bad%01task/issue-token", strings.NewReader(`{}`))
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{u}}}}}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatalf("status=%d want=400", w.Code)
	}
}
