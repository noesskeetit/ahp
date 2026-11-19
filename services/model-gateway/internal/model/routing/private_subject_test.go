package routing_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/model/routing"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

const privateSubjectTable = `{"version":1,"providers":[{"name":"trusted-private","subject_from_scope":true,"audience":{"kind":"private"},"secret_ref":"AHP_MEMORY_EMBEDDING_KEY_FILE","models":["embed"]}],"routes":[{"name":"explicit-tenant","when":{"tenants":["synthetic"]},"then":{"provider":"trusted-private","model":"embed"}}]}`

func TestPrivateAudienceBindsVerifiedSubjectWithoutSharedMutation(t *testing.T) {
	r := mustRouter(t, privateSubjectTable)
	var wg sync.WaitGroup
	for _, subject := range []string{"alice", "bob"} {
		wg.Add(1)
		go func(subject string) {
			defer wg.Done()
			scope := contracts.Scope{Tenant: "synthetic", Deployment: "agent", Subject: subject}
			for i := 0; i < 100; i++ {
				if _, err := r.Resolve(scope, routing.Request{Model: "embed", Labels: labels.Set{Subjects: []string{subject}}}); err != nil {
					t.Errorf("%s own private route: %v", subject, err)
					return
				}
			}
		}(subject)
	}
	wg.Wait()
	if r.Table().Providers[0].Audience.Subject != "" {
		t.Fatal("request mutated shared provider policy")
	}
}

func TestCallerBoundPrivateRouteDeniesForeignData(t *testing.T) {
	r := mustRouter(t, privateSubjectTable)
	for _, tc := range []struct {
		name  string
		scope contracts.Scope
		set   labels.Set
	}{
		{"wrong-label", contracts.Scope{Tenant: "synthetic", Deployment: "agent", Subject: "bob"}, labels.Set{Subjects: []string{"alice"}}},
		{"foreign-tenant", contracts.Scope{Tenant: "other", Deployment: "agent", Subject: "alice"}, labels.Set{Subjects: []string{"alice"}}},
		{"secret", contracts.Scope{Tenant: "synthetic", Deployment: "agent", Subject: "alice"}, labels.Set{Taints: []string{labels.TaintSecret}}},
		{"empty-subject", contracts.Scope{Tenant: "synthetic", Deployment: "agent"}, labels.Set{}},
		{"empty-deployment", contracts.Scope{Tenant: "synthetic", Subject: "alice"}, labels.Set{}},
		{"external-not-opted-in", contracts.Scope{Tenant: "synthetic", Deployment: "agent", Subject: "alice"}, labels.Set{Taints: []string{labels.TaintUntrustedExternal}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := r.Resolve(tc.scope, routing.Request{Model: "embed", Labels: tc.set}); err == nil {
				t.Fatal("unsafe private route accepted")
			}
		})
	}
}

func TestCallerBoundPrivatePolicyRequiresExplicitTrustAndTenants(t *testing.T) {
	for _, body := range []string{
		strings.Replace(privateSubjectTable, `"kind":"private"`, `"kind":"tenant"`, 1),
		strings.Replace(privateSubjectTable, `"kind":"private"`, `"kind":"private","subject":"alice"`, 1),
		strings.Replace(privateSubjectTable, `"tenants":["synthetic"]`, `"tenants":["*"]`, 1),
		strings.Replace(privateSubjectTable, `"tenants":["synthetic"]`, `"tenants":[]`, 1),
		strings.Replace(privateSubjectTable, `"tenants":["synthetic"]`, `"tenants":[""]`, 1),
	} {
		if _, err := routing.Parse([]byte(body)); err == nil {
			t.Fatal("unsafe caller-bound policy accepted")
		}
	}
}
