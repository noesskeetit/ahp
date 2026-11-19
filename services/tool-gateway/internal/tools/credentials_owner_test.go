package tools

import (
	"context"
	"fmt"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestVaultCredentialsResolveOwnerSpecificPaths(t *testing.T) {
	paths := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		fmt.Fprint(w, `{"data":{"data":{"bearer":"fixture-access-token"}}}`)
	}))
	defer server.Close()
	store, err := secretstore.New(server.URL, "fixture-vault-token")
	if err != nil {
		t.Fatal(err)
	}
	resolver := NewVaultResolver(store, map[string]string{"crm": "secret/data/tools/{tenant}/{subject}/crm"})
	for _, owner := range []string{"alice", "bob"} {
		scope := contracts.Scope{Tenant: "tenant-a", Deployment: "runner", Subject: owner}
		if _, err := resolver.Resolve(context.Background(), scope, "crm"); err != nil {
			t.Fatal(err)
		}
		if got := <-paths; got != "/v1/secret/data/tools/tenant-a/"+owner+"/crm" {
			t.Errorf("owner %s resolved %s", owner, got)
		}
	}
	unscoped := NewVaultResolver(store, map[string]string{"crm": "secret/data/shared/crm"})
	if _, err := unscoped.Resolve(context.Background(), contracts.Scope{Tenant: "tenant-a", Deployment: "runner", Subject: "alice"}, "crm"); contracts.Code(err) != contracts.ErrForbidden {
		t.Errorf("unscoped resolver got %v", err)
	}
	select {
	case path := <-paths:
		t.Errorf("unscoped secret was read: %s", path)
	default:
	}
}
