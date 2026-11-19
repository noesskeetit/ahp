package main

import (
	"context"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The gateway must never call tool servers without a credential source, and it
// must not quietly use one global bearer where per-connection secrets exist
// (PDF rev3.2, pp. 44-45: MCP pools are owned by the authority holder).
func TestToolsCredentialSourceIsExplicit(t *testing.T) {
	t.Run("no source configured fails closed", func(t *testing.T) {
		t.Setenv("AHP_TOOLS_VAULT_PATHS", "")
		t.Setenv("AHP_TOOLS_DEV_BEARER", "")
		_, _, err := toolsCredentialResolver()
		if err == nil || !strings.Contains(err.Error(), "no credential source") {
			t.Fatalf("want fail-closed, got %v", err)
		}
	})

	t.Run("invalid vault paths are rejected", func(t *testing.T) {
		t.Setenv("AHP_TOOLS_VAULT_PATHS", "{not json")
		t.Setenv("AHP_TOOLS_DEV_BEARER", "dev")
		if _, _, err := toolsCredentialResolver(); err == nil {
			t.Fatal("want parse error even when a dev bearer is available")
		}
	})

	t.Run("vault paths select the vault resolver", func(t *testing.T) {
		t.Setenv("AHP_TOOLS_VAULT_PATHS", `{"mcp-prod":"secret/mcp/prod"}`)
		t.Setenv("AHP_TOOLS_DEV_BEARER", "dev-must-not-win")
		t.Setenv(secretstore.EnvVaultAddr, "https://127.0.0.1:1")
		t.Setenv(secretstore.EnvVaultToken, "test-token")
		res, source, err := toolsCredentialResolver()
		if err != nil {
			t.Fatalf("resolver: %v", err)
		}
		if source != "vault" {
			t.Fatalf("want vault source, got %q", source)
		}
		// An unmapped connection must fail closed with ErrNotFound instead of
		// falling back to any ambient or development bearer.
		_, err = res.Resolve(context.Background(), contracts.Scope{Tenant: "t1"}, "not-mapped")
		if err == nil {
			t.Fatal("unmapped connection must not resolve a credential")
		}
		if contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("explicit dev bearer is honoured and labelled", func(t *testing.T) {
		t.Setenv("AHP_TOOLS_VAULT_PATHS", "")
		t.Setenv("AHP_TOOLS_DEV_BEARER", "dev-bearer")
		res, source, err := toolsCredentialResolver()
		if err != nil {
			t.Fatalf("resolver: %v", err)
		}
		if source != "dev-static" {
			t.Fatalf("want dev-static, got %q", source)
		}
		cred, err := res.Resolve(context.Background(), contracts.Scope{Tenant: "t1"}, "any")
		if err != nil || cred.Bearer != "dev-bearer" {
			t.Fatalf("want the configured bearer, got %+v err %v", cred, err)
		}
	})
}
