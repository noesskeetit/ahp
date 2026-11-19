package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The registry mount must not be able to come up with a silent-off supply chain:
// the mode is mandatory, "enforce" needs an operator root bundle, and only an
// explicit "disabled" turns verification off. These cases lock that contract for
// the composed front without needing a database.
func TestRegistrySupplyChainOptionFailClosed(t *testing.T) {
	t.Run("unset fails closed", func(t *testing.T) {
		t.Setenv("AHP_REGISTRY_SUPPLY_CHAIN", "")
		if _, _, err := registrySupplyChainOption(); err == nil {
			t.Fatal("unset mode must fail closed")
		}
	})
	t.Run("unknown mode fails closed", func(t *testing.T) {
		t.Setenv("AHP_REGISTRY_SUPPLY_CHAIN", "sometimes")
		if _, _, err := registrySupplyChainOption(); err == nil {
			t.Fatal("unknown mode must fail closed")
		}
	})
	t.Run("enforce without roots path fails closed", func(t *testing.T) {
		t.Setenv("AHP_REGISTRY_SUPPLY_CHAIN", "enforce")
		t.Setenv("AHP_REGISTRY_TRUST_ROOTS_PEM", "")
		if _, _, err := registrySupplyChainOption(); err == nil {
			t.Fatal("enforce without AHP_REGISTRY_TRUST_ROOTS_PEM must fail closed")
		}
	})
	t.Run("enforce with unreadable roots fails closed", func(t *testing.T) {
		t.Setenv("AHP_REGISTRY_SUPPLY_CHAIN", "enforce")
		t.Setenv("AHP_REGISTRY_TRUST_ROOTS_PEM", filepath.Join(t.TempDir(), "missing-ca.pem"))
		if _, _, err := registrySupplyChainOption(); err == nil {
			t.Fatal("enforce with a missing root bundle must fail closed")
		}
	})
	t.Run("enforce with a valid bundle installs roots", func(t *testing.T) {
		dir := t.TempDir()
		ca := filepath.Join(dir, "ca.pem")
		if err := os.WriteFile(ca, selfCA(t), 0o600); err != nil {
			t.Fatalf("write ca: %v", err)
		}
		t.Setenv("AHP_REGISTRY_SUPPLY_CHAIN", "enforce")
		t.Setenv("AHP_REGISTRY_TRUST_ROOTS_PEM", ca)
		roots, mode, err := registrySupplyChainOption()
		if err != nil {
			t.Fatalf("enforce with valid roots: %v", err)
		}
		if mode != "enforce" || roots == nil {
			t.Fatalf("mode=%q roots==nil? %v, want enforce with roots", mode, roots == nil)
		}
	})
	// The unverified mode exists, but it is named SYNTHETIC so no evidence from
	// such a stand can be read as real signature/SBOM verification. The legacy
	// spelling stays accepted and resolves to the same named mode.
	for _, spelling := range []string{"synthetic", "disabled"} {
		t.Run("explicit synthetic via "+spelling, func(t *testing.T) {
			t.Setenv("AHP_REGISTRY_SUPPLY_CHAIN", spelling)
			roots, mode, err := registrySupplyChainOption()
			if err != nil {
				t.Fatalf("explicit %s must not error: %v", spelling, err)
			}
			if mode != "synthetic" || roots != nil {
				t.Fatalf("mode=%q roots==nil? %v, want synthetic with no roots", mode, roots != nil)
			}
		})
	}
}

// selfCA writes a throwaway CA certificate PEM used only as operator trust
// material to prove the bundle loads; it is never a signing key the product uses.
func selfCA(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "ahp-registry-test-root"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
