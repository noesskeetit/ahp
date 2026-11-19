package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// writeWorkloadCert writes a self-signed workload certificate carrying the SPIFFE URI
// for (tenant, deployment) - the same shape the stand PKI issues for a service.
func writeWorkloadCert(t *testing.T, tenant, deployment string) string {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	uri := &url.URL{Scheme: "spiffe", Host: "ahp", Path: "/tenant/" + tenant + "/deployment/" + deployment}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "ahp-test-workload"},
		URIs:         []*url.URL{uri},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	path := filepath.Join(t.TempDir(), "workload.crt")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write certificate: %v", err)
	}
	return path
}

func writeCertWithoutSPIFFE(t *testing.T) string {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "ahp-test-no-uri"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	path := filepath.Join(t.TempDir(), "plain.crt")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write certificate: %v", err)
	}
	return path
}

func TestExpectedWorkloadIDReadsOwnCertificateSPIFFE(t *testing.T) {
	path := writeWorkloadCert(t, "ahpstand", "spacesvc")
	got, err := expectedWorkloadID(path)
	if err != nil {
		t.Fatalf("expected a workload id, got %v", err)
	}
	if want := "spiffe://ahp/tenant/ahpstand/deployment/spacesvc"; got != want {
		t.Fatalf("workload id = %q, want %q", got, want)
	}
}

func TestExpectedWorkloadIDFailsClosed(t *testing.T) {
	if _, err := expectedWorkloadID(""); err == nil {
		t.Fatal("an absent certificate must not yield a workload identity")
	}
	if _, err := expectedWorkloadID(writeCertWithoutSPIFFE(t)); err == nil {
		t.Fatal("a certificate without a SPIFFE URI must not yield a workload identity")
	}
	if _, err := expectedWorkloadID(filepath.Join(t.TempDir(), "absent.crt")); err == nil {
		t.Fatal("an unreadable certificate must not yield a workload identity")
	}
}

// TestVerifiedScopeCompletesDeploymentOnlyForOwnWorkload is the teeth test for the
// measured stand failure "save_artifact: run task token is not verifiable: auth: invalid
// token: task token verification requires a workload identity". The old code called
// Verify(..., "") and therefore refused EVERY real run task token, so the model could
// never deliver a file; at the same time passing an attacker-supplied workload, or the
// one read back out of the token, would have made the guard a no-op.
func TestVerifiedScopeCompletesDeploymentOnlyForOwnWorkload(t *testing.T) {
	seed := make([]byte, ed25519.PrivateKeySize)
	if _, err := rand.Read(seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	key := ed25519.NewKeyFromSeed(seed[:32])
	signer, err := auth.NewSigner("ahp", "dev-1", key)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"dev-1": signer.Public()})
	mint := func(tenant, deployment string) string {
		token, err := signer.Sign(auth.Claims{
			Subject: "demo-human", Audience: "ahp", Kind: auth.KindTask,
			Tenant: tenant, Deployment: deployment,
			Task: "rtask-journey-1", Root: "root-journey-1", Context: "ctx-journey-1",
			Grant: "grant-journey-1", Depth: 1, Actors: []string{"spacesvc"},
		}, 5*time.Minute)
		if err != nil {
			t.Fatalf("mint task token: %v", err)
		}
		return token
	}
	own := "spiffe://ahp/tenant/ahpstand/deployment/spacesvc"
	base := contracts.Scope{Tenant: "ahpstand", Subject: "demo-human"}

	t.Run("own workload completes the scope", func(t *testing.T) {
		s := &apiArtifactSink{verifier: verifier, taskAudience: "ahp", workload: own}
		got, err := s.verifiedScope(base, mint("ahpstand", "spacesvc"))
		if err != nil {
			t.Fatalf("a task token minted for our own workload must verify, got %v", err)
		}
		if got.Deployment != "spacesvc" {
			t.Fatalf("deployment = %q, want spacesvc", got.Deployment)
		}
	})

	t.Run("foreign workload is refused", func(t *testing.T) {
		s := &apiArtifactSink{verifier: verifier, taskAudience: "ahp", workload: own}
		_, err := s.verifiedScope(base, mint("ahpstand", "othervc"))
		if err == nil || !strings.Contains(err.Error(), "not verifiable") {
			t.Fatalf("a token minted for another deployment must be refused, got %v", err)
		}
	})

	t.Run("unpinned workload is refused", func(t *testing.T) {
		s := &apiArtifactSink{verifier: verifier, taskAudience: "ahp"}
		_, err := s.verifiedScope(base, mint("ahpstand", "spacesvc"))
		if err == nil || !strings.Contains(err.Error(), "workload identity is not pinned") {
			t.Fatalf("without a pinned workload the tool must fail closed, got %v", err)
		}
	})

	t.Run("tenant mismatch is refused", func(t *testing.T) {
		s := &apiArtifactSink{verifier: verifier, taskAudience: "ahp", workload: own}
		_, err := s.verifiedScope(contracts.Scope{Tenant: "ahpalt", Subject: "alt-human"}, mint("ahpstand", "spacesvc"))
		if err == nil || !strings.Contains(err.Error(), "tenant does not match") {
			t.Fatalf("a token from another tenant must not name our scope, got %v", err)
		}
	})

	t.Run("human token cannot name a deployment", func(t *testing.T) {
		human, err := signer.Sign(auth.Claims{Subject: "demo-human", Audience: "ahp-browser",
			Kind: auth.KindHuman, Tenant: "ahpstand"}, time.Hour)
		if err != nil {
			t.Fatalf("mint human token: %v", err)
		}
		s := &apiArtifactSink{verifier: verifier, taskAudience: "ahp", workload: own}
		if _, err := s.verifiedScope(base, human); err == nil {
			t.Fatal("a human token must never complete an artifact scope")
		}
	})
}
