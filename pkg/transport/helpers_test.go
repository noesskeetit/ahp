package transport

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
)

type testCA struct {
	cert *x509.Certificate
	pem  []byte
	key  *ecdsa.PrivateKey
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ahp-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key,
		pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// clientCert mints a client certificate carrying exactly the given SPIFFE URIs.
func (ca *testCA) clientCert(t *testing.T, uris ...string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	parsed := make([]*url.URL, 0, len(uris))
	for _, u := range uris {
		p, err := url.Parse(u)
		if err != nil {
			t.Fatalf("bad test uri %q: %v", u, err)
		}
		parsed = append(parsed, p)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "ahp-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:         parsed,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// signTask mints a task token for the given scope.
func signTask(t *testing.T, s *auth.Signer, audience, tenant, deployment, task string) string {
	t.Helper()
	tok, err := s.Sign(auth.Claims{
		Audience: audience, Kind: auth.KindTask, Tenant: tenant, Deployment: deployment,
		Subject: "human:root-user", Task: task, Root: "run-1", Context: "ctx-1",
		Grant: "cap:test", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// signTaskWithSubject mints a task token delegating from an explicit subject.
func signTaskWithSubject(t *testing.T, s *auth.Signer, audience, tenant, deployment, task, subject string) string {
	t.Helper()
	tok, err := s.Sign(auth.Claims{
		Audience: audience, Kind: auth.KindTask, Tenant: tenant, Deployment: deployment,
		Subject: subject, Task: task, Root: "run-1", Context: "ctx-1",
		Grant: "cap:test", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// signHuman mints a human token.
func signHuman(t *testing.T, s *auth.Signer, audience, tenant, subject string) string {
	t.Helper()
	tok, err := s.Sign(auth.Claims{
		Audience: audience, Kind: auth.KindHuman, Tenant: tenant, Subject: subject,
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func newSignerVerifier(t *testing.T) (*auth.Signer, *auth.Verifier) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("https://identity.test", "k1", priv)
	if err != nil {
		t.Fatal(err)
	}
	return signer, auth.NewVerifier("https://identity.test",
		map[string]ed25519.PublicKey{"k1": pub})
}
