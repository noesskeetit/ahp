package harness

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// CertMaterial points at freshly minted, run-scoped TLS material. The CA key
// exists only for the duration of one harness run, inside a 0700 temp dir.
type CertMaterial struct {
	Dir        string
	CACert     string
	ServerCert string
	ServerKey  string
}

func writePEM(path, typ string, der []byte) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600)
}

func newKey() (*ecdsa.PrivateKey, error) { return ecdsa.GenerateKey(elliptic.P256(), rand.Reader) }

func serial() (*big.Int, error) { return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128)) }

// MintCerts creates a throwaway CA plus a server certificate for localhost.
func MintCerts(dir string) (CertMaterial, error) {
	cm := CertMaterial{Dir: dir}
	caKey, err := newKey()
	if err != nil {
		return cm, err
	}
	sr, err := serial()
	if err != nil {
		return cm, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          sr,
		Subject:               pkix.Name{CommonName: "ahp-profiling-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return cm, err
	}
	cm.CACert = filepath.Join(dir, "ca.crt")
	if err := writePEM(cm.CACert, "CERTIFICATE", caDER); err != nil {
		return cm, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return cm, err
	}

	sKey, err := newKey()
	if err != nil {
		return cm, err
	}
	sr2, err := serial()
	if err != nil {
		return cm, err
	}
	sTmpl := &x509.Certificate{
		SerialNumber: sr2,
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	sDER, err := x509.CreateCertificate(rand.Reader, sTmpl, caCert, &sKey.PublicKey, caKey)
	if err != nil {
		return cm, err
	}
	cm.ServerCert = filepath.Join(dir, "server.crt")
	if err := writePEM(cm.ServerCert, "CERTIFICATE", sDER); err != nil {
		return cm, err
	}
	keyDER, err := x509.MarshalECPrivateKey(sKey)
	if err != nil {
		return cm, err
	}
	cm.ServerKey = filepath.Join(dir, "server.key")
	if err := writePEM(cm.ServerKey, "EC PRIVATE KEY", keyDER); err != nil {
		return cm, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return cm, err
	}
	if err := saveCAPEM(cm, caKey); err != nil {
		return cm, err
	}
	return cm, nil
}

// ClientCertFile is a minted workload identity usable by the load client.
type ClientCertFile struct {
	Cert     string
	Key      string
	Tenant   string
	Deployme string
}

// MintClientCert creates a workload certificate whose single URI SAN is
// spiffe://ahp/tenant/{tenant}/deployment/{deployment}.
func MintClientCert(cm CertMaterial, tenant, deployment string) (ClientCertFile, error) {
	caPem, err := os.ReadFile(cm.CACert)
	if err != nil {
		return ClientCertFile{}, err
	}
	block, _ := pem.Decode(caPem)
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return ClientCertFile{}, err
	}
	// The CA private key lives in memory of the minting process only; we keep
	// it alongside the material in the 0700 dir so repeated mints stay cheap.
	caKeyAny, err := loadCAPEM(cm.Dir)
	if err != nil {
		return ClientCertFile{}, err
	}
	key, err := newKey()
	if err != nil {
		return ClientCertFile{}, err
	}
	sr, err := serial()
	if err != nil {
		return ClientCertFile{}, err
	}
	uri, err := url.Parse(fmt.Sprintf("spiffe://ahp/tenant/%s/deployment/%s", tenant, deployment))
	if err != nil {
		return ClientCertFile{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: sr,
		Subject:      pkix.Name{CommonName: tenant + "/" + deployment},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKeyAny)
	if err != nil {
		return ClientCertFile{}, err
	}
	certPath := filepath.Join(cm.Dir, fmt.Sprintf("client-%s-%s.crt", sanitize(tenant), sanitize(deployment)))
	keyPath := filepath.Join(cm.Dir, fmt.Sprintf("client-%s-%s.key", sanitize(tenant), sanitize(deployment)))
	if err := writePEM(certPath, "CERTIFICATE", der); err != nil {
		return ClientCertFile{}, err
	}
	kd, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return ClientCertFile{}, err
	}
	if err := writePEM(keyPath, "EC PRIVATE KEY", kd); err != nil {
		return ClientCertFile{}, err
	}
	return ClientCertFile{Cert: certPath, Key: keyPath, Tenant: tenant, Deployme: deployment}, nil
}

func sanitize(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			out = append(out, r)
		}
	}
	return string(out)
}

// saveCAPEM persists the run-scoped CA key inside the 0700 material dir.
func saveCAPEM(cm CertMaterial, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	return writePEM(filepath.Join(cm.Dir, "ca.key"), "EC PRIVATE KEY", der)
}

func loadCAPEM(dir string) (*ecdsa.PrivateKey, error) {
	pemBytes, err := os.ReadFile(filepath.Join(dir, "ca.key"))
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(pemBytes)
	return x509.ParseECPrivateKey(block.Bytes)
}
