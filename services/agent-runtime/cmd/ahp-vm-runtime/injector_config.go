//go:build unix

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/vmruntime"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func runtimeInjector(grants *identity.Store) (*vmruntime.Injector, error) {
	key, err := readInjectorSigningKey(os.Getenv("AHP_VM_RUNTIME_JWT_PRIVATE_KEY"))
	if err != nil {
		return nil, err
	}
	signer, err := auth.NewSigner(os.Getenv("AHP_VM_RUNTIME_JWT_ISSUER"), os.Getenv("AHP_VM_RUNTIME_JWT_KID"), key)
	if err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(os.Getenv("AHP_VM_RUNTIME_STATE_CA"))
	if err != nil {
		return nil, fmt.Errorf("read State API CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid State API CA")
	}
	dir := os.Getenv("AHP_VM_RUNTIME_WORKLOAD_CERT_DIR")
	if !filepath.IsAbs(dir) {
		return nil, errors.New("AHP_VM_RUNTIME_WORKLOAD_CERT_DIR must be an absolute host directory")
	}

	var clients sync.Map
	return vmruntime.NewInjector(vmruntime.InjectorConfig{
		StateURL: os.Getenv("AHP_VM_RUNTIME_STATE_URL"),
		ClientFor: func(_ context.Context, scope contracts.Scope) (*http.Client, error) {
			if err := scope.Validate(); err != nil {
				return nil, err
			}
			workload := auth.WorkloadID(scope)
			if client, exists := clients.Load(workload); exists {
				return client.(*http.Client), nil
			}
			path := filepath.Join(dir, "tenant-"+scope.Tenant, "deployment-"+scope.Deployment)
			config := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots,
				GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
					return loadWorkloadCert(path, workload)
				}}
			client := &http.Client{
				Transport: &http.Transport{TLSClientConfig: config, ForceAttemptHTTP2: true,
					IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second},
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
				Timeout:       90 * time.Second,
			}
			actual, _ := clients.LoadOrStore(workload, client)
			return actual.(*http.Client), nil
		},
		MintToken: func(ctx context.Context, exec vmruntime.Execution) (string, error) {
			return grants.MintTask(ctx, exec.Scope, exec.ID, signer, "ahp", 5*time.Minute)
		},
	})
}

func loadWorkloadCert(path, workload string) (*tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(filepath.Join(path, "tls.crt"), filepath.Join(path, "tls.key"))
	if err != nil {
		return nil, fmt.Errorf("load deployment workload certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, err
	}
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != workload {
		return nil, errors.New("deployment workload certificate has the wrong SPIFFE identity")
	}
	cert.Leaf = leaf
	return &cert, nil
}

func readInjectorSigningKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read injector private key: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, errors.New("injector private key must be PKCS#8 PEM")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ed, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("injector private key must be Ed25519")
	}
	return ed, nil
}
