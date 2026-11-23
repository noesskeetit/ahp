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
	"strings"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/vmruntime"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func runtimeInjector(grants *identity.Store) (*vmruntime.Injector, func(contracts.Scope) bool, error) {
	toolsAllowed, err := runtimeToolsAllowlist(os.Getenv("AHP_VM_RUNTIME_TOOLS_DEPLOYMENTS"))
	if err != nil {
		return nil, nil, err
	}
	toolsURL := os.Getenv("AHP_VM_RUNTIME_TOOLS_URL")
	if toolsAllowed != nil && toolsURL == "" {
		return nil, nil, errors.New("AHP_VM_RUNTIME_TOOLS_DEPLOYMENTS requires AHP_VM_RUNTIME_TOOLS_URL")
	}
	key, err := readInjectorSigningKey(os.Getenv("AHP_VM_RUNTIME_JWT_PRIVATE_KEY"))
	if err != nil {
		return nil, nil, err
	}
	signer, err := auth.NewSigner(os.Getenv("AHP_VM_RUNTIME_JWT_ISSUER"), os.Getenv("AHP_VM_RUNTIME_JWT_KID"), key)
	if err != nil {
		return nil, nil, err
	}
	stateRoots, err := readInjectorRoots("AHP_VM_RUNTIME_STATE_CA")
	if err != nil {
		return nil, nil, err
	}
	dir := os.Getenv("AHP_VM_RUNTIME_WORKLOAD_CERT_DIR")
	if !filepath.IsAbs(dir) {
		return nil, nil, errors.New("AHP_VM_RUNTIME_WORKLOAD_CERT_DIR must be an absolute host directory")
	}
	config := vmruntime.InjectorConfig{
		StateURL:  os.Getenv("AHP_VM_RUNTIME_STATE_URL"),
		ClientFor: injectorClientProvider(dir, stateRoots),
		ToolsURL:  toolsURL,
		MintToken: func(ctx context.Context, exec vmruntime.Execution) (string, error) {
			return grants.MintTask(ctx, exec.Scope, exec.ID, signer, "ahp", 5*time.Minute)
		},
	}
	if toolsURL != "" {
		toolsRoots := stateRoots.Clone()
		if os.Getenv("AHP_VM_RUNTIME_TOOLS_CA") != "" {
			toolsRoots, err = readInjectorRoots("AHP_VM_RUNTIME_TOOLS_CA")
			if err != nil {
				return nil, nil, err
			}
		}
		config.ToolsClientFor = injectorClientProvider(dir, toolsRoots)
	}
	injector, err := vmruntime.NewInjector(config)
	return injector, toolsAllowed, err
}

func runtimeToolsAllowlist(raw string) (func(contracts.Scope) bool, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	allowed := make(map[[2]string]struct{})
	for _, entry := range strings.Split(raw, ",") {
		tenant, deployment, found := strings.Cut(strings.TrimSpace(entry), "/")
		scope := contracts.Scope{Tenant: tenant, Deployment: deployment, Subject: "runtime"}
		if !found || scope.Validate() != nil {
			return nil, errors.New("AHP_VM_RUNTIME_TOOLS_DEPLOYMENTS must contain comma-separated tenant/deployment pairs")
		}
		allowed[[2]string{tenant, deployment}] = struct{}{}
	}
	return func(scope contracts.Scope) bool {
		_, ok := allowed[[2]string{scope.Tenant, scope.Deployment}]
		return ok
	}, nil
}

func readInjectorRoots(env string) (*x509.CertPool, error) {
	ca, err := os.ReadFile(os.Getenv(env))
	if err != nil {
		return nil, fmt.Errorf("read CA from %s: %w", env, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("invalid CA in %s", env)
	}
	return roots, nil
}

// Each upstream has its own cache and TLS trust pool, partitioned by workload.
func injectorClientProvider(dir string, roots *x509.CertPool) func(context.Context, contracts.Scope) (*http.Client, error) {
	var clients sync.Map
	return func(_ context.Context, scope contracts.Scope) (*http.Client, error) {
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
	}
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
