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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/vmruntime"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sys/unix"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("vm runtime stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	for _, key := range []string{
		"AHP_VM_RUNTIME_HOST_ID", "AHP_VM_RUNTIME_SLOT_DIR",
		"AHP_VM_RUNTIME_DSN", "AHP_VM_RUNTIME_IDENTITY_DSN",
		"AHP_VM_RUNTIME_TLS_CERT", "AHP_VM_RUNTIME_TLS_KEY", "AHP_VM_RUNTIME_CA",
		"AHP_VM_RUNTIME_JWT_ISSUER", "AHP_VM_RUNTIME_JWT_KID", "AHP_VM_RUNTIME_JWT_PUBLIC_KEY",
		"AHP_VM_RUNTIME_COORDINATOR_WORKLOAD_IDS",
		"AHP_VM_RUNTIME_JWT_PRIVATE_KEY", "AHP_VM_RUNTIME_STATE_URL",
		"AHP_VM_RUNTIME_STATE_CA", "AHP_VM_RUNTIME_WORKLOAD_CERT_DIR",
	} {
		if os.Getenv(key) == "" {
			return fmt.Errorf("%s is required", key)
		}
	}
	stateDir := os.Getenv("AHP_VM_RUNTIME_SLOT_DIR")
	if !filepath.IsAbs(stateDir) {
		return errors.New("AHP_VM_RUNTIME_SLOT_DIR must be an absolute host directory")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return fmt.Errorf("create host slot directory: %w", err)
	}
	stateInfo, err := os.Stat(stateDir)
	if err != nil || !stateInfo.IsDir() || stateInfo.Mode().Perm()&0o077 != 0 {
		return errors.New("host slot directory must be private to vm runtime")
	}
	lock, err := lockHostSlots(stateDir)
	if err != nil {
		return err
	}
	defer lock.Close()
	driver, err := runtimeDriver(stateDir)
	if err != nil {
		return err
	}

	tlsConfig, verifier, err := credentials()
	if err != nil {
		return err
	}
	allowed := strings.FieldsFunc(os.Getenv("AHP_VM_RUNTIME_COORDINATOR_WORKLOAD_IDS"),
		func(r rune) bool { return r == ',' || r == ' ' })
	if len(allowed) == 0 {
		return errors.New("no coordinator workloads in AHP_VM_RUNTIME_COORDINATOR_WORKLOAD_IDS")
	}

	pool, err := pgxpool.New(ctx, os.Getenv("AHP_VM_RUNTIME_DSN"))
	if err != nil {
		return errors.New("invalid runtime database configuration")
	}
	defer pool.Close()
	identityPool, err := pgxpool.New(ctx, os.Getenv("AHP_VM_RUNTIME_IDENTITY_DSN"))
	if err != nil {
		return errors.New("invalid Identity database configuration")
	}
	defer identityPool.Close()
	grants := identity.New(identityPool)
	if err := grants.Migrate(ctx); err != nil {
		return err
	}
	injector, err := runtimeInjector(grants)
	if err != nil {
		return err
	}
	host := vmruntime.Host{ID: os.Getenv("AHP_VM_RUNTIME_HOST_ID"), Dir: stateDir, Driver: driver, Injector: injector}
	manager := vmruntime.Manager{Pool: pool, Grants: grants,
		PlaceHost: func(vmruntime.Execution) string { return host.ID },
		Host: func(id string) (vmruntime.ExecutionHost, error) {
			if id != host.ID {
				return nil, contracts.NewError(contracts.ErrUnavailable, "execution assigned to a different host")
			}
			return host, nil
		},
	}
	if err := manager.Migrate(ctx); err != nil {
		return err
	}
	addr := os.Getenv("AHP_VM_RUNTIME_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8454"
	}
	handler := vmruntime.Handler(manager, verifier, allowed, grants.Authorize)
	server := &http.Server{Addr: addr, TLSConfig: tlsConfig, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
		Handler: http.TimeoutHandler(handler, 85*time.Second, "execution deadline exceeded")}
	stopped := make(chan error, 1)
	go func() { stopped <- server.ListenAndServeTLS("", "") }()
	select {
	case err := <-stopped:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		defer server.Close()
		return server.Shutdown(shutdown)
	}
}

// Attempt listeners belong to one local daemon. Across a restart the slot
// records remain, but a simultaneous second daemon cannot own their sockets.
func lockHostSlots(dir string) (*os.File, error) {
	lock, err := os.OpenFile(filepath.Join(dir, ".runtime.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open runtime host slot lock: %w", err)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("runtime host slots are already owned or unavailable: %w", err)
	}
	return lock, nil
}

func runtimeDriver(dir string) (vmruntime.Driver, error) {
	switch os.Getenv("AHP_VM_RUNTIME_DRIVER") {
	case "", "firecracker":
		return vmruntime.FirecrackerDriver(os.Getenv("AHP_VM_RUNTIME_CGROUP_ROOT"))
	case "process":
		return vmruntime.NewProcessDriver(filepath.Join(dir, "trusted-workers"), os.Getenv("AHP_VM_RUNTIME_WORKER_BINARY"))
	default:
		return nil, errors.New("AHP_VM_RUNTIME_DRIVER must be firecracker or process")
	}
}

func credentials() (*tls.Config, *auth.Verifier, error) {
	cert, err := tls.LoadX509KeyPair(os.Getenv("AHP_VM_RUNTIME_TLS_CERT"), os.Getenv("AHP_VM_RUNTIME_TLS_KEY"))
	if err != nil {
		return nil, nil, fmt.Errorf("load runtime TLS key pair: %w", err)
	}
	ca, err := os.ReadFile(os.Getenv("AHP_VM_RUNTIME_CA"))
	if err != nil {
		return nil, nil, fmt.Errorf("read runtime client CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, nil, errors.New("invalid runtime client CA")
	}
	publicPEM, err := os.ReadFile(os.Getenv("AHP_VM_RUNTIME_JWT_PUBLIC_KEY"))
	if err != nil {
		return nil, nil, fmt.Errorf("read runtime JWT key: %w", err)
	}
	block, _ := pem.Decode(publicPEM)
	if block == nil {
		return nil, nil, errors.New("invalid runtime JWT public key PEM")
	}
	pk, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, nil, err
	}
	pub, ok := pk.(ed25519.PublicKey)
	if !ok {
		return nil, nil, errors.New("runtime JWT public key must be Ed25519")
	}
	verifier := auth.NewVerifier(os.Getenv("AHP_VM_RUNTIME_JWT_ISSUER"),
		map[string]ed25519.PublicKey{os.Getenv("AHP_VM_RUNTIME_JWT_KID"): pub})
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}, verifier, nil
}
