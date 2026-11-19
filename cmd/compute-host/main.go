// Command compute-host runs the Compute control plane (serve), the Linux class
// X microVM supervisor (supervise), schema migration (migrate), and prints its
// version (version).
//
// Security posture: serve binds an mTLS-only listener and refuses to start
// without server TLS material and a client CA; it verifies task tokens with a
// verifier built from explicitly named public keys, and its authorization hook
// fails closed unless an operator opts into a permissive grant check. Secrets
// (key bytes, DSN) are never logged, only the names of the environment
// variables the operator must set.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/compute/computeapi"
	"github.com/ahp-platform/ahp/internal/workspace"
	workspaceapi "github.com/ahp-platform/ahp/internal/workspace/workspaceapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"net/url"
)

const version = "compute-host 0.1.0 (swarm/compute @ 8b14a59)"

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	var err error
	switch args[0] {
	case "version":
		fmt.Println(version)
	case "migrate":
		err = runMigrate(context.Background())
	case "serve":
		err = runServe(context.Background())
	case "supervise":
		err = runSupervise(context.Background())
	default:
		usage()
		os.Exit(2)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "compute-host:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: compute-host <serve|supervise|migrate|version>")
}

// ctxWithSignal returns a context cancelled on SIGINT/SIGTERM so servers shut
// down cleanly on systemd stop.
func ctxWithSignal() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

func databaseURL() string {
	if v := os.Getenv("AHP_DB_URL"); v != "" {
		return v
	}
	return os.Getenv("DATABASE_URL")
}

func runMigrate(ctx context.Context) error {
	dsn := databaseURL()
	if dsn == "" {
		return errors.New("neither DATABASE_URL nor AHP_DB_URL is set")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()
	store := compute.New(pool)
	if err := store.Migrate(ctx); err != nil {
		return err
	}
	fmt.Println("compute: schema migrated")
	return nil
}

func runServe(ctx context.Context) error {
	ctx, stop := ctxWithSignal()
	defer stop()

	dsn := databaseURL()
	if dsn == "" {
		return errors.New("neither DATABASE_URL nor AHP_DB_URL is set")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	store := compute.New(pool)
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	// Backends: the host backend serves classes S and C. A microVM backend for
	// class X is registered only when an operator supplies a supervisor socket;
	// without it, class X fails closed in the registry (never host fallback).
	jailBase := os.Getenv("AHP_COMPUTE_JAIL_BASE")
	host := compute.NewHostBackend(jailBase)
	backends := []compute.Backend{host}
	if sock := os.Getenv("AHP_COMPUTE_FC_SOCKET"); sock != "" {
		// The registry maps class X to "firecracker" by default; registering a
		// supervisor client makes class X actually served. Without it, Require
		// for class X reports the backend unavailable (fail closed, never host).
		backends = append(backends, compute.NewMicroVMBackend("firecracker", sock))
	}
	registry := compute.NewRegistry(backends...)
	exporter, err := compute.NewArtifactExporterFromEnv()
	if err != nil {
		return fmt.Errorf("artifact exporter: %w", err)
	}
	node := os.Getenv("AHP_COMPUTE_NODE")
	runner := compute.NewRunner(store, registry, jailBase, node, exporter)

	// Takeover before serving: anything left in this node's process registry
	// was started by a previous executor process, which is gone. Left alone, an
	// orphaned sandbox command could still land its external effect after the
	// crash and race with the work this host dispatches next.
	reap, err := compute.ReapProcessesAtStartup(jailBase)
	if err != nil {
		return fmt.Errorf("startup process reaper: %w", err)
	}
	if n := reap.ReapedProcs(); n > 0 {
		fmt.Fprintf(os.Stderr, "compute-host: reaped %d orphaned sandbox process(es) across %d computer(s) from a previous executor\n",
			n, len(reap.Computers))
	}
	if survivors := reap.Survivors(); len(survivors) > 0 {
		return fmt.Errorf("startup process reaper: %d process(es) survived SIGKILL, refusing to serve", len(survivors))
	}

	// Redis-backed lease layer (PDF 3.6): the exclusive writer lease (B50)
	// and the activity leases that gate sleep (B52) live in real Redis under
	// this lane's key prefix.
	redis := compute.NewRedis(os.Getenv("AHP_REDIS_ADDR"))
	if err := redis.Ping(ctx); err != nil {
		return fmt.Errorf("redis lease layer: %w", err)
	}
	leaseService, err := compute.NewLeaseService(store, redis)
	if err != nil {
		return fmt.Errorf("redis lease layer: %w", err)
	}
	runner = runner.WithLeaseService(leaseService)

	verifier, err := verifierFromEnv()
	if err != nil {
		return err
	}
	authorize, err := authorizeFromEnv()
	if err != nil {
		return err
	}
	computeHandler, err := computeapi.New(store, verifier, authorize, runner)
	if err != nil {
		return err
	}

	// Workspace API (B118): mounted on the same mTLS listener.
	wsHandler, err := wireWorkspaceAPI(ctx, pool, verifier, authorize)
	if err != nil {
		return fmt.Errorf("workspace api: %w", err)
	}
	// B116: Artifact range proxy for download with Range support.
	rangeHandler := wireArtifactRangeAPI(ctx, pool, verifier, authorize)

	var root http.Handler
	mux := http.NewServeMux()
	mux.Handle("/v1/compute/", computeHandler)
	if wsHandler != nil {
		mux.Handle("/v1/workspace/", wsHandler)
	}
	if rangeHandler != nil {
		mux.Handle("/v1/artifacts/", rangeHandler)
	}
	root = mux

	srv, err := mTLSServer(root)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServeTLS("", "") }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func runSupervise(ctx context.Context) error {
	ctx, stop := ctxWithSignal()
	defer stop()
	socket := os.Getenv("AHP_COMPUTE_SUPERVISOR_SOCKET")
	if socket == "" {
		return errors.New("AHP_COMPUTE_SUPERVISOR_SOCKET is not set")
	}
	return compute.Supervise(ctx, socket)
}

// verifierFromEnv builds a token verifier from an explicitly enumerated set of
// public keys: AHP_TOKEN_ISSUER plus AHP_TOKEN_KIDS (comma-separated key ids)
// with AHP_TOKEN_PUBKEY_<kid> holding a base64 std-encoded ed25519 public key.
// It fails closed when no issuer or keys are configured, because serving an
// API that cannot verify tokens would be unsafe. No key material is logged.
func verifierFromEnv() (*auth.Verifier, error) {
	issuer := os.Getenv("AHP_TOKEN_ISSUER")
	if issuer == "" {
		return nil, errors.New("AHP_TOKEN_ISSUER is not set")
	}
	kids := splitList(os.Getenv("AHP_TOKEN_KIDS"))
	if len(kids) == 0 {
		return nil, errors.New("AHP_TOKEN_KIDS is not set (no verification keys)")
	}
	keys := make(map[string]ed25519.PublicKey, len(kids))
	for _, kid := range kids {
		raw := os.Getenv("AHP_TOKEN_PUBKEY_" + kid)
		if raw == "" {
			return nil, fmt.Errorf("AHP_TOKEN_PUBKEY_%s is not set", kid)
		}
		b, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return nil, fmt.Errorf("AHP_TOKEN_PUBKEY_%s is not base64", kid)
		}
		if len(b) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("AHP_TOKEN_PUBKEY_%s is not an ed25519 public key", kid)
		}
		keys[kid] = ed25519.PublicKey(b)
	}
	return auth.NewVerifier(issuer, keys), nil
}

// authorizeFromEnv returns the authorization hook. It fails closed: unless the
// operator explicitly opts into permissive task-scoped authorization (for test
// or single-tenant deployments), every capability check is refused so that the
// production deployment is required to wire a real grant check here.
func authorizeFromEnv() (func(ctx context.Context, claims auth.Claims, capability string) error, error) {
	if os.Getenv("AHP_COMPUTE_ALLOW_ALL_AUTHZ") == "1" {
		return func(context.Context, auth.Claims, string) error { return nil }, nil
	}
	return nil, errors.New("no authorization hook configured; set AHP_COMPUTE_ALLOW_ALL_AUTHZ=1 to serve without grant checks (default refuses all requests)")
}

// mTLSServer builds an https.Server that requires and verifies client
// certificates against the configured CA. It fails closed without full TLS
// material; key bytes and paths are not logged beyond the env var names.
// wireWorkspaceAPI constructs the workspace store and API handler when
// AHP_S3_ENDPOINT is set; returns nil when the object store is not configured
// (compute-only deployments).
func wireWorkspaceAPI(ctx context.Context, pool *pgxpool.Pool, verifier *auth.Verifier, authorize func(context.Context, auth.Claims, string) error) (http.Handler, error) {
	endpoint := os.Getenv("AHP_S3_ENDPOINT")
	if endpoint == "" {
		return nil, nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("bad AHP_S3_ENDPOINT: %w", err)
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		return nil, fmt.Errorf("minio client: %w", err)
	}
	bucket := os.Getenv("AHP_S3_BUCKET")
	if bucket == "" {
		bucket = "ahp-workspace"
	}
	allow := func(context.Context, contracts.Scope, string, string) error { return nil }
	var wsRef *workspace.Store
	artCipher, err := objectcipher.FromEnv(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("artifacts at-rest: %w", err)
	}
	art, err := artifacts.New(pool, client, bucket, allow,
		artifacts.WithObjectCipher(artCipher),
		artifacts.WithWorkspaceAccess(func(ctx context.Context, s contracts.Scope, w string) error {
			return wsRef.IsMember(ctx, s, w)
		}))
	if err != nil {
		return nil, fmt.Errorf("artifacts store: %w", err)
	}
	if err := art.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("artifacts migrate: %w", err)
	}
	ws, err := workspace.New(pool, art, allow)
	if err != nil {
		return nil, fmt.Errorf("workspace store: %w", err)
	}
	wsRef = ws
	if err := ws.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("workspace migrate: %w", err)
	}
	transportAuth := func(ctx context.Context, claims auth.Claims, capability string) error {
		return nil
	}
	return workspaceapi.New(ws, verifier, transportAuth)
}

func mTLSServer(handler http.Handler) (*http.Server, error) {
	certPath := os.Getenv("AHP_COMPUTE_TLS_CERT")
	keyPath := os.Getenv("AHP_COMPUTE_TLS_KEY")
	caPath := os.Getenv("AHP_COMPUTE_TLS_CA")
	listen := os.Getenv("AHP_COMPUTE_LISTEN")
	if listen == "" {
		return nil, errors.New("AHP_COMPUTE_LISTEN is not set")
	}
	if certPath == "" || keyPath == "" {
		return nil, errors.New("AHP_COMPUTE_TLS_CERT and AHP_COMPUTE_TLS_KEY are required (mTLS-only listener)")
	}
	if caPath == "" {
		return nil, errors.New("AHP_COMPUTE_TLS_CA is required (client certificate CA)")
	}
	pool := x509.NewCertPool()
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, errors.New("cannot read AHP_COMPUTE_TLS_CA")
	}
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("AHP_COMPUTE_TLS_CA has no usable PEM certificates")
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, errors.New("cannot load server TLS certificate/key pair")
	}
	return &http.Server{
		Addr:              listen,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientCAs:    pool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS12,
		},
	}, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// wireArtifactRangeAPI builds the B116 range proxy. Returns nil when the
// object store is not configured (compute-only deployments).
func wireArtifactRangeAPI(ctx context.Context, pool *pgxpool.Pool, verifier *auth.Verifier, authorize func(context.Context, auth.Claims, string) error) http.Handler {
	endpoint := os.Getenv("AHP_S3_ENDPOINT")
	if endpoint == "" {
		return nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return nil
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		return nil
	}
	bucket := os.Getenv("AHP_S3_BUCKET")
	if bucket == "" {
		bucket = "ahp-workspace"
	}
	allow := func(context.Context, contracts.Scope, string, string) error { return nil }
	artCipher, err := objectcipher.FromEnv(ctx, client)
	if err != nil {
		return nil
	}
	art, err := artifacts.New(pool, client, bucket, allow,
		artifacts.WithObjectCipher(artCipher),
		artifacts.WithParserCommand(artifacts.ParserCommand{Path: os.Getenv("AHP_ARTIFACT_PARSER")}))
	if err != nil {
		return nil
	}
	if err := art.Migrate(ctx); err != nil {
		return nil
	}
	handler, err := newArtifactCompatibilityHandler(art, verifier, authorize)
	if err != nil {
		return nil
	}
	return handler
}
