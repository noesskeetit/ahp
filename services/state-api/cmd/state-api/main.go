// Command state-api serves the compact AHP State API on PostgreSQL and S3.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Getenv); err != nil {
		slog.Error("State API stopped", "error", err)
		os.Exit(1)
	}
}

// An optional local grant file authorizes explicitly configured offline grants.
type grant struct {
	Tenant       string   `json:"tenant_id"`
	Deployment   string   `json:"deployment_id"`
	Subject      string   `json:"subject_id"`
	ID           string   `json:"grant_id"`
	Capabilities []string `json:"capabilities"`
}

func (g grant) scope() contracts.Scope {
	return contracts.Scope{Tenant: g.Tenant, Deployment: g.Deployment, Subject: g.Subject}
}

type grantKey struct {
	scope contracts.Scope
	id    string
}

type grantPolicy struct {
	grants map[grantKey][]string
}

func readGrants(path string) (grantPolicy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return grantPolicy{}, errors.New("could not read State API grant file")
	}
	var rules []grant
	if err := json.Unmarshal(data, &rules); err != nil || len(rules) == 0 {
		return grantPolicy{}, errors.New("State API grant file must contain a non-empty array")
	}
	policy := grantPolicy{grants: make(map[grantKey][]string, len(rules))}
	for _, rule := range rules {
		if rule.scope().Validate() != nil || strings.TrimSpace(rule.ID) == "" || !slices.Contains(rule.Capabilities, "state") {
			return grantPolicy{}, errors.New("grant requires tenant, deployment, subject, grant ID and state capability")
		}
		for _, capability := range rule.Capabilities {
			if capability != "state" && capability != "state.shared" && capability != "state.retention" && capability != "state.coordinate" && capability != "state.vm_claim" {
				return grantPolicy{}, errors.New("unexpected capability in State API grant file")
			}
		}
		key := grantKey{scope: rule.scope(), id: rule.ID}
		if _, duplicate := policy.grants[key]; duplicate {
			return grantPolicy{}, errors.New("duplicate State API grant")
		}
		policy.grants[key] = rule.Capabilities
	}
	return policy, nil
}

func (p grantPolicy) authorize(_ context.Context, claims auth.Claims, capability string) error {
	if !slices.Contains(p.grants[grantKey{scope: claims.Scope(), id: claims.Grant}], capability) {
		return contracts.NewError(contracts.ErrForbidden, "task has no active local State API grant for this capability")
	}
	return nil
}

func authorizeClaims(_ context.Context, claims auth.Claims, capability string) error {
	if claims.Kind != auth.KindTask || !slices.Contains(claims.Capabilities, capability) {
		return contracts.NewError(contracts.ErrForbidden, "task token does not grant the requested State capability")
	}
	return nil
}

func grantAuthorizer(ctx context.Context, path string) (transport.Authorize, error) {
	if path == "" {
		return authorizeClaims, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, errors.New("could not read State API grant file metadata")
	}
	initial, err := readGrants(path)
	if err != nil {
		return nil, err
	}
	var active atomic.Pointer[grantPolicy]
	active.Store(&initial)
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	go func() {
		defer signal.Stop(reload)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		last := info
		for {
			force := false
			select {
			case <-ctx.Done():
				return
			case <-reload:
				force = true
			case <-ticker.C:
			}
			next, err := os.Stat(path)
			if !force && err == nil && last != nil && os.SameFile(last, next) &&
				next.ModTime().Equal(last.ModTime()) && next.Size() == last.Size() {
				continue
			}
			policy, err := readGrants(path)
			if err != nil {
				if active.Swap(nil) != nil {
					slog.Warn("State grant policy unavailable; authorization denied until reload", "error", err)
				}
				last = nil
				continue
			}
			active.Store(&policy)
			last = next
		}
	}()
	return func(ctx context.Context, claims auth.Claims, capability string) error {
		policy := active.Load()
		if policy == nil {
			return contracts.NewError(contracts.ErrForbidden, "local State grant policy is unavailable")
		}
		return policy.authorize(ctx, claims, capability)
	}, nil
}

func stateAuthorizer(ctx context.Context, env func(string) string) (transport.Authorize, *pgxpool.Pool, error) {
	dsn := env("AHP_STATE_IDENTITY_DSN")
	if dsn == "" {
		authorize, err := grantAuthorizer(ctx, env("AHP_STATE_GRANTS_FILE"))
		return authorize, nil, err
	}
	startup, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(startup, dsn)
	if err != nil {
		return nil, nil, errors.New("could not configure the Identity PostgreSQL pool")
	}
	if err := pool.Ping(startup); err != nil {
		pool.Close()
		return nil, nil, errors.New("could not connect to Identity PostgreSQL")
	}
	return identity.New(pool).Authorize, pool, nil
}

func run(ctx context.Context, env func(string) string) error {
	for _, key := range []string{
		"AHP_STATE_DSN", "AHP_STATE_TLS_CERT", "AHP_STATE_TLS_KEY", "AHP_STATE_CLIENT_CA",
		"AHP_STATE_TOKEN_ISSUER", "AHP_STATE_TOKEN_KID", "AHP_STATE_TOKEN_PUBLIC_KEY",
		"AHP_STATE_S3_ENDPOINT", "AHP_STATE_S3_ACCESS_KEY",
		"AHP_STATE_S3_SECRET_KEY", "AHP_STATE_S3_BUCKET",
	} {
		if env(key) == "" {
			return fmt.Errorf("%s must be set", key)
		}
	}
	tlsConfig, err := stateTLS(env)
	if err != nil {
		return err
	}
	publicKey, err := readPublicKey(env("AHP_STATE_TOKEN_PUBLIC_KEY"))
	if err != nil {
		return err
	}
	policyCtx, stopPolicy := context.WithCancel(ctx)
	defer stopPolicy()
	authorize, identityPool, err := stateAuthorizer(policyCtx, env)
	if err != nil {
		return err
	}
	if identityPool != nil {
		defer identityPool.Close()
	}
	retention, err := durationSetting(env, "AHP_STATE_RETENTION", 30*24*time.Hour, time.Second)
	if err != nil {
		return err
	}
	staging, err := durationSetting(env, "AHP_STATE_STAGING_TTL", time.Hour, 30*time.Second)
	if err != nil {
		return err
	}
	useTLS := true
	if value := env("AHP_STATE_S3_TLS"); value != "" {
		var err error
		useTLS, err = strconv.ParseBool(value)
		if err != nil {
			return errors.New("AHP_STATE_S3_TLS must be a boolean")
		}
	}
	s3, err := minio.New(env("AHP_STATE_S3_ENDPOINT"), &minio.Options{
		Secure: useTLS,
		Creds:  credentials.NewStaticV4(env("AHP_STATE_S3_ACCESS_KEY"), env("AHP_STATE_S3_SECRET_KEY"), ""),
	})
	if err != nil {
		return errors.New("could not configure the S3 client")
	}

	startup, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	pool, err := pgxpool.New(startup, env("AHP_STATE_DSN"))
	if err != nil {
		return errors.New("could not configure the PostgreSQL pool")
	}
	defer pool.Close()
	store, err := statejournal.NewWithS3(startup, pool, s3, env("AHP_STATE_S3_BUCKET"))
	if err != nil {
		return err
	}
	if err := store.Migrate(startup); err != nil {
		return fmt.Errorf("compact journal migration failed: %w", err)
	}
	verifier := auth.NewVerifier(env("AHP_STATE_TOKEN_ISSUER"), map[string]ed25519.PublicKey{
		env("AHP_STATE_TOKEN_KID"): publicKey,
	})
	handler, err := stateHandler(store, verifier, authorize, identityPool != nil, env)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if pool.Ping(ctx) != nil || (identityPool != nil && identityPool.Ping(ctx) != nil) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if exists, err := s3.BucketExists(ctx, env("AHP_STATE_S3_BUCKET")); err != nil || !exists {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/", handler)

	addr := env("AHP_STATE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8443"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler: mux, TLSConfig: tlsConfig, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 35 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: 60 * time.Second,
	}
	maintenanceCtx, stopMaintenance := context.WithCancel(ctx)
	maintenanceDone := make(chan struct{})
	go func() {
		defer close(maintenanceDone)
		runMaintenance(maintenanceCtx, store, retention, staging)
	}()
	defer func() {
		stopMaintenance()
		<-maintenanceDone
	}()
	sweeperDone := make(chan struct{})
	go func() {
		defer close(sweeperDone)
		runSweeper(maintenanceCtx, store)
	}()
	defer func() {
		stopMaintenance()
		<-sweeperDone
	}()

	result := make(chan error, 1)
	go func() { result <- server.ServeTLS(listener, "", "") }()
	slog.Info("State API ready", "address", listener.Addr().String())
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}

func stateHandler(store *statejournal.Store, verifier *auth.Verifier, authorize transport.Authorize, liveIdentity bool, env func(string) string) (http.Handler, error) {
	var coordinatorWorkloads []string
	if liveIdentity {
		coordinatorWorkloads = strings.Split(env("AHP_STATE_COORDINATOR_WORKLOAD_IDS"), ",")
	}
	file := env("AHP_STATE_TOOL_LEASE_KEY")
	issuer := env("AHP_STATE_TOOL_LEASE_ISSUER")
	kid := env("AHP_STATE_TOOL_LEASE_KID")
	if file == "" && issuer == "" && kid == "" {
		return statejournal.NewHTTPWithCoordinator(store, verifier, authorize, nil, coordinatorWorkloads)
	}
	if file == "" || issuer == "" || kid == "" {
		return nil, errors.New("State Tool Gateway proof requires key, issuer and kid")
	}
	raw, err := readPEM(file, "PRIVATE KEY")
	if err != nil {
		return nil, err
	}
	key, err := x509.ParsePKCS8PrivateKey(raw)
	private, ok := key.(ed25519.PrivateKey)
	if err != nil || !ok {
		return nil, errors.New("tool lease key must be Ed25519")
	}
	signer, err := auth.NewSigner(issuer, kid, private)
	if err != nil {
		return nil, err
	}
	return statejournal.NewHTTPWithCoordinator(store, verifier, authorize, signer, coordinatorWorkloads)
}

func stateTLS(env func(string) string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(env("AHP_STATE_TLS_CERT"), env("AHP_STATE_TLS_KEY"))
	if err != nil {
		return nil, errors.New("could not load State API TLS certificate and private key")
	}
	clientCA, err := os.ReadFile(env("AHP_STATE_CLIENT_CA"))
	if err != nil {
		return nil, errors.New("could not read State API client CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(clientCA) {
		return nil, errors.New("State API client CA must contain a PEM certificate")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots,
	}, nil
}

func readPublicKey(path string) (ed25519.PublicKey, error) {
	raw, err := readPEM(path, "PUBLIC KEY")
	if err != nil {
		return nil, err
	}
	parsed, err := x509.ParsePKIXPublicKey(raw)
	key, ok := parsed.(ed25519.PublicKey)
	if err != nil || !ok {
		return nil, errors.New("State API JWT public key must be Ed25519")
	}
	return key, nil
}

func readPEM(path, kind string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read %s PEM file", kind)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != kind {
		return nil, fmt.Errorf("expected %s PEM block", kind)
	}
	return block.Bytes, nil
}

func durationSetting(env func(string) string, name string, fallback, minimum time.Duration) (time.Duration, error) {
	raw := env(name)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < minimum {
		return 0, fmt.Errorf("%s must be a duration of at least %s", name, minimum)
	}
	return d, nil
}

func runMaintenance(ctx context.Context, store *statejournal.Store, retention, staging time.Duration) {
	ticker := time.NewTicker(45 * time.Second)
	defer ticker.Stop()
	for {
		now := time.Now()
		discover, cancel := context.WithTimeout(ctx, 5*time.Second)
		tenants, err := store.FindMaintenanceTenants(discover, now.Add(-retention), now.Add(-staging))
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Warn("State API maintenance tenant discovery will retry", "error", err)
		}
		for _, tenant := range tenants {
			if ctx.Err() != nil {
				return
			}
			tick, cancel := context.WithTimeout(ctx, 35*time.Second)
			if err := store.SweepDeletedSessions(tick, tenant, now.Add(-retention), 64); err != nil && ctx.Err() == nil {
				slog.Warn("State API retention sweep will retry", "error", err)
			}
			if err := store.SweepOrphanBlobs(tick, tenant, now.Add(-staging)); err != nil && ctx.Err() == nil {
				slog.Warn("State API staging sweep will retry", "error", err)
			}
			cancel()
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func runSweeper(ctx context.Context, store *statejournal.Store) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			discover, cancel := context.WithTimeout(ctx, time.Second)
			tenants, err := store.FindExpiredTurnTenants(discover)
			cancel()
			if err != nil && ctx.Err() == nil {
				slog.Warn("State API recovery tenant discovery will retry", "error", err)
			}
			for _, tenant := range tenants {
				tick, cancel := context.WithTimeout(ctx, time.Second)
				_, err := store.RequeueExpiredTurns(tick, tenant, 64)
				cancel()
				if err != nil && ctx.Err() == nil {
					slog.Warn("State API expired turn sweep will retry", "error", err)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
