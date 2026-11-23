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
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/internal/runcoord"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	ahpotel "github.com/ahp-platform/ahp/pkg/otel"
	statesdk "github.com/ahp-platform/ahp/sdk/go/state/journal"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Getenv); err != nil {
		slog.Error("run coordinator stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, env func(string) string) error {
	for _, key := range []string{
		"AHP_RC_STATE_URL", "AHP_RC_RUNTIME_URL", "AHP_RC_TLS_CERT", "AHP_RC_TLS_KEY", "AHP_RC_CA",
		"AHP_RC_STATE_TOKEN_FILE", "AHP_RC_RUNTIME_TOKEN_FILE", "AHP_RC_JWT_ISSUER", "AHP_RC_JWT_KID",
		"AHP_RC_JWT_PUBLIC_KEY", "AHP_RC_NOTIFY_DSN", "AHP_RC_AGENT_ACCESS_FILE",
	} {
		if env(key) == "" {
			return fmt.Errorf("%s is required", key)
		}
	}
	cert, err := tls.LoadX509KeyPair(env("AHP_RC_TLS_CERT"), env("AHP_RC_TLS_KEY"))
	if err != nil {
		return fmt.Errorf("load coordinator TLS key pair: %w", err)
	}
	ca, err := os.ReadFile(env("AHP_RC_CA"))
	if err != nil {
		return fmt.Errorf("read coordinator CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid coordinator CA")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, RootCAs: roots, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}
	client := &http.Client{
		Transport:     &http.Transport{TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true},
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	state, err := statesdk.New(statesdk.Config{BaseURL: env("AHP_RC_STATE_URL"), HTTPClient: client,
		TokenSource: tokenFileSource(env("AHP_RC_STATE_TOKEN_FILE"))})
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(env("AHP_RC_JWT_PUBLIC_KEY"))
	if err != nil {
		return err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return errors.New("invalid coordinator JWT public key PEM")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return err
	}
	pub, ok := key.(ed25519.PublicKey)
	if !ok {
		return errors.New("coordinator JWT public key must be Ed25519")
	}
	verify := auth.NewVerifier(env("AHP_RC_JWT_ISSUER"), map[string]ed25519.PublicKey{env("AHP_RC_JWT_KID"): pub})
	accessFile, err := os.Open(env("AHP_RC_AGENT_ACCESS_FILE"))
	if err != nil {
		return fmt.Errorf("open coordinator agent access policy: %w", err)
	}
	authorize, err := runcoord.ReadAgentAccess(accessFile)
	accessFile.Close()
	if err != nil {
		return err
	}
	policy := runcoord.AccessPolicy{
		Verifier: verify, UserAudience: env("AHP_RC_USER_AUDIENCE"), AuthorizeAgent: authorize,
		BrokerWorkloadIDs:       splitList(env("AHP_RC_BROKER_WORKLOAD_IDS")),
		PlatformEdgeWorkloadIDs: splitList(env("AHP_RC_PLATFORM_EDGE_WORKLOAD_IDS")),
		UserEdgeDeployments:     splitList(env("AHP_RC_USER_EDGE_DEPLOYMENTS")),
	}
	shutdown, err := ahpotel.Setup(ctx, ahpotel.Config{ServiceName: "ahp-run-coordinator", Endpoint: env("AHP_RC_OTLP_ENDPOINT")})
	if err != nil {
		return err
	}
	defer shutdown(context.Background())

	pool, err := pgxpool.New(ctx, env("AHP_RC_NOTIFY_DSN"))
	if err != nil {
		return errors.New("invalid coordinator notification database configuration")
	}
	defer pool.Close()
	bus := runcoord.NewPGSignalBus(pool)
	runtimeClient := *client
	runtimeClient.Timeout = 90 * time.Second
	dispatcher := runcoord.HTTPDispatcher{BaseURL: env("AHP_RC_RUNTIME_URL"), Client: &runtimeClient,
		TokenSource: tokenFileSource(env("AHP_RC_RUNTIME_TOKEN_FILE"))}
	global := state.Coordinator()
	c := &runcoord.Coordinator{GlobalState: global, Signals: bus, Dispatcher: dispatcher, AccessPolicy: policy,
		StateFor: func(scope contracts.Scope) runcoord.State { return global.ForScope(scope) }}
	protected := c.Handler()
	instrumented := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parent := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer("ahp-run-coordinator").Start(parent, "runCoordinator.HTTP")
		defer span.End()
		protected.ServeHTTP(w, r.WithContext(ctx))
	})
	addr := env("AHP_RC_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8453"
	}
	server := &http.Server{Addr: addr, Handler: instrumented, TLSConfig: tlsConfig, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second,
		BaseContext: func(_ net.Listener) context.Context { return ctx }}
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	go bus.Run(work)
	go c.RunReconcile(work, 2*time.Second)
	result := make(chan error, 1)
	go func() { result <- server.ListenAndServeTLS("", "") }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		grace, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		return server.Shutdown(grace)
	}
}

func tokenFileSource(path string) func(context.Context) (string, error) {
	return func(context.Context) (string, error) {
		token, err := os.ReadFile(path)
		return strings.TrimSpace(string(token)), err
	}
}

func splitList(value string) []string {
	var entries []string
	for entry := range strings.SplitSeq(value, ",") {
		if entry = strings.TrimSpace(entry); entry != "" {
			entries = append(entries, entry)
		}
	}
	return entries
}
