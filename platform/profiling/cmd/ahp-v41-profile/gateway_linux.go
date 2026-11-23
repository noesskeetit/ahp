package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/internal/tools/toolsapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	profileIssuer          = "ahp-profile-identity"
	profileKeyID           = "identity"
	profileLeaseIssuer     = "ahp-profile-state"
	profileLeaseKeyID      = "state-lease"
	profileAudience        = "ahp-browser"
	profileConnection      = "profile-mcp"
	profileMCPTransportEnv = "AHP_PROFILE_MCP_TRANSPORT"
)

type connectorCredential struct{ bearer string }

func (c connectorCredential) Resolve(_ context.Context, _ contracts.Scope, name string) (tools.Credential, error) {
	if name != profileConnection {
		return tools.Credential{}, errors.New("unknown profiling connector")
	}
	return tools.Credential{Bearer: c.bearer}, nil
}

type countedMCPTransport struct {
	base          *http.Transport
	bearer        string
	fresh, reused *atomic.Uint64
}

func (t countedMCPTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	req := r.Clone(httptrace.WithClientTrace(r.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				t.reused.Add(1)
			} else {
				t.fresh.Add(1)
			}
		},
	}))
	if t.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+t.bearer)
	}
	return t.base.RoundTrip(req)
}

type profileMCPClient struct{ *mcp.ClientSession }

func (c profileMCPClient) Close() error { return c.ClientSession.Close() }

func loadVerifier(path, issuer, kid string) (*auth.Verifier, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != "PUBLIC KEY" {
		return nil, errors.New("invalid Ed25519 public key PEM")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	public, ok := key.(ed25519.PublicKey)
	if err != nil || !ok {
		return nil, errors.New("invalid Ed25519 public key")
	}
	return auth.NewVerifier(issuer, map[string]ed25519.PublicKey{kid: public}), nil
}

func endpointTLS(ca, cert, key string) (*tls.Config, error) {
	tlsCert, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, errors.New("invalid profiling CA")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{tlsCert},
		RootCAs: pool, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}, nil
}

func profileS3(endpoint, access, secret string) (*minio.Client, string, bool, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.User != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return nil, "", false, errors.New("invalid profiling MinIO endpoint")
	}
	secure := u.Scheme == "https"
	client, err := minio.New(u.Host, &minio.Options{Secure: secure, Creds: credentials.NewStaticV4(access, secret, "")})
	return client, u.Host, secure, err
}

func runGateway(ctx context.Context) error {
	env := os.Getenv
	for _, name := range []string{"ADDR", "DEBUG", "DSN", "IDENTITY_DSN", "TOKEN_PUBLIC", "LEASE_PUBLIC", "CA", "CERT", "KEY", "S3_ENDPOINT", "S3_ACCESS", "S3_SECRET", "S3_BUCKET", "MCP_URL", "MCP_CANARY", "TENANTS"} {
		if env("AHP_PROFILE_GATEWAY_"+name) == "" {
			return fmt.Errorf("missing gateway setting %s", name)
		}
	}
	get := func(name string) string { return env("AHP_PROFILE_GATEWAY_" + name) }
	transportMode := env(profileMCPTransportEnv)
	if transportMode == "" {
		transportMode = "per-call"
	}
	var pooled *http.Transport
	switch transportMode {
	case "per-call":
	case "pooled":
		// The stand has one endpoint and one trust profile. Match the
		// static pool limits in cmd/ahp/mount_tools.go.
		pooled = &http.Transport{
			TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     30 * time.Second,
		}
		defer pooled.CloseIdleConnections()
	default:
		return fmt.Errorf("%s must be per-call or pooled", profileMCPTransportEnv)
	}
	pool, err := pgxpool.New(ctx, get("DSN"))
	if err != nil {
		return errors.New("invalid Tools pool configuration")
	}
	defer pool.Close()
	idPool, err := pgxpool.New(ctx, get("IDENTITY_DSN"))
	if err != nil {
		return errors.New("invalid Tools Identity pool configuration")
	}
	defer idPool.Close()
	ids := identity.New(idPool)
	if err := idPool.Ping(ctx); err != nil {
		return fmt.Errorf("connect Tools Identity: %w", err)
	}
	verify, err := loadVerifier(get("TOKEN_PUBLIC"), profileIssuer, profileKeyID)
	if err != nil {
		return err
	}
	proof, err := loadVerifier(get("LEASE_PUBLIC"), profileLeaseIssuer, profileLeaseKeyID)
	if err != nil {
		return err
	}
	s3, _, _, err := profileS3(get("S3_ENDPOINT"), get("S3_ACCESS"), get("S3_SECRET"))
	if err != nil {
		return err
	}
	store, err := artifacts.New(pool, s3, get("S3_BUCKET"), func(ctx context.Context, scope contracts.Scope, _, _ string) error {
		p, ok := transport.FromContext(ctx)
		if !ok || p.Scope != scope {
			return contracts.NewError(contracts.ErrForbidden, "artifact scope must match the verified tool call")
		}
		return ids.Authorize(ctx, p.Claims, "tools")
	})
	if err != nil {
		return err
	}
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate journal artifacts: %w", err)
	}

	var fresh, reused atomic.Uint64
	service, err := tools.New(ctx, pool, "ahp_tools", tools.Config{
		JournalArtifacts: store, Guardian: tools.NewPolicyGuardian("profile"), Credentials: connectorCredential{get("MCP_CANARY")},
		Connections: map[string]tools.Connector{profileConnection: func(ctx context.Context, cred tools.Credential) (tools.MCPClient, error) {
			base := pooled
			if base == nil {
				// Preserve the pinned mount_tools.go per-invocation transport.
				base = &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
			}
			client := tools.LimitUpstream(&http.Client{Transport: countedMCPTransport{base: base, bearer: cred.Bearer, fresh: &fresh, reused: &reused}}, tools.DefaultMaxUpstreamBytes)
			session, err := mcp.NewClient(&mcp.Implementation{Name: "ahp-tools-gateway", Version: "1"}, nil).Connect(ctx,
				&mcp.StreamableClientTransport{Endpoint: get("MCP_URL"), HTTPClient: client}, nil)
			if err != nil {
				return nil, err
			}
			return profileMCPClient{session}, nil
		}},
		Tools:    map[string]tools.ToolSpec{profileTool: {Name: profileTool, Connection: profileConnection, SideEffecting: true}},
		Verifier: verify, Audience: "ahp",
	})
	if err != nil {
		return err
	}
	for _, t := range strings.Split(get("TENANTS"), ",") {
		if err := service.GrantOperatorPolicy(ctx, contracts.Scope{Tenant: t, Deployment: profileDeployment, Subject: profileSubject},
			profileConnection, profileTool, "profile-operator"); err != nil {
			return fmt.Errorf("provision tool policy: %w", err)
		}
	}
	routes, err := toolsapi.NewCoexistingHandlerWithToolLeases(ctx, service, verify, ids.Authorize, proof, "", nil)
	if err != nil {
		return err
	}

	var held sync.Map
	faults := get("FAULTS") == "true"
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !faults || r.Method != http.MethodPost || r.URL.Path != "/v1/tools/Call" {
			routes.ServeHTTP(w, r)
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "cannot read tool request", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		var call struct {
			Arguments effectArgs `json:"arguments"`
		}
		if json.Unmarshal(raw, &call) != nil || !call.Arguments.HoldReply {
			routes.ServeHTTP(w, r)
			return
		}
		recorded := httptest.NewRecorder()
		routes.ServeHTTP(recorded, r)
		var reply struct {
			Status string `json:"status"`
		}
		if recorded.Code == http.StatusOK && json.Unmarshal(recorded.Body.Bytes(), &reply) == nil && reply.Status == "SUCCEEDED" {
			if _, alreadyHeld := held.LoadOrStore(call.Arguments.EffectID, struct{}{}); !alreadyHeld {
				// This is only the guest-facing acknowledgement. The real owner has already committed the receipt.
				<-r.Context().Done()
				return
			}
		}
		for name, values := range recorded.Header() {
			w.Header()[name] = values
		}
		w.WriteHeader(recorded.Code)
		_, _ = w.Write(recorded.Body.Bytes())
	}))
	tlsConfig, err := endpointTLS(get("CA"), get("CERT"), get("KEY"))
	if err != nil {
		return err
	}
	main := &http.Server{Addr: get("ADDR"), Handler: mux, TLSConfig: tlsConfig, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx }}
	monitor := http.NewServeMux()
	installPprofRoutes(monitor)
	monitor.HandleFunc("GET /debug/ahp-profile/held", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := held.Load(r.URL.Query().Get("effect_id")); !ok {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	monitor.HandleFunc("GET /debug/ahp-profile/stats", func(w http.ResponseWriter, r *http.Request) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"heap_inuse": m.HeapInuse, "goroutines": runtime.NumGoroutine(),
			"pools":                map[string]any{"tools": readPoolStats(pool), "identity": readPoolStats(idPool)},
			"upstream_connections": map[string]any{"mcp": map[string]uint64{"fresh": fresh.Load(), "reused": reused.Load()}},
			"mcp_transport_mode":   transportMode,
		})
	})
	debug, err := net.Listen("tcp", get("DEBUG"))
	if err != nil {
		return err
	}
	runtime.SetMutexProfileFraction(10)
	admin := &http.Server{Handler: monitor, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = admin.Serve(debug) }()
	defer admin.Close()
	done := make(chan error, 1)
	go func() { done <- main.ListenAndServeTLS("", "") }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		deadline, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		defer main.Close()
		return main.Shutdown(deadline)
	}
}

func installPprofRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
}

func readPoolStats(pool *pgxpool.Pool) map[string]any {
	s := pool.Stat()
	return map[string]any{"acquire_count": s.AcquireCount(), "acquire_duration_ns": s.AcquireDuration().Nanoseconds(),
		"empty_acquire_count": s.EmptyAcquireCount(), "empty_acquire_wait_ns": s.EmptyAcquireWaitTime().Nanoseconds(),
		"total_conns": s.TotalConns(), "acquired_conns": s.AcquiredConns(), "idle_conns": s.IdleConns(), "max_conns": s.MaxConns()}
}
