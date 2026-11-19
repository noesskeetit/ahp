// Package stand boots a REAL State API process for out-of-process clients.
// Command cmd/state-stand is the public entry point; tests/state/serve is the
// historical one and runs exactly the same code.
//
// It exists so non-Go consumers (the Python LangGraph adapter) talk to the
// production stateapi handler over real mTLS with real Ed25519 task tokens,
// against a real PostgreSQL database. Nothing is emulated: the same handler,
// middleware, store and migrations the Go SDK exercises are used here.
//
// It is a PUBLIC tool for the SDK owner and for integration validation:
//
//	go run ./cmd/state-stand -env private/root-acceptance-pg/state-owner.env \
//	                         -handshake /tmp/state-handshake/handshake.json
//
// which loads the lane env file, refuses any database outside -db-allowlist,
// runs the product migrations and prints READY once it is serving. Every flag
// also exists as an environment variable, and the environment stays the single
// source of truth (flags are written into it before they are read):
//
//	AHP_STATE_DSN | DATABASE_URL   isolated database (required)
//	AHP_STATE_TENANT               tenant of the mounted workload (required)
//	AHP_STATE_DEPLOYMENT           deployment of the mounted workload (required)
//	AHP_STATE_SUBJECT              human subject minted into the task token
//	AHP_STATE_HANDSHAKE            path of the handshake JSON to write
//	AHP_STATE_ADDR                 listen address (default 127.0.0.1:0)
//	AHP_STATE_STAND_COMPACT_JOURNAL mount only the compact journal when set to 1
//	AHP_STATE_FOREIGN_TENANT       tenant of a second, foreign workload that the
//	                               evidence driver uses to prove scope isolation
//	                               on the wire (default: <tenant>-foreign)
//	AHP_STATE_REFRESH_SECONDS      re-mint the handshake task token every N
//	                               seconds (default 240; a task token lives at
//	                               most 15 minutes - pkg/auth maxTaskTTL)
//
// Secrets (DSN, keys, tokens) are never printed; only the handshake file,
// written 0600 inside a 0700 directory, carries them for the child process.
package stand

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	audience = "ahp"
	issuer   = "https://identity.state-serve.test"
	keyID    = "k1"
	// tokenTTL is the product ceiling for a task token (pkg/auth maxTaskTTL):
	// the bridge never mints a credential that is longer than the platform
	// allows, so a long session re-mints instead of extending.
	tokenTTL = 15 * time.Minute
	// certTTL bounds the harness PKI, not a run. It outlives a token on
	// purpose: one acceptance session spans many token re-mints, and the
	// workload certificate is not what fences a run.
	certTTL   = 24 * time.Hour
	migBudget = 90 * time.Second
	// refreshDefault is how often the handshake task token is re-minted so a
	// long out-of-process session (SDK + custom A2A agent) can pick up fresh
	// credentials without restarting the candidate.
	refreshDefault = 240 * time.Second
)

// handshake is the bootstrap material handed to the child client process.
type handshake struct {
	BaseURL    string `json:"base_url"`
	CAFile     string `json:"ca_file"`
	CertFile   string `json:"cert_file"`
	KeyFile    string `json:"key_file"`
	TaskToken  string `json:"task_token"`
	Tenant     string `json:"tenant"`
	Deployment string `json:"deployment"`
	Subject    string `json:"subject"`
	// Foreign workload material: a second certificate and token bound to a
	// DIFFERENT tenant, so a client can prove on the wire that a foreign scope
	// cannot read or mutate this scope's resources.
	ForeignTenant    string `json:"foreign_tenant,omitempty"`
	ForeignCertFile  string `json:"foreign_cert_file,omitempty"`
	ForeignKeyFile   string `json:"foreign_key_file,omitempty"`
	ForeignTaskToken string `json:"foreign_task_token,omitempty"`
	PID              int    `json:"pid"`
}

var serial int64 = 7

func mustEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		fail(fmt.Errorf("%s must be set", name))
	}
	return v
}

// runFailure unwinds a fatal failure out of run(). Background goroutines use
// guard(), which turns the same panic into an error on the fatal channel, so a
// failure never escapes as a crash and - unlike the os.Exit this replaces -
// never terminates a host process either.
type runFailure struct{ err error }

func fail(err error) {
	if err != nil {
		panic(runFailure{err})
	}
}

// guard wraps a goroutine body so that fail() inside it reports on fatal.
func guard(fatal chan<- error, body func()) func() {
	return func() {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			rf, ok := r.(runFailure)
			if !ok {
				panic(r)
			}
			select {
			case fatal <- rf.err:
			default:
			}
		}()
		body()
	}
}

func caPair() (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		fail(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ahp-state-serve-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(certTTL),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		fail(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		fail(err)
	}
	return cert, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func leaf(caCert *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, server bool, uris []string) (tls.Certificate, []byte, []byte) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		fail(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(atomic.AddInt64(&serial, 1)),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour), NotAfter: time.Now().Add(certTTL),
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		for _, u := range uris {
			parsed, err := url.Parse(u)
			if err != nil {
				fail(fmt.Errorf("bad workload uri %q: %w", u, err))
			}
			tmpl.URIs = append(tmpl.URIs, parsed)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		fail(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		fail(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		fail(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, certPEM, keyPEM
}

// workloadURI is the SPIFFE identity of one tenant/deployment workload.
func workloadURI(tenant, deployment string) string {
	return "spiffe://ahp/tenant/" + tenant + "/deployment/" + deployment
}

// writeHandshake replaces the handshake file atomically: a client reading it
// concurrently observes either the previous or the new complete document, and
// the file never exists with a token it has already used.
func writeHandshake(path string, hs handshake) {
	data, err := json.Marshal(hs)
	if err != nil {
		fail(err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		fail(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		fail(err)
	}
}

// envSeconds reads a positive interval in seconds, falling back to def.
func envSeconds(name string, def time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	return time.Duration(n) * time.Second
}

// writeSecret writes data with owner-only permissions and returns the path.
func writeSecret(dir, name string, data []byte) string {
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		fail(err)
	}
	return path
}

// Run starts the stand and serves until SIGINT or SIGTERM. It returns the
// failure that stopped it, or nil after a clean shutdown. Like any library
// entry point it never exits the process - the command decides the exit code.
func Run(args []string) error { return run(args, DefaultDBAllowlist) }

// RunLegacy is the compatibility entry point of tests/state/serve, which the
// director's acceptance tooling still builds: the same stand, the historical
// log prefix, and no database allowlist unless the environment asks for one.
func RunLegacy(args []string) error {
	Prefix = "state-serve"
	return run(args, "")
}

func run(args []string, defaultAllowlist string) (stopped error) {
	defer func() {
		if r := recover(); r != nil {
			rf, ok := r.(runFailure)
			if !ok {
				panic(r)
			}
			stopped = rf.err
		}
	}()
	// fatal carries the first failure raised inside a background goroutine.
	fatal := make(chan error, 1)
	if err := setupFlags(args, defaultAllowlist); err != nil {
		return err
	}
	dsn := os.Getenv("AHP_STATE_DSN")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		fail(errors.New("AHP_STATE_DSN or DATABASE_URL must be set"))
	}
	if err := guardDatabase(dsn, os.Getenv("AHP_STATE_DB_ALLOWLIST")); err != nil {
		fail(err)
	}
	tenant := mustEnv("AHP_STATE_TENANT")
	deployment := mustEnv("AHP_STATE_DEPLOYMENT")
	subject := os.Getenv("AHP_STATE_SUBJECT")
	if subject == "" {
		subject = "user-ada"
	}
	handshakePath := mustEnv("AHP_STATE_HANDSHAKE")
	addr := os.Getenv("AHP_STATE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:0"
	}

	ctx, cancel := context.WithTimeout(context.Background(), migBudget)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fail(err)
	}
	defer pool.Close()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		fail(err)
	}
	signer, err := auth.NewSigner(issuer, keyID, priv)
	if err != nil {
		fail(err)
	}
	verifier := auth.NewVerifier(issuer, map[string]ed25519.PublicKey{keyID: pub})

	// An authorizer that admits every grant: authorization policy belongs to
	// the control plane, this bridge only proves the wire path is real.
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	issue := func(_ context.Context, scope contracts.Scope, taskID string) (string, error) {
		return signer.Sign(auth.Claims{
			Audience: audience, Kind: auth.KindTask,
			Tenant: scope.Tenant, Deployment: scope.Deployment,
			Subject: scope.Subject, Task: taskID, Root: "root-" + taskID,
			Context: "ctx-" + taskID, Grant: "grant-" + taskID, Depth: 1,
		}, tokenTTL)
	}

	var handler http.Handler
	if os.Getenv("AHP_STATE_STAND_COMPACT_JOURNAL") == "1" {
		store := statejournal.New(pool)
		if err := store.Migrate(ctx); err != nil {
			fail(fmt.Errorf("migrate compact journal schema: %w", err))
		}
		handler, err = statejournal.NewHTTP(store, verifier, authorize)
	} else {
		store := state.New(pool)
		if err := store.Migrate(ctx); err != nil {
			fail(fmt.Errorf("migrate state schema: %w", err))
		}
		if err := store.MigrateControl(ctx); err != nil {
			fail(fmt.Errorf("migrate state control schema: %w", err))
		}
		handler, err = stateapi.New(store, verifier, authorize, issue)
	}
	if err != nil {
		fail(err)
	}

	dir := filepath.Dir(handshakePath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		fail(err)
	}
	caCert, caKey, caPEM := caPair()
	caFile := writeSecret(dir, "ca.pem", caPEM)

	serverCert, serverPEM, serverKeyPEM := leaf(caCert, caKey, "ahp-state-serve", true, nil)
	_, clientPEM, clientKeyPEM := leaf(caCert, caKey, "ahp-state-serve-client", false,
		[]string{workloadURI(tenant, deployment)})
	certFile := writeSecret(dir, "client-cert.pem", clientPEM)
	keyFile := writeSecret(dir, "client-key.pem", clientKeyPEM)
	_ = writeSecret(dir, "server-cert.pem", serverPEM)
	_ = writeSecret(dir, "server-key.pem", serverKeyPEM)

	// A second, foreign workload: its own certificate and task token, bound to
	// a different tenant. The evidence driver uses it to show on the wire that
	// scope isolation is real - foreign credentials must not reach this scope's
	// threads, and a foreign token on this scope's certificate must be refused.
	foreignTenant := os.Getenv("AHP_STATE_FOREIGN_TENANT")
	if foreignTenant == "" {
		foreignTenant = tenant + "-foreign"
	}
	_, foreignPEM, foreignKeyPEM := leaf(caCert, caKey, "ahp-state-serve-foreign", false,
		[]string{workloadURI(foreignTenant, deployment)})
	foreignCertFile := writeSecret(dir, "foreign-client-cert.pem", foreignPEM)
	foreignKeyFile := writeSecret(dir, "foreign-client-key.pem", foreignKeyPEM)

	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(caPEM) {
		fail(fmt.Errorf("ca pem rejected"))
	}

	// The bootstrap token is bound to the human subject, never to a task id:
	// the subject is the isolation identity for threads and state.
	mint := func(tenantID, subjectID string) string {
		token, err := signer.Sign(auth.Claims{
			Audience: audience, Kind: auth.KindTask,
			Tenant: tenantID, Deployment: deployment,
			Subject: subjectID, Task: "task-bootstrap", Root: "root-bootstrap",
			Context: "ctx-bootstrap", Grant: "grant-bootstrap", Depth: 0,
		}, tokenTTL)
		if err != nil {
			fail(err)
		}
		return token
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fail(err)
	}
	tlsListener := tls.NewListener(listener, &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    serverCAs,
		MinVersion:   tls.VersionTLS12,
	})

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go guard(fatal, func() {
		if err := srv.Serve(tlsListener); err != nil && err != http.ErrServerClosed {
			fail(err)
		}
	})()

	hs := handshake{
		BaseURL:          "https://" + tlsListener.Addr().String(),
		CAFile:           caFile,
		CertFile:         certFile,
		KeyFile:          keyFile,
		TaskToken:        mint(tenant, subject),
		Tenant:           tenant,
		Deployment:       deployment,
		Subject:          subject,
		ForeignTenant:    foreignTenant,
		ForeignCertFile:  foreignCertFile,
		ForeignKeyFile:   foreignKeyFile,
		ForeignTaskToken: mint(foreignTenant, subject),
		PID:              os.Getpid(),
	}
	writeHandshake(handshakePath, hs)

	// A task token is capped at 15 minutes by the platform (pkg/auth). A long
	// acceptance session therefore re-mints instead of stretching the
	// credential: the handshake file is replaced atomically, so an
	// out-of-process client that re-reads it always finds a live token.
	refresh := envSeconds("AHP_STATE_REFRESH_SECONDS", refreshDefault)
	refreshDone := make(chan struct{})
	go guard(fatal, func() {
		ticker := time.NewTicker(refresh)
		defer ticker.Stop()
		for {
			select {
			case <-refreshDone:
				return
			case <-ticker.C:
				hs.TaskToken = mint(tenant, subject)
				hs.ForeignTaskToken = mint(foreignTenant, subject)
				writeHandshake(handshakePath, hs)
			}
		}
	})()

	fmt.Println("READY")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-stop:
	case stopped = <-fatal:
	}
	close(refreshDone)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = os.Remove(handshakePath)
	return stopped
}
