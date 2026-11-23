// Command ahp runs AHP platform services. Each process assumes one role
// selected by --role or AHP_ROLE; the "all" role co-locates every service on
// separate ports for development.
//
// Usage:
//
//	ahp --role state --listen :8080 [--tls-cert path --tls-key path --tls-ca path]
//	ahp --role identity --listen :8081
//	ahp --role execution --listen :8082   (requires -tags ahp_exec)
//	ahp --role data --listen :8083        (requires -tags ahp_data)
//	ahp --role all  (development: every role on AHP_LISTEN ports)
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/internal/buildinfo"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/identityapi"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/runtimeprofile"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	aot "github.com/ahp-platform/ahp/pkg/otel"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
)

var version = "dev"

func main() {
	os.Exit(run())
}

func run() int {
	var (
		role    = flag.String("role", envOr("AHP_ROLE", ""), "service role: state|identity|model|execution|data|all")
		listen  = flag.String("listen", envOr("AHP_LISTEN", ""), "listen address (single-role)")
		tlsCert = flag.String("tls-cert", envOr("AHP_TLS_CERT", ""), "TLS certificate file (PEM)")
		tlsKey  = flag.String("tls-key", envOr("AHP_TLS_KEY", ""), "TLS private key file (PEM)")
		tlsCA   = flag.String("tls-ca", envOr("AHP_TLS_CA", ""), "client CA for mTLS (PEM)")
		otelEP  = flag.String("otel-endpoint", envOr("OTEL_EXPORTER_OTLP_ENDPOINT", ""), "OTLP gRPC endpoint")
		verb    = flag.Bool("verbose", envOr("AHP_VERBOSE", "") == "1", "debug logging")
		showVer = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVer {
		fmt.Fprintf(os.Stderr, "ahp %s %s\n", version, buildinfo.Resolve().String())
		return 0
	}

	lvl := slog.LevelInfo
	if *verb {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(logger)
	logger.Info("build identity", buildinfo.Resolve().LogAttrs()...)
	stopProfile, profileErr := runtimeprofile.StartFromEnv("ahp")
	if profileErr != nil {
		logger.Warn("runtime diagnostic profile unavailable", "code", "profile_init_failed")
	}
	defer stopProfile()

	if *role == "" {
		logger.Error("role is required: --role state|identity|model|all")
		return 1
	}

	dbURL := envOr("AHP_DATABASE_URL", "")
	if dbURL == "" {
		logger.Error("AHP_DATABASE_URL is required")
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	otelShutdown, err := aot.Setup(ctx, aot.Config{
		ServiceName: "ahp-" + *role,
		Endpoint:    *otelEP,
		Insecure:    envOr("OTEL_EXPORTER_OTLP_INSECURE", "") == "true",
	})
	if err != nil {
		logger.Error("otel setup failed", "err", err)
		return 1
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer scancel()
		_ = otelShutdown(sctx)
	}()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		logger.Error("database connection pool failed", "err", err)
		return 1
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		logger.Error("database ping failed", "err", err)
		return 1
	}

	tlsCfg, err := buildTLSConfig(*tlsCert, *tlsKey, *tlsCA)
	if err != nil {
		logger.Error("TLS config failed", "err", err)
		return 1
	}

	s := &server{
		pool:   pool,
		logger: logger,
		tlsCfg: tlsCfg,
	}

	if err := s.configureIdentityProfile(); err != nil {
		logger.Error("identity profile configuration failed", "err", err)
		return 1
	}
	switch strings.ToLower(*role) {
	case "state":
		if *listen == "" {
			logger.Error("--listen is required for single-role mode")
			return 1
		}
		if err := s.serveState(ctx, *listen); err != nil {
			logger.Error("state service failed", "err", err)
			return 1
		}
	case "identity":
		if *listen == "" {
			logger.Error("--listen is required for single-role mode")
			return 1
		}
		if err := s.serveIdentity(ctx, *listen); err != nil {
			logger.Error("identity service failed", "err", err)
			return 1
		}
	case "model":
		if *listen == "" {
			logger.Error("--listen is required for single-role mode")
			return 1
		}
		if err := s.serveModel(ctx, *listen); err != nil {
			logger.Error("model service failed", "err", err)
			return 1
		}
	case "all":
		if err := s.serveAll(ctx); err != nil {
			logger.Error("all-in-one failed", "err", err)
			return 1
		}
	case "execution":
		if *listen == "" {
			logger.Error("--listen is required for single-role mode")
			return 1
		}
		if err := s.serveExecution(ctx, *listen); err != nil {
			logger.Error("execution service failed", "err", err)
			return 1
		}
	case "provision-synthetic-iam":
		if err := s.provisionSyntheticIAM(ctx); err != nil {
			logger.Error("IAM provisioning failed", "err", err)
			return 1
		}
		return 0
	case "provision-browser-users":
		if err := s.provisionBrowserUsers(ctx); err != nil {
			logger.Error("provision-browser-users failed", "err", err)
			return 1
		}
		return 0
	case "data":
		if *listen == "" {
			logger.Error("--listen is required for single-role mode")
			return 1
		}
		if err := s.serveData(ctx, *listen); err != nil {
			logger.Error("data service failed", "err", err)
			return 1
		}
	default:
		logger.Error("unknown role", "role", *role)
		return 1
	}
	return 0
}

type server struct {
	artifactExportOwner   artifactExportOwner
	iam                   identity.ResourceEvaluator
	authenticator         identity.Authenticator
	exchangeSigner        *auth.Signer
	stateOwner            *state.Store
	stateOwnerAuthorize   transport.Authorize
	compactStateReady     func(context.Context) error
	tenantDeletionOwners  map[string]TenantDeletionOwner
	subjectDeletionOwners map[string]SubjectDeletionOwner
	pool                  *pgxpool.Pool
	logger                *slog.Logger
	tlsCfg                *tls.Config
	// cascade is the artifact-plane half of the deletion cascade, installed by
	// the data plane when it is mounted in this process. It is an interface so
	// the untagged build stays free of internal/artifacts and minio-go.
	cascade artifactCascade

	// gcDrainOnShutdown is a hook the background GC scheduler sets when it mounts
	// so graceful shutdown waits for a sweep already in flight instead of dropping
	// it (and the connection pool it still uses) mid-collect. It is a plain func
	// so the untagged build -- which has no GC scheduler -- never references the
	// data-plane type; it stays nil for every role that does not mount the
	// scheduler, in which case shutdown is byte-for-byte what it was.
	gcDrainOnShutdown func(context.Context)

	// computePlaneMounted says this process answered the Compute contract itself
	// (role_compute.go mounted computeapi). computeRouteOwned says this process
	// answers the /v1/compute/ prefix at all -- either with the plane or with the
	// explicit UNAVAILABLE face that replaces it when the mount failed. The single-
	// origin edge reads the second one: forwarding a prefix this process already
	// owns is the panic DEF-COMPUTE-1 measured, and forwarding it while the mount
	// failed would hide a broken plane behind a runner's 404.
	//
	// They are separate fields because the operator answer differs: a plane that is
	// simply not asked for forwards (the composition behaves as it always did),
	// while a plane that was asked for and could not mount must refuse out loud.
	computePlaneMounted bool
	computeRouteOwned   bool
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}` + "\n"))
}

func (s *server) handleReadiness(w http.ResponseWriter, _ *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	if err := s.pool.Ping(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready","reason":"database unreachable"}` + "\n"))
		return
	}
	if s.compactStateReady != nil && s.compactStateReady(ctx) != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready","reason":"compact State bucket unreachable"}` + "\n"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}` + "\n"))
}

func (s *server) baseMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReadiness)
	mux.HandleFunc("GET /debug/stats", s.handleStats)
	return mux
}

func (s *server) serveState(ctx context.Context, addr string) error {
	if s.iam != nil {
		return fmt.Errorf("synthetic IAM requires the Identity-backed state owner in role=all")
	}
	options, optionErr := loadStateStoreOptions()
	if optionErr != nil {
		return optionErr
	}
	st := state.New(s.pool, options...)
	if err := migrateStateAll(ctx, st); err != nil {
		return err
	}

	verifier, taskIssuer := devAuth(st)
	authorize := func(_ context.Context, _ auth.Claims, _ string) error { return nil }

	stateHandler, err := stateapi.New(st, verifier, authorize, taskIssuer)
	if err != nil {
		return fmt.Errorf("stateapi: %w", err)
	}

	mux := s.baseMux()
	mux.Handle("/v1/state/", s.withDeletionCascade(stateHandler, st, verifier))
	stopCompactState, err := s.mountCompactState(ctx, mux, verifier, compactStateClaimsAuthorize)
	if err != nil {
		return err
	}
	defer stopCompactState()

	handler := aot.HTTPMiddleware("state")(mux)
	return s.runServer(ctx, "state", addr, handler)
}

// devAuth returns the development verifier and task issuer. It trusts a single
// Ed25519 key under kid "dev-1". When AHP_DEV_JWT_SEED is set (hex or base64 of
// a 32-byte Ed25519 seed) the key is deterministic, so an out-of-process test
// harness holding the same seed can mint tokens this server verifies, and the
// server can mint the fenced task tokens that ClaimRun returns. Without the
// seed a fresh ephemeral key is used (self-consistent in-process only).
//
// The issued task token carries the context (thread) of the run it is minted
// for, resolved from State, because every record written under that token is
// supposed to know its context ("каждая запись знает tenant, деплой, субъект и
// контекст"). A task State does not know keeps its task id as the context
// rather than a placeholder that would name no thread at all.
func devAuth(st *state.Store) (*auth.Verifier, stateapi.TaskIssuer) {
	priv, pub := devSigningKey()
	keys := map[string]ed25519.PublicKey{"dev-1": pub}
	if ext, kid, ok := devExternalKey(); ok {
		keys[kid] = ext
	}
	verifier := auth.NewVerifier("ahp", keys)
	signer, err := auth.NewSigner("ahp", "dev-1", priv)
	if err != nil {
		panic("ahp: dev signer: " + err.Error())
	}
	issue := func(ctx context.Context, scope contracts.Scope, taskID string) (string, error) {
		contextID := taskContext(ctx, st, scope, taskID)
		if contextID == "" {
			contextID = taskID
		}
		claims := auth.Claims{
			Audience:   "ahp",
			Kind:       auth.KindTask,
			Tenant:     scope.Tenant,
			Deployment: scope.Deployment,
			Subject:    scope.Subject,
			Task:       taskID,
			Root:       taskID,
			Context:    contextID,
			Grant:      "dev",
			Depth:      0,
		}
		return signer.Sign(claims, 10*time.Minute)
	}
	return verifier, issue
}

// devSigningKey derives the dev Ed25519 keypair from AHP_DEV_JWT_SEED when a
// valid 32-byte seed is configured, otherwise it generates an ephemeral pair.
// Secrets are never logged.
func devSigningKey() (ed25519.PrivateKey, ed25519.PublicKey) {
	if raw := strings.TrimSpace(envOr("AHP_DEV_JWT_SEED", "")); raw != "" {
		if seed, err := decodeSeed(raw); err != nil {
			slog.Warn("AHP_DEV_JWT_SEED not decodable; using ephemeral dev key", "err", err)
		} else if len(seed) != ed25519.SeedSize {
			slog.Warn("AHP_DEV_JWT_SEED must decode to 32 bytes; using ephemeral dev key")
		} else {
			priv := ed25519.NewKeyFromSeed(seed)
			return priv, priv.Public().(ed25519.PublicKey)
		}
	}
	return mustEd25519Pair()
}

// decodeSeed decodes a seed from hex, then standard, then raw URL base64.
func decodeSeed(s string) ([]byte, error) {
	if b, err := hex.DecodeString(s); err == nil {
		return b, nil
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.RawURLEncoding.DecodeString(s)
}

// devExternalKey loads an additional trusted Ed25519 public key from the PEM
// file named by AHP_DEV_JWT_PUBKEY, registered under kid AHP_DEV_KID (default
// "ivk1"). It accepts a "PUBLIC KEY" (PKIX) or "PRIVATE KEY" (PKCS8) PEM so a
// development harness can point it at its own signing key to mint task tokens
// this server verifies, mirroring the integration-validation receiver. Returns
// ok=false when unset or unusable. Secrets are never logged.
func devExternalKey() (ed25519.PublicKey, string, bool) {
	path := strings.TrimSpace(envOr("AHP_DEV_JWT_PUBKEY", ""))
	if path == "" {
		return nil, "", false
	}
	kid := strings.TrimSpace(envOr("AHP_DEV_KID", "ivk1"))
	raw, err := os.ReadFile(path)
	if err != nil {
		slog.Warn("AHP_DEV_JWT_PUBKEY unreadable; skipping external verifier key", "err", err)
		return nil, "", false
	}
	blk, _ := pem.Decode(raw)
	if blk == nil {
		slog.Warn("AHP_DEV_JWT_PUBKEY has no PEM block; skipping external verifier key")
		return nil, "", false
	}
	switch blk.Type {
	case "PUBLIC KEY":
		pk, err := x509.ParsePKIXPublicKey(blk.Bytes)
		if err != nil {
			slog.Warn("AHP_DEV_JWT_PUBKEY PKIX parse failed; skipping", "err", err)
			return nil, "", false
		}
		ed, ok := pk.(ed25519.PublicKey)
		if !ok {
			slog.Warn("AHP_DEV_JWT_PUBKEY is not ed25519; skipping")
			return nil, "", false
		}
		return ed, kid, true
	case "PRIVATE KEY":
		k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
		if err != nil {
			slog.Warn("AHP_DEV_JWT_PUBKEY PKCS8 parse failed; skipping", "err", err)
			return nil, "", false
		}
		ed, ok := k.(ed25519.PrivateKey)
		if !ok {
			slog.Warn("AHP_DEV_JWT_PUBKEY private key is not ed25519; skipping")
			return nil, "", false
		}
		return ed.Public().(ed25519.PublicKey), kid, true
	default:
		slog.Warn("AHP_DEV_JWT_PUBKEY unsupported PEM type; skipping", "type", blk.Type)
		return nil, "", false
	}
}

func (s *server) serveIdentity(ctx context.Context, addr string) error {
	idStore := s.identityStore()
	if err := idStore.Migrate(ctx); err != nil {
		return fmt.Errorf("identity migrate: %w", err)
	}

	verifier, signer := devIdentityAuth()
	mux := s.baseMux()
	idConfig := identityapi.Config{
		Auth:     s.authenticator,
		Store:    idStore,
		Verifier: verifier,
		Signer:   signer,
		Audience: "ahp",
	}
	if envOr("AHP_ENABLE_BROWSER", "") == "1" {
		idConfig.Browser = &identityapi.BrowserConfig{AccessAudience: envOr("AHP_BROWSER_AUDIENCE", "ahp-browser")}
	}
	idConfig.ControlExchange = envOr("AHP_ENABLE_CONTROL_EXCHANGE", "") == "1"
	idConfig.ExchangerDeployments = envListOr("AHP_IDENTITY_EXCHANGER_DEPLOYMENTS")
	s.configureIdentityAPIAuthority(&idConfig)
	handlerAPI := identityapi.Handler(idConfig)
	mux.Handle("/v1/identity/", handlerAPI)
	if idConfig.Browser != nil {
		mux.Handle("/u-ahp/v1/", handlerAPI)
	}

	handler := aot.HTTPMiddleware("identity")(mux)
	return s.runServer(ctx, "identity", addr, handler)
}

func (s *server) serveModel(ctx context.Context, addr string) error {
	ms := model.NewBudgetStore(s.pool)
	if err := ms.Migrate(ctx); err != nil {
		return fmt.Errorf("model migrate: %w", err)
	}

	mux := s.baseMux()
	ids := s.identityStore()
	if err := ids.Migrate(ctx); err != nil {
		return err
	}
	verifier, _ := devIdentityAuth()
	if err := s.tryMountModelGateway(ctx, mux, verifier, ids.Authorize); err != nil {
		return err
	}
	if err := s.tryMountModelCost(ctx, mux, verifier, ids.Authorize); err != nil {
		return err
	}
	handler := aot.HTTPMiddleware("model")(mux)
	return s.runServer(ctx, "model", addr, handler)
}

func (s *server) serveAll(ctx context.Context) error {
	options, optionErr := loadStateStoreOptions()
	if optionErr != nil {
		return optionErr
	}
	st := state.New(s.pool, options...)
	if err := migrateStateAll(ctx, st); err != nil {
		return err
	}
	idStore := s.identityStore()
	if err := idStore.Migrate(ctx); err != nil {
		return fmt.Errorf("identity migrate: %w", err)
	}
	ms := model.NewBudgetStore(s.pool)
	if err := ms.Migrate(ctx); err != nil {
		return fmt.Errorf("model migrate: %w", err)
	}

	verifier, devTaskIssuer := devAuth(st)
	_, idSigner := devIdentityAuth()
	s.exchangeSigner = idSigner
	// Who owns the run credential: Identity renews live grant tokens and re-checks the live
	// grant, or the legacy dev shortcut (which the mode line below always announces).
	authority := selectStateTaskAuthority(envOr("AHP_ENABLE_BROWSER", "") == "1" || s.iam != nil, idStore, idSigner, devTaskIssuer)
	if authority.mode == authorityIdentityMode {
		s.logger.Info("state task authority: Identity-backed (ClaimRun renews live grant tokens, every task call re-checks the live grant)")
	} else {
		s.logger.Warn("state task authority: DEV issuer with a permissive authorize - run tokens are NOT Identity grants and cannot revoke themselves")
	}
	s.stateOwner, s.stateOwnerAuthorize = st, authority.authorize
	stateHandler, err := stateapi.NewWithAdmin(st, verifier, authority.authorize, authority.issue, stateapi.AdminConfig{
		Audience:  envOr("AHP_BROWSER_AUDIENCE", "ahp-browser"),
		Authorize: stateAdminAuthority(idStore, envListOr("AHP_STATE_ADMIN_DEPLOYMENTS")),
	})
	if err != nil {
		return fmt.Errorf("stateapi: %w", err)
	}

	mux := s.baseMux()
	// The optional planes are mounted BEFORE the State face is registered: mounting the
	// data plane installs the artifact cascade that wraps State, and a cascade wired to
	// a store that is not there yet would be silently absent - the deletion then answers
	// 200 while the thread's artifacts keep serving bytes, which is exactly what the
	// live composition measured as cascade-delete-20261031T102134Z. Registration order
	// inside the mux is irrelevant; what matters is which store the wrapper closes
	// over. Both gates are OFF by default, and in the default (untagged) build the
	// mount helpers return errFeatureNotBuilt, so neither path can break the state /
	// identity / model roles that share this mux.
	dataPlaneMounted := false
	if envOr("AHP_ENABLE_DATA", "") == "1" {
		if err := s.tryMountData(ctx, mux); err != nil {
			return fmt.Errorf("data plane: %w", err)
		} else {
			dataPlaneMounted = true
		}
	}
	if envOr("AHP_ENABLE_EXECUTION", "") == "1" {
		if err := s.tryMountExecution(ctx, mux); err != nil {
			return fmt.Errorf("execution plane: %w", err)
		}
	} else if envOr("AHP_ENABLE_COMPUTE", "") == "1" {
		// The literal env name and prefix are deliberate: this file compiles in the
		// default (untagged) build too, where the compute role's own symbols do not
		// exist. The data plane gate above reads its env the same way.
		// The Compute plane is mounted by the execution role, because running a
		// command IS execution. An operator who sets AHP_ENABLE_COMPUTE=1 without
		// AHP_ENABLE_EXECUTION=1 therefore gets no compute face at all -- which used
		// to be silent. Saying it is the difference between a misconfigured stand and
		// a mysterious 404; the prefix still forwards to the runner, so nothing here
		// claims a plane that does not exist.
		s.logger.Warn("compute plane NOT mounted: the execution role is off, and Compute is mounted by it",
			"remedy", "set AHP_ENABLE_EXECUTION=1 as well", "prefix", singleOriginComputePrefix)
	}
	if envOr("AHP_ENABLE_WORKSPACE", "") == "1" && !dataPlaneMounted {
		if err := s.tryMountWorkspace(ctx, mux); err != nil {
			return fmt.Errorf("workspace plane: %w", err)
		}
	}
	// Registry and Triggers were accepted (W2-A) as DB Store/Service packages
	// without an HTTP face, so the composed front could not serve the catalog or
	// the trigger CRUD the product PDF requires. Both mounts are OFF by default
	// (AHP_ENABLE_REGISTRY / AHP_ENABLE_TRIGGERS) and add no business branch: they
	// only expose the accepted methods behind the same authorize + verifier + mTLS
	// contract as the State face. They are mounted before the State face to keep
	// the registration-order rule the data-cascade comment above relies on.
	if envOr("AHP_ENABLE_REGISTRY", "") == "1" {
		if err := s.tryMountRegistry(ctx, mux, verifier, authority.authorize); err != nil {
			return fmt.Errorf("registry plane: %w", err)
		}
	}
	if envOr("AHP_ENABLE_TRIGGERS", "") == "1" {
		if err := s.tryMountTriggers(ctx, mux, envOr("AHP_LISTEN", ":8080"), verifier, authority.authorize, idSigner); err != nil {
			return fmt.Errorf("triggers plane: %w", err)
		}
	}
	// Model Gateway cost read face (PDF :347 "стоимость задачи"). OFF by default
	// (AHP_ENABLE_MODEL_COST); it exposes the accepted TreeCostByTask read and
	// writes nothing.
	if envOr("AHP_ENABLE_MODEL_COST", "") == "1" {
		if err := s.tryMountModelCost(ctx, mux, verifier, authority.authorize); err != nil {
			s.logger.Warn("model cost read face not mounted", "err", err)
		}
	}
	if envOr("AHP_ENABLE_MODEL", "") == "1" {
		if err := s.tryMountModelGateway(ctx, mux, verifier, idStore.Authorize); err != nil {
			return fmt.Errorf("model gateway: %w", err)
		}
	}
	// Tool & Credential Gateway (internal/tools, accepted as a package): the PDF
	// requires an MCP tool/credential face at /v1/tools/ with Call/Get/Fence/
	// Approve and a real effect ledger keyed by operation. OFF by default
	// (AHP_ENABLE_TOOLS) so the accepted faces stay byte-identical; it uses the
	// same authorize + verifier + mTLS contract and mounts before the State face
	// for the same registration-order reason as the planes above.
	if envOr("AHP_ENABLE_TOOLS", "") == "1" {
		// Human approvals legs exist only on the Identity-composed front, where
		// a live permission store can answer "does THIS human hold
		// tools:approve right now" (DEF-TGW-2). Dev-authorize stands keep the
		// accepted single-audience shape with no permission store.
		var toolsHumans humanPermissions
		if envOr("AHP_ENABLE_BROWSER", "") == "1" || s.iam != nil {
			toolsHumans = idStore
		}
		if err := s.tryMountTools(ctx, mux, verifier, authority.authorize, toolsHumans); err != nil {
			return fmt.Errorf("tools plane: %w", err)
		}
	}
	// Optional memory uses Hindsight by default and live Identity authority.
	// The explicit legacy backend retains the earlier proposal/review contract.
	// Mount before State so subject deletion receives the actual memory owner.
	if envOr("AHP_ENABLE_MEMORY", "") == "1" {
		var memGrants GrantLabels
		if idStore != nil {
			memGrants = identityGrantLabels{evaluate: idStore}
		}
		if err := s.tryMountMemory(ctx, mux, verifier, authority.authorize, authority.mode, memGrants); err != nil {
			return fmt.Errorf("memory plane: %w", err)
		}
	}
	// Background garbage collection. The deletion plane is otherwise only ever
	// run on demand (an HTTP call to GC/RunOnce), so retention is enforced only
	// while somebody remembers to poke it; the PDF's retention-and-deletion
	// obligation (с. 48) is a standing one. OFF by default (AHP_ENABLE_GC_SCHEDULER)
	// so every accepted plane stays byte-identical, and mounted before the State
	// face for the same registration-order reason as the data plane above.
	if envOr("AHP_ENABLE_GC_SCHEDULER", "") == "1" {
		if err := s.tryMountGCScheduler(ctx); err != nil {
			return fmt.Errorf("gc scheduler: %w", err)
		}
	}
	controlConfig, err := buildControlDeletionConfig(s.pool)
	if err != nil {
		return err
	}
	controlConfig.ConsumerContext = ctx
	controlConfig.TenantOwners = s.tenantDeletionOwners
	s.registerControlSubjectOwners()
	controlConfig.SubjectOwners = s.subjectDeletionOwners
	controlConfig.RequiredSubjectOwners = append([]string(nil), requiredSubjectOwners...)
	stateHandler = s.withDeletionCascade(stateHandler, st, verifier, controlConfig)
	mux.Handle("/v1/state/", stateHandler)
	mux.Handle("/v1/state-admin/", stateHandler)
	stopCompactState, err := s.mountCompactState(ctx, mux, verifier, idStore.Authorize,
		strings.Split(os.Getenv("AHP_STATE_COORDINATOR_WORKLOAD_IDS"), ",")...)
	if err != nil {
		return err
	}
	defer stopCompactState()
	idCfg := identityapi.Config{
		Auth:     s.authenticator,
		Store:    idStore,
		Verifier: verifier,
		Signer:   idSigner,
		Audience: "ahp",
		// Composition (PDF 3.5 / 6.2): the Human -> authorized Space -> scoped task
		// boundary is fail-closed - an empty allowlist authorises nobody. A deployment
		// becomes an authorized exchanger ONLY when the operator names it here, and it
		// must still present its OWN verified workload certificate. Measured
		// consequence of this line being absent (the base this branch was cut from):
		// /v1/identity/control/exchange answers 403 "deployment is not an authorized
		// exchanger" for every caller, so no leg that needs a real task token can run.
		ExchangerDeployments:    envListOr("AHP_IDENTITY_EXCHANGER_DEPLOYMENTS"),
		DelegatorDeployments:    envListOr("AHP_IDENTITY_DELEGATOR_DEPLOYMENTS"),
		ResolveDelegationTarget: runtimeAuthority{ids: idStore, registry: registry.New(s.pool), verifier: verifier, signer: idSigner}.resolve,
	}
	s.configureIdentityAPIAuthority(&idCfg)
	// Isolated browser-demo composition: when AHP_ENABLE_BROWSER=1, additionally
	// mount the Agent Space browser login/current-user surface (/u-ahp/v1/*).
	// Off by default so the accepted control/agent planes stay byte-identical.
	// This exposes login + whoami + flags only; the human turn path runs through
	// the Space facade (browser AccessAudience), not a direct browser->state call.
	if envOr("AHP_ENABLE_BROWSER", "") == "1" {
		idCfg.Browser = &identityapi.BrowserConfig{
			AccessAudience: envOr("AHP_BROWSER_AUDIENCE", "ahp-browser"),
			FeatureFlags: map[string]bool{
				"space_chat_ui": true,
			},
		}
	}
	// AHP_ENABLE_CONTROL_EXCHANGE=1 mounts the Human -> authorized Space -> scoped
	// task exchange (and introspection) on a deployment that serves no browser UI.
	// Off by default; with AHP_ENABLE_BROWSER=1 that surface is already mounted by
	// mountBrowser and this flag changes nothing.
	idCfg.ControlExchange = envOr("AHP_ENABLE_CONTROL_EXCHANGE", "") == "1"
	idHandler := identityapi.Handler(idCfg)
	mux.Handle("/v1/identity/", idHandler)
	// The browser surface uses absolute /u-ahp/v1/* paths inside its own mux, so
	// it must ALSO be reachable under that prefix (mounting only under
	// /v1/identity/ would 404 every browser route).
	if idCfg.Browser != nil {
		mux.Handle("/u-ahp/v1/", idHandler)
	}
	// Single-origin edge (single_origin.go): when the deployment names a runner
	// origin, the Space face, the artifact download and the SPA are PROXIED
	// there. This is deliberately last: the mounts above own /v1/state/,
	// /v1/identity/ and the rest of /u-ahp/v1/ (login, whoami, flags), and the
	// proxy must never shadow a face this process is accountable for. Unset env
	// means no mount at all, so an edge that does not opt in is unchanged.
	//
	// The public Agent Card is mounted just before it for the same reason: once a
	// card is configured, the well-known path is a face this process is
	// accountable for, and leaving it to the "/" proxy made the card
	// undiscoverable on every origin that is not the execution plane
	// (agent_card_face.go).
	s.mountPublicAgentCard(mux)
	// The edge must know who owns /v1/compute/ before it registers anything for it
	// (DEF-COMPUTE-1): mountSingleOriginEdge registers the prefix exactly when this
	// process does not answer it, so the front that mounted the mock plane serves it
	// and the front that did not keeps forwarding to the runner.
	mountSingleOriginEdge(mux, envOr(envSingleOrigin, ""), idCfg.Browser != nil, dataPlaneMounted, s.computeRouteOwned, s.logger)

	handler := aot.HTTPMiddleware("all")(mux)

	listenAddr := envOr("AHP_LISTEN", ":8080")
	err = s.runServer(ctx, "all", listenAddr, handler)
	// Graceful shutdown: a GC sweep already in flight is not abandoned mid-pass
	// (that is the crash window the collector is written to survive -- a row gone
	// while its bytes are not yet), and the drain happens while main's deferred
	// pool.Close() has not run, so the pass still has the pool it needs. Nil for
	// every role that did not mount the scheduler, so the default path is unchanged.
	if s.gcDrainOnShutdown != nil {
		s.gcDrainOnShutdown(context.Background())
	}
	return err
}

func (s *server) runServer(ctx context.Context, name, addr string, handler http.Handler) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		TLSConfig:         s.tlsCfg,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		s.logger.Info("shutting down", "service", name)
		sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	s.logger.Info("listening", "service", name, "addr", addr, "tls", s.tlsCfg != nil)

	var err error
	if s.tlsCfg != nil {
		err = srv.ListenAndServeTLS("", "")
	} else {
		err = srv.ListenAndServe()
	}
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	wg.Wait()
	return err
}

func buildTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("tls keypair: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("tls ca read: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("tls ca: no valid certificates in %s", caFile)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envListOr splits a comma-separated environment list, dropping blanks. An unset or blank
// variable yields nil, which every allowlist downstream treats as "authorises nobody".
func envListOr(key string) []string {
	raw := strings.TrimSpace(envOr(key, ""))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func mustEd25519Pair() (ed25519.PrivateKey, ed25519.PublicKey) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic("ahp: ed25519 key generation failed: " + err.Error())
	}
	return priv, pub
}

// devIdentityAuth returns the verifier and signer for the identity service.
// It uses the same development Ed25519 key as devAuth, so it is deterministic
// when AHP_DEV_JWT_SEED is set and an out-of-process harness holding the same
// seed can mint tokens both the identity and state services verify. The signer
// mints fenced task tokens; the verifier is shared so identity-minted tokens
// are accepted by the state service and vice versa. Secrets are never logged.
func devIdentityAuth() (*auth.Verifier, *auth.Signer) {
	priv, pub := devSigningKey()
	keys := map[string]ed25519.PublicKey{"dev-1": pub}
	if ext, kid, ok := devExternalKey(); ok {
		keys[kid] = ext
	}
	verifier := auth.NewVerifier("ahp", keys)
	signer, err := auth.NewSigner("ahp", "dev-1", priv)
	if err != nil {
		panic("ahp: dev identity signer: " + err.Error())
	}
	return verifier, signer
}
