package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/identityapi"
	"github.com/ahp-platform/ahp/internal/runcoord"
	"github.com/ahp-platform/ahp/internal/vmruntime"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/profiling/harness"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sys/unix"
)

const (
	profileDeployment   = "profile-agent"
	profileSubject      = "profile-user"
	profileDecisionTask = "coordinator"
)

const enterCgroupScript = `printf '%s\n' "$$" > "$1/cgroup.procs" || exit
shift`

const joinCgroupScript = enterCgroupScript + "\n" + `exec "$@"`

// Preserve the ProcessDriver's four-variable contract across the shell exec.
// Some shells add SHLVL, PWD or _ even when the parent passed an exact Env.
const joinWorkerCgroupScript = enterCgroupScript + "\n" + `
set -- -i "PATH=$PATH" "HOME=$HOME" "TMPDIR=$TMPDIR" "AHP_INJECTOR_SOCKET=$AHP_INJECTOR_SOCKET" "$@"
if [ -x /usr/bin/env ]; then
    exec /usr/bin/env "$@"
fi
exec /bin/busybox env "$@"`

type standConfig struct {
	BinDir, OutputDir, PostgresDSN                              string
	MinIOEndpoint, MinIOAccessKey, MinIOSecretKey               string
	SUTCgroup, GuestCgroup, FirecrackerCgroup                   string
	Driver, WorkerPath, FirecrackerPath, KernelPath, RootFSPath string
	Faults                                                      bool
}

type stand struct {
	CoordinatorURL, StateURL              string
	CoordinatorHTTP, StateHTTP            *http.Client
	AdminToken                            string
	SubjectTokens, Tenants                []string
	PIDs                                  map[string]int
	DebugURLs                             map[string]string
	PG, RuntimePG                         *pgxpool.Pool
	DatabaseNames                         map[string]string
	Effects                               *effectRecorder
	SlotDir, FirecrackerCgroup, FCDataDir string

	cfg                    standConfig
	tempDir                string
	runtimeDir             string
	createdDatabases       []string
	storeAdmin, identityPG *pgxpool.Pool
	ids                    *identity.Store
	s3Cleanup              func(context.Context) error
	coordinatorScope       contracts.Scope
	signer                 *auth.Signer
	tokenFile              string
	clients                []browserPrincipal
	browser                http.Handler
	dispatch               runcoord.HTTPDispatcher

	mcp         *http.Server
	roles       []*ownerProcess
	stopRefresh context.CancelFunc
	refreshDone chan struct{}
}

type browserPrincipal struct {
	mu     sync.Mutex
	cookie *http.Cookie
	token  string
	expiry time.Time
}

type ownerProcess struct {
	name    string
	cmd     *exec.Cmd
	done    chan struct{}
	waitErr error
}

func (s *stand) Scope(i int) contracts.Scope {
	return contracts.Scope{Tenant: s.Tenants[i], Deployment: profileDeployment, Subject: profileSubject}
}

func startStand(ctx context.Context, cfg standConfig) (_ *stand, err error) {
	if cfg.Driver != "" && cfg.Driver != "process" && cfg.Driver != "firecracker" {
		return nil, errors.New("profiling driver must be process or firecracker")
	}
	if cfg.Driver == "" {
		cfg.Driver = "process"
	}
	if cfg.WorkerPath == "" {
		var err error
		cfg.WorkerPath, err = os.Executable()
		if err != nil {
			return nil, err
		}
	}
	if err = os.MkdirAll(cfg.OutputDir, 0700); err != nil {
		return nil, err
	}
	for _, group := range []string{cfg.SUTCgroup, cfg.GuestCgroup} {
		if !filepath.IsAbs(group) || unix.Access(filepath.Join(group, "cgroup.procs"), unix.W_OK) != nil {
			return nil, errors.New("profiling requires writable SUT and worker cgroups")
		}
	}
	if cfg.SUTCgroup == cfg.GuestCgroup {
		return nil, errors.New("profiling requires separate SUT and worker cgroups")
	}
	if cfg.Driver == "firecracker" && (!filepath.IsAbs(cfg.FirecrackerCgroup) ||
		unix.Access(filepath.Join(cfg.FirecrackerCgroup, "cgroup.procs"), unix.W_OK) != nil) {
		return nil, errors.New("profiling requires a writable Firecracker cgroup")
	}

	dir, err := os.MkdirTemp("/tmp", "v41-")
	if err != nil {
		return nil, err
	}
	s := &stand{cfg: cfg, tempDir: dir, runtimeDir: dir, FirecrackerCgroup: cfg.FirecrackerCgroup,
		PIDs: make(map[string]int), DebugURLs: make(map[string]string), DatabaseNames: make(map[string]string),
		coordinatorScope: contracts.Scope{Tenant: "platform", Deployment: "run-coordinator", Subject: "service"},
		clients:          make([]browserPrincipal, 5)}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}()
	if parent := os.Getenv("AHP_PROFILE_SLOT_PARENT"); parent != "" {
		if !filepath.IsAbs(parent) {
			return nil, errors.New("profiling slot parent must be an absolute path")
		}
		s.runtimeDir, err = os.MkdirTemp(parent, "v41-")
		if err != nil {
			return nil, fmt.Errorf("create runtime directory on profiling volume: %w", err)
		}
	}
	s.SlotDir = filepath.Join(s.runtimeDir, "slots")
	const longestAttempt = "exec-0123456789abcdef01234567-9223372036854775807"
	socketPath := filepath.Join(s.SlotDir, longestAttempt, "ahp.sock")
	if cfg.Driver == "firecracker" {
		socketPath = filepath.Join(s.runtimeDir, "fc", longestAttempt,
			"vsock.sock_"+strconv.FormatUint(uint64(journal.RuntimeInjectorVSockPort), 10))
	}
	if len(socketPath) >= len(unix.RawSockaddrUnix{}.Path) {
		return nil, errors.New("profiling slot parent is too long for runtime UNIX sockets")
	}
	if err = os.Mkdir(s.SlotDir, 0700); err != nil {
		return nil, err
	}
	for i := range s.clients {
		s.Tenants = append(s.Tenants, fmt.Sprintf("profile-tenant-%d", i+1))
	}
	for _, name := range []string{"state", "tools", "identity", "runtime"} {
		s.DatabaseNames[name] = "ahp_profile_" + strings.ReplaceAll(filepath.Base(dir), "-", "_") + "_" + strings.ToLower(name)
	}
	ds, err := s.prepareDatabases(ctx)
	if err != nil {
		return nil, err
	}
	s.ids = identity.New(s.identityPG)
	if err = s.ids.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("migrate fixture Identity: %w", err)
	}

	pki, err := harness.MintCerts(dir)
	if err != nil {
		return nil, err
	}
	coordinator, err := mintCoordinatorCert(pki, s.coordinatorScope)
	if err != nil {
		return nil, err
	}
	// The public coordinator accepts a multi-tenant platform edge workload. This
	// leaf only carries that edge identity; it cannot use coordinator State routes.
	edge, err := harness.MintClientCert(pki, "platform", "profile-edge")
	if err != nil {
		return nil, err
	}
	s.CoordinatorHTTP, err = profileHTTPClient(pki.CACert, edge.Cert, edge.Key)
	if err != nil {
		return nil, err
	}
	s.StateHTTP, err = profileHTTPClient(pki.CACert, coordinator.Cert, coordinator.Key)
	if err != nil {
		return nil, err
	}
	accessFile, err := s.prepareAccess(pki)
	if err != nil {
		return nil, err
	}
	jwtKey, jwtPub, signer, err := profileSigningKey(dir, "identity", profileIssuer, profileKeyID)
	if err != nil {
		return nil, err
	}
	s.signer = signer
	leaseKey, leasePub, _, err := profileSigningKey(dir, "state-lease", profileLeaseIssuer, profileLeaseKeyID)
	if err != nil {
		return nil, err
	}
	s.tokenFile = filepath.Join(dir, "coordinator.jwt")
	if _, err = s.ids.CreateRoot(ctx, s.coordinatorScope, identity.RootRequest{Task: profileDecisionTask, Deployment: s.coordinatorScope.Deployment,
		Context: "profile-coordinator", Capabilities: []string{"state", "state.coordinate", "runtime.dispatch"}, TTL: 2 * time.Hour}); err != nil {
		return nil, fmt.Errorf("provision coordinator grant: %w", err)
	}
	token, err := s.refreshCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	s.AdminToken = token
	if err = s.prepareBrowser(ctx, jwtPub); err != nil {
		return nil, err
	}

	s3, endpoint, secure, err := profileS3(cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey)
	if err != nil {
		return nil, err
	}
	bucket := "ahp-profile-" + strings.ToLower(filepath.Base(dir))
	if err = s3.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		return nil, fmt.Errorf("create profile bucket: %w", err)
	}
	s.s3Cleanup = func(ctx context.Context) error {
		return s3.RemoveBucketWithOptions(ctx, bucket, minio.RemoveBucketOptions{ForceDelete: true})
	}
	canary, err := randomProfileSecret()
	if err != nil {
		return nil, err
	}
	var mcpURL string
	s.Effects, s.mcp, mcpURL, err = startProfileMCP(canary)
	if err != nil {
		return nil, err
	}
	addr, err := availableProfilePorts(8)
	if err != nil {
		return nil, err
	}
	roles := []string{"state-api", "tools-gateway", "ahp-vm-runtime", "run-coordinator"}
	for i, name := range roles {
		s.DebugURLs[name] = "http://" + addr[i+4]
	}
	s.Effects.debugURL = s.DebugURLs["tools-gateway"]
	s.StateURL = "https://" + addr[0]
	toolURL := "https://" + addr[1]
	runtimeURL := "https://" + addr[2]
	s.CoordinatorURL = "https://" + addr[3]
	workload := auth.WorkloadID(s.coordinatorScope)
	certs := filepath.Join(dir, "workloads")
	toolsScope := make([]string, len(s.Tenants))
	for i, t := range s.Tenants {
		toolsScope[i] = t + "/" + profileDeployment
	}
	s.dispatch = runcoord.HTTPDispatcher{BaseURL: runtimeURL, Client: s.StateHTTP, TokenSource: func(context.Context) (string, error) {
		raw, err := os.ReadFile(s.tokenFile)
		return strings.TrimSpace(string(raw)), err
	}}
	stateEnv := map[string]string{
		"AHP_STATE_ADDR": addr[0], "AHP_STATE_DSN": profilePoolDSN(ds["state"], 32, 8),
		"AHP_PROFILE_FINISH_TRACE": filepath.Join(cfg.OutputDir, "state-finish-commits.tsv"),
		"AHP_STATE_IDENTITY_DSN":   profilePoolDSN(ds["identity"], 16, 4), "AHP_STATE_COORDINATOR_WORKLOAD_IDS": workload,
		"AHP_STATE_TLS_CERT": pki.ServerCert, "AHP_STATE_TLS_KEY": pki.ServerKey, "AHP_STATE_CLIENT_CA": pki.CACert,
		"AHP_STATE_TOKEN_ISSUER": profileIssuer, "AHP_STATE_TOKEN_KID": profileKeyID, "AHP_STATE_TOKEN_PUBLIC_KEY": jwtPub,
		"AHP_STATE_TOOL_LEASE_ISSUER": profileLeaseIssuer, "AHP_STATE_TOOL_LEASE_KID": profileLeaseKeyID, "AHP_STATE_TOOL_LEASE_KEY": leaseKey,
		"AHP_STATE_S3_ENDPOINT": endpoint, "AHP_STATE_S3_ACCESS_KEY": cfg.MinIOAccessKey, "AHP_STATE_S3_SECRET_KEY": cfg.MinIOSecretKey,
		"AHP_STATE_S3_TLS": strconv.FormatBool(secure), "AHP_STATE_S3_BUCKET": bucket,
	}
	gatewayEnv := profileEnvPrefix("AHP_PROFILE_GATEWAY_", map[string]string{
		"ADDR": addr[1], "DEBUG": addr[5], "DSN": profilePoolDSN(ds["tools"], 24, 6), "IDENTITY_DSN": profilePoolDSN(ds["identity"], 16, 4),
		"TOKEN_PUBLIC": jwtPub, "LEASE_PUBLIC": leasePub, "CA": pki.CACert, "CERT": pki.ServerCert, "KEY": pki.ServerKey,
		"S3_ENDPOINT": cfg.MinIOEndpoint, "S3_ACCESS": cfg.MinIOAccessKey, "S3_SECRET": cfg.MinIOSecretKey, "S3_BUCKET": bucket,
		"MCP_URL": mcpURL, "MCP_CANARY": canary, "TENANTS": strings.Join(s.Tenants, ","), "FAULTS": strconv.FormatBool(cfg.Faults),
	})
	gatewayEnv[profileMCPTransportEnv] = os.Getenv(profileMCPTransportEnv)
	runtimeEnv := map[string]string{
		"AHP_VM_RUNTIME_HOST_ID": "profile-linux-host", "AHP_VM_RUNTIME_ADDR": addr[2], "AHP_VM_RUNTIME_SLOT_DIR": s.SlotDir,
		"AHP_VM_RUNTIME_DSN": profilePoolDSN(ds["runtime"], 20, 4), "AHP_VM_RUNTIME_IDENTITY_DSN": profilePoolDSN(ds["identity"], 16, 4),
		"AHP_VM_RUNTIME_TLS_CERT": pki.ServerCert, "AHP_VM_RUNTIME_TLS_KEY": pki.ServerKey, "AHP_VM_RUNTIME_CA": pki.CACert,
		"AHP_VM_RUNTIME_JWT_ISSUER": profileIssuer, "AHP_VM_RUNTIME_JWT_KID": profileKeyID,
		"AHP_VM_RUNTIME_JWT_PUBLIC_KEY": jwtPub, "AHP_VM_RUNTIME_JWT_PRIVATE_KEY": jwtKey,
		"AHP_VM_RUNTIME_COORDINATOR_WORKLOAD_IDS": workload,
		"AHP_VM_RUNTIME_STATE_URL":                s.StateURL, "AHP_VM_RUNTIME_STATE_CA": pki.CACert, "AHP_VM_RUNTIME_WORKLOAD_CERT_DIR": certs,
		"AHP_VM_RUNTIME_TOOLS_URL": toolURL, "AHP_VM_RUNTIME_TOOLS_DEPLOYMENTS": strings.Join(toolsScope, ","),
		"AHP_VM_RUNTIME_DRIVER": cfg.Driver,
	}
	if cfg.Driver == "process" {
		wrapper := filepath.Join(dir, "worker")
		script := fmt.Sprintf("#!/bin/sh\nset -- %s %s \"$@\"\nexec 2>> %s\n%s\n",
			shellQuote(cfg.GuestCgroup), shellQuote(cfg.WorkerPath), shellQuote(filepath.Join(cfg.OutputDir, "process-worker.stderr.log")), joinWorkerCgroupScript)
		if err = os.WriteFile(wrapper, []byte(script), 0700); err != nil {
			return nil, err
		}
		runtimeEnv["AHP_VM_RUNTIME_WORKER_BINARY"] = wrapper
	} else {
		dataDir := filepath.Join(s.runtimeDir, "fc")
		s.FCDataDir = dataDir
		if err = os.Mkdir(dataDir, 0700); err != nil {
			return nil, err
		}
		runtimeEnv["AHP_COMPUTE_RUNTIME"] = "firecracker"
		runtimeEnv["AHP_PROFILE_BOOT_TIMING"] = "true"
		runtimeEnv["AHP_FC_BINARY"] = cfg.FirecrackerPath
		runtimeEnv["AHP_FC_KERNEL"] = cfg.KernelPath
		runtimeEnv["AHP_FC_ROOTFS"] = cfg.RootFSPath
		runtimeEnv["AHP_FC_DATADIR"] = dataDir
		runtimeEnv["AHP_VM_RUNTIME_CGROUP_ROOT"] = cfg.FirecrackerCgroup
	}
	coordinatorEnv := map[string]string{
		"AHP_RC_ADDR": addr[3], "AHP_RC_STATE_URL": s.StateURL, "AHP_RC_RUNTIME_URL": runtimeURL,
		"AHP_RC_TLS_CERT": coordinator.Cert, "AHP_RC_TLS_KEY": coordinator.Key, "AHP_RC_CA": pki.CACert,
		"AHP_RC_STATE_TOKEN_FILE": s.tokenFile, "AHP_RC_RUNTIME_TOKEN_FILE": s.tokenFile,
		"AHP_RC_JWT_ISSUER": profileIssuer, "AHP_RC_JWT_KID": profileKeyID, "AHP_RC_JWT_PUBLIC_KEY": jwtPub,
		"AHP_RC_NOTIFY_DSN": profilePoolDSN(ds["state"], 8, 2), "AHP_RC_AGENT_ACCESS_FILE": accessFile,
		"AHP_RC_USER_AUDIENCE": profileAudience, "AHP_RC_PLATFORM_EDGE_WORKLOAD_IDS": auth.WorkloadID(contracts.Scope{Tenant: "platform", Deployment: "profile-edge"}),
	}
	appPath, err := os.Executable()
	if err != nil {
		return nil, err
	}
	binaries := []string{filepath.Join(cfg.BinDir, "state-api"), appPath, filepath.Join(cfg.BinDir, "ahp-vm-runtime"), filepath.Join(cfg.BinDir, "run-coordinator")}
	envs := []map[string]string{stateEnv, gatewayEnv, runtimeEnv, coordinatorEnv}
	budgets := []string{"1100MiB", "900MiB", "800MiB", "600MiB"}
	targets := []string{s.StateURL + "/readyz", toolURL + "/readyz", runtimeURL + "/readyz", s.CoordinatorURL + "/readyz"}
	for i, name := range roles {
		env := envs[i]
		env["AHP_PROFILE_DEBUG"] = addr[i+4]
		env["GOMAXPROCS"] = "4"
		env["GOMEMLIMIT"] = budgets[i]
		args := []string{}
		if name == "tools-gateway" {
			args = []string{"gateway"}
		}
		if err = s.launch(name, binaries[i], env, args); err != nil {
			return nil, err
		}
		if err = s.waitReady(ctx, s.roles[i], targets[i], s.DebugURLs[name], i >= 2); err != nil {
			return nil, err
		}
	}
	refreshCtx, stop := context.WithCancel(ctx)
	s.stopRefresh = stop
	s.refreshDone = make(chan struct{})
	go s.keepCoordinatorToken(refreshCtx)
	return s, nil
}

func (s *stand) prepareDatabases(ctx context.Context) (map[string]string, error) {
	config, err := pgxpool.ParseConfig(s.cfg.PostgresDSN)
	if err != nil {
		return nil, errors.New("invalid lab PostgreSQL DSN")
	}
	if (config.ConnConfig.Host != "127.0.0.1" && config.ConnConfig.Host != "::1") || len(config.ConnConfig.Fallbacks) != 0 {
		return nil, errors.New("fixture requires dedicated loopback lab PostgreSQL")
	}
	s.storeAdmin, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	dsns := make(map[string]string, len(s.DatabaseNames))
	for label, name := range s.DatabaseNames {
		if _, err = s.storeAdmin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
			return nil, fmt.Errorf("create lab %s DB: %w", label, err)
		}
		s.createdDatabases = append(s.createdDatabases, name)
		dsns[label] = s.cfg.PostgresDSN + " dbname=" + name
	}
	for label, target := range map[string]**pgxpool.Pool{"state": &s.PG, "runtime": &s.RuntimePG, "identity": &s.identityPG} {
		*target, err = pgxpool.New(ctx, profilePoolDSN(dsns[label], 8, 0))
		if err != nil {
			return nil, fmt.Errorf("connect fixture %s DB: %w", label, err)
		}
	}
	if _, err := s.PG.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"); err != nil {
		return nil, fmt.Errorf("install profiling pg_stat_statements: %w", err)
	}
	return dsns, nil
}

// profilePoolDSN retains URI and keyword DSN support without relying on the
// pool defaults. For URI DSNs the selected database and query are encoded again.
func profilePoolDSN(dsn string, maxConns, minConns int) string {
	base, db, hasDB := strings.Cut(dsn, " dbname=")
	if u, err := url.Parse(base); err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		if hasDB {
			u.Path = "/" + db
			u.RawPath = ""
		}
		q := u.Query()
		q.Set("pool_max_conns", strconv.Itoa(maxConns))
		q.Set("pool_min_conns", strconv.Itoa(minConns))
		u.RawQuery = q.Encode()
		return u.String()
	}
	return fmt.Sprintf("%s pool_max_conns=%d pool_min_conns=%d", dsn, maxConns, minConns)
}

func (s *stand) prepareAccess(pki harness.CertMaterial) (string, error) {
	type access struct {
		Tenant     string   `json:"tenant"`
		Deployment string   `json:"deployment"`
		Subjects   []string `json:"subjects"`
	}
	policy := struct {
		Agents []access `json:"agents"`
	}{}
	for i, t := range s.Tenants {
		scope := s.Scope(i)
		path := filepath.Join(s.tempDir, "workloads", "tenant-"+t, "deployment-"+scope.Deployment)
		if err := os.MkdirAll(path, 0700); err != nil {
			return "", err
		}
		cert, err := harness.MintClientCert(pki, t, scope.Deployment)
		if err != nil {
			return "", err
		}
		if err = os.Rename(cert.Cert, filepath.Join(path, "tls.crt")); err != nil {
			return "", err
		}
		if err = os.Rename(cert.Key, filepath.Join(path, "tls.key")); err != nil {
			return "", err
		}
		policy.Agents = append(policy.Agents, access{Tenant: t, Deployment: scope.Deployment, Subjects: []string{scope.Subject}})
	}
	raw, err := json.Marshal(policy)
	if err != nil {
		return "", err
	}
	path := filepath.Join(s.tempDir, "coordinator-access.json")
	return path, os.WriteFile(path, raw, 0600)
}

func (s *stand) prepareBrowser(ctx context.Context, key string) error {
	verifier, err := loadVerifier(key, profileIssuer, profileKeyID)
	if err != nil {
		return err
	}
	s.browser = identityapi.Handler(identityapi.Config{Store: s.ids, Signer: s.signer, Verifier: verifier, Audience: "ahp",
		Browser: &identityapi.BrowserConfig{AccessAudience: profileAudience}})
	for i, t := range s.Tenants {
		user, pass := t, ""
		pass, err = randomProfileSecret()
		if err != nil {
			return err
		}
		if err = s.ids.SeedBrowserUser(ctx, identity.BrowserUser{UserID: t, Tenant: t, Subject: s.Scope(i).Subject, DisplayName: "Profile User"},
			user, pass, nil, nil); err != nil {
			return fmt.Errorf("provision lab browser session: %w", err)
		}
		body, err := json.Marshal(map[string]string{"username": user, "password": pass})
		if err != nil {
			return err
		}
		rec := s.requestBrowser(ctx, "login", bytes.NewReader(body), nil)
		if rec.Code != http.StatusOK {
			return fmt.Errorf("Identity login: HTTP %d", rec.Code)
		}
		for _, c := range rec.Result().Cookies() {
			if c.Name == "ahp_session" {
				s.clients[i].cookie = c
			}
		}
		if s.clients[i].cookie == nil {
			return errors.New("Identity login omitted session cookie")
		}
		token, err := s.SubjectToken(ctx, i)
		if err != nil {
			return err
		}
		s.SubjectTokens = append(s.SubjectTokens, token)
	}
	return nil
}

func (s *stand) requestBrowser(ctx context.Context, verb string, body io.Reader, cookie *http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "https://localhost/u-ahp/v1/auth/"+verb, body)
	req.Header.Set("X-AHP-Session", "1")
	req.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	rec := httptest.NewRecorder()
	s.browser.ServeHTTP(rec, req)
	return rec
}

func (s *stand) SubjectToken(ctx context.Context, i int) (string, error) {
	user := &s.clients[i]
	user.mu.Lock()
	defer user.mu.Unlock()
	if time.Until(user.expiry) > time.Minute {
		return user.token, nil
	}
	rec := s.requestBrowser(ctx, "token", nil, user.cookie)
	if rec.Code != http.StatusOK {
		return "", fmt.Errorf("Identity token refresh: HTTP %d", rec.Code)
	}
	for _, c := range rec.Result().Cookies() {
		if c.Name == "ahp_session" {
			user.cookie = c
		}
	}
	var token struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &token); err != nil || token.AccessToken == "" || token.ExpiresIn <= 0 {
		return "", errors.New("Identity returned invalid access token response")
	}
	user.token = token.AccessToken
	user.expiry = time.Now().Add(time.Duration(token.ExpiresIn) * time.Second)
	return user.token, nil
}

func (s *stand) refreshCoordinator(ctx context.Context) (string, error) {
	token, err := s.ids.MintTask(ctx, s.coordinatorScope, profileDecisionTask, s.signer, "ahp", 15*time.Minute)
	if err != nil {
		return "", fmt.Errorf("mint live coordinator task token: %w", err)
	}
	tmp := s.tokenFile + ".tmp"
	if err = os.WriteFile(tmp, []byte(token), 0600); err != nil {
		return "", err
	}
	return token, os.Rename(tmp, s.tokenFile)
}

func (s *stand) keepCoordinatorToken(ctx context.Context) {
	defer close(s.refreshDone)
	ticker := time.NewTicker(8 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.refreshCoordinator(ctx); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "refresh coordinator Identity token: %v\n", err)
				ticker.Reset(time.Minute)
			} else {
				ticker.Reset(8 * time.Minute)
			}
		}
	}
}

func (s *stand) launch(name, binary string, env map[string]string, args []string) error {
	logFile, err := os.OpenFile(filepath.Join(s.cfg.OutputDir, name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	argv := append([]string{"-c", joinCgroupScript, "profile", s.cfg.SUTCgroup, binary}, args...)
	cmd := exec.Command("/bin/sh", argv...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if err = cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", name, err)
	}
	p := &ownerProcess{name: name, cmd: cmd, done: make(chan struct{})}
	s.roles = append(s.roles, p)
	s.PIDs[name] = cmd.Process.Pid
	go func() { p.waitErr = cmd.Wait(); close(p.done) }()
	return nil
}

func (s *stand) waitReady(ctx context.Context, p *ownerProcess, endpoint, debug string, notFoundOK bool) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	tick := time.NewTicker(40 * time.Millisecond)
	defer tick.Stop()
	ownerReady, debugReady := false, false
	for {
		for _, probe := range []struct {
			client      *http.Client
			url         string
			ready       *bool
			knownStatus int
		}{
			{s.StateHTTP, endpoint, &ownerReady, func() int {
				if notFoundOK {
					return http.StatusNotFound
				}
				return http.StatusNoContent
			}()},
			{http.DefaultClient, debug + "/debug/ahp-profile/stats", &debugReady, http.StatusOK},
		} {
			if *probe.ready {
				continue
			}
			attempt, stop := context.WithTimeout(ctx, time.Second)
			req, _ := http.NewRequestWithContext(attempt, http.MethodGet, probe.url, nil)
			res, err := probe.client.Do(req)
			if err == nil {
				res.Body.Close()
				*probe.ready = res.StatusCode == probe.knownStatus
			}
			stop()
		}
		if ownerReady && debugReady {
			return nil
		}
		select {
		case <-p.done:
			return fmt.Errorf("%s exited before readiness: %v, see role log", p.name, p.waitErr)
		case <-ctx.Done():
			return fmt.Errorf("%s startup (owner=%t, debug=%t): %w", p.name, ownerReady, debugReady, ctx.Err())
		case <-tick.C:
		}
	}
}

func (s *stand) Fence(ctx context.Context, scope contracts.Scope, session string, launchTurnSeq, generation int64) error {
	return s.dispatch.Fence(ctx, runcoord.Execution{Scope: scope, SessionID: session, LaunchTurnSeq: launchTurnSeq, Generation: generation,
		ExecutionID: journal.DeterministicExecutionID(scope, session, launchTurnSeq, generation)})
}

func (s *stand) Freeze(ctx context.Context, scope contracts.Scope, session string, launchTurnSeq, generation int64) (int, error) {
	id := journal.DeterministicExecutionID(scope, session, launchTurnSeq, generation)
	var attempt int64
	err := s.RuntimePG.QueryRow(ctx, `SELECT attempt_no FROM ahp_runtime.executions WHERE tenant_id=$1 AND deployment_id=$2 AND subject_id=$3
		AND session_id=$4 AND execution_id=$5 AND generation=$6 AND status='RUNNING'`,
		scope.Tenant, scope.Deployment, scope.Subject, session, id, generation).Scan(&attempt)
	if err != nil {
		return 0, fmt.Errorf("verify running attempt for fault: %w", err)
	}
	guest := vmruntime.Guest{Attempt: attempt, Execution: vmruntime.Execution{Scope: scope, ID: id}}
	group := s.cfg.GuestCgroup
	if s.cfg.Driver == "firecracker" {
		group = filepath.Join(s.FirecrackerCgroup, guest.ID())
	}
	pids, err := os.ReadFile(filepath.Join(group, "cgroup.procs"))
	if err != nil {
		return 0, err
	}
	for _, raw := range strings.Fields(string(pids)) {
		pid, err := strconv.Atoi(raw)
		if err != nil {
			continue
		}
		fd, err := unix.PidfdOpen(pid, 0)
		if err != nil {
			continue
		}
		name, err := os.Readlink(filepath.Join("/proc", raw, "cwd"))
		match := err == nil && name == filepath.Join(s.SlotDir, "trusted-workers", guest.ID())
		if s.cfg.Driver == "firecracker" {
			match = len(strings.Fields(string(pids))) == 1
		}
		if match {
			err = unix.PidfdSendSignal(fd, unix.SIGSTOP, nil, 0)
		}
		unix.Close(fd)
		if match {
			return pid, err
		}
	}
	return 0, errors.New("cannot locate verified guest process for fault")
}

func (s *stand) BootstrapNS(ctx context.Context, executionID string) (int64, error) {
	u := s.DebugURLs["ahp-vm-runtime"] + "/debug/ahp-profile/bootstrap?execution_id=" + url.QueryEscape(executionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("bootstrap observation unavailable: HTTP %d", res.StatusCode)
	}
	var d struct {
		Elapsed int64 `json:"elapsed_ns"`
	}
	err = json.NewDecoder(res.Body).Decode(&d)
	return d.Elapsed, err
}

// The caller gathers cleanup evidence first. This is fallback shutdown through
// the real runtime owner if a failed scenario left a guest running.
func (s *stand) closeRunningGuests() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rows, err := s.RuntimePG.Query(ctx, `SELECT tenant_id,deployment_id,subject_id,session_id,
      execution_id,generation,COALESCE(launch_turn_seq,0) FROM ahp_runtime.executions
      WHERE status != 'FENCED'`)
	if err != nil {
		// Runtime startup can fail before the schema exists.
		var exists bool
		if lookup := s.RuntimePG.QueryRow(ctx, "SELECT to_regclass('ahp_runtime.executions') IS NOT NULL").Scan(&exists); lookup == nil && !exists {
			return nil
		}
		return err
	}
	var exits []runcoord.Execution
	for rows.Next() {
		var e runcoord.Execution
		if err := rows.Scan(&e.Scope.Tenant, &e.Scope.Deployment, &e.Scope.Subject,
			&e.SessionID, &e.ExecutionID, &e.Generation, &e.LaunchTurnSeq); err != nil {
			rows.Close()
			return err
		}
		exits = append(exits, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	var g sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	gate := make(chan struct{}, 16)
	for _, e := range exits {
		select {
		case gate <- struct{}{}:
		case <-ctx.Done():
			g.Wait()
			return errors.Join(append(errs, ctx.Err())...)
		}
		g.Go(func() {
			defer func() { <-gate }()
			if err := s.dispatch.Fence(ctx, e); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	g.Wait()
	return errors.Join(errs...)
}

func (s *stand) Close() error {
	var errs []error
	if s.stopRefresh != nil {
		s.stopRefresh()
		<-s.refreshDone
	}
	if s.CoordinatorHTTP != nil {
		s.CoordinatorHTTP.CloseIdleConnections()
	}
	if s.StateHTTP != nil {
		s.StateHTTP.CloseIdleConnections()
	}
	for i := len(s.roles) - 1; i >= 0; i-- {
		p := s.roles[i]
		if p.name == "ahp-vm-runtime" {
			errs = append(errs, s.closeRunningGuests())
		}
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(15 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
			errs = append(errs, fmt.Errorf("%s required SIGKILL", p.name))
		}
	}
	if s.mcp != nil {
		errs = append(errs, s.mcp.Close())
	}
	for _, pool := range []*pgxpool.Pool{s.PG, s.RuntimePG, s.identityPG} {
		if pool != nil {
			pool.Close()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if s.s3Cleanup != nil {
		errs = append(errs, s.s3Cleanup(ctx))
	}
	if s.storeAdmin != nil {
		for _, name := range s.createdDatabases {
			_, err := s.storeAdmin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize())
			errs = append(errs, err)
		}
		s.storeAdmin.Close()
	}
	if s.runtimeDir != "" && s.runtimeDir != s.tempDir {
		errs = append(errs, os.RemoveAll(s.runtimeDir))
	}
	errs = append(errs, os.RemoveAll(s.tempDir))
	return errors.Join(errs...)
}

// The pinned coordinator uses the same key pair for TLS serving and mTLS
// requests. Its leaf needs the server IP SAN and its own client SPIFFE SAN.
func mintCoordinatorCert(pki harness.CertMaterial, scope contracts.Scope) (harness.ClientCertFile, error) {
	var result harness.ClientCertFile
	caCert, err := os.ReadFile(pki.CACert)
	if err != nil {
		return result, err
	}
	block, _ := pem.Decode(caCert)
	if block == nil {
		return result, errors.New("missing profiling CA PEM")
	}
	root, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return result, err
	}
	caKey, err := os.ReadFile(filepath.Join(pki.Dir, "ca.key"))
	if err != nil {
		return result, err
	}
	block, _ = pem.Decode(caKey)
	if block == nil {
		return result, errors.New("missing profiling CA private key PEM")
	}
	sign, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return result, err
	}
	leaf, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return result, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return result, err
	}
	workload, err := url.Parse(auth.WorkloadID(scope))
	if err != nil {
		return result, err
	}
	template := &x509.Certificate{
		SerialNumber: serial, NotBefore: root.NotBefore, NotAfter: root.NotAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		URIs:        []*url.URL{workload}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, root, &leaf.PublicKey, sign)
	if err != nil {
		return result, err
	}
	key, err := x509.MarshalECPrivateKey(leaf)
	if err != nil {
		return result, err
	}
	result.Cert, result.Key = filepath.Join(pki.Dir, "coordinator.crt"), filepath.Join(pki.Dir, "coordinator.key")
	if err := os.WriteFile(result.Cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		return result, err
	}
	return result, os.WriteFile(result.Key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: key}), 0600)
}

func runCgroupExec(args []string) error {
	if len(args) < 2 || !filepath.IsAbs(args[0]) || !filepath.IsAbs(args[1]) {
		return errors.New("cgexec requires absolute cgroup and program paths")
	}
	fd, err := os.OpenFile(filepath.Join(args[0], "cgroup.procs"), os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, writeErr := fmt.Fprintf(fd, "%d\n", os.Getpid())
	if err := errors.Join(writeErr, fd.Close()); err != nil {
		return err
	}
	return unix.Exec(args[1], args[1:], os.Environ())
}

func profileHTTPClient(ca, cert, key string) (*http.Client, error) {
	tlsConfig, err := endpointTLS(ca, cert, key)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true, MaxIdleConns: 256, MaxIdleConnsPerHost: 256,
		TLSHandshakeTimeout: 5 * time.Second, IdleConnTimeout: 90 * time.Second},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}

func profileSigningKey(dir, name, issuer, kid string) (string, string, *auth.Signer, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", "", nil, err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", "", nil, err
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", "", nil, err
	}
	private, public := filepath.Join(dir, name+".key"), filepath.Join(dir, name+".pub")
	if err = os.WriteFile(public, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0600); err != nil {
		return "", "", nil, err
	}
	if err = os.WriteFile(private, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), 0600); err != nil {
		return "", "", nil, err
	}
	signer, err := auth.NewSigner(issuer, kid, priv)
	return private, public, signer, err
}

func randomProfileSecret() (string, error) {
	var bits [24]byte
	_, err := rand.Read(bits[:])
	return hex.EncodeToString(bits[:]), err
}

func profileEnvPrefix(prefix string, src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[prefix+k] = v
	}
	return dst
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

func availableProfilePorts(n int) ([]string, error) {
	var addr []string
	var reserved []net.Listener
	defer func() {
		for _, l := range reserved {
			_ = l.Close()
		}
	}()
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		addr = append(addr, l.Addr().String())
		reserved = append(reserved, l)
	}
	return addr, nil
}
