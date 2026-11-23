//go:build unix

package runcoord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/internal/vmruntime"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/pkg/transport"
	stateclient "github.com/ahp-platform/ahp/sdk/go/state/journal"
	"golang.org/x/sys/unix"
)

type processIntegration struct {
	ctx       context.Context
	state     *statejournal.Store
	identity  *identity.Store
	verifier  *auth.Verifier
	signer    *auth.Signer
	started   chan struct{}
	aborted   chan struct{}
	hostDir   string
	coord     *Coordinator
	driver    vmruntime.Driver
	injector  *vmruntime.Injector
	manager   vmruntime.Manager
	workerDir string
	callers   map[contracts.Scope]processCaller
}

type processCaller struct {
	origin, token string
	client        *http.Client
}

func setupProcessIntegration(t *testing.T, scopes func(string) []contracts.Scope) *processIntegration {
	t.Helper()
	return setupV32RuntimeIntegration(t, scopes, "chat", processFixtureDriver)
}

type v32FixtureDriver func(*testing.T, string) (vmruntime.Driver, string)

func processFixtureDriver(t *testing.T, dir string) (vmruntime.Driver, string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	workerDir := filepath.Join(dir, "p")
	driver, err := vmruntime.NewProcessDriver(workerDir, executable, "-test.run=^TestV32CoordinatorProcessGuest$")
	if err != nil {
		t.Fatal(err)
	}
	return driver, workerDir
}

func setupV32RuntimeIntegration(t *testing.T, scopes func(string) []contracts.Scope, conversation string, fixtureDriver v32FixtureDriver, toolFixtures ...*v41GatewayFixture) *processIntegration {
	t.Helper()
	ctx, pool, db := openPG(t)
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	signer, verify := coordinatorAuth(t)
	pki := makeCoordinatorTLS(t)
	service := contracts.Scope{Tenant: db.scope.Tenant, Deployment: "run-coordinator", Subject: "coordinator"}
	workers := scopes(db.scope.Tenant)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, scope := range workers {
			if _, err := pool.Exec(cleanup, "DELETE FROM ahp_journal.sessions WHERE tenant_id=$1", scope.Tenant); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := ids.CreateRoot(ctx, service, identity.RootRequest{
		Task: "coordinate", Context: service.Tenant + "-coordinate", Deployment: service.Deployment,
		Capabilities: []string{"state", "state.coordinate", "runtime.dispatch"}, TTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	serviceToken, err := ids.MintTask(ctx, service, "coordinate", signer, "ahp", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tokenSource := func(context.Context) (string, error) { return serviceToken, nil }
	var stateSigner *auth.Signer
	var toolsConfig vmruntime.InjectorConfig
	var toolsAllowed func(contracts.Scope) bool
	if len(toolFixtures) == 1 {
		stateSigner, toolsConfig = toolFixtures[0].mount(t, ctx, pool, pki, ids, verify, workers)
		toolsAllowed = func(scope contracts.Scope) bool {
			for _, worker := range workers {
				if scope == worker {
					return true
				}
			}
			return false
		}
	}
	stateHandler, err := statejournal.NewHTTPWithCoordinator(db.store, verify, ids.Authorize, stateSigner, []string{auth.WorkloadID(service)})
	if err != nil {
		t.Fatal(err)
	}
	started, aborted := make(chan struct{}, 1), make(chan struct{}, 1)
	hold := transport.Task(verify, "ahp", "state", ids.Authorize)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-r.Context().Done()
		aborted <- struct{}{}
	}))
	stateServer := pki.server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("q") == "hold-fence" {
			hold.ServeHTTP(w, r)
			return
		}
		stateHandler.ServeHTTP(w, r)
	}))
	sdk, err := stateclient.New(stateclient.Config{BaseURL: stateServer.URL, HTTPClient: pki.client(t, stateServer, service), TokenSource: tokenSource})
	if err != nil {
		t.Fatal(err)
	}
	stateClients := make(map[contracts.Scope]*http.Client)
	for _, scope := range workers {
		stateClients[scope] = pki.client(t, stateServer, scope)
	}
	injector, err := vmruntime.NewInjector(vmruntime.InjectorConfig{
		StateURL: stateServer.URL, ToolsURL: toolsConfig.ToolsURL, ToolsClientFor: toolsConfig.ToolsClientFor,
		ClientFor: func(_ context.Context, scope contracts.Scope) (*http.Client, error) {
			if client := stateClients[scope]; client != nil {
				return client, nil
			}
			return nil, fmt.Errorf("unexpected execution workload %s", auth.WorkloadID(scope))
		},
		MintToken: func(ctx context.Context, e vmruntime.Execution) (string, error) {
			return ids.MintTask(ctx, e.Scope, e.ID, signer, "ahp", 5*time.Minute)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Keep the host directory short enough for a Unix socket on macOS.
	dir, err := os.MkdirTemp("/tmp", "ahp-p-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	driver, workerDir := fixtureDriver(t, dir)
	host := vmruntime.Host{ID: "process-test", Dir: filepath.Join(dir, "s"), Injector: injector, Driver: driver}
	if err := os.Mkdir(host.Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	manager := vmruntime.Manager{Pool: pool, Grants: ids, ToolsAllowed: toolsAllowed, PlaceHost: func(vmruntime.Execution) string { return host.ID },
		Host: func(string) (vmruntime.ExecutionHost, error) { return host, nil }}
	if err := manager.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	runtimeServer := pki.server(t, vmruntime.Handler(manager, verify, []string{auth.WorkloadID(service)}, ids.Authorize))
	global := sdk.Coordinator()
	authorize := func(_ context.Context, caller CallerPrincipal, target contracts.Scope) error {
		if stateClients[target] == nil || caller.Claims.Subject != target.Subject {
			return contracts.NewError(contracts.ErrForbidden, "fixture denied unregistered scope")
		}
		return nil
	}
	coordinator := &Coordinator{
		AccessPolicy: AccessPolicy{Verifier: verify, UserAudience: "ahp-browser", UserEdgeDeployments: []string{"user-edge"},
			AuthorizeAgent: authorize},
		GlobalState: global, StateFor: func(scope contracts.Scope) State { return global.ForScope(scope) },
		Signals:    NewPGSignalBus(pool),
		Dispatcher: HTTPDispatcher{BaseURL: runtimeServer.URL, Client: pki.client(t, runtimeServer, service), TokenSource: tokenSource},
	}
	notifyCtx, stopNotify := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		coordinator.Signals.(*PGSignalBus).Run(notifyCtx)
	}()
	t.Cleanup(func() { stopNotify(); <-done })
	coordServer := pki.server(t, coordinator.Handler())
	callers := make(map[contracts.Scope]processCaller)
	for _, scope := range workers {
		edge := contracts.Scope{Tenant: scope.Tenant, Deployment: "user-edge", Subject: "edge"}
		callers[scope] = processCaller{origin: coordServer.URL + "/v1/agents/" + scope.Deployment + "/sessions/" + conversation,
			token: signedCaller(t, signer, humanCaller(scope.Tenant, scope.Subject)), client: pki.client(t, coordServer, edge)}
	}
	return &processIntegration{ctx: ctx, state: db.store, identity: ids, verifier: verify, signer: signer,
		started: started, aborted: aborted, hostDir: host.Dir, coord: coordinator,
		driver: driver, injector: injector, manager: manager, workerDir: workerDir, callers: callers}
}

func (p *processIntegration) request(t *testing.T, scope contracts.Scope, method, suffix, input string) string {
	t.Helper()
	caller := p.callers[scope]
	code, response := coordinatorTLSRequest(t, p.ctx, caller.client, method, caller.origin+suffix, caller.token, input)
	want := http.StatusOK
	if method == http.MethodPost {
		want = http.StatusAccepted
	}
	if code != want {
		t.Fatalf("%s %s got HTTP %d: %s", method, suffix, code, response)
	}
	return response
}

func (p *processIntegration) send(t *testing.T, scope contracts.Scope, key, mode string, target int64, input any) {
	t.Helper()
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(journal.EnqueueRequest{Harness: "go", FormatVersion: 1, Key: key,
		DeliveryMode: mode, TargetTurnSeq: target, Input: journal.Payload{Inline: payload}})
	if err != nil {
		t.Fatal(err)
	}
	p.request(t, scope, http.MethodPost, ":send", string(request))
}

func (p *processIntegration) wait(t *testing.T, what string, check func() bool) {
	t.Helper()
	p.waitFor(t, 8*time.Second, what, check)
}

func (p *processIntegration) waitFor(t *testing.T, timeout time.Duration, what string, check func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()
	tick := time.NewTicker(15 * time.Millisecond)
	defer tick.Stop()
	for {
		if p.workerDir != "" {
			failures, _ := filepath.Glob(filepath.Join(p.workerDir, "*", "worker.error"))
			for _, path := range failures {
				if data, err := os.ReadFile(path); err == nil {
					t.Fatalf("guest %s: %s", filepath.Base(filepath.Dir(path)), data)
				}
			}
		}
		if check() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-tick.C:
		}
	}
}

type processInput struct {
	Mode   string `json:"mode"`
	Marker string `json:"marker"`
}

type processOutput struct {
	PID        int    `json:"pid"`
	Execution  string `json:"execution"`
	Marker     string `json:"marker"`
	Steer      string `json:"steer,omitempty"`
	ToolStatus string `json:"tool_status,omitempty"`
}

func TestV32CoordinatorProcessGuest(t *testing.T) {
	socket := os.Getenv("AHP_INJECTOR_SOCKET")
	if socket == "" {
		t.Skip("worker is launched by ProcessDriver")
	}
	if err := runV32CoordinatorProcessGuest(t.Context(), socket); err != nil {
		_ = os.WriteFile("worker.error", []byte(err.Error()), 0o600)
		t.Fatal(err)
	}
}

func runV32CoordinatorProcessGuest(parent context.Context, socket string) error {
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "PATH", "HOME", "TMPDIR", "AHP_INJECTOR_SOCKET":
		default:
			return fmt.Errorf("unexpected guest environment variable %q", key)
		}
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	sdk, err := stateclient.NewUnixInjectorClient(socket)
	if err != nil {
		return err
	}
	boot, err := sdk.RuntimeBootstrap(ctx)
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	turn, err := sdk.ClaimVMTurn(ctx, boot.SessionID, journal.VMTurnClaimRequest{TurnSeq: boot.LaunchTurnSeq,
		Generation: boot.Generation, ExecutionID: boot.ExecutionID, TTLMilliseconds: 3000})
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	var input processInput
	if err := json.Unmarshal(turn.Input.Inline, &input); err != nil {
		return err
	}
	// The work mode is carried by the claimed input. Keep the short claim for
	// the fence scenario, and give the other scenarios the normal lease.
	if input.Mode != "fence" {
		renewed, err := sdk.HeartbeatLease(ctx, boot.SessionID, turn.Epoch, 30*time.Second)
		if err != nil {
			return fmt.Errorf("extend lease for a non-frozen guest: %w", err)
		}
		turn.Lease, turn.ToolLeaseProof = renewed.Lease, renewed.ToolLeaseProof
	}
	switch input.Mode {
	case "fence":
		return runV32FrozenGuest(ctx, sdk, boot, turn)
	case "policy":
		return runV32PolicyGuest(ctx, socket, sdk, boot, turn)
	case "warm":
	default:
		return fmt.Errorf("unsupported process test mode %q", input.Mode)
	}
	return runV32WarmGuest(ctx, sdk, boot, turn, input)
}

func runV32WarmGuest(ctx context.Context, sdk *stateclient.Client, boot journal.RuntimeBootstrap, turn stateclient.ClaimedTurn, input processInput) error {
	var ack journal.SteerAck
	var steer string
	var head int64
	tick := time.NewTicker(15 * time.Millisecond)
	defer tick.Stop()
	for {
		page, err := sdk.GetEvents(ctx, boot.SessionID, 0, 64, "")
		if err != nil {
			return fmt.Errorf("read control events: %w", err)
		}
		var hasFollowup bool
		for _, event := range page.Events {
			switch event.Type {
			case "turn.steer":
				var control processInput
				if err := json.Unmarshal(event.Payload.Inline, &control); err != nil {
					return err
				}
				steer = control.Marker
				ack = journal.SteerAck{TurnSeq: turn.InputSeq, Count: 1, UpToSeq: event.Seq}
			case "turn.followup":
				hasFollowup = true
			}
		}
		if hasFollowup && ack.Count == 1 {
			head = page.HeadSeq
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	makeOutput := func(marker, steer string) journal.Payload {
		data, _ := json.Marshal(processOutput{PID: os.Getpid(), Execution: boot.ExecutionID, Marker: marker, Steer: steer})
		return journal.Payload{Inline: data}
	}
	finished, err := sdk.FinishVMTurn(ctx, turn, head, makeOutput(input.Marker, steer), &ack, false)
	if err != nil {
		return fmt.Errorf("finish initial turn: %w", err)
	}
	if finished.Control == nil || finished.Control.NextTurn == nil || finished.Control.NextTurn.Worker != boot.ExecutionID {
		return fmt.Errorf("no follow-up on active owner")
	}
	next := stateclient.ClaimedTurn{TurnLease: *finished.Control.NextTurn}
	if err := json.Unmarshal(next.Input.Inline, &input); err != nil {
		return err
	}
	if _, err := sdk.FinishVMTurn(ctx, next, next.HeadSeq, makeOutput(input.Marker, ""), nil, false); err != nil {
		return fmt.Errorf("finish follow-up: %w", err)
	}
	return nil
}

func TestLiveProcessWorkerMultiTenantZeroSecretsE2E(t *testing.T) {
	var scopes []contracts.Scope
	process := setupProcessIntegration(t, func(tenant string) []contracts.Scope {
		scopes = []contracts.Scope{{Tenant: tenant + "-a", Deployment: "agent-1", Subject: "alice"},
			{Tenant: tenant + "-b", Deployment: "agent-2", Subject: "bob"}}
		return scopes
	})
	defer func() {
		for _, scope := range scopes {
			session := journal.ScopedSessionID(scope, "chat")
			process.cleanupExecution(t, vmruntime.Execution{Scope: scope, SessionID: session, Generation: 1,
				ID: journal.DeterministicExecutionID(scope, session, 1, 1)})
		}
	}()
	for _, scope := range scopes {
		process.send(t, scope, "initial", journal.DeliveryTurn, 0, processInput{Mode: "warm", Marker: scope.Subject})
	}
	for _, scope := range scopes {
		session := journal.ScopedSessionID(scope, "chat")
		process.wait(t, "guest claim", func() bool {
			status, err := process.state.GetTurnStatus(process.ctx, scope, session)
			return err == nil && status.Status == "ACTIVE"
		})
	}
	pids := make(map[int]bool)
	for _, scope := range scopes {
		session := journal.ScopedSessionID(scope, "chat")
		owner := journal.DeterministicExecutionID(scope, session, 1, 1)
		process.send(t, scope, "steer", journal.DeliverySteer, 1, processInput{Marker: "steer-" + scope.Subject})
		process.send(t, scope, "followup", journal.DeliveryFollowup, 0, processInput{Marker: "followup-" + scope.Subject})
		process.wait(t, "warm completion", func() bool {
			status, err := process.state.GetTurnStatus(process.ctx, scope, session)
			return err == nil && status.Status == "IDLE"
		})
		stream := process.request(t, scope, http.MethodGet, "/stream?turn_seq=1", "")
		if !strings.Contains(stream, "event: end") || !strings.Contains(stream, `"next_turn_seq":3`) {
			t.Fatalf("initial turn stream: %s", stream)
		}
		first := processStreamOutput(t, stream)
		next := processStreamOutput(t, process.request(t, scope, http.MethodGet, "/stream?turn_seq=3", ""))
		if first.PID <= 1 || pids[first.PID] || first.Execution != owner || first.Marker != scope.Subject ||
			first.Steer != "steer-"+scope.Subject || next.PID != first.PID || next.Execution != owner ||
			next.Marker != "followup-"+scope.Subject {
			t.Fatalf("unexpected outputs %v / %v", first, next)
		}
		pids[first.PID] = true
	}
}

func processStreamOutput(t *testing.T, stream string) processOutput {
	t.Helper()
	for _, line := range strings.Split(stream, "\n") {
		var event journal.Event
		if strings.HasPrefix(line, "data: ") && json.Unmarshal([]byte(line[6:]), &event) == nil && event.Type == "turn.output" {
			var result processOutput
			if err := json.Unmarshal(event.Payload.Inline, &result); err != nil {
				t.Fatal(err)
			}
			return result
		}
	}
	t.Fatalf("no output in %s", stream)
	return processOutput{}
}

const processToolCall = "test-tool"

func runV32FrozenGuest(ctx context.Context, sdk *stateclient.Client, boot journal.RuntimeBootstrap, turn stateclient.ClaimedTurn) error {
	tool := journal.StandaloneToolRequest{CallID: processToolCall, TurnSeq: turn.InputSeq, ToolName: "test", ArgsDigest: journal.SHA256Digest{1}}
	if boot.Generation == 1 {
		_, err := sdk.AppendEvents(ctx, boot.SessionID, turn.Epoch, journal.AppendRequest{
			Key: "prepare-tool", ExpectedSeq: turn.HeadSeq, TurnSeq: turn.InputSeq,
			Intents: []journal.PreparedIntent{{CallID: tool.CallID, ToolName: tool.ToolName,
				ArgsDigest: tool.ArgsDigest, IdempotencyKey: "once"}},
		})
		if err != nil {
			return fmt.Errorf("prepare tool: %w", err)
		}
		decision, err := sdk.BeginStandaloneTool(ctx, boot.SessionID, turn.Epoch, tool)
		if err != nil || !decision.CanExecute || decision.Status != statejournal.IntentDispatched {
			return fmt.Errorf("dispatch tool: %+v, %v", decision, err)
		}
		if _, err := sdk.HeartbeatLease(ctx, boot.SessionID, turn.Epoch, 3*time.Second); err != nil {
			return fmt.Errorf("renew lease before freezing: %w", err)
		}
		if err := os.WriteFile("worker.pid", []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			return err
		}
		if err := syscall.Kill(os.Getpid(), syscall.SIGSTOP); err != nil {
			return err
		}
		return errors.New("frozen guest unexpectedly resumed")
	}

	decision, err := sdk.BeginStandaloneTool(ctx, boot.SessionID, turn.Epoch, tool)
	if err != nil || decision.CanExecute || decision.Status != statejournal.IntentUnknown {
		return fmt.Errorf("tool recovery: %+v, %v", decision, err)
	}
	output, _ := json.Marshal(processOutput{PID: os.Getpid(), Execution: boot.ExecutionID, ToolStatus: decision.Status})
	if _, err := sdk.FinishVMTurn(ctx, turn, turn.HeadSeq, journal.Payload{Inline: output}, nil, false); err != nil {
		return fmt.Errorf("finish recovered turn: %w", err)
	}
	return nil
}

func processGuest(exec vmruntime.Execution) vmruntime.Guest {
	return vmruntime.Guest{Execution: exec, Attempt: 1}
}

func processSocketClient(socket string) *http.Client {
	dialer := net.Dialer{}
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", socket)
	}}}
}

func (p *processIntegration) cleanupExecution(t *testing.T, execution vmruntime.Execution) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.manager.Fence(ctx, execution); err != nil {
		t.Errorf("retire fixture execution: %v", err)
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		guest := processGuest(execution)
		if err := errors.Join(p.injector.Close(guest), p.driver.StopAndWait(cleanup, guest)); err != nil {
			t.Errorf("retire fixture resources: %v", err)
		}
	}
}

func TestFenceKillsFrozenProcessRevokesGrantAndCutsOffInjector(t *testing.T) {
	var scope contracts.Scope
	process := setupProcessIntegration(t, func(tenant string) []contracts.Scope {
		scope = contracts.Scope{Tenant: tenant, Deployment: "agent-1", Subject: "alice"}
		return []contracts.Scope{scope}
	})
	session := journal.ScopedSessionID(scope, "chat")
	first := vmruntime.Execution{Scope: scope, SessionID: session, Generation: 1, LaunchTurnSeq: 1, Harness: "go",
		ID: journal.DeterministicExecutionID(scope, session, 1, 1)}
	recovered := vmruntime.Execution{Scope: scope, SessionID: session, Generation: 2, LaunchTurnSeq: 1, Harness: "go",
		ID: journal.DeterministicExecutionID(scope, session, 1, 2)}
	defer process.cleanupExecution(t, recovered)
	defer process.cleanupExecution(t, first)
	process.send(t, scope, "initial", journal.DeliveryTurn, 0, processInput{Mode: "fence"})

	guest := processGuest(first)
	var pid int
	process.wait(t, "frozen guest pid", func() bool {
		raw, err := os.ReadFile(filepath.Join(process.workerDir, guest.ID(), "worker.pid"))
		if err != nil {
			return false
		}
		pid, _ = strconv.Atoi(string(raw))
		return pid > 1
	})
	if pgid, err := unix.Getpgid(pid); err != nil || pgid != pid {
		t.Fatalf("guest PID %d group %d: %v", pid, pgid, err)
	}
	intent, err := process.state.GetToolIntent(process.ctx, scope, session, processToolCall)
	if err != nil || intent.Status != statejournal.IntentDispatched {
		t.Fatalf("expected durable DISPATCHED intent: %+v, %v", intent, err)
	}
	oldToken, err := process.identity.MintTask(process.ctx, scope, first.ID, process.signer, "ahp", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := process.verifier.Verify(oldToken, "ahp", auth.KindTask, auth.WorkloadID(scope))
	if err != nil || process.identity.Authorize(process.ctx, claims, "state") != nil {
		t.Fatalf("could not authorize old attempt before fencing: %v", err)
	}

	socket := filepath.Join(process.hostDir, guest.ID(), "ahp.sock")
	client := processSocketClient(socket)
	defer client.CloseIdleConnections()
	requestCtx, cancel := context.WithTimeout(process.ctx, 10*time.Second)
	defer cancel()
	pending := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet,
			"http://injector/v1/state/sessions/"+session+"/events?q=hold-fence", nil)
		if err != nil {
			pending <- err
			return
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			err = fmt.Errorf("in-flight request unexpectedly received HTTP %d", resp.StatusCode)
		} else if requestCtx.Err() != nil {
			err = fmt.Errorf("request timed out before fence closed it: %w", err)
		} else {
			err = nil // expected transport interruption before any HTTP response
		}
		pending <- err
	}()
	select {
	case <-process.started:
	case <-requestCtx.Done():
		t.Fatal("injector did not forward an authenticated in-flight request")
	}
	process.wait(t, "lease expiry", func() bool {
		page, err := process.coord.GlobalState.ListGlobalRecoverableVMPage(process.ctx, "ACTIVE", nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range page.Sessions {
			if s.Scope == scope && s.SessionID == session && s.ExecutionID == first.ID && s.Epoch == 1 {
				return true
			}
		}
		return false
	})
	if err := process.coord.Reconcile(process.ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-pending:
		if err != nil {
			t.Fatal(err)
		}
	case <-requestCtx.Done():
		t.Fatal("fencing did not abort the injector client")
	}
	select {
	case <-process.aborted:
	case <-requestCtx.Done():
		t.Fatal("fencing did not cancel the upstream State request")
	}
	if _, err := os.Stat(socket); !errors.Is(err, os.ErrNotExist) || process.injector.Listening(guest) {
		t.Fatalf("fenced injector is still active: %v", err)
	}
	if alive, err := process.driver.Alive(process.ctx, guest); err != nil || alive {
		t.Fatalf("fenced process is still registered: alive %t, err %v", alive, err)
	}
	if err := syscall.Kill(-pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("old process group %d still exists: %v", pid, err)
	}
	grant, err := process.identity.Get(process.ctx, scope, first.ID)
	if err != nil || grant.Status != identity.StatusRevoked ||
		contracts.Code(process.identity.Authorize(process.ctx, claims, "state")) != contracts.ErrForbidden {
		t.Fatalf("old execution grant still accepted: %s, %v", grant.Status, err)
	}

	process.wait(t, "recovered turn completion", func() bool {
		status, err := process.state.GetTurnStatus(process.ctx, scope, session)
		return err == nil && status.Status == "IDLE" && status.Epoch == 2
	})
	output := processStreamOutput(t, process.request(t, scope, http.MethodGet, "/stream?turn_seq=1", ""))
	after, err := process.state.GetToolIntent(process.ctx, scope, session, processToolCall)
	if err != nil || after.Status != statejournal.IntentUnknown || after.GatewayOperationID != intent.GatewayOperationID ||
		output.PID == pid || output.PID <= 1 || output.Execution != recovered.ID || output.ToolStatus != statejournal.IntentUnknown {
		t.Fatalf("unexpected recovery: %v, intent %+v, err %v", output, after, err)
	}
}

func runV32PolicyGuest(ctx context.Context, socket string, sdk *stateclient.Client, boot journal.RuntimeBootstrap, turn stateclient.ClaimedTurn) error {
	client := processSocketClient(socket)
	defer client.CloseIdleConnections()
	const session = "/v1/state/sessions/"
	sibling := journal.ScopedSessionID(boot.Scope, "sibling")
	for _, probe := range []struct{ method, path, body string }{
		{http.MethodGet, "/v1/state/sessions", ""},
		{http.MethodGet, session + sibling + "/events", ""},
		{http.MethodDelete, session + boot.SessionID, ""},
		{http.MethodDelete, session + sibling, ""},
		{http.MethodPatch, session + boot.SessionID + "/legal-hold", `{"hold":true}`},
		{http.MethodPost, session + boot.SessionID + ":heartbeat", fmt.Sprintf(`{"lease_epoch":%d,"release":true}`, turn.Epoch)},
		{http.MethodGet, "/v1/state/kv?scope_type=session&session_id=" + sibling, ""},
		{http.MethodPost, "/v1/state/kv:mutate", fmt.Sprintf(`{"session_id":%q,"lease_epoch":%d,"key":"policy","kv":[{"scope_type":"session","namespace":"test","key":"probe","expected_version":0,"payload":{"inline":"e30="}}]}`, sibling, turn.Epoch)},
		{http.MethodGet, "/internal/v1/state/vm-reconcile?status=QUEUED", ""},
	} {
		r, err := http.NewRequestWithContext(ctx, probe.method, "http://injector"+probe.path, strings.NewReader(probe.body))
		if err != nil {
			return err
		}
		if probe.body != "" {
			r.Header.Set("Content-Type", "application/json")
		}
		res, err := client.Do(r)
		if err != nil {
			return fmt.Errorf("%s %s: %w", probe.method, probe.path, err)
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil || res.StatusCode != http.StatusForbidden ||
			!strings.Contains(string(body), "outside the attempt policy") {
			return fmt.Errorf("%s %s: got HTTP %d %s (%v)", probe.method, probe.path, res.StatusCode, body, err)
		}
	}
	output, _ := json.Marshal(processOutput{PID: os.Getpid(), Execution: boot.ExecutionID, Marker: "9-policy-denials"})
	_, err := sdk.FinishVMTurn(ctx, turn, turn.HeadSeq, journal.Payload{Inline: output}, nil, false)
	return err
}

func TestInjectorBlocksSiblingSessionReadDeleteAndAdminRoutes(t *testing.T) {
	var scope contracts.Scope
	process := setupProcessIntegration(t, func(tenant string) []contracts.Scope {
		scope = contracts.Scope{Tenant: tenant, Deployment: "agent-1", Subject: "alice"}
		return []contracts.Scope{scope}
	})
	sibling := journal.ScopedSessionID(scope, "sibling")
	_, err := process.coord.StateFor(scope).EnqueueTurn(process.ctx, journal.EnqueueRequest{
		SessionID: sibling, Harness: "go", FormatVersion: 1, Key: "sibling-input",
		ExecutionMode: journal.ExecutionVMManaged, Input: journal.Payload{Inline: []byte(`{"secret":"sibling input"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	session := journal.ScopedSessionID(scope, "chat")
	exec := vmruntime.Execution{Scope: scope, SessionID: session, Generation: 1, LaunchTurnSeq: 1, Harness: "go",
		ID: journal.DeterministicExecutionID(scope, session, 1, 1)}
	defer process.cleanupExecution(t, exec)
	process.send(t, scope, "initial", journal.DeliveryTurn, 0, processInput{Mode: "policy"})
	process.wait(t, "guest policy probes", func() bool {
		status, err := process.state.GetTurnStatus(process.ctx, scope, session)
		return err == nil && status.Status == "IDLE"
	})
	output := processStreamOutput(t, process.request(t, scope, http.MethodGet, "/stream?turn_seq=1", ""))
	status, err := process.state.GetTurnStatus(process.ctx, scope, sibling)
	if err != nil || status.Status != "QUEUED" || status.HeadSeq != 1 ||
		output.PID <= 1 || output.Execution != exec.ID || output.Marker != "9-policy-denials" {
		t.Fatalf("guest probes changed sibling: status %+v, err %v, result %+v", status, err, output)
	}
}
