//go:build linux

package runcoord

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/internal/vmruntime"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	stateclient "github.com/ahp-platform/ahp/sdk/go/state/journal"
	"golang.org/x/sys/unix"
)

const (
	fcConversation = "chat-1"
	fcCheckpoint   = "before-effect"
	fcNamespace    = "worker"
	fcEffectMarker = "test.effect"
)

func TestV32FirecrackerPostgresE2E(t *testing.T) {
	if os.Getenv("AHP_FC_E2E") != "1" {
		t.Skip("requires a Linux KVM host, a prepared guest rootfs and real PostgreSQL 18")
	}
	if os.Getenv("AHP_JOURNAL_TEST_DATABASE_URL") == "" {
		t.Fatal("AHP_JOURNAL_TEST_DATABASE_URL is required")
	}
	root := os.Getenv("AHP_FC_E2E_CGROUP")
	if !filepath.IsAbs(root) {
		t.Fatal("AHP_FC_E2E_CGROUP must point to a writable cgroup v2 directory")
	}
	var scopes []contracts.Scope
	var dataDir, cgroupDir string
	p := setupV32RuntimeIntegration(t, func(tenant string) []contracts.Scope {
		scopes = []contracts.Scope{
			{Tenant: tenant + "-a", Deployment: "agent-1", Subject: "alice"},
			{Tenant: tenant + "-b", Deployment: "agent-2", Subject: "bob"},
		}
		return scopes
	}, fcConversation, func(t *testing.T, dir string) (vmruntime.Driver, string) {
		dataDir = filepath.Join(dir, "v")
		cgroupDir = filepath.Join(root, filepath.Base(dir))
		for _, path := range []string{dataDir, cgroupDir} {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() {
			if err := os.Remove(cgroupDir); err != nil {
				t.Error(err)
			}
		})
		t.Setenv("AHP_COMPUTE_RUNTIME", "firecracker")
		t.Setenv("AHP_FC_DATADIR", dataDir)
		t.Setenv("AHP_FC_TAP", "")
		t.Setenv("AHP_FC_GUEST_MAC", "")
		driver, err := vmruntime.FirecrackerDriver(cgroupDir)
		if err != nil {
			t.Fatal(err)
		}
		return driver, ""
	})
	a, b := scopes[0], scopes[1]
	sibling := journal.ScopedSessionID(a, "sibling")
	if _, err := p.state.AcquireOrCreateSession(p.ctx, a, sibling, "go", 1, "fixture-owner", time.Minute); err != nil {
		t.Fatalf("create real sibling session for the isolation probe: %v", err)
	}
	execution := func(scope contracts.Scope, generation int64) vmruntime.Execution {
		session := journal.ScopedSessionID(scope, fcConversation)
		return vmruntime.Execution{Scope: scope, SessionID: session, Harness: "go", LaunchTurnSeq: 1,
			Generation: generation, ID: journal.DeterministicExecutionID(scope, session, 1, generation)}
	}
	frozen, recovered, warm := execution(a, 1), execution(a, 2), execution(b, 1)
	for _, e := range []vmruntime.Execution{warm, recovered, frozen} {
		defer p.cleanupExecution(t, e)
	}
	reconcileCtx, cancel := context.WithCancel(p.ctx)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		p.coord.RunReconcile(reconcileCtx, 500*time.Millisecond)
	}()
	defer func() { cancel(); <-stopped }()

	p.send(t, a, "initial", journal.DeliveryTurn, 0, processInput{Mode: "fence"})
	p.send(t, b, "initial", journal.DeliveryTurn, 0, processInput{Mode: "warm", Marker: "warm-first"})

	var frozenGuest vmruntime.Guest
	p.wait(t, "durable checkpoint and external effect", func() bool {
		view, err := p.state.InspectSession(p.ctx, a, frozen.SessionID)
		if err != nil || view.HeadCheckpoint == nil || *view.HeadCheckpoint != fcCheckpoint ||
			len(view.Unresolved) != 1 || view.Unresolved[0].Status != statejournal.IntentDispatched {
			return false
		}
		if fcEffectCount(t, p, frozen) != 1 {
			return false
		}
		frozenGuest = fcAttempt(t, p, frozen)
		return true
	})
	token, err := p.identity.MintTask(p.ctx, a, frozen.ID, p.signer, "ahp", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := p.verifier.Verify(token, "ahp", auth.KindTask, auth.WorkloadID(a))
	if err != nil || p.identity.Authorize(p.ctx, claims, "state") != nil {
		t.Fatalf("initial live grant: %v", err)
	}
	recoveredStream := fcReadStream(t, p, a, "/stream?turn_seq=1")

	p.wait(t, "second tenant claims its initial turn", func() bool {
		status, err := p.state.GetTurnStatus(p.ctx, b, warm.SessionID)
		return err == nil && status.Status == "ACTIVE"
	})
	warmGuest := fcAttempt(t, p, warm)
	procsPath := filepath.Join(cgroupDir, warmGuest.ID(), "cgroup.procs")
	before, err := os.ReadFile(procsPath)
	if err != nil || len(strings.TrimSpace(string(before))) == 0 {
		t.Fatalf("warm cgroup must contain the VMM: %q %v", before, err)
	}
	p.send(t, b, "steer", journal.DeliverySteer, 1, processInput{Marker: "warm-steer"})
	p.send(t, b, "followup", journal.DeliveryFollowup, 0, processInput{Marker: "warm-second"})
	p.wait(t, "warm turns finish", func() bool {
		status, err := p.state.GetTurnStatus(p.ctx, b, warm.SessionID)
		return err == nil && status.Status == "IDLE"
	})
	first := processStreamOutput(t, p.request(t, b, http.MethodGet, "/stream?turn_seq=1", ""))
	second := processStreamOutput(t, p.request(t, b, http.MethodGet, "/stream?turn_seq=3", ""))
	after, err := os.ReadFile(procsPath)
	if err != nil || string(after) != string(before) || fcAttempt(t, p, warm).Attempt != warmGuest.Attempt ||
		first.Execution != warm.ID || second.Execution != warm.ID || first.PID != second.PID ||
		first.Marker != "warm-first" || first.Steer != "warm-steer" || second.Marker != "warm-second" {
		t.Fatalf("warm delivery changed guest or lost control: %+v %+v, cgroup %q -> %q: %v", first, second, before, after, err)
	}
	t.Log("both tenants shared chat-1 without scope collisions; warm follow-up used the same Firecracker process")

	p.waitFor(t, 25*time.Second, "replacement finishes the recovered turn", func() bool {
		status, err := p.state.GetTurnStatus(p.ctx, a, frozen.SessionID)
		return err == nil && status.Status == "IDLE" && status.Epoch == 2
	})
	select {
	case response := <-recoveredStream:
		if response.err != nil || response.code != http.StatusOK ||
			strings.Count(response.body, `"type":"turn.output"`) != 1 || !strings.Contains(response.body, "event: end") {
			t.Fatalf("recovered SSE: %d, %v\n%s", response.code, response.err, response.body)
		}
		out := processStreamOutput(t, response.body)
		if out.Execution != recovered.ID || out.ToolStatus != statejournal.IntentUnknown || out.Marker != fcCheckpoint {
			t.Fatalf("replacement did not return recovered state: %+v", out)
		}
	case <-p.ctx.Done():
		t.Fatalf("waiting for recovered SSE: %v", p.ctx.Err())
	}

	if alive, err := p.driver.Alive(p.ctx, frozenGuest); err != nil || alive || p.injector.Listening(frozenGuest) {
		t.Fatalf("old guest was not stopped: alive=%t, err=%v", alive, err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, frozenGuest.ID(), "vsock.sock_5000")); !os.IsNotExist(err) {
		t.Fatalf("old guest socket remained: %v", err)
	}
	old, err := p.identity.Get(p.ctx, a, frozen.ID)
	if err != nil || old.Status != identity.StatusRevoked ||
		contracts.Code(p.identity.Authorize(p.ctx, claims, "state")) != contracts.ErrForbidden {
		t.Fatalf("old grant was not revoked: %s, %v", old.Status, err)
	}
	durable, err := p.state.GetToolIntent(p.ctx, a, frozen.SessionID, processToolCall)
	if err != nil || durable.Status != statejournal.IntentUnknown || fcEffectCount(t, p, frozen) != 1 {
		t.Fatalf("external effect was not fenced: %+v %v", durable, err)
	}
	t.Log("real guest restored its durable checkpoint, classified the dispatched effect as OUTCOME_UNKNOWN, and returned one SSE result")

	for _, e := range []vmruntime.Execution{warm, recovered} {
		p.cleanupExecution(t, e)
	}
	for _, path := range []string{cgroupDir, dataDir} {
		entries, err := os.ReadDir(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.IsDir() {
				t.Errorf("leaked runtime resource %s", filepath.Join(path, e.Name()))
			}
		}
	}
	t.Log("Firecracker cgroups and attempt socket directories cleared")
}

func fcAttempt(t *testing.T, p *processIntegration, e vmruntime.Execution) vmruntime.Guest {
	t.Helper()
	var attempt int64
	if err := p.manager.Pool.QueryRow(p.ctx, `SELECT attempt_no FROM ahp_runtime.executions
		WHERE tenant_id=$1 AND deployment_id=$2 AND execution_id=$3`, e.Scope.Tenant, e.Scope.Deployment, e.ID).Scan(&attempt); err != nil || attempt < 1 {
		t.Fatalf("lookup Firecracker attempt: %d %v", attempt, err)
	}
	return vmruntime.Guest{Execution: e, Attempt: attempt}
}

func fcEffectCount(t *testing.T, p *processIntegration, e vmruntime.Execution) int {
	t.Helper()
	events, err := p.state.GetEvents(p.ctx, e.Scope, e.SessionID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events.Events {
		if event.Type == fcEffectMarker {
			count++
		}
	}
	return count
}

type fcStreamResult struct {
	code int
	body string
	err  error
}

func fcReadStream(t *testing.T, p *processIntegration, scope contracts.Scope, suffix string) <-chan fcStreamResult {
	t.Helper()
	caller := p.callers[scope]
	req, err := http.NewRequestWithContext(p.ctx, http.MethodGet, caller.origin+suffix, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+caller.token)
	ch := make(chan fcStreamResult, 1)
	go func() {
		resp, err := caller.client.Do(req)
		if err != nil {
			ch <- fcStreamResult{err: err}
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		ch <- fcStreamResult{code: resp.StatusCode, body: string(body), err: err}
	}()
	return ch
}

func TestV32FirecrackerGuest(t *testing.T) {
	if os.Getenv("AHP_FC_E2E_GUEST") != "1" {
		t.Skip("run inside the Firecracker guest image")
	}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "AHP_FC_E2E_GUEST", "PATH", "HOME", "TERM":
		default:
			t.Fatalf("unexpected guest environment variable %q", key)
		}
	}
	interfaces, err := os.ReadDir("/sys/class/net")
	if err != nil || len(interfaces) != 1 || interfaces[0].Name() != "lo" {
		t.Fatalf("unexpected guest interfaces %v: %v", interfaces, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	client, err := stateclient.NewVSockInjectorClient(journal.RuntimeInjectorVSockPort)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := client.RuntimeBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	turn, err := client.ClaimVMTurn(ctx, boot.SessionID, journal.VMTurnClaimRequest{TurnSeq: boot.LaunchTurnSeq,
		Generation: boot.Generation, ExecutionID: boot.ExecutionID, TTLMilliseconds: 7000})
	if err != nil {
		t.Fatal(err)
	}
	var input processInput
	if err := json.Unmarshal(turn.Input.Inline, &input); err != nil {
		t.Fatal(err)
	}
	// The work mode is carried by the claimed input. Keep the short claim for
	// the fence scenario, and give the other scenarios the normal lease.
	if input.Mode != "fence" {
		renewed, err := client.HeartbeatLease(ctx, boot.SessionID, turn.Epoch, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		turn.Lease, turn.ToolLeaseProof = renewed.Lease, renewed.ToolLeaseProof
	}
	switch input.Mode {
	case "warm":
		err = runV32WarmGuest(ctx, client, boot, turn, input)
	case "fence":
		err = runV32FirecrackerFenceGuest(ctx, client, boot, turn)
	default:
		t.Fatalf("unsupported guest mode %q", input.Mode)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func runV32FirecrackerFenceGuest(ctx context.Context, client *stateclient.Client, boot journal.RuntimeBootstrap, turn stateclient.ClaimedTurn) error {
	tool := journal.StandaloneToolRequest{CallID: processToolCall, TurnSeq: turn.InputSeq, ToolName: "test", ArgsDigest: journal.SHA256Digest{1}}
	if boot.Generation == 1 {
		_, err := client.GetEvents(ctx, journal.ScopedSessionID(boot.Scope, "sibling"), 0, 5, "")
		if contracts.Code(err) != contracts.ErrForbidden {
			return fmt.Errorf("sibling session read: %v", err)
		}
		if err := client.ReleaseLease(ctx, boot.SessionID, turn.Epoch); contracts.Code(err) != contracts.ErrForbidden {
			return fmt.Errorf("guest can release its lease: %v", err)
		}
		if err := fcRejectInternalRoute(); err != nil {
			return err
		}
		receipt, err := client.CommitCheckpoint(ctx, boot.SessionID, turn.Epoch, journal.CheckpointCommit{
			TurnSeq: turn.InputSeq, Key: "durable-before-effect",
			Checkpoint: journal.Checkpoint{Namespace: fcNamespace, ID: fcCheckpoint, SnapshotSeq: turn.HeadSeq,
				Metadata: json.RawMessage(`{}`), ChannelVersions: json.RawMessage(`{}`), Codec: "json",
				Payload: journal.Payload{Inline: []byte(`{"step":"before-effect"}`)}},
			Intents: []journal.PreparedIntent{{CallID: tool.CallID, ToolName: tool.ToolName,
				ArgsDigest: tool.ArgsDigest, IdempotencyKey: "once"}},
		})
		if err != nil || receipt.ID != fcCheckpoint {
			return fmt.Errorf("save durable checkpoint and intent: %+v %v", receipt, err)
		}
		if _, err := client.HeartbeatLease(ctx, boot.SessionID, turn.Epoch, 7*time.Second); err != nil {
			return fmt.Errorf("renew lease before freezing: %w", err)
		}
		decision, err := client.BeginStandaloneTool(ctx, boot.SessionID, turn.Epoch, tool)
		if err != nil || !decision.CanExecute || decision.Status != statejournal.IntentDispatched {
			return fmt.Errorf("first tool dispatch: %+v %v", decision, err)
		}
		if err := fcRecordEffect(ctx, client, boot, turn); err != nil {
			return err
		}
		if err := unix.Kill(os.Getpid(), unix.SIGSTOP); err != nil {
			return err
		}
		return fmt.Errorf("frozen guest unexpectedly resumed")
	}
	view, err := client.InspectSession(ctx, boot.SessionID)
	if err != nil || view.HeadCheckpoint == nil || *view.HeadCheckpoint != fcCheckpoint ||
		len(view.Unresolved) != 1 || view.Unresolved[0].Status != statejournal.IntentDispatched {
		return fmt.Errorf("resume snapshot must still show DISPATCHED: %+v %v", view, err)
	}
	checkpoint, err := client.GetCheckpoint(ctx, boot.SessionID, fcNamespace, *view.HeadCheckpoint)
	if err != nil || string(checkpoint.Checkpoint.Payload.Inline) != `{"step":"before-effect"}` {
		return fmt.Errorf("restored checkpoint: %+v %v", checkpoint, err)
	}
	decision, err := client.BeginStandaloneTool(ctx, boot.SessionID, turn.Epoch, tool)
	if decision.CanExecute {
		if markErr := fcRecordEffect(ctx, client, boot, turn); markErr != nil {
			return markErr
		}
	}
	if err != nil || decision.CanExecute || decision.Status != statejournal.IntentUnknown {
		return fmt.Errorf("replayed tool: %+v %v", decision, err)
	}
	output, err := json.Marshal(processOutput{PID: os.Getpid(), Execution: boot.ExecutionID,
		Marker: checkpoint.Checkpoint.ID, ToolStatus: decision.Status})
	if err != nil {
		return err
	}
	_, err = client.FinishVMTurn(ctx, turn, view.HeadSeq, journal.Payload{Inline: output}, nil, false)
	return err
}

func fcRecordEffect(ctx context.Context, client *stateclient.Client, boot journal.RuntimeBootstrap, turn stateclient.ClaimedTurn) error {
	page, err := client.GetEvents(ctx, boot.SessionID, 0, 64, "")
	if err != nil {
		return err
	}
	key := fmt.Sprintf("effect-generation-%d", boot.Generation)
	_, err = client.AppendEvents(ctx, boot.SessionID, turn.Epoch, journal.AppendRequest{
		Key: key, ExpectedSeq: page.HeadSeq, TurnSeq: turn.InputSeq,
		Events: []journal.EventInput{{ID: key, Type: fcEffectMarker, Role: "assistant", Codec: "json",
			Payload: journal.Payload{Inline: []byte(`{"invoked":true}`)}}},
	})
	return err
}

func fcRejectInternalRoute() error {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), "injector-policy-probe")
	defer file.Close()
	timeout := &unix.Timeval{Sec: 5}
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, timeout); err != nil {
		return err
	}
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_SNDTIMEO, timeout); err != nil {
		return err
	}
	if err := unix.Connect(fd, &unix.SockaddrVM{CID: unix.VMADDR_CID_HOST, Port: journal.RuntimeInjectorVSockPort}); err != nil {
		return err
	}
	if _, err := io.WriteString(file, "GET /internal/v1/state/coordinator/vm-reconcile HTTP/1.0\r\nHost: injector\r\n\r\n"); err != nil {
		return err
	}
	resp, err := http.ReadResponse(bufio.NewReader(file), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		return fmt.Errorf("internal route status %d", resp.StatusCode)
	}
	return nil
}
