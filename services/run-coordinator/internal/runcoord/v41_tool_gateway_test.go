//go:build unix

package runcoord

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/internal/tools/toolsapi"
	"github.com/ahp-platform/ahp/internal/vmruntime"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	state "github.com/ahp-platform/ahp/sdk/go/state/journal"
	sdktools "github.com/ahp-platform/ahp/sdk/go/tools"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	v41Canary       = "fixture-secret-gateway-only-v41"
	v41Conversation = "native-mcp"
	v41Write        = "write"
)

type v41UnconnectedSecrets struct{}

func (v41UnconnectedSecrets) ReadVersion(context.Context, string) (map[string]any, int, error) {
	return nil, 0, errors.New("an unconnected user must not read Vault")
}
func (v41UnconnectedSecrets) WriteCAS(context.Context, string, map[string]any, int) (int, error) {
	return 0, errors.New("an unconnected user must not write Vault")
}
func (v41UnconnectedSecrets) DeleteMetadata(context.Context, string) error {
	return errors.New("an unconnected user must not delete Vault entries")
}

type v41Credentials struct{}

func (v41Credentials) Resolve(context.Context, contracts.Scope, string) (tools.Credential, error) {
	return tools.Credential{Bearer: v41Canary}, nil
}

type v41MCPTransport struct{ token string }

func (rt v41MCPTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	req := r.Clone(r.Context())
	req.Header.Set("Authorization", "Bearer "+rt.token)
	return http.DefaultTransport.RoundTrip(req)
}

type v41MCPClient struct{ *mcp.ClientSession }

func (c v41MCPClient) Close() error { return c.ClientSession.Close() }

type v41GatewayFixture struct {
	effects          atomic.Int32
	lost             atomic.Bool
	release          chan struct{}
	owner            *tools.Service
	schema           string
	artifactEndpoint string
	requireAuth      bool
}

func v41Args(mode string) json.RawMessage {
	args, _ := json.Marshal(map[string]string{"mode": mode})
	return args
}

func (fixture *v41GatewayFixture) mount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pki *coordinatorTLS, ids *identity.Store, verify *auth.Verifier, workers []contracts.Scope) (*auth.Signer, vmruntime.InjectorConfig) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	stateSigner, err := auth.NewSigner("v41-state", "lease", priv)
	if err != nil {
		t.Fatal(err)
	}
	leaseVerifier := auth.NewVerifier("v41-state", map[string]ed25519.PublicKey{"lease": pub})
	mcpServer := mcp.NewServer(&mcp.Implementation{Name: "v41-loopback-mcp", Version: "1"}, nil)
	mcpServer.AddTool(&mcp.Tool{Name: v41Write, InputSchema: map[string]any{"type": "object"}},
		func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var args map[string]string
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				return nil, err
			}
			fixture.effects.Add(1)
			if args["mode"] == "late" {
				select {
				case <-fixture.release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			text := "mcp-effect-" + args["mode"]
			if args["mode"] == "artifact" {
				text += strings.Repeat("x", 300<<10)
			}
			return &mcp.CallToolResult{IsError: args["mode"] == "business-error", Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil
		})
	mcpRoute := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return mcpServer }, &mcp.StreamableHTTPOptions{JSONResponse: true})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+v41Canary {
			http.Error(w, "MCP credential missing", http.StatusUnauthorized)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "cannot read MCP body", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		if bytes.Contains(raw, []byte(`"mode":"lost-upstream"`)) && bytes.Contains(raw, []byte(`"tools/call"`)) {
			recorder := httptest.NewRecorder()
			mcpRoute.ServeHTTP(recorder, r)
			if recorder.Code != 200 {
				t.Errorf("MCP fault before effect reply: %d", recorder.Code)
			}
			conn, _, err := http.NewResponseController(w).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		mcpRoute.ServeHTTP(w, r)
	}))
	t.Cleanup(upstream.Close)
	guardian := tools.NewPolicyGuardian("v41-fixture")
	for _, tool := range []string{v41Write, "gated"} {
		for _, mode := range []string{"prepared", "lost-receipt", "lost-upstream", "warm-first", "warm-second", "late", "business-error", "approve", "decline", "artifact"} {
			digest, err := tools.ApprovalDigest(tool, v41Args(mode))
			if err != nil {
				t.Fatal(err)
			}
			guardian.AllowDigest(digest)
		}
	}
	n, err := rand.Int(rand.Reader, big.NewInt(1<<60))
	if err != nil {
		t.Fatal(err)
	}
	fixture.schema = fmt.Sprintf("ahp_v41_%x", n)
	var receiptOwner *artifacts.Store
	if fixture.artifactEndpoint != "" {
		s3, err := minio.New(fixture.artifactEndpoint, &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("AHP_JOURNAL_TEST_S3_ACCESS"), os.Getenv("AHP_JOURNAL_TEST_S3_SECRET"), "")})
		if err != nil {
			t.Fatal(err)
		}
		receiptOwner, err = artifacts.New(pool, s3, fmt.Sprintf("ahp-v41-%x", n), func(context.Context, contracts.Scope, string, string) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := receiptOwner.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var oauth *tools.OAuthManager
	connection := "mcp"
	if fixture.requireAuth {
		oauth, err = tools.NewOAuthManager(ctx, pool, fixture.schema, tools.OAuthConfig{
			Secrets:      v41UnconnectedSecrets{},
			SecretPrefix: "secret/data/never-used-in-gate-test",
			Providers: map[string]tools.OAuthProvider{connection: {
				AuthorizeURL: "https://oauth.invalid/authorize", TokenURL: "https://oauth.invalid/token",
				RedirectURI: "https://browser.invalid/callback", ClientID: "fixture",
				Policy: netpolicy.Policy{AllowedHosts: []string{"oauth.invalid"}, AllowedMethods: []string{"GET", "POST"}, AllowedPathPrefixes: []string{"/authorize", "/token"}},
			}},
			Authorize: func(context.Context, contracts.Scope) error { return nil },
			LabelPrivate: func(context.Context, contracts.Scope) error {
				return errors.New("unconnected user reached private label gate")
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	fixture.owner, err = tools.New(ctx, pool, fixture.schema, tools.Config{
		JournalArtifacts: receiptOwner, OAuth: oauth,
		Guardian: guardian, Credentials: v41Credentials{},
		Connections: map[string]tools.Connector{connection: func(ctx context.Context, cred tools.Credential) (tools.MCPClient, error) {
			client, err := mcp.NewClient(&mcp.Implementation{Name: "v41-gateway", Version: "1"}, nil).Connect(ctx,
				&mcp.StreamableClientTransport{Endpoint: upstream.URL, HTTPClient: &http.Client{Transport: v41MCPTransport{cred.Bearer}}, DisableStandaloneSSE: true}, nil)
			if err != nil {
				return nil, err
			}
			return v41MCPClient{client}, nil
		}},
		Tools: map[string]tools.ToolSpec{
			v41Write: {Name: v41Write, Connection: connection, SideEffecting: true},
			"gated":  {Name: v41Write, Connection: connection, SideEffecting: true, RequiresApproval: true},
		}, Verifier: verify, Audience: "ahp", UserAudience: "ahp-browser",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := fixture.owner.Drop(ctx); err != nil {
			t.Error(err)
		}
	})
	handler, err := toolsapi.NewHandlerWithToolLeases(ctx, fixture.owner, verify, ids.Authorize, leaseVerifier)
	if err != nil {
		t.Fatal(err)
	}
	gateway := pki.server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tools/Call" {
			handler.ServeHTTP(w, r)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "cannot read tool request", 400)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		// W3: the owner finishes the operation before the guest loses the
		// acknowledgement. Only this guest-facing leg is held.
		if bytes.Contains(raw, []byte(`"mode":"lost-receipt"`)) && !fixture.lost.Load() {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, r)
			if rec.Code != 200 || !bytes.Contains(rec.Body.Bytes(), []byte(`"status":"SUCCEEDED"`)) {
				t.Errorf("fault did not occur after owner commit: %d %s", rec.Code, rec.Body.String())
			}
			fixture.lost.Store(true)
			<-r.Context().Done()
			return
		}
		if bytes.Contains(raw, []byte(`"mode":"late"`)) {
			late, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 15*time.Second)
			defer cancel()
			handler.ServeHTTP(w, r.WithContext(late))
			return
		}
		handler.ServeHTTP(w, r)
	}))
	clients := make(map[contracts.Scope]*http.Client)
	for _, scope := range workers {
		clients[scope] = pki.client(t, gateway, scope)
	}
	return stateSigner, vmruntime.InjectorConfig{ToolsURL: gateway.URL, ToolsClientFor: func(_ context.Context, scope contracts.Scope) (*http.Client, error) {
		if client := clients[scope]; client != nil {
			return client, nil
		}
		return nil, fmt.Errorf("unregistered tool workload")
	}}
}

func v41Execution(scope contracts.Scope, turn, gen int64) vmruntime.Execution {
	session := journal.ScopedSessionID(scope, v41Conversation)
	return vmruntime.Execution{Scope: scope, SessionID: session, Generation: gen, LaunchTurnSeq: turn, Harness: "go",
		ID: journal.DeterministicExecutionID(scope, session, turn, gen)}
}

func v41ProcessDriver(t *testing.T, dir string) (vmruntime.Driver, string) {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, "p")
	driver, err := vmruntime.NewProcessDriver(base, bin, "-test.run=^TestV41ProcessGuest$")
	if err != nil {
		t.Fatal(err)
	}
	return driver, base
}

func TestV41ProcessGuest(t *testing.T) {
	socket := os.Getenv("AHP_INJECTOR_SOCKET")
	if socket == "" {
		t.Skip("started by ProcessDriver")
	}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "PATH", "HOME", "TMPDIR", "AHP_INJECTOR_SOCKET":
		default:
			t.Fatalf("guest environment leaked %s", key)
		}
	}
	sdk, err := state.NewUnixInjectorClient(socket)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := sdktools.NewUnixInjectorClient(socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := runV41Guest(t.Context(), sdk, gw); err != nil {
		_ = os.WriteFile("worker.error", []byte(err.Error()), 0600)
		t.Fatal(err)
	}
}

func runV41Guest(parent context.Context, sdk *state.Client, gw *sdktools.Client) error {
	ctx, cancel := context.WithTimeout(parent, 40*time.Second)
	defer cancel()
	boot, err := sdk.RuntimeBootstrap(ctx)
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	first, err := sdk.ClaimVMTurn(ctx, boot.SessionID, journal.VMTurnClaimRequest{TurnSeq: boot.LaunchTurnSeq, Generation: boot.Generation, ExecutionID: boot.ExecutionID, TTLMilliseconds: 3000})
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	if first.ToolLeaseProof == "" {
		return errors.New("VM claim omitted proof")
	}
	lease := state.NewVMLease(first)
	for {
		turn := lease.Current()
		var input processInput
		if err := json.Unmarshal(turn.Input.Inline, &input); err != nil {
			return err
		}
		mode := input.Mode
		call := sdktools.LeasedCall{Scope: boot.Scope, CallID: "call-" + mode, Key: "effect-" + mode, Tool: v41Write, Arguments: v41Args(mode)}
		if mode == "approve" || mode == "decline" {
			call.Tool = "gated"
		}
		intent, err := sdk.GetToolIntent(ctx, boot.SessionID, call.CallID)
		if contracts.Code(err) == contracts.ErrNotFound {
			page, err := sdk.InspectSession(ctx, boot.SessionID)
			if err != nil {
				return err
			}
			body := []byte(`{"prepared":true}`)
			_, err = sdk.AppendEvents(ctx, boot.SessionID, turn.Epoch, journal.AppendRequest{
				TurnSeq: turn.InputSeq, Key: "prepare-" + mode, ExpectedSeq: page.HeadSeq,
				Events:  []journal.EventInput{{ID: "intent-" + mode, Type: "tool.intent", Role: "tool", Codec: "json", Payload: journal.Payload{Inline: body}}},
				Intents: []journal.PreparedIntent{{CallID: call.CallID, ToolName: call.Tool, ArgsDigest: sha256.Sum256(call.Arguments), IdempotencyKey: call.Key}},
			})
			if err != nil {
				return fmt.Errorf("prepare: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("intent: %w", err)
		}
		// No dispatch happened in generation 1 in W1.
		if mode == "prepared" && boot.Generation == 1 {
			<-ctx.Done()
			return nil
		}
		// Renew beyond the initial deadline, then consume a warm receipt
		// after the original proof has actually expired.
		if mode == "warm-first" {
			renewal, err := sdk.HeartbeatLease(ctx, boot.SessionID, turn.Epoch, 12*time.Second)
			if err != nil {
				return err
			}
			lease.ApplyHeartbeat(renewal)
			// Firecracker's wall clock can differ from the host's. Only
			// the Gateway's actual rejection proves this proof expired.
			expiry, stop := context.WithTimeout(ctx, 6*time.Second)
			err = v41WaitProofExpiry(expiry, gw, call.Key, sdktools.LeaseProof{
				SessionID: boot.SessionID, Epoch: first.Epoch, Proof: first.ToolLeaseProof,
			})
			stop()
			if err != nil {
				return err
			}
		}
		callCtx := ctx
		stop := func() {}
		if mode == "late" {
			callCtx, stop = context.WithTimeout(ctx, 400*time.Millisecond)
		}
		settled, err := gw.SettleLeasedCall(callCtx, sdk, lease, call)
		stop()
		if mode == "lost-receipt" && boot.Generation == 1 {
			return nil
		}
		if err != nil {
			return fmt.Errorf("settle %s (prior %s): %w", mode, intent.Status, err)
		}
		switch settled.Status {
		case journal.IntentAwaitingApproval, journal.IntentAwaitingAuth:
			return nil
		case journal.IntentDispatched:
			if mode != "late" {
				return errors.New("unexpected in-flight effect")
			}
			// Keep the current lease for the late receipt; every iteration
			// after timeout is lookup of the same key.
			for settled.Status == journal.IntentDispatched {
				timer := time.NewTimer(20 * time.Millisecond)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				}
				settled, err = gw.SettleLeasedCall(ctx, sdk, lease, call)
				if err != nil {
					return err
				}
			}
		}
		if settled.Result != nil && bytes.Contains(settled.Result.Inline, []byte(v41Canary)) {
			return errors.New("credential entered the guest result")
		}
		head, err := sdk.InspectSession(ctx, boot.SessionID)
		if err != nil {
			return err
		}
		output, err := json.Marshal(processOutput{PID: os.Getpid(), Execution: boot.ExecutionID, Marker: mode, ToolStatus: settled.Status})
		if err != nil {
			return err
		}
		receipt, err := sdk.FinishVMTurn(ctx, lease.Current(), head.HeadSeq, journal.Payload{Inline: output}, nil, false)
		if err != nil {
			return fmt.Errorf("finish: %w", err)
		}
		if !lease.ApplyFinish(turn.InputSeq, receipt) {
			if mode == "warm-first" {
				return errors.New("warm follow-up was not promoted")
			}
			return nil
		}
		if lease.Current().ToolLeaseProof == "" || lease.Current().ToolLeaseProof == first.ToolLeaseProof {
			return errors.New("warm follow-up omitted fresh proof")
		}
	}
}

func v41WaitProofExpiry(ctx context.Context, gw *sdktools.Client, key string, proof sdktools.LeaseProof) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := gw.FenceAndLookup(ctx, key, proof)
		if contracts.Code(err) == contracts.ErrStaleEpoch {
			return nil
		}
		if err != nil {
			return fmt.Errorf("checking prior tool lease expiry: %w", err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("prior tool lease did not expire: %w", ctx.Err())
		}
	}
}

func TestV41ProcessToolGatewayPostgres(t *testing.T) {
	scenarios := []struct {
		mode, status string
		effects      int32
		recover      bool
	}{
		{"prepared", journal.IntentCompleted, 1, true},
		{"lost-receipt", journal.IntentCompleted, 1, true},
		{"lost-upstream", journal.IntentUnknown, 1, false},
		{"late", journal.IntentCompleted, 1, false},
		{"business-error", journal.IntentCompleted, 1, false},
		{"policy-denied", journal.IntentRefused, 0, false},
		{"auth-required", journal.IntentAwaitingAuth, 0, false},
		{"approve", journal.IntentCompleted, 1, false},
		{"decline", journal.IntentRefused, 0, false},
	}
	for _, tc := range scenarios {
		t.Run(tc.mode, func(t *testing.T) {
			var scope contracts.Scope
			fixture := &v41GatewayFixture{release: make(chan struct{}), requireAuth: tc.mode == "auth-required"}
			p := setupV32RuntimeIntegration(t, func(tenant string) []contracts.Scope {
				scope = contracts.Scope{Tenant: tenant, Deployment: "worker", Subject: "alice"}
				return []contracts.Scope{scope}
			}, v41Conversation, v41ProcessDriver, fixture)
			first := v41Execution(scope, 1, 1)
			defer p.cleanupExecution(t, first)
			if tc.recover {
				defer p.cleanupExecution(t, v41Execution(scope, 1, 2))
			}
			stopReconcile := v41Reconcile(p)
			defer stopReconcile()
			p.send(t, scope, "initial", journal.DeliveryTurn, 0, processInput{Mode: tc.mode})
			if tc.mode == "late" {
				p.wait(t, "live MCP call", func() bool { return fixture.effects.Load() == 1 })
				// The original SDK call expires at 400ms. Leave the owner
				// request active with a durably dispatched operation.
				time.Sleep(700 * time.Millisecond)
				intent := v41ReadIntent(p, scope, tc.mode)
				if intent.Status != journal.IntentDispatched {
					t.Fatalf("late result was classified prematurely: %s", intent.Status)
				}
				close(fixture.release)
			}
			if tc.mode == "approve" || tc.mode == "decline" {
				v41WaitState(t, p, scope, 1, "WAITING_INPUT")
				if intent := v41ReadIntent(p, scope, tc.mode); intent.Status != journal.IntentAwaitingApproval || fixture.effects.Load() != 0 {
					t.Fatalf("approval parked state=%s effects=%d", intent.Status, fixture.effects.Load())
				}
				v41Decide(t, p, fixture, scope, tc.mode)
				p.send(t, scope, "answer", journal.DeliveryTurn, 0, processInput{Mode: tc.mode})
				s, err := p.state.GetTurnStatus(p.ctx, scope, first.SessionID)
				if err != nil || s.InputSeq == nil {
					t.Fatalf("read resumed turn: %+v %v", s, err)
				}
				defer p.cleanupExecution(t, v41Execution(scope, *s.InputSeq, 2))
			}
			epoch := int64(1)
			if tc.recover || tc.mode == "approve" || tc.mode == "decline" {
				epoch = 2
			}
			disposition := "IDLE"
			if tc.mode == "auth-required" {
				disposition = "WAITING_INPUT"
			}
			v41WaitState(t, p, scope, epoch, disposition)
			v41AssertOutcome(t, p, fixture, scope, tc.mode, tc.status, tc.effects)
			if tc.recover {
				root, err := p.identity.Get(p.ctx, scope, first.ID)
				if err != nil || root.Status != identity.StatusRevoked {
					t.Fatalf("old grant: %+v %v", root, err)
				}
				if tc.mode == "lost-receipt" && !fixture.lost.Load() {
					t.Fatal("Gateway reply was not cut")
				}
				v41AssertSSE(t, p, scope, 1, v41Execution(scope, 1, 2).ID)
			}
			t.Logf("mode=%s epoch=%d durable=%s actual_mcp_effects=%d", tc.mode, epoch, tc.status, tc.effects)
		})
	}
}

func v41Reconcile(p *processIntegration) func() {
	ctx, cancel := context.WithCancel(p.ctx)
	done := make(chan struct{})
	go func() { defer close(done); p.coord.RunReconcile(ctx, 150*time.Millisecond) }()
	return func() { cancel(); <-done }
}

func v41ReadIntent(p *processIntegration, scope contracts.Scope, mode string) journal.ToolIntent {
	intent, _ := p.state.GetToolIntent(p.ctx, scope, journal.ScopedSessionID(scope, v41Conversation), "call-"+mode)
	return intent
}

func v41WaitState(t *testing.T, p *processIntegration, scope contracts.Scope, epoch int64, status string) {
	t.Helper()
	session := journal.ScopedSessionID(scope, v41Conversation)
	p.waitFor(t, 25*time.Second, fmt.Sprintf("%s epoch %d", status, epoch), func() bool {
		state, err := p.state.GetTurnStatus(p.ctx, scope, session)
		return err == nil && state.Status == status && state.Epoch == epoch
	})
}

func v41AssertOutcome(t *testing.T, p *processIntegration, f *v41GatewayFixture, scope contracts.Scope, mode, want string, effects int32) {
	t.Helper()
	intent := v41ReadIntent(p, scope, mode)
	if intent.Status != want || f.effects.Load() != effects || intent.IdempotencyKey != "effect-"+mode || intent.GatewayOperationID != intent.IdempotencyKey {
		t.Fatalf("durable intent=%+v effects=%d want=%s/%d", intent, f.effects.Load(), want, effects)
	}
	if intent.Result != nil && bytes.Contains(intent.Result.Inline, []byte(v41Canary)) {
		t.Fatal("State result contains Gateway credential")
	}
	if mode == "business-error" && (intent.Result == nil || !bytes.Contains(intent.Result.Inline, []byte(`"isError":true`))) {
		t.Fatal("known MCP error result lost")
	}
	if want == journal.IntentUnknown && intent.Result != nil {
		t.Fatal("unknown effect has an invented result")
	}
	events, err := p.state.GetEvents(p.ctx, scope, journal.ScopedSessionID(scope, v41Conversation), 0, 128)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events.Events {
		if bytes.Contains(event.Payload.Inline, []byte(v41Canary)) || bytes.Contains(event.Payload.Inline, []byte("tool_lease_proof")) {
			t.Fatal("secret or proof copied into event log")
		}
		if event.Type == "tool.outcome" && bytes.Contains(event.Payload.Inline, []byte(`"status":"`+want+`"`)) && bytes.Contains(event.Payload.Inline, []byte(`"call_id":"call-`+mode+`"`)) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected one journal outcome for %s, got %d", mode, count)
	}
}

func v41Decide(t *testing.T, p *processIntegration, f *v41GatewayFixture, scope contracts.Scope, mode string) {
	t.Helper()
	digest, err := tools.ApprovalDigest("gated", v41Args(mode))
	if err != nil {
		t.Fatal(err)
	}
	token := signedCaller(t, p.signer, humanCaller(scope.Tenant, scope.Subject))
	if mode == "approve" {
		if _, err = f.owner.Approve(p.ctx, scope, digest, "gated", token); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err = f.owner.DeclineRequest(p.ctx, scope, "", digest, "gated", token); err != nil {
		t.Fatal(err)
	}
}

func v41AssertSSE(t *testing.T, p *processIntegration, scope contracts.Scope, turn int64, exec string) {
	t.Helper()
	suffix := fmt.Sprintf("/stream?turn_seq=%d", turn)
	body := p.request(t, scope, http.MethodGet, suffix, "")
	out := processStreamOutput(t, body)
	if out.Execution != exec || strings.Count(body, `"type":"turn.output"`) != 1 || strings.Contains(body, v41Canary) || strings.Contains(body, "tool_lease_proof") {
		t.Fatalf("unexpected SSE output for %s", exec)
	}
	var first string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "id: ") {
			first = strings.TrimPrefix(line, "id: ")
			break
		}
	}
	if first == "" {
		t.Fatal("SSE did not provide cursor")
	}
	resumed := p.request(t, scope, http.MethodGet, suffix+"&after_seq="+first, "")
	if strings.Contains(resumed, "id: "+first+"\n") || strings.Count(resumed, `"type":"turn.output"`) != 1 ||
		!strings.Contains(resumed, "event: end") {
		t.Fatal("SSE cursor replay failed")
	}
}

func TestV41LargeToolResultPostgres(t *testing.T) {
	endpoint := os.Getenv("AHP_JOURNAL_TEST_S3")
	if endpoint == "" {
		t.Skip("requires disposable PostgreSQL and local MinIO")
	}
	if host, _, err := net.SplitHostPort(endpoint); err != nil || host != "127.0.0.1" {
		t.Fatal("use a disposable loopback MinIO")
	}
	var scope contracts.Scope
	fixture := &v41GatewayFixture{artifactEndpoint: endpoint}
	p := setupV32RuntimeIntegration(t, func(tenant string) []contracts.Scope {
		scope = contracts.Scope{Tenant: tenant, Deployment: "artifact", Subject: "alice"}
		return []contracts.Scope{scope}
	}, v41Conversation, v41ProcessDriver, fixture)
	execution := v41Execution(scope, 1, 1)
	defer p.cleanupExecution(t, execution)
	p.send(t, scope, "initial", journal.DeliveryTurn, 0, processInput{Mode: "artifact"})
	v41WaitState(t, p, scope, 1, "IDLE")
	v41AssertOutcome(t, p, fixture, scope, "artifact", journal.IntentCompleted, 1)
	intent := v41ReadIntent(p, scope, "artifact")
	var descriptor struct {
		Storage string               `json:"result_storage"`
		Ref     sdktools.ArtifactRef `json:"artifact_ref"`
	}
	if intent.Result == nil || intent.Result.Blob != nil || len(intent.Result.Inline) > 1024 ||
		json.Unmarshal(intent.Result.Inline, &descriptor) != nil || descriptor.Storage != sdktools.StorageArtifact ||
		descriptor.Ref.Size < 300<<10 || descriptor.Ref.ArtifactID == "" || descriptor.Ref.Version == "" || descriptor.Ref.Digest == "" {
		t.Fatal("State must store the compact, versioned Gateway artifact descriptor")
	}
	t.Logf("MCP result %d bytes, State receipt %d bytes, response_blob_ref=null, actual_mcp_effects=1",
		descriptor.Ref.Size, len(intent.Result.Inline))
}
