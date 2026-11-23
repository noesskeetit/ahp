package toolsapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type observeMCPCall struct {
	tools.MCPClient
	proven        chan struct{}
	ambiguousCall chan error
}

func (c observeMCPCall) CallTool(ctx context.Context, p *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	result, err := c.MCPClient.CallTool(ctx, p)
	if c.ambiguousCall != nil {
		c.ambiguousCall <- err
	} else if err == nil {
		close(c.proven)
		<-ctx.Done()
	}
	return result, err
}

func TestLeasedCallFinalizesAfterRequestCancellation(t *testing.T) {
	for _, mode := range []string{"ambiguous", "inline", "artifact", "complete_failure"} {
		t.Run(mode, func(t *testing.T) { testLeasedRequestCancellation(t, mode) })
	}
}

func testLeasedRequestCancellation(t *testing.T, mode string) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	fixture := newMCPFixture(t)
	ready, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseUpstream := release
	var ambiguousCall chan error
	if mode == "ambiguous" {
		ambiguousCall = make(chan error, 1)
	}
	var effects atomic.Int32
	record := func() error {
		if effects.Add(1) == 1 && mode == "ambiguous" {
			close(ready)
			select {
			case <-releaseUpstream:
			case <-ctx.Done():
			}
		}
		return nil
	}
	fixture.onWrite.Store(&record)
	if mode == "artifact" {
		fixture.writePadding.Store(300 << 10)
	}
	connect := fixture.connector()
	connector := func(ctx context.Context, cred tools.Credential) (tools.MCPClient, error) {
		client, err := connect(ctx, cred)
		if err != nil {
			return nil, err
		}
		return observeMCPCall{client, ready, ambiguousCall}, nil
	}
	var owner *artifacts.Store
	var service *tools.Service
	if mode == "artifact" {
		dsn := os.Getenv("AHP_JOURNAL_TEST_DATABASE_URL")
		if dsn == "" {
			t.Skip("large receipts require the disposable State PostgreSQL database")
		}
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		owner = newReceiptArtifactOwner(t, ctx, pool, func() *tools.Service { return service })
	}
	e := newEnv(t, func(cfg *tools.Config) {
		cfg.Connections["c"] = connector
		cfg.JournalArtifacts = owner
	})
	service = e.svc
	if mode == "complete_failure" {
		_, err := e.pool.Exec(ctx, "ALTER TABLE "+e.schema+".operations ADD CHECK (status <> 'SUCCEEDED')")
		if err != nil {
			t.Fatal(err)
		}
	}
	sign, err := auth.NewSigner("cancel-state", "lease", newLeaseSigningKey(t))
	if err != nil {
		t.Fatal(err)
	}
	verify := auth.NewVerifier("cancel-state", map[string]ed25519.PublicKey{"lease": sign.Public()})
	handler, err := NewHandlerWithToolLeases(ctx, e.svc, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, verify)
	if err != nil {
		t.Fatal(err)
	}
	const session, key = "cancel-session", "cancelled-operation"
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: tSubject}
	worker := auth.Claims{
		Audience: tAudience, Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment,
		Subject: scope.Subject, Task: "replacement", Root: "root", Context: "vm-session:worker", Grant: "grant",
		Capabilities: []string{"tools", "tools.leased"},
	}
	token, err := e.signer.Sign(worker, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := sign.SignToolLease(worker, session, 1, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	args := json.RawMessage(`{"target":"cancel-test"}`)
	e.allowDigest(t, "write", args)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Cancel-Test") == "1" {
			defer close(finished)
		}
		handler.ServeHTTP(w, r)
	}))
	srv.TLS = e.srv.TLS.Clone()
	srv.StartTLS()
	t.Cleanup(srv.Close)
	defer func() {
		if release != nil {
			close(release)
		}
	}()
	client := srv.Client()
	client.Transport.(*http.Transport).TLSClientConfig.Certificates = []tls.Certificate{e.currentClient()}
	request := func(ctx context.Context, route, key string, first bool) (int, []byte, error) {
		body := map[string]any{"key": key, "session_id": session, "lease_epoch": 1, "tool_lease_proof": proof}
		if route == "Call" {
			body["tool"], body["arguments"] = "write", args
		}
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/tools/"+route, bytes.NewReader(raw))
		if err != nil {
			return 0, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		if first {
			req.Header.Set("X-Cancel-Test", "1")
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		raw, err = io.ReadAll(resp.Body)
		return resp.StatusCode, raw, err
	}
	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	done := make(chan error, 1)
	go func() {
		_, _, err := request(callCtx, "Call", key, true)
		done <- err
	}()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("did not reach the upstream cancellation boundary")
	}
	internalKey := fmt.Sprintf("leased:%x", sha256.Sum256([]byte(session+"\x00"+key)))
	op, err := e.svc.Get(ctx, scope, internalKey)
	if err != nil || op.Status != operations.StatusDispatched || op.Guard == "" {
		t.Fatalf("expected dispatched operation and guard before cancellation, got %s (%v)", op.Status, err)
	}
	cancelCall()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("caller did not observe cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("HTTP caller did not return after cancellation")
	}
	if ambiguousCall != nil {
		select {
		case err := <-ambiguousCall:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("upstream MCP call did not observe caller cancellation: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("MCP call did not return before releasing the held upstream reply")
		}
		close(release)
		release = nil
	}
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("Gateway handler did not return while the ambiguous upstream call was held")
	}

	wantLedger, wantLookup, wantHTTP := operations.StatusSucceeded, "COMPLETED", http.StatusOK
	if mode == "ambiguous" || mode == "complete_failure" {
		wantLedger, wantLookup, wantHTTP = operations.StatusSideEffectUnknown, "OUTCOME_UNKNOWN", http.StatusConflict
	}
	final, err := e.svc.Get(ctx, scope, internalKey)
	if err != nil || final.Status != wantLedger || final.Guard != op.Guard {
		t.Fatalf("operation %s after HTTP cancel: ledger=%s, guard=%t (%v), want %s", key, final.Status, final.Guard == op.Guard, err, wantLedger)
	}
	status, reply, err := request(ctx, "FenceAndLookup", key, false)
	if err != nil || status != wantHTTP || !bytes.Contains(reply, []byte(wantLookup)) {
		t.Fatalf("lookup of same operation after cancellation: HTTP %d %s (%v), want %s", status, reply, err, wantLookup)
	}
	if mode == "artifact" {
		verifySpilledReceipt(t, ctx, e, owner, session, reply)
		if len(reply) > 2048 {
			t.Fatalf("artifact receipt is not compact: %d bytes", len(reply))
		}
	} else if mode == "inline" && !strings.Contains(string(reply), "written") {
		t.Fatal("known inline result was lost")
	}
	if _, _, err := request(ctx, "Call", key, false); err != nil {
		t.Fatalf("same-key retry check: %v", err)
	}
	if wantLookup == "OUTCOME_UNKNOWN" {
		status, reply, err := request(ctx, "Call", "sibling-key", false)
		if err != nil || status != http.StatusConflict || !bytes.Contains(reply, []byte("guard")) {
			t.Fatalf("sibling key bypassed uncertainty guard: HTTP %d %s (%v)", status, reply, err)
		}
	}
	if got := effects.Load(); got != 1 {
		t.Fatalf("external effects = %d, want one", got)
	}
	t.Logf("operation %s: %s / %s; exactly one upstream effect", key, wantLookup, wantLedger)
}
