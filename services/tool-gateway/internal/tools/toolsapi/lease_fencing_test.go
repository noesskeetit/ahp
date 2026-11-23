package toolsapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLeaseFencingDelayedWorkerRequest(t *testing.T) {
	t.Run("expired", func(t *testing.T) { testDelayedWorkerRequest(t, false, false) })
	t.Run("proof_still_valid", func(t *testing.T) { testDelayedWorkerRequest(t, true, false) })
}

func TestLeaseFencingLargeReceipt(t *testing.T) {
	testDelayedWorkerRequest(t, true, true)
}

func testDelayedWorkerRequest(t *testing.T, earlyRelease, spillReceipt bool) {
	stateDSN, gatewayDSN := os.Getenv("AHP_JOURNAL_TEST_DATABASE_URL"), os.Getenv("AHP_GATEWAY_TEST_DATABASE_URL")
	if stateDSN == "" || gatewayDSN == "" {
		t.Skip("requires separate disposable State and Gateway PostgreSQL databases")
	}
	databases := make(map[string]bool)
	for _, dsn := range []string{stateDSN, gatewayDSN} {
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil || !strings.HasPrefix(cfg.ConnConfig.Database, "ahp_carbon_compact") || databases[cfg.ConnConfig.Database] {
			t.Fatal("use two distinct disposable ahp_carbon_compact PostgreSQL databases")
		}
		databases[cfg.ConnConfig.Database] = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	statePool, err := pgxpool.New(ctx, stateDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer statePool.Close()
	journal := statejournal.New(statePool)
	if err := journal.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var owner *artifacts.Store
	var service *tools.Service
	if spillReceipt {
		owner = newReceiptArtifactOwner(t, ctx, statePool, func() *tools.Service { return service })
		journal = statejournal.NewWithArtifacts(statePool, owner)
	}
	e := newEnvForDatabase(t, gatewayDSN, func(config *tools.Config) { config.JournalArtifacts = owner })
	service = e.svc
	if spillReceipt {
		e.fixture.writePadding.Store(300 << 10)
	}
	effectFile, err := os.OpenFile(filepath.Join(t.TempDir(), "external-effects.log"), os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer effectFile.Close()
	recordEffect := func() error {
		if _, err := effectFile.WriteString("WRITE delayed-worker\n"); err != nil {
			return err
		}
		return syncEffectFile(effectFile)
	}
	e.fixture.onWrite.Store(&recordEffect)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	stateSigner, err := auth.NewSigner("compact-state", "compact-key", priv)
	if err != nil {
		t.Fatal(err)
	}
	leaseVerifier := auth.NewVerifier("compact-state", map[string]ed25519.PublicKey{"compact-key": pub})
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	stateHTTP, err := statejournal.NewHTTPWithToolLeases(journal, e.verifier, authorize, stateSigner)
	if err != nil {
		t.Fatal(err)
	}
	toolHTTP, err := NewHandlerWithToolLeases(ctx, e.svc, e.verifier, authorize, leaseVerifier)
	if err != nil {
		t.Fatal(err)
	}
	oldToken, newToken := func() (string, string) {
		mint := func(task string) string {
			token, err := e.signer.Sign(auth.Claims{
				Audience: tAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment,
				Subject: tSubject, Task: task, Root: "run-1", Context: "ctx-1", Grant: "grant-1",
			}, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			return token
		}
		return mint("original"), mint("replacement")
	}()

	held := make(chan struct{})
	release := make(chan struct{})
	routes := http.NewServeMux()
	routes.Handle("/v1/state/", stateHTTP)
	routes.Handle("/v1/tools/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Hold") == "1" {
			close(held)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		toolHTTP.ServeHTTP(w, r)
	}))
	server := httptest.NewUnstartedServer(routes)
	server.TLS = e.srv.TLS.Clone()
	server.StartTLS()
	defer server.Close()
	defer close(release)
	tr := server.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{e.currentClient()}
	client := &http.Client{Transport: tr}
	defer client.CloseIdleConnections()

	type response struct {
		Status int
		Body   []byte
		Err    error
	}
	request := func(path, token string, in any, hold bool) response {
		body, err := json.Marshal(in)
		if err != nil {
			return response{Err: err}
		}
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+path, bytes.NewReader(body))
		if err != nil {
			return response{Err: err}
		}
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		if hold {
			r.Header.Set("X-Test-Hold", "1")
		}
		out, err := client.Do(r)
		if err != nil {
			return response{Err: err}
		}
		defer out.Body.Close()
		body, err = io.ReadAll(out.Body)
		return response{Status: out.StatusCode, Body: body, Err: err}
	}
	check := func(response response, status int, word string) {
		t.Helper()
		if response.Err != nil || response.Status != status || !strings.Contains(string(response.Body), word) {
			t.Fatalf("want HTTP %d %q, got HTTP %d %s (%v)", status, word, response.Status, response.Body, response.Err)
		}
	}
	type leaseResponse struct {
		statejournal.Lease
		Proof string `json:"tool_lease_proof"`
	}
	session, key := "delayed-worker", "external-operation-1"
	acquire := func(token string, ttl int) leaseResponse {
		t.Helper()
		out := request("/v1/state/sessions:acquire", token, map[string]any{
			"session_id": session, "harness_type": "langgraph", "format_version": 1, "ttl_ms": ttl,
		}, false)
		check(out, 200, "tool_lease_proof")
		var lease leaseResponse
		if err := json.Unmarshal(out.Body, &lease); err != nil || lease.Proof == "" {
			t.Fatalf("missing proof: %v", err)
		}
		return lease
	}
	ttl := 1500
	if earlyRelease {
		ttl = 15000
	}
	first := acquire(oldToken, ttl)
	args := json.RawMessage(`{"target":"delayed-packet-test"}`)
	e.allowDigest(t, "write", args)
	check(request("/v1/state/sessions/"+session+"/events:append", oldToken, struct {
		Epoch int64 `json:"lease_epoch"`
		statejournal.AppendRequest
	}{first.Epoch, statejournal.AppendRequest{
		Key: "prepare-external-effect",
		Events: []statejournal.EventInput{{
			ID: "native-call-event", Type: "model.tool_call", Role: "assistant", Codec: "json",
			Payload: statejournal.Payload{Inline: args},
		}},
		Intents: []statejournal.PreparedIntent{{
			CallID: "native-tool-call", ToolName: "write", ArgsDigest: sha256.Sum256(args), IdempotencyKey: key,
		}},
	}}, false), 200, "head_seq")
	call := func(lease leaseResponse, includeTool bool) map[string]any {
		body := map[string]any{"key": key, "session_id": session, "lease_epoch": lease.Epoch, "tool_lease_proof": lease.Proof}
		if includeTool {
			body["tool"], body["arguments"] = "write", args
		}
		return body
	}

	pending := make(chan response, 1)
	go func() { pending <- request("/v1/tools/Call", oldToken, call(first, true), true) }()
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal("old request never reached proxy")
	}
	if earlyRelease {
		check(request("/v1/state/sessions/"+session+":heartbeat", oldToken,
			map[string]any{"lease_epoch": first.Epoch, "release": true}, false), 200, "{}")
	} else {
		wait := time.NewTimer(time.Until(first.ExpiresAt) + 20*time.Millisecond)
		defer wait.Stop()
		select {
		case <-wait.C:
		case <-ctx.Done():
			t.Fatal("lease did not expire")
		}
	}
	second := acquire(newToken, 5000)
	if first.Epoch != 1 || second.Epoch != 2 {
		t.Fatalf("unexpected epochs: %d, %d", first.Epoch, second.Epoch)
	}
	check(request("/v1/tools/FenceAndLookup", newToken, call(second, false), false), 404, "SAFE_TO_RETRY")
	check(request("/v1/tools/Call", newToken, call(second, true), false), 200, "SUCCEEDED")
	receipt := request("/v1/tools/FenceAndLookup", newToken, call(second, false), false)
	if spillReceipt {
		check(receipt, 200, "artifact_ref")
		blob := verifySpilledReceipt(t, ctx, e, owner, session, receipt.Body)
		check(request("/v1/state/aux:mutate", newToken, map[string]any{
			"session_id": session, "lease_epoch": second.Epoch,
			"transition": statejournal.IntentTransition{
				CallID: "native-tool-call", GatewayOperationID: key, Status: statejournal.IntentDispatched,
			},
		}, false), 200, "{}")
		payload := statejournal.Payload{Blob: &blob}
		check(request("/v1/state/sessions/"+session+"/events:append", newToken, struct {
			Epoch int64 `json:"lease_epoch"`
			statejournal.AppendRequest
		}{second.Epoch, statejournal.AppendRequest{
			Key: "publish-artifact-receipt", ExpectedSeq: 1,
			Events: []statejournal.EventInput{{
				ID: "artifact-receipt", Type: "tool.result", Role: "tool", Codec: "json", Payload: payload,
			}},
			ToolOutcomes: []statejournal.IntentTransition{{
				CallID: "native-tool-call", GatewayOperationID: key, Status: statejournal.IntentCompleted, Result: &payload,
			}},
		}}, false), 200, "\"head_seq\":2")
	} else {
		check(receipt, 200, "written")
	}
	check(request("/v1/tools/Call", oldToken, call(second, true), false), 403, "FORBIDDEN")
	tampered := call(second, true)
	tampered["arguments"] = json.RawMessage(`{"target":"different-arguments"}`)
	check(request("/v1/tools/Call", newToken, tampered, false), 400, "KEY_REUSE_MISMATCH")
	check(request("/v1/tools/Call", newToken, call(second, true), false), 200, "SUCCEEDED")
	if got := atomic.LoadInt32(&e.fixture.effCount); got != 1 {
		t.Fatalf("external actions before releasing old request: %d", got)
	}
	release <- struct{}{}
	select {
	case old := <-pending:
		check(old, 409, "STALE_EPOCH")
	case <-ctx.Done():
		t.Fatal("no response for delayed request")
	}
	if got := atomic.LoadInt32(&e.fixture.effCount); got != 1 {
		t.Fatalf("external actions after releasing old request: %d", got)
	}
	effects, err := os.ReadFile(effectFile.Name())
	if err != nil || string(effects) != "WRITE delayed-worker\n" {
		t.Fatalf("expected one durable external effect, got %q (%v)", effects, err)
	}
	var floor int64
	if err := e.pool.QueryRow(ctx, "SELECT min_allowed_epoch FROM "+e.schema+".gateway_session_epochs WHERE tenant_id=$1 AND session_id=$2", e.tenant, session).Scan(&floor); err != nil || floor != 2 {
		t.Fatalf("expected durable Gateway floor 2, got %d (%v)", floor, err)
	}
	var proofStillValid bool
	if err := e.pool.QueryRow(ctx, "SELECT clock_timestamp() < $1::timestamptz", first.ExpiresAt).Scan(&proofStillValid); err != nil {
		t.Fatal(err)
	}
	if proofStillValid != earlyRelease {
		t.Fatalf("proof validity at the Gateway clock: got %t, want %t", proofStillValid, earlyRelease)
	}
	t.Logf("distinct databases; Gateway floor %d; old proof within expiry: %t; delayed epoch 1 -> 409 STALE_EPOCH; durable external log: %q", floor, proofStillValid, effects)
}

func TestLeasedPrincipalCannotDowngrade(t *testing.T) {
	e := newEnv(t)
	worker := auth.Claims{
		Audience: tAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment,
		Subject: tSubject, Task: "worker", Root: "root", Context: "vm-session:worker", Grant: "grant",
	}
	for _, v := range []auth.Claims{worker, {
		Audience: tAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment,
		Subject: tSubject, Task: "worker", Root: "root", Context: "ctx", Grant: "grant", Capabilities: []string{"tools", "tools.leased"},
	}} {
		token, err := e.signer.Sign(v, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		for route, body := range map[string]string{
			"Call": `{"key":"downgrade","tool":"echo","arguments":{}}`,
			"Get":  `{"key":"downgrade"}`, "GetResult": `{"key":"downgrade"}`, "Fence": `{"key":"downgrade"}`,
		} {
			status, out := e.call(t, route, token, body)
			mustStatus(t, status, http.StatusForbidden, out)
		}
	}
}

func TestLeasedToolsAndUsers(t *testing.T) {
	t.Run("coexisting", func(t *testing.T) { testLeasedToolsAndUsers(t, false) })
	t.Run("strict", func(t *testing.T) { testLeasedToolsAndUsers(t, true) })
}

func testLeasedToolsAndUsers(t *testing.T, strict bool) {
	e := newEnv(t, func(cfg *tools.Config) { cfg.UserAudience = "browser" })
	proofSigner, err := auth.NewSigner("state", "leases", newLeaseSigningKey(t))
	if err != nil {
		t.Fatal(err)
	}
	proofVerifier := auth.NewVerifier("state", map[string]ed25519.PublicKey{"leases": proofSigner.Public()})
	taskAuth := func(context.Context, auth.Claims, string) error { return nil }
	humanAuth := func(_ context.Context, claims auth.Claims, capability string) error {
		if claims.Kind != auth.KindHuman || capability != ApproveCapability {
			return contracts.NewError(contracts.ErrForbidden, "test: approval permission required")
		}
		return nil
	}
	newHandler := NewCoexistingHandlerWithToolLeases
	if strict {
		newHandler = NewHandlerWithToolLeasesAndUsers
	}
	handler, err := newHandler(t.Context(), e.svc, e.verifier, taskAuth, proofVerifier, "browser", humanAuth)
	if err != nil {
		t.Fatal(err)
	}
	e.srv.Config.Handler = handler
	worker := auth.Claims{
		Audience: tAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment,
		Subject: tSubject, Task: "worker", Root: "root", Context: "vm-session:worker", Grant: "grant",
		Capabilities: []string{"tools", "tools.leased"},
	}
	token, err := e.signer.Sign(worker, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	human, err := e.signer.Sign(auth.Claims{
		Audience: "browser", Kind: auth.KindHuman, Tenant: e.tenant, Subject: tSubject,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := proofSigner.SignToolLease(worker, "worker-session", 1, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	legacy := e.signTask(t)
	const lookup = `{"key":"tool-key"}`
	const call = `{"key":"tool-key","tool":"echo","arguments":{}}`
	leasedLookup := func(key string) string {
		return fmt.Sprintf(`{"key":%q,"session_id":"worker-session","lease_epoch":1,"tool_lease_proof":%q}`, key, proof)
	}
	leasedCall := func(key, tool string) string {
		return fmt.Sprintf(`{"key":%q,"tool":%q,"arguments":{},"session_id":"worker-session","lease_epoch":1,"tool_lease_proof":%q}`, key, tool, proof)
	}
	expect := func(route, bearer, input string, want int) map[string]any {
		t.Helper()
		status, out := e.call(t, route, bearer, input)
		mustStatus(t, status, want, out)
		return out
	}
	expect("Call", token, call, 403)
	expect("Call", human, call, 403)
	expect("Call", token, leasedCall("tool-key", "echo"), 200)
	expect("FenceAndLookup", token, leasedLookup("tool-key"), 200)
	expect("FenceAndLookup", legacy, lookup, 403)
	expect("FenceAndLookup", legacy, leasedLookup("tool-key"), 403)
	expect("ConnectionState", token, "{}", 200)
	expect("Approvals", human, "{}", 200)
	expect("Approvals", token, "{}", 403)
	expect("OAuthConnections", human, "{}", 403)
	if strict {
		expect("Call", legacy, call, 403)
		for _, route := range []string{"Get", "GetResult", "Fence"} {
			expect(route, token, lookup, 404)
			expect(route, legacy, lookup, 404)
		}
	} else {
		expect("Call", legacy, call, 200)
		expect("Get", legacy, lookup, 200)
		expect("GetResult", legacy, lookup, 200)
		for _, route := range []string{"Get", "GetResult", "Fence"} {
			expect(route, token, lookup, 403)
		}
		internalKey := fmt.Sprintf("leased:%x", sha256.Sum256([]byte("worker-session\x00tool-key")))
		internalLookup := fmt.Sprintf(`{"key":%q}`, internalKey)
		expect("Get", legacy, internalLookup, 403)
		expect("Fence", legacy, internalLookup, 403)
	}

	for _, decision := range []string{"Approve", "Decline"} {
		input := leasedCall("human-"+decision, "gated")
		if reason := expect("Call", token, input, 409)["reason"]; reason != "APPROVAL_REQUIRED" {
			t.Fatalf("expected approval prerequisite, got %v", reason)
		}
		pending := worklist(t, e, human)
		if len(pending) != 1 {
			t.Fatalf("want one approval, got %v", pending)
		}
		row := pending[0].(map[string]any)
		sign := fmt.Sprintf(`{"request_id":%q,"digest":%q,"tool":"gated"}`, row["request_id"], row["digest"])
		expect(decision, token, sign, 403)
		expect(decision, human, sign, 200)
		if decision == "Approve" {
			expect("Call", token, input, 200)
			expect("FenceAndLookup", token, leasedLookup("human-"+decision), 200)
		} else if reason := expect("Call", token, input, 409)["reason"]; reason != "APPROVAL_DECLINED" {
			t.Fatalf("expected refusal, got %v", reason)
		}
	}

	if reason := expect("Call", token, leasedCall("policy", "write"), 403)["reason"]; reason != "POLICY_DENIED" {
		t.Fatalf("expected policy refusal, got %v", reason)
	}
	e.allowDigest(t, "business", json.RawMessage(`{}`))
	expect("Call", token, leasedCall("business", "business"), 200)
	out := expect("FenceAndLookup", token, leasedLookup("business"), 200)
	receipt, _ := out["receipt"].(map[string]any)
	payload, _ := receipt["payload"].(map[string]any)
	if out["status"] != "COMPLETED" || receipt["status"] != "SUCCEEDED" || payload["isError"] != true {
		t.Fatalf("business error should be a known tool outcome: %v", out)
	}
	if n := atomic.LoadInt32(&e.fixture.effCount); n != 1 {
		t.Fatalf("expected only the business tool to reach the side-effecting MCP fixture, calls=%d", n)
	}
}

func newLeaseSigningKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}
