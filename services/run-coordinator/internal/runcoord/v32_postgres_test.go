package runcoord

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/pkg/transport"
	stateclient "github.com/ahp-platform/ahp/sdk/go/state/journal"
)

type coordinatorTLS struct {
	issuer *x509.Certificate
	key    ed25519.PrivateKey
	pool   *x509.CertPool
	serial int64
}

func makeCoordinatorTLS(t *testing.T) *coordinatorTLS {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	issuer := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	raw, err := x509.CreateCertificate(nil, issuer, issuer, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(raw)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(root)
	return &coordinatorTLS{issuer: root, key: key, pool: pool, serial: 1}
}

func (p *coordinatorTLS) server(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: p.pool, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func (p *coordinatorTLS) client(t *testing.T, server *httptest.Server, scope contracts.Scope) *http.Client {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	workload, err := url.Parse(auth.WorkloadID(scope))
	if err != nil {
		t.Fatal(err)
	}
	p.serial++
	cert := &x509.Certificate{SerialNumber: big.NewInt(p.serial), URIs: []*url.URL{workload},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	raw, err := x509.CreateCertificate(nil, cert, p.issuer, pub, p.key)
	if err != nil {
		t.Fatal(err)
	}
	clientTransport := server.Client().Transport.(*http.Transport).Clone()
	clientTransport.TLSClientConfig.Certificates = []tls.Certificate{{Certificate: [][]byte{raw}, PrivateKey: key}}
	t.Cleanup(clientTransport.CloseIdleConnections)
	return &http.Client{Transport: clientTransport, Timeout: 10 * time.Second}
}

func coordinatorStateAPI(t *testing.T, store *statejournal.Store, pki *coordinatorTLS, verifier *auth.Verifier, coordinator contracts.Scope) *httptest.Server {
	t.Helper()
	handler, err := statejournal.NewHTTPWithCoordinator(store, verifier, func(_ context.Context, claims auth.Claims, capability string) error {
		if !slices.Contains(claims.Capabilities, capability) {
			return contracts.NewError(contracts.ErrForbidden, "state capability denied")
		}
		return nil
	}, nil, []string{auth.WorkloadID(coordinator)})
	if err != nil {
		t.Fatal(err)
	}
	return pki.server(t, handler)
}

func coordinatorStateClient(t *testing.T, server *httptest.Server, pki *coordinatorTLS, signer *auth.Signer, scope contracts.Scope) *stateclient.Client {
	t.Helper()
	claims := taskCaller(scope, "state")
	claims.Capabilities = append(claims.Capabilities, "state.coordinate")
	token := signedCaller(t, signer, claims)
	sdk, err := stateclient.New(stateclient.Config{BaseURL: server.URL, HTTPClient: pki.client(t, server, scope),
		TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return sdk
}

func coordinatorTLSRequest(t *testing.T, ctx context.Context, client *http.Client, method, address, token, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, address, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	bytes, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, string(bytes)
}

func TestScopedSessionIDAllowsSameConversationNameAcrossUsersAndAgents(t *testing.T) {
	ctx, _, db := openPG(t)
	signer, verifier := coordinatorAuth(t)
	pki := makeCoordinatorTLS(t)
	service := contracts.Scope{Tenant: "platform", Deployment: "run-coordinator", Subject: "service"}
	edge := contracts.Scope{Tenant: db.scope.Tenant, Deployment: "user-edge", Subject: "service"}
	stateServer := coordinatorStateAPI(t, db.store, pki, verifier, service)
	global := coordinatorStateClient(t, stateServer, pki, signer, service).Coordinator()
	agentAccess, err := ReadAgentAccess(strings.NewReader(fmt.Sprintf(`{"agents":[{"tenant":%q,"deployment":"agent-1","subjects":["alice","bob"]},{"tenant":%q,"deployment":"agent-2","subjects":["alice","bob"]}]}`, edge.Tenant, edge.Tenant)))
	if err != nil {
		t.Fatal(err)
	}
	c := Coordinator{GlobalState: global, StateFor: func(s contracts.Scope) State { return global.ForScope(s) },
		AccessPolicy: AccessPolicy{Verifier: verifier, UserAudience: "ahp-browser", UserEdgeDeployments: []string{edge.Deployment},
			AuthorizeAgent: agentAccess}}
	server := pki.server(t, c.Handler())
	client := pki.client(t, server, edge)

	const conversation = "chat-1"
	ids := make(map[string]bool)
	type dialog struct {
		path, token, marker string
	}
	var dialogs []dialog
	for _, agent := range []string{"agent-1", "agent-2"} {
		for _, subject := range []string{"alice", "bob"} {
			target := contracts.Scope{Tenant: edge.Tenant, Deployment: agent, Subject: subject}
			id := journal.ScopedSessionID(target, conversation)
			if ids[id] {
				t.Fatalf("duplicate internal session %q", id)
			}
			ids[id] = true
			dialogs = append(dialogs, dialog{
				path:  server.URL + "/v1/agents/" + agent + "/sessions/" + conversation,
				token: signedCaller(t, signer, humanCaller(edge.Tenant, subject)), marker: subject + "-" + agent,
			})
		}
	}
	t.Run("simultaneous admissions", func(t *testing.T) {
		for _, d := range dialogs {
			t.Run(d.marker, func(t *testing.T) {
				t.Parallel()
				body, err := json.Marshal(journal.EnqueueRequest{SessionID: "caller-supplied-id", Harness: "go", FormatVersion: 1, Key: "same-key",
					Input: journal.Payload{Inline: []byte(fmt.Sprintf(`{"marker":%q}`, d.marker))}})
				if err != nil {
					t.Fatal(err)
				}
				code, response := coordinatorTLSRequest(t, ctx, client, http.MethodPost, d.path+":send", d.token, string(body))
				if code != http.StatusAccepted || !strings.Contains(response, `"conversation_id":"chat-1"`) ||
					!strings.Contains(response, `"session_id":"chat-1"`) || strings.Contains(response, "vms-") {
					t.Fatalf("send %s: HTTP %d: %s", d.marker, code, response)
				}
			})
		}
	})
	for i, d := range dialogs {
		code, response := coordinatorTLSRequest(t, ctx, client, http.MethodPost, d.path+":cancel", d.token, `{"turn_seq":1}`)
		if code != http.StatusAccepted || !strings.Contains(response, `"conversation_id":"chat-1"`) ||
			!strings.Contains(response, `"status":"INTERRUPTED"`) {
			t.Fatalf("cancel %s: HTTP %d: %s", d.marker, code, response)
		}
		for j, other := range dialogs {
			wantStatus := "QUEUED"
			if j <= i {
				wantStatus = "INTERRUPTED"
			}
			code, response := coordinatorTLSRequest(t, ctx, client, http.MethodGet, other.path, other.token, "")
			if code != http.StatusOK || !strings.Contains(response, `"conversation_id":"chat-1"`) ||
				!strings.Contains(response, `"status":"`+wantStatus+`"`) || strings.Contains(response, "vms-") {
				t.Fatalf("status %s after cancel %s: HTTP %d: %s", other.marker, d.marker, code, response)
			}
		}
		code, response = coordinatorTLSRequest(t, ctx, client, http.MethodGet, d.path+"/stream?turn_seq=1", d.token, "")
		if code != http.StatusOK || !strings.Contains(response, "event: end") {
			t.Fatalf("stream %s: HTTP %d: %s", d.marker, code, response)
		}
		var inputCount int
		for _, line := range strings.Split(response, "\n") {
			var event journal.Event
			if strings.HasPrefix(line, "data: ") && json.Unmarshal([]byte(line[6:]), &event) == nil && event.Type == "turn.input" {
				inputCount++
				if !strings.Contains(string(event.Payload.Inline), d.marker) {
					t.Fatalf("stream %s disclosed another session: %s", d.marker, line)
				}
			}
		}
		if inputCount != 1 {
			t.Fatalf("stream %s had %d input events: %s", d.marker, inputCount, response)
		}
	}
}

func TestAgentAccessPolicyAndDistinctGlobalCoordinatorRoutes(t *testing.T) {
	ctx, _, db := openPG(t)
	signer, verifier := coordinatorAuth(t)
	pki := makeCoordinatorTLS(t)
	service := contracts.Scope{Tenant: "platform", Deployment: "run-coordinator", Subject: "service"}
	broker := contracts.Scope{Tenant: db.scope.Tenant, Deployment: "broker", Subject: "alice"}
	edge := contracts.Scope{Tenant: broker.Tenant, Deployment: "user-edge", Subject: "service"}
	stateServer := coordinatorStateAPI(t, db.store, pki, verifier, service)
	scoped := coordinatorStateClient(t, stateServer, pki, signer, broker)
	if _, err := scoped.ListRecoverableVMPage(ctx, "QUEUED", nil); err != nil {
		t.Fatalf("scoped admission: %v", err)
	}
	for _, operation := range []func() error{
		func() error {
			_, err := scoped.Coordinator().ListGlobalRecoverableVMPage(ctx, "QUEUED", nil)
			return err
		},
		func() error {
			_, err := scoped.Coordinator().ForScope(broker).GetTurnStatus(ctx, "missing")
			return err
		},
	} {
		if err := operation(); contracts.Code(err) != contracts.ErrForbidden {
			t.Fatalf("global State allowed non-coordinator: %v", err)
		}
	}

	global := coordinatorStateClient(t, stateServer, pki, signer, service).Coordinator()
	c := Coordinator{GlobalState: global, StateFor: func(contracts.Scope) State {
		t.Error("denied request attempted to make a State client")
		return nil
	}, AccessPolicy: AccessPolicy{
		Verifier: verifier, UserAudience: "ahp-browser", UserEdgeDeployments: []string{edge.Deployment},
		BrokerWorkloadIDs: []string{auth.WorkloadID(broker)},
		AuthorizeAgent: func(context.Context, CallerPrincipal, contracts.Scope) error {
			return contracts.NewError(contracts.ErrForbidden, "agent access denied")
		},
	}}
	server := pki.server(t, c.Handler())
	for _, tc := range []struct {
		peer   contracts.Scope
		claims auth.Claims
	}{
		{edge, humanCaller(edge.Tenant, "alice")},
		{broker, taskCaller(broker, "run.coordinate")},
		{broker, humanCaller(edge.Tenant, "alice")},
	} {
		token := signedCaller(t, signer, tc.claims)
		code, body := coordinatorTLSRequest(t, ctx, pki.client(t, server, tc.peer), http.MethodGet,
			server.URL+"/v1/agents/agent-1/sessions/chat-1", token, "")
		if code != http.StatusForbidden {
			t.Fatalf("peer %+v got HTTP %d: %s", tc.peer, code, body)
		}
	}
}

func TestGlobalReconcileSweepsAcrossTenantsAndDeployments(t *testing.T) {
	ctx, pool, db := openPG(t)
	signer, verifier := coordinatorAuth(t)
	pki := makeCoordinatorTLS(t)
	service := contracts.Scope{Tenant: "platform", Deployment: "run-coordinator", Subject: "service"}
	stateServer := coordinatorStateAPI(t, db.store, pki, verifier, service)
	global := coordinatorStateClient(t, stateServer, pki, signer, service).Coordinator()

	type lifecycle struct {
		ensure Execution
		fence  *Execution
	}
	type dispatch struct {
		route     string
		execution Execution
	}
	var (
		mu       sync.Mutex
		actual   = make(map[string][]dispatch)
		expected = make(map[string]lifecycle)
	)
	runtimeAuthorize := func(_ context.Context, claims auth.Claims, capability string) error {
		if !slices.Contains(claims.Capabilities, capability) || auth.WorkloadID(claims.Scope()) != auth.WorkloadID(service) {
			return contracts.NewError(contracts.ErrForbidden, "runtime dispatch denied")
		}
		return nil
	}
	runtimeHandler := transport.Task(verifier, "ahp", "runtime.dispatch", runtimeAuthorize)
	runtimeServer := pki.server(t, runtimeHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var wire struct {
			Scope         contracts.Scope `json:"scope"`
			SessionID     string          `json:"session_id"`
			Harness       string          `json:"harness_type"`
			LaunchTurnSeq *int64          `json:"launch_turn_seq"`
			Generation    int64           `json:"generation"`
			ExecutionID   string          `json:"execution_id"`
		}
		if err := transport.ReadJSON(w, r, &wire, transport.DefaultMaxBodyBytes); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		e := Execution{Scope: wire.Scope, SessionID: wire.SessionID, Harness: wire.Harness, Generation: wire.Generation,
			ExecutionID: wire.ExecutionID}
		switch r.URL.Path {
		case "/internal/v1/executions:ensure":
			if wire.LaunchTurnSeq == nil {
				t.Error("ensure omitted launch_turn_seq")
			} else {
				e.LaunchTurnSeq = *wire.LaunchTurnSeq
			}
		case "/internal/v1/executions:fence":
			if wire.LaunchTurnSeq != nil {
				t.Error("fence specified a launch turn")
			}
		}
		mu.Lock()
		actual[e.SessionID] = append(actual[e.SessionID], dispatch{route: r.URL.Path, execution: e})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})))

	var tenants []string
	for i := 0; i < 5; i++ {
		tenants = append(tenants, fmt.Sprintf("%s-%d", db.scope.Tenant, i))
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := pool.Exec(cleanupCtx, "DELETE FROM ahp_journal.sessions WHERE tenant_id=ANY($1)", tenants); err != nil {
			t.Errorf("delete recovery fixtures: %v", err)
		}
	})
	const sessionsPerStatus = 70
	for i := range sessionsPerStatus {
		scope := contracts.Scope{Tenant: tenants[i%len(tenants)], Deployment: fmt.Sprintf("agent-%d", i%10), Subject: fmt.Sprintf("user-%d", i%20)}
		for _, active := range []bool{false, true} {
			id := journal.ScopedSessionID(scope, fmt.Sprintf("reconcile-%t-%d", active, i))
			input := journal.EnqueueRequest{SessionID: id, Harness: "go", FormatVersion: 1, Key: "input",
				ExecutionMode: journal.ExecutionVMManaged, Input: journal.Payload{Inline: []byte(`{}`)}}
			admission, err := global.ForScope(scope).EnqueueTurn(ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			turnSeq := admission.HeadSeq
			generation := int64(1)
			var fence *Execution
			if active {
				owner := journal.DeterministicExecutionID(scope, id, turnSeq, generation)
				turn, err := db.store.ClaimVMTurn(ctx, scope, id, journal.VMTurnClaimRequest{
					TurnSeq: turnSeq, Generation: generation, ExecutionID: owner,
				}, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				fence = &Execution{Scope: scope, SessionID: id, Harness: "go", Generation: generation, ExecutionID: owner}
				generation++
				if i == sessionsPerStatus-1 {
					input.Key, input.DeliveryMode = "followup", journal.DeliveryFollowup
					followup, err := global.ForScope(scope).EnqueueTurn(ctx, input)
					if err != nil {
						t.Fatal(err)
					}
					finished, err := db.store.AppendEvents(ctx, scope, id, turn.Lease, false, journal.AppendRequest{
						TurnSeq: turn.InputSeq, ExpectedSeq: followup.HeadSeq, Key: "finish-first-turn", FinishTurn: "IDLE",
					})
					if err != nil || finished.Control == nil || finished.Control.NextTurn == nil ||
						finished.Control.NextTurn.InputSeq <= turnSeq || finished.Control.NextTurn.Worker != owner {
						t.Fatalf("warm follow-up was not promoted using the physical owner: %+v, %v", finished, err)
					}
					turnSeq = finished.Control.NextTurn.InputSeq
				}
			}
			expected[id] = lifecycle{fence: fence, ensure: Execution{
				Scope: scope, SessionID: id, Harness: "go", LaunchTurnSeq: turnSeq, Generation: generation,
				ExecutionID: journal.DeterministicExecutionID(scope, id, turnSeq, generation),
			}}
		}
	}
	// Give each queue one timestamp so a page boundary depends on its tenant
	// and session tie breakers.
	if _, err := pool.Exec(ctx, `UPDATE ahp_journal.sessions
		SET queued_at=now()-interval '3 seconds',
		    lease_expires_at=CASE WHEN status='ACTIVE' THEN now()-interval '1 minute' END
		WHERE tenant_id=ANY($1)`, tenants); err != nil {
		t.Fatal(err)
	}
	token := signedCaller(t, signer, taskCaller(service, "runtime.dispatch"))
	dispatcher := HTTPDispatcher{BaseURL: runtimeServer.URL, Client: pki.client(t, runtimeServer, service),
		TokenSource: func(context.Context) (string, error) { return token, nil }}
	c := Coordinator{GlobalState: global, StateFor: func(s contracts.Scope) State { return global.ForScope(s) }, Dispatcher: dispatcher}
	if err := c.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(actual) != len(expected) {
		t.Fatalf("got %d distinct sessions, want %d", len(actual), len(expected))
	}
	for id, e := range expected {
		want := []dispatch{{route: "/internal/v1/executions:ensure", execution: e.ensure}}
		if e.fence != nil {
			want = append([]dispatch{{route: "/internal/v1/executions:fence", execution: *e.fence}}, want...)
		}
		if !slices.Equal(actual[id], want) {
			t.Errorf("%s: dispatches %+v, want %+v", id, actual[id], want)
		}
	}
}
