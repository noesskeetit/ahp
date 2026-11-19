package gateway_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// DEF-H10-8. A delegation hop that crosses deployments presents the delegating
// run's own task token, and the parent task it names lives in the delegating
// plane's store, not in this one. Auditing that parent against the local task
// table can therefore only ever answer "not found", which refused every
// cross-deployment child: the tree the platform could audit stopped at the
// deployment boundary, and the acceptance requirement (PDF rev3.2 section 6:
// the child of a running agent A is accepted by B under the same subject, with
// the remaining budget travelling with the task) could not be met at all.
//
// The leg below pins the intended behaviour: the parent is resolved through the
// verified task credential that is already presenting itself (tenant, subject,
// deployment, task id, root and depth are its own signed claims), the parent is
// recorded as attested-external, and the tree fields stay gateway-set.

// externalRunnerClaims is the credential the delegating runner presents at the
// accepting plane: a task token for its OWN task, minted under the same tenant
// and subject, in a different deployment.
func externalRunnerClaims(e *env, taskID, rootID string, depth int) auth.Claims {
	return auth.Claims{
		Kind:       auth.KindTask,
		Subject:    e.scope.Subject,
		Tenant:     e.scope.Tenant,
		Deployment: "dep-runner-a-" + e.sfx,
		Task:       taskID,
		Root:       rootID,
		Context:    "ctx-runner-a-" + e.sfx,
		Grant:      "grant-runner-a-" + e.sfx,
		Depth:      depth,
	}
}

func attestCtx(claims auth.Claims) context.Context {
	return transport.WithPrincipal(context.Background(), transport.Principal{
		Claims: claims,
		Scope:  claims.Scope(),
	})
}

func delegatedSend(msgID, parentID string, grant int) *a2a.SendMessageRequest {
	req := &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("continue the delegated turn")),
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
		Metadata: map[string]any{
			gateway.MetaParentTask: parentID,
			// The reserve travels with the task (PDF rev3.2 :975): the child is
			// bounded by what its parent could still spend, not by the accepting
			// deployment's default.
			"max_steps": grant,
		},
	}
	req.Message.ID = msgID
	return req
}

// TestDelegatedChildWithAttestedExternalParentIsAccepted is the permanent leg:
// a cross-deployment child must be accepted with its lineage intact.
func TestDelegatedChildWithAttestedExternalParentIsAccepted(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)

	const externalRoot = "task-runner-a-root"
	const externalParent = "task-runner-a-leaf"
	const attestedDepth = 2

	claims := externalRunnerClaims(e, externalParent, externalRoot, attestedDepth)
	ctx, cancel := context.WithTimeout(attestCtx(claims), 20*time.Second)
	defer cancel()

	task, err := svc.SendMessage(ctx, claims.Scope(), externalParent, delegatedSend("msg-deleg-child-"+e.sfx, externalParent, 4))
	if err != nil {
		t.Fatalf("cross-deployment delegation refused: %v", err)
	}

	// The child is filed under the scope the presenting credential carries --
	// that is how the platform already places a task accepted on a task
	// credential; reading it back uses the same scope.
	md := treeMetaIn(t, e, claims.Scope(), string(task.ID))
	if got, _ := md[gateway.MetaTreeRoot].(string); got != externalRoot {
		t.Fatalf("ahp.root = %v, want the attested root %q (lineage must survive the deployment hop)", md[gateway.MetaTreeRoot], externalRoot)
	}
	if depth, _ := md[gateway.MetaTreeDepth].(float64); depth != attestedDepth+1 {
		t.Fatalf("ahp.depth = %v, want %d", md[gateway.MetaTreeDepth], attestedDepth+1)
	}
	if got, _ := md[gateway.MetaParentTask].(string); got != externalParent {
		t.Fatalf("ahp.parent = %v, want the attested parent %q", md[gateway.MetaParentTask], externalParent)
	}
	// The audit must say that this parent was attested rather than read from a
	// local row: an operator has to be able to tell the two apart out of band.
	if got, _ := md["ahp.parent_attested"].(string); got != "external" {
		t.Fatalf("ahp.parent_attested = %v, want \"external\"", md["ahp.parent_attested"])
	}
	if got, _ := md["ahp.parent_attested_deployment"].(string); got != claims.Deployment {
		t.Fatalf("ahp.parent_attested_deployment = %v, want %q", md["ahp.parent_attested_deployment"], claims.Deployment)
	}
	// The child stays inside the reserve that travelled with it.
	if steps, _ := md["max_steps"].(float64); steps != 4 {
		t.Fatalf("max_steps = %v, want the travelled reserve 4", md["max_steps"])
	}
}

// TestLocalParentStillRequiresLocalAudit pins that the attestation path does not
// loosen the local case: a parent this plane is expected to hold is still
// audited against its own rows, and an unrelated presenting credential cannot
// re-root that chain or mark it attested.
func TestLocalParentStillRequiresLocalAudit(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)

	root := mustSubmit(t, svc, e.scope, e.caller, "msg-attest-root-"+e.sfx, nil)
	// The presenting credential names a task that is NOT the declared parent.
	claims := externalRunnerClaims(e, "task-unrelated-attestation", "task-unrelated-root", 5)
	ctx, cancel := context.WithTimeout(attestCtx(claims), 20*time.Second)
	defer cancel()
	child, err := svc.SendMessage(ctx, e.scope, e.caller, delegatedSend("msg-attest-child-"+e.sfx, string(root.ID), 3))
	if err != nil {
		t.Fatalf("local-parent send refused: %v", err)
	}
	md := treeMeta(t, e, string(child.ID))
	if got, _ := md[gateway.MetaTreeRoot].(string); got != string(root.ID) {
		t.Fatalf("ahp.root = %v, want the locally audited root %s, not an attested one", md[gateway.MetaTreeRoot], root.ID)
	}
	if depth, _ := md[gateway.MetaTreeDepth].(float64); depth != 1 {
		t.Fatalf("ahp.depth = %v, want the locally derived 1", md[gateway.MetaTreeDepth])
	}
	if v, ok := md["ahp.parent_attested"]; ok {
		t.Fatalf("a locally audited parent must not be marked attested: %v", v)
	}
}

// TestAttestedParentCannotCrossTenantOrSubject documents the boundary the fix
// must keep: attested fields come from the credential, and a credential whose
// tenant or subject differs from the accepting scope is not an attestation.
func TestAttestedParentCannotCrossTenantOrSubject(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)

	foreign := externalRunnerClaims(e, "task-foreign-parent", "task-foreign-root", 1)
	foreign.Tenant = "t-someone-else"
	ctx, cancel := context.WithTimeout(attestCtx(foreign), 20*time.Second)
	defer cancel()
	_, err := svc.SendMessage(ctx, e.scope, foreign.Task, delegatedSend("msg-foreign-"+e.sfx, foreign.Task, 2))
	if err == nil {
		t.Fatal("a foreign-tenant attestation was accepted")
	}
	if contracts.ReasonOf(err) != gateway.ReasonAttestationTenantMismatch {
		t.Fatalf("refusal does not name the tenant audit failure: %v", err)
	}
	var typed *contracts.Error
	if !errors.As(err, &typed) || typed.Code != contracts.ErrForbidden {
		t.Fatalf("foreign attestation error = %v, want a FORBIDDEN refusal", err)
	}
}

// treeMetaIn reads a task's metadata under an explicit scope.
func treeMetaIn(t *testing.T, e *env, scope contracts.Scope, taskID string) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	row, err := e.store.Task(ctx, scope, taskID)
	if err != nil {
		t.Fatalf("Task(%s): %v", taskID, err)
	}
	return row.Metadata
}

// ---------------------------------------------------------------------------
// The acceptance point verifies the credential itself.
//
// These legs mount the plane with its own Verifier (the same published-key
// scheme every face uses) and drive the hop over real JSON-RPC with a real
// bearer, so nothing rests on a harness-injected claim. Every refusal below must
// reach the client as the same content-free protocol answer it always was, while
// the operator journal names the reason the attestation failed.
// ---------------------------------------------------------------------------

type issuer struct {
	signer        *auth.Signer
	foreignSigner *auth.Signer
	verifier      *auth.Verifier
	audience      string
}

func newIssuer(t *testing.T) *issuer {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	_, foreignPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate foreign key: %v", err)
	}
	signer, err := auth.NewSigner("https://identity.test", "k1", priv)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	// A second key is published too: a token signed by it is well-formed and
	// still not verifiable by this issuer, which is what "forged" means here.
	foreignSigner, err := auth.NewSigner("https://identity.test", "k9", foreignPriv)
	if err != nil {
		t.Fatalf("NewSigner(foreign): %v", err)
	}
	return &issuer{
		signer:        signer,
		foreignSigner: foreignSigner,
		// Only this plane's own key is published. A token minted by another
		// authority has no key here at all, which is what a forged credential
		// looks like once published-key verification is the norm.
		verifier: auth.NewVerifier("https://identity.test", map[string]ed25519.PublicKey{"k1": pub}),
		audience: "ahp-test",
	}
}

func (i *issuer) token(t *testing.T, claims auth.Claims, ttl time.Duration) string {
	t.Helper()
	claims.Audience = i.audience
	tok, err := i.signer.Sign(claims, ttl)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return tok
}

// tokenByForeignIssuer is signed by a key this issuer also publishes but marks
// as belonging to another key id -- the signature itself is valid, so the only
// thing that can reject a token minted elsewhere is the binding check below.
func (i *issuer) tampered(t *testing.T, claims auth.Claims) string {
	t.Helper()
	tok := i.token(t, claims, 5*time.Minute)
	cut := strings.LastIndex(tok, ".")
	return tok[:cut] + ".AAAA-tampered-signature"
}

// byForeignAuthority signs with a key this plane does not publish: the token is
// internally well formed, correctly signed, and still not this plane's credential.
func (i *issuer) byForeignAuthority(t *testing.T, claims auth.Claims) string {
	t.Helper()
	claims.Audience = i.audience
	tok, err := i.foreignSigner.Sign(claims, 5*time.Minute)
	if err != nil {
		t.Fatalf("Sign(foreign): %v", err)
	}
	return tok
}

// attestFront mounts the plane with a verifier of its own and a permissive
// ingress: the acceptance decision must not need the ingress to be strict.
func attestFront(t *testing.T, svc *gateway.Service, frame contracts.Scope, journal *slog.Logger) *httptest.Server {
	t.Helper()
	srv, err := gateway.NewServer(
		gateway.NewHandler(svc, gateway.HandlerConfig{Logger: journal}),
		gateway.ServerConfig{
			Public: gateway.AuthConfig{Middleware: func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					next.ServeHTTP(w, r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{
						Claims: auth.Claims{Kind: auth.KindTask, Subject: frame.Subject, Tenant: frame.Tenant,
							Deployment: frame.Deployment, Task: "frame-holder"},
						Scope: frame,
					})))
				})
			}},
			Internal: gateway.AuthConfig{Middleware: func(http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					http.Error(w, "internal disabled", http.StatusForbidden)
				})
			}},
			PublicPath:   "/a2a/public",
			InternalPath: "/a2a/internal",
			HealthPath:   "/a2a/healthz",
		})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func rpcBearer(t *testing.T, ts *httptest.Server, params any, bearer string, id int) rpcReply {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": gateway.MethodSendMessage, "params": params})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/a2a/public", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out rpcReply
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("reply not JSON-RPC: %v (body=%s)", err, raw)
	}
	out.RawBody = string(raw)
	return out
}

// TestParentAttestationRefusalsAreNamed covers the forged and mismatched
// credentials: each is refused, each names its reason in the journal, and none
// of them changes the answer the caller sees.
func TestParentAttestationRefusalsAreNamed(t *testing.T) {
	iss := newIssuer(t)
	const externalRoot = "task-runner-a-root"
	const externalParent = "task-runner-a-leaf"

	cases := []struct {
		name       string
		frame      func(e *env, c auth.Claims) contracts.Scope
		bearer     func(t *testing.T, e *env, i *issuer) string
		wantReason string
	}{
		{
			name:  "verifiable task credential attests the parent",
			frame: func(e *env, c auth.Claims) contracts.Scope { return c.Scope() },
			bearer: func(t *testing.T, e *env, i *issuer) string {
				return i.token(t, taskClaims(e, externalParent, externalRoot, 2), 5*time.Minute)
			},
		},
		{
			name:       "no credential at all",
			frame:      func(e *env, c auth.Claims) contracts.Scope { return c.Scope() },
			bearer:     func(_ *testing.T, _ *env, _ *issuer) string { return "" },
			wantReason: gateway.ReasonAttestationMissing,
		},
		{
			name:  "tampered signature",
			frame: func(e *env, c auth.Claims) contracts.Scope { return c.Scope() },
			bearer: func(t *testing.T, e *env, i *issuer) string {
				return i.tampered(t, taskClaims(e, externalParent, externalRoot, 2))
			},
			wantReason: gateway.ReasonAttestationUnverifiable,
		},
		{
			name:  "credential signed by an unpublished key",
			frame: func(e *env, c auth.Claims) contracts.Scope { return c.Scope() },
			bearer: func(t *testing.T, e *env, i *issuer) string {
				return i.byForeignAuthority(t, taskClaims(e, externalParent, externalRoot, 2))
			},
			wantReason: gateway.ReasonAttestationUnverifiable,
		},
		{
			name:  "expired credential",
			frame: func(e *env, c auth.Claims) contracts.Scope { return c.Scope() },
			bearer: func(t *testing.T, e *env, i *issuer) string {
				tok := i.token(t, taskClaims(e, externalParent, externalRoot, 2), time.Second)
				time.Sleep(2 * time.Second)
				return tok
			},
			wantReason: gateway.ReasonAttestationUnverifiable,
		},
		{
			name:  "human credential",
			frame: func(e *env, c auth.Claims) contracts.Scope { return c.Scope() },
			bearer: func(t *testing.T, e *env, i *issuer) string {
				return i.token(t, auth.Claims{Kind: auth.KindHuman, Subject: e.scope.Subject,
					Tenant: e.scope.Tenant}, 5*time.Minute)
			},
			wantReason: gateway.ReasonAttestationUnverifiable,
		},
		{
			name:  "credential names another task than the declared parent",
			frame: func(e *env, c auth.Claims) contracts.Scope { return c.Scope() },
			bearer: func(t *testing.T, e *env, i *issuer) string {
				return i.token(t, taskClaims(e, "task-some-other-leaf", externalRoot, 2), 5*time.Minute)
			},
			wantReason: gateway.ReasonAttestationTaskMismatch,
		},
		{
			// A credential of another tenant never reaches the attestation fields:
			// the workload binding of the verified scope rejects it first. The
			// tenant check inside attestParent is therefore defence in depth and is
			// exercised directly on the fallback path, where no verifier is mounted.
			name:  "credential from another tenant",
			frame: func(e *env, c auth.Claims) contracts.Scope { return c.Scope() },
			bearer: func(t *testing.T, e *env, i *issuer) string {
				other := taskClaims(e, externalParent, externalRoot, 2)
				other.Tenant = "t-someone-else"
				return i.token(t, other, 5*time.Minute)
			},
			wantReason: gateway.ReasonAttestationUnverifiable,
		},
		{
			// The shape the run plane produces when it pairs a frame scope with a
			// separately obtained token (PDF :975 keeps attribution with the
			// original subject): a credential whose subject is not the one the
			// accepting scope claims cannot attest a parent.
			name: "credential from another subject",
			frame: func(e *env, c auth.Claims) contracts.Scope {
				sc := c.Scope()
				sc.Subject = "subj-intruder"
				return sc
			},
			bearer: func(t *testing.T, e *env, i *issuer) string {
				return i.token(t, taskClaims(e, externalParent, externalRoot, 2), 5*time.Minute)
			},
			wantReason: gateway.ReasonAttestationSubjectMismatch,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			local := newEnv(t)
			// One journal for the whole plane: the acceptance decision inside the
			// service and the protocol answer at the face must be readable
			// together, which is exactly how they land in the process journal.
			logs := &syncBuffer{}
			journal := slog.New(slog.NewJSONHandler(logs, nil))
			svc := local.service(t, echoExecutor(), func(cfg *gateway.Config) {
				cfg.TaskVerifier = iss.verifier
				cfg.TaskAudience = iss.audience
				cfg.Logger = journal
			})
			claims := taskClaims(local, externalParent, externalRoot, 2)
			ts := attestFront(t, svc, tc.frame(local, claims), journal)
			sent := rpcBearer(t, ts, delegatedSend("msg-hopp-"+local.sfx, externalParent, 4), tc.bearer(t, local, iss), 1)

			if tc.wantReason == "" {
				if sent.Error != nil {
					t.Fatalf("attested delegation refused: %+v (journal: %s)", sent.Error, logs.String())
				}
				return
			}
			if sent.Error == nil {
				t.Fatalf("forged attestation was accepted (journal: %s)", logs.String())
			}
			// Invalid authority is refused before durable grant binding; a valid
			// authority that misstates its parent still fails the tree audit.
			wantCode, wantMessage := -31403, "gateway: operation not permitted for this principal"
			if tc.wantReason == gateway.ReasonAttestationTaskMismatch {
				wantCode, wantMessage = -32602, "gateway: request rejected"
			}
			if sent.Error.Code != wantCode || sent.Error.Message != wantMessage {
				t.Fatalf("wrong content-free refusal: %+v", sent.Error)
			}
			for _, leak := range []string{"Bearer", "ey", tc.wantReason, "attest"} {
				if strings.Contains(sent.RawBody, leak) {
					t.Fatalf("client answer leaked %q: %s", leak, sent.RawBody)
				}
			}
			records := refusalRecords(t, logs)
			hits := findByReason(records, tc.wantReason)
			if len(hits) != 1 {
				t.Fatalf("journal does not name %q: %v (raw: %s)", tc.wantReason, reasonsOf(records), logs.String())
			}
			rec := hits[0]
			if tc.wantReason == gateway.ReasonAttestationTaskMismatch && rec["task_id"] != externalParent {
				t.Fatalf("refusal does not name the parent it refused: %v", rec)
			}
			for _, banned := range []string{"ey.", "Authorization", "Bearer"} {
				if strings.Contains(logs.String(), banned) {
					t.Fatalf("journal carries a credential fragment %q:\n%s", banned, logs.String())
				}
			}
		})
	}
}

// TestAttestedChildWithoutTravellingReserveIsRefused pins the budget rule: a
// child the accepting plane cannot audit locally is bounded only by the reserve
// that travels with it, so a hop that carries no reserve is refused rather than
// run under this deployment's default.
func TestAttestedChildWithoutTravellingReserveIsRefused(t *testing.T) {
	e := newEnv(t)
	iss := newIssuer(t)
	logs := &syncBuffer{}
	journal := slog.New(slog.NewJSONHandler(logs, nil))
	svc := e.service(t, echoExecutor(), func(cfg *gateway.Config) {
		cfg.TaskVerifier = iss.verifier
		cfg.TaskAudience = iss.audience
		cfg.Logger = journal
	})
	claims := taskClaims(e, "task-runner-a-leaf", "task-runner-a-root", 2)
	ts := attestFront(t, svc, claims.Scope(), journal)

	req := delegatedSend("msg-no-reserve-"+e.sfx, "task-runner-a-leaf", 4)
	delete(req.Metadata, "max_steps")
	sent := rpcBearer(t, ts, req, iss.token(t, claims, 5*time.Minute), 1)
	if sent.Error == nil {
		t.Fatalf("a delegated child without a travelling reserve was accepted:\n%s", logs.String())
	}
	if sent.Error.Code != -32602 || sent.Error.Message != "gateway: request rejected" {
		t.Fatalf("client answer changed: %+v", sent.Error)
	}
	records := refusalRecords(t, logs)
	if len(findByReason(records, gateway.ReasonAttestationGrantMissing)) != 1 {
		t.Fatalf("journal does not name the missing reserve: %v", reasonsOf(records))
	}
}

// taskClaims mints the credential a delegating runner presents at B: its own
// task, under the same tenant and subject, in its own deployment.
func taskClaims(e *env, taskID, rootID string, depth int) auth.Claims {
	return externalRunnerClaims(e, taskID, rootID, depth)
}

// TestAttestedParentCannotCrossSubject is the direct form of the subject binding:
// with no verifier mounted the acceptance point reads the principal its ingress
// verified, and a principal whose subject differs from the accepting scope is not
// an attestation for this subject's tree.
func TestAttestedParentCannotCrossSubject(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)

	other := externalRunnerClaims(e, "task-foreign-parent", "task-foreign-root", 1)
	other.Subject = "subj-someone-else"
	ctx, cancel := context.WithTimeout(attestCtx(other), 20*time.Second)
	defer cancel()
	_, err := svc.SendMessage(ctx, e.scope, e.caller, delegatedSend("msg-foreign-subj-"+e.sfx, other.Task, 2))
	if err == nil {
		t.Fatal("a foreign-subject attestation was accepted")
	}
	var typed *contracts.Error
	if !errors.As(err, &typed) || typed.Code != contracts.ErrForbidden || contracts.ReasonOf(err) != gateway.ReasonAttestationSubjectMismatch {
		t.Fatalf("foreign-subject attestation error = %v, want a named FORBIDDEN refusal", err)
	}
}
