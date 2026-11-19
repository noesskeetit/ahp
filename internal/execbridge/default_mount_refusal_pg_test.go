package execbridge

// DEF-DOTE2E-2: the DEFAULT composition must be able to serve a model turn.
//
// The execution role mounts the conversation label journal unconditionally
// (cmd/ahp/role_execution.go), while the provider table is optional
// (AHP_MODEL_PROVIDERS_FILE). The two facts were combined as a hard failure:
// a mounted journal plus an absent provider table returned a bare, unclassified
// error, which transport.WriteError renders as a bare 500 with no reason and no
// journal line -- so every SubmitModel on a Tier-1 human task died inside the
// gateway, before any generation row existed, and the stand said nothing.
//
// These legs pin the three facts acceptance needs:
//   (a) no provider table = one client, no provider claim, turn SERVED; the
//       journal is not consulted, because there is no routing decision to inform;
//   (b) a routing refusal names its reason to the operator and stays a
//       structured contract error, never a bare 500;
//   (c) neither the client answer nor the journal carries request content
//       (PDF :347/:277).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const doteSentinel = "SENTINEL-prompt-must-never-be-journalled"

// captureJournal turns the process journal into a buffer for one leg.
func captureJournal(t *testing.T) *strings.Builder {
	t.Helper()
	var buf strings.Builder
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

// stubTaskJournal is the production adapter's shape: a label reader that may
// also be the grant authority (identityContextLabels is both).
type stubTaskJournal struct {
	set        labels.Set
	labelErr   error
	grantErr   error
	labelCalls int
	authCalls  int
}

func (s *stubTaskJournal) LabelsForContext(context.Context, contracts.Scope, string) (labels.Set, error) {
	s.labelCalls++
	if s.labelErr != nil {
		return labels.Set{}, s.labelErr
	}
	return s.set, nil
}

func (s *stubTaskJournal) AuthorizeModelTask(context.Context, contracts.Scope, string) error {
	s.authCalls++
	return s.grantErr
}

// httpModelCall drives the same face a runtime uses: POST /v1/model/SubmitModel
// under verified task+context claims.
func httpModelCall(t *testing.T, b *Bridge, scope contracts.Scope, task, body string) *httptest.ResponseRecorder {
	t.Helper()
	return httpModelCallCtx(t, b, context.Background(), scope, task, body)
}

func httpModelCallCtx(t *testing.T, b *Bridge, ctx context.Context, scope contracts.Scope, task, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/model/SubmitModel", bytes.NewBufferString(body))
	claims := auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment,
		Subject: scope.Subject, Task: task, Context: "ctx-" + task}
	r = r.WithContext(transport.WithPrincipal(ctx, transport.Principal{Scope: scope, Claims: claims}))
	w := httptest.NewRecorder()
	b.serveModel(w, r)
	return w
}

func submitBody(key string) string {
	return `{"key":"` + key + `","messages":[{"role":"user","content":"` + doteSentinel + `"}]}`
}

// Leg A (the P0): journal mounted, NO provider table -- the turn must be served
// by the one configured client, with no provider claim and no journal read.
func TestDefaultMountWithJournalButNoProviderTableServesTheTurn(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "tier-one answer", usage: modelUsage()}
	journal := &stubTaskJournal{}
	// Capture from before the mount: the mount itself is one of the assertions.
	logs := captureJournal(t)

	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 20000, StepReserve: 4000})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	b.WithTaskLabels(journal)
	if b.RoutingMounted() {
		t.Fatal("this leg is about an UNconfigured provider table")
	}

	w := httpModelCall(t, b, scope, "dtask-"+randHex(4), submitBody("answer-1"))
	if w.Code != 200 {
		t.Fatalf("SubmitModel on the default mount got %d body=%s -- the Tier-1 human turn is bricked: %s",
			w.Code, w.Body.String(), strings.TrimSpace(logs.String()))
	}
	var gen model.Generation
	if err := json.Unmarshal(w.Body.Bytes(), &gen); err != nil {
		t.Fatalf("decode generation: %v body=%s", err, w.Body.String())
	}
	if gen.Status != model.GenCompleted || gen.Output != "tier-one answer" {
		t.Fatalf("unexpected generation %+v", gen)
	}
	// No routing table means no provider may be claimed on the durable row.
	if gen.Provider != "" {
		t.Fatalf("an unconfigured table invented a provider attribution: %q", gen.Provider)
	}
	if inf.count() != 1 {
		t.Fatalf("provider calls=%d, want exactly 1", inf.count())
	}
	// The journal informs a routing decision that does not exist here; reading it
	// would make an unavailable journal brick turns that have no routing at all.
	if journal.labelCalls != 0 {
		t.Fatalf("label journal consulted %d times with no routing table mounted", journal.labelCalls)
	}
	// The authorization half of the adapter is NOT part of that exemption.
	if journal.authCalls != 1 {
		t.Fatalf("grant authority ran %d times, want 1: routing may be off, authorization is not", journal.authCalls)
	}
	// An operator must be able to see why the journal is inert here.
	if !strings.Contains(logs.String(), "not consulted") {
		t.Fatalf("mount must state that the journal will not be consulted, got: %s", strings.TrimSpace(logs.String()))
	}
}

// Leg B: with no provider table the GRANT authority still decides. A refused
// grant is a structured 403, books nothing and never reaches the provider.
func TestGrantAuthorityStillRefusesWithoutProviderTable(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "must never be produced", usage: modelUsage()}
	journal := &stubTaskJournal{grantErr: contracts.NewError(contracts.ErrForbidden, "identity: grant is revoked")}

	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 20000, StepReserve: 4000})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	b.WithTaskLabels(journal)

	w := httpModelCall(t, b, scope, "dtask-"+randHex(4), submitBody("answer-1"))
	if w.Code != http.StatusForbidden {
		t.Fatalf("a revoked grant got %d body=%s", w.Code, w.Body.String())
	}
	if inf.count() != 0 {
		t.Fatalf("provider was called %d times after a grant refusal", inf.count())
	}
	assertNothingBooked(t, pool, scope.Tenant)
}

// Leg C: production routing mounted but the conversation has no journal row.
// The refusal stays fail-closed, yet it must arrive as a named contract reason
// and not as the bare 500 that hid this defect class for two measurement legs.
func TestAbsentTaskLabelJournalRefusesWithAReasonNotA500(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "must never be produced", usage: modelUsage()}
	journal := &stubTaskJournal{labelErr: ErrTaskLabelJournalAbsent}

	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 20000, StepReserve: 4000,
		RoutingFile: writeProviderTable(t, tableOverrideModel)})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	b.WithTaskLabels(journal)
	if !b.RoutingMounted() {
		t.Fatal("this leg requires production routing mounted")
	}
	logs := captureJournal(t)

	w := httpModelCall(t, b, scope, "dtask-"+randHex(4), submitBody("answer-1"))
	if w.Code == http.StatusInternalServerError {
		t.Fatalf("journal refusal is still an unclassified 500: %s", w.Body.String())
	}
	if w.Code != http.StatusConflict {
		t.Fatalf("journal refusal got %d body=%s, want 409 CONFLICT with a reason", w.Code, w.Body.String())
	}
	if got := contracts.ReasonOf(decodeContractError(t, w.Body.Bytes())); got != ReasonTaskLabelJournalAbsent {
		t.Fatalf("refusal reason=%q, want %q body=%s", got, ReasonTaskLabelJournalAbsent, w.Body.String())
	}
	if inf.count() != 0 {
		t.Fatalf("provider was called %d times after a journal refusal", inf.count())
	}
	assertNothingBooked(t, pool, scope.Tenant)

	// The operator learns the reason; the caller's content stays out of the log.
	text := logs.String()
	if !strings.Contains(text, ReasonTaskLabelJournalAbsent) {
		t.Fatalf("journal line must name %q, got: %s", ReasonTaskLabelJournalAbsent, strings.TrimSpace(text))
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, "generation refused before admission") {
			t.Logf("refusal journal line: %s", line)
		}
	}
	if strings.Contains(text, doteSentinel) {
		t.Fatal("the refusal journalled request content")
	}
	if strings.Contains(w.Body.String(), doteSentinel) {
		t.Fatal("the refusal answer echoed request content")
	}
}

// Leg D: the untouched legacy mount (no journal, no table) keeps working, so the
// fix above is a repair of the bricked combination, not a new behaviour.
func TestLegacyMountWithoutJournalOrTableIsUnchanged(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "legacy answer", usage: modelUsage()}

	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 20000, StepReserve: 4000})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	w := httpModelCall(t, b, scope, "dtask-"+randHex(4), submitBody("answer-1"))
	if w.Code != 200 {
		t.Fatalf("legacy mount regressed: %d body=%s", w.Code, w.Body.String())
	}
	if inf.count() != 1 {
		t.Fatalf("provider calls=%d, want 1", inf.count())
	}
}

// Leg E: an unreadable journal (a dependency failure, not an absent row) is a
// named 503 with the reason journalled content-free. Fail-closed stays closed.
func TestUnreadableTaskLabelJournalIsANamed503(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "must never be produced", usage: modelUsage()}
	journal := &stubTaskJournal{labelErr: errors.New("identity: connection reset by peer")}

	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 20000, StepReserve: 4000,
		RoutingFile: writeProviderTable(t, tableOverrideModel)})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	b.WithTaskLabels(journal)
	logs := captureJournal(t)

	w := httpModelCall(t, b, scope, "dtask-"+randHex(4), submitBody("answer-1"))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("unreadable journal got %d body=%s, want 503", w.Code, w.Body.String())
	}
	if got := contracts.ReasonOf(decodeContractError(t, w.Body.Bytes())); got != ReasonTaskLabelJournalUnreadable {
		t.Fatalf("refusal reason=%q, want %q", got, ReasonTaskLabelJournalUnreadable)
	}
	if inf.count() != 0 {
		t.Fatalf("provider was called %d times after a journal failure", inf.count())
	}
	assertNothingBooked(t, pool, scope.Tenant)
	if strings.Contains(logs.String(), doteSentinel) {
		t.Fatal("the refusal journalled request content")
	}
	// The dependency's own wording must not reach the caller verbatim.
	if strings.Contains(w.Body.String(), "connection reset") {
		t.Fatalf("internal dependency detail leaked to the caller: %s", w.Body.String())
	}
}

func decodeContractError(t *testing.T, body []byte) error {
	t.Helper()
	var e struct {
		Code   string `json:"code"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("decode error body: %v raw=%s", err, string(body))
	}
	if e.Code == "" {
		t.Fatalf("error body carries no code: %s", string(body))
	}
	if e.Reason != "" {
		return contracts.NewReasoned(e.Code, e.Reason, e.Code)
	}
	return contracts.NewError(e.Code, e.Code)
}

// Leg F: the Tier-1 human turn itself, not just the HTTP face. A bridged turn on
// the default mount -- journal mounted, no provider table, verified grant
// binding -- must reach the model and complete. This is the leg Dot's
// "TurnRunsPinnedConfigOnRealModel" acceptance needs: the configuration that
// ships by default cannot end a personal task before the model is ever asked.
func TestDefaultMountCompletesABridgedTurnWithVerifiedGrant(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "tier-one turn", usage: modelUsage()}
	journal := &stubTaskJournal{}
	logs := captureJournal(t)

	b, err := New(ctx, pool, Config{Inference: inf, MaxSteps: 2, TaskTokenLimit: 20000, StepReserve: 4000})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	b.WithTaskLabels(journal)
	if b.RoutingMounted() {
		t.Fatal("this leg is about the default mount: no provider table")
	}

	taskID := "dtask-" + randHex(4)
	res, err := b.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task: gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID, Metadata: map[string]any{
			gateway.MetaGrantContext:    "ctx-" + taskID,
			gateway.MetaGrantTask:       taskID,
			gateway.MetaGrantDeployment: scope.Deployment,
		}},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hello")),
	})
	if err != nil {
		t.Fatalf("the default mount refused the turn: %v (journal: %s)", err, strings.TrimSpace(logs.String()))
	}
	if res.State != a2a.TaskStateCompleted {
		t.Fatalf("Tier-1 turn ended %s: %s -- the default mount must reach the model",
			res.State, taskFailure(res, err))
	}
	if inf.count() != 1 {
		t.Fatalf("provider calls=%d, want the turn to have asked the model once", inf.count())
	}
	if journal.authCalls != 1 {
		t.Fatalf("grant authority ran %d times, want 1", journal.authCalls)
	}
	if journal.labelCalls != 0 {
		t.Fatalf("label journal consulted %d times with no routing table", journal.labelCalls)
	}
	var gens int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ahp_model.generations WHERE tenant=$1`,
		scope.Tenant).Scan(&gens); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if gens != 1 {
		t.Fatalf("generations=%d, want the completed turn's one durable row", gens)
	}
}

// Leg G: a refusal is an auditable event. The reason must live in the operation's
// span, where an auditor looks, not only in a log stream that may not be shipped
// -- the span is what made this defect invisible ("status_msg":"Internal Server
// Error" and nothing else). Content-free by construction.
func TestRefusalIsAuditedInTheSpanWithItsReason(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	inf := &fakeInference{text: "must never be produced", usage: modelUsage()}
	journal := &stubTaskJournal{labelErr: ErrTaskLabelJournalAbsent}
	captureJournal(t)

	b, err := New(ctx, pool, Config{Inference: inf, TaskTokenLimit: 20000, StepReserve: 4000,
		RoutingFile: writeProviderTable(t, tableOverrideModel)})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	b.WithTaskLabels(journal)

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(old) })
	ctx, span := provider.Tracer("ahp/test").Start(ctx, "POST /v1/model/SubmitModel")

	w := httpModelCallCtx(t, b, ctx, scope, "dtask-"+randHex(4), submitBody("answer-1"))
	span.End()
	if w.Code != http.StatusConflict {
		t.Fatalf("refusal got %d body=%s", w.Code, w.Body.String())
	}

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("recorded %d spans, want the refusal to have been recorded on one", len(ended))
	}
	got := map[attribute.Key]attribute.Value{}
	for _, kv := range ended[0].Attributes() {
		got[kv.Key] = kv.Value
	}
	if got[attribute.Key("ahp.refusal.reason")] != attribute.StringValue(ReasonTaskLabelJournalAbsent) {
		t.Fatalf("span carries no refusal reason, got %v", ended[0].Attributes())
	}
	if got[attribute.Key("ahp.refusal.code")] != attribute.StringValue(contracts.ErrConflict) {
		t.Fatalf("span carries no contract code, got %v", ended[0].Attributes())
	}
	if got[attribute.Key("ahp.tenant")] != attribute.StringValue(scope.Tenant) {
		t.Fatalf("span carries no tenant to triage by, got %v", ended[0].Attributes())
	}
	if ended[0].Status().Code.String() != "Error" || ended[0].Status().Description != ReasonTaskLabelJournalAbsent {
		t.Fatalf("span status must name the refusal, got %+v", ended[0].Status())
	}
	if strings.Contains(fmt.Sprint(ended[0].Attributes()), doteSentinel) {
		t.Fatal("the refusal span carried request content")
	}
}
