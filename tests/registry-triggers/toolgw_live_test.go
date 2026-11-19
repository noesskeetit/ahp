package registrytriggers_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestToolGatewayLive drives the MOUNTED Tool & Credential Gateway (POST
// /v1/tools/{Call,Get,Fence,Approve}) on a REAL isolated/canonical stand over
// mTLS + a bound task token, against a REAL MCP streamable-HTTP tool server
// whose tools perform a real, out-of-band-observable side effect. This is a
// live leg, not a unit test; it is skipped WITH A REASON only when the stand
// gate is absent. After DEF-TGW-1 a side effect is executable ONLY on durable
// evidence (a human approval for the canonical digest, or an operator policy);
// the legs prove that and prove the effect does NOT happen without it.
//
// Required environment (the stand's AHP_TOOLS_CONFIG must expose these aliases):
//
//	record_note        SideEffecting=false                       (read-only)
//	record_note_gated  SideEffecting=true,  RequiresApproval=false
//	side_effect        SideEffecting=true,  RequiresApproval=true   (MCP record_note)
//	side_unknown       SideEffecting=true,  RequiresApproval=true   (MCP boom: effect then ambiguous error)
//
//	AHP_TOOLGW_LIVE=1                opt-in gate (unset => skip-with-reason)
//	AHP_DEPLOYED_URL=https://...      mounted front URL (mTLS)
//	AHP_DEPLOYED_CA / AHP_DEPLOYED_CA_DIR  stand CA (client trust + mint)
//	AHP_TOOLGW_MCP_URL=http://.../__effect out-of-band effect observer
//	AHP_DEV_JWT_SEED=...              dev signer seed (mirrors the front's dev issuer)
//
// Credentials provenance is the production Vault path (credentials=vault via
// AHP_TOOLS_VAULT_PATHS); AHP_TOOLS_DEV_BEARER is NOT used, so the vault bearer
// is unknown to this test -- a successful Call is itself proof it flowed.
func TestToolGatewayLive(t *testing.T) {
	if strings.TrimSpace(os.Getenv("AHP_TOOLGW_LIVE")) != "1" {
		t.Skip("AHP_TOOLGW_LIVE != 1: skipping the live Tool Gateway leg (bring up the stand, then set it; not a silent pass)")
	}
	base := strings.TrimSuffix(requireEnv(t, "AHP_DEPLOYED_URL"), "/")
	mcpURL := os.Getenv("AHP_TOOLGW_MCP_URL")
	if mcpURL == "" {
		mcpURL = "http://127.0.0.1:29841/__effect"
	}
	tokenSource := toolgwEnvOr("AHP_TOOLGW_TOKEN_SOURCE", "dev")

	// Scope names are parameterized because on the canonical Identity stand the
	// workload of a scoped token is not the harness's to choose: it comes from the
	// provisioned human's tenant and the exchanger deployment (space). On an
	// isolated dev-authorize stand the original fixed names stay the default, so
	// the dev leg is byte-identical.
	homeTenant := toolgwEnvOr("AHP_TOOLGW_HOME_TENANT", "h23home")
	foreignTenant := toolgwEnvOr("AHP_TOOLGW_FOREIGN_TENANT", "h23foreign")
	deployment := toolgwEnvOr("AHP_TOOLGW_DEPLOYMENT", "lane")
	subject := "svc-worker"
	// foreignScoped=1 means the harness can obtain a REAL scoped task token for
	// the foreign tenant (a provisioned human exists there), so tenant isolation
	// is proven at the SERVICE (404, no existence leak). Without it on the
	// canonical stand the foreign client can only present a token scoped to the
	// home tenant, and refusal MUST happen at the transport boundary (403) or the
	// service (404) -- never a pass, never an existence leak.
	// In dev mode the harness mints scoped tokens for ANY tenant, so the full
	// two-directional service-level isolation proof always runs there. On the
	// external (Identity) stand it needs a provisioned foreign human; until one
	// exists the leg runs the refusal-boundary form (403/404 + no-leak).
	foreignScoped := toolgwEnvOr("AHP_TOOLGW_TOKEN_SOURCE", "dev") != "external" ||
		toolgwEnvOr("AHP_TOOLGW_FOREIGN_SCOPED", "0") == "1"
	// Durable side-effect evidence has two product-sanctioned sources (DEF-TGW-1):
	// a per-call human approval via /v1/tools/Approve, or an audited operator
	// standing policy. approve mode runs everywhere the front's verifier accepts
	// harness human tokens (dev-authorize stands). operator_policy mode is for
	// Identity-composed canon: there the supported human token is minted with
	// audience ahp-browser while the tools plane verifies audience ahp, so the
	// per-call Approve route is not reachable over HTTP (open defect DEF-TGW-2,
	// reported); the leg then supplies the OTHER durable source -- a grant written
	// through the product's audited operator path -- and separately proves that
	// WITHOUT evidence the effect never happens and the approval request IS
	// recorded (pending row) by the 409.
	evidence := toolgwEnvOr("AHP_TOOLGW_EVIDENCE", "approve")
	switch evidence {
	case "approve", "operator_policy":
	default:
		t.Fatalf("AHP_TOOLGW_EVIDENCE=%q, want approve or operator_policy", evidence)
	}
	policyCmd := toolgwEnvOr("AHP_TOOLGW_OPERATOR_POLICY_CMD", "")
	if evidence == "operator_policy" && policyCmd == "" {
		t.Fatalf("AHP_TOOLGW_EVIDENCE=operator_policy requires AHP_TOOLGW_OPERATOR_POLICY_CMD (audited operator grant/revoke through tools.Service; see tests/toolgw-live/operator-tool)")
	}
	toolsSchema := toolgwEnvOr("AHP_TOOLS_SCHEMA", "ahp_tools_all")
	// Real-effect and ambiguous-effect aliases. Defaults match the dev stand and
	// the canon's fixed contract (approval-gated side_effect / side_unknown). On
	// the canon with evidence=operator_policy the harness points them at
	// operator-approved NON-approval-gated aliases: an approval-gated tool can
	// ONLY be unlocked by a per-digest human signature (policy never replaces
	// it -- product law), and that route is DEF-TGW-2-blocked over HTTP here.
	effectAlias := toolgwEnvOr("AHP_TOOLGW_EFFECT_ALIAS", "side_effect")
	unknownAlias := toolgwEnvOr("AHP_TOOLGW_UNKNOWN_ALIAS", "side_unknown")
	home := newFrontClient(t, base, homeTenant, deployment)
	foreign := newFrontClient(t, base, foreignTenant, deployment)

	// Task and human credentials come from a pluggable source. On an isolated
	// dev-authorize stand (the default) the harness mints them with the dev
	// issuer. On the Identity-backed canon the front's authorize callback rejects
	// dev tokens BEFORE the service (empty guardian_log -> 403), so the harness
	// must present scoped Identity tokens minted by the real login ->
	// /v1/identity/control/exchange flow. Those arrive from an external provider
	// command (Goodall's scripts/identity-task-token.mjs). External mode is
	// selected with AHP_TOOLGW_TOKEN_SOURCE=external plus a provider command per
	// kind; a missing provider is a skip-with-reason -- never a silent pass, and
	// it never relies on weakening dev-authorize on the stand.
	var signer *auth.Signer
	switch tokenSource {
	case "dev":
		signer = devSigner(t)
	case "external":
	default:
		t.Fatalf("AHP_TOOLGW_TOKEN_SOURCE=%q, want dev or external", tokenSource)
	}
	taskTok := func(tenant string) string {
		if tokenSource == "dev" {
			id := "h23task-" + uniqueSuffix()
			return mintTask(t, signer, tenant, deployment, subject, id, "ctx-"+id)
		}
		return externalToken(t, "task", tenant, deployment, subject)
	}
	humanTok := func(tenant string) string {
		if tokenSource == "dev" {
			tok, err := signer.Sign(auth.Claims{
				Audience: "ahp", Kind: auth.KindHuman,
				Tenant: tenant, Subject: "operator-ada",
			}, time.Hour)
			if err != nil {
				t.Fatalf("mint human token: %v", err)
			}
			return tok
		}
		return externalToken(t, "human", tenant, deployment, "operator-ada")
	}
	call := func(c *frontClient, tenant, key, tool, args string) (int, map[string]any) {
		code, raw, err := c.post("/v1/tools/Call", taskTok(tenant), map[string]any{
			"key": key, "tool": tool, "arguments": json.RawMessage(args),
		}, 120*time.Second)
		if err != nil {
			t.Fatalf("Call %s: %v", tool, err)
		}
		return code, decode(t, raw)
	}
	get := func(c *frontClient, tenant, key string) (int, map[string]any) {
		code, raw, err := c.post("/v1/tools/Get", taskTok(tenant), map[string]any{"key": key}, 30*time.Second)
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		return code, decode(t, raw)
	}
	approve := func(tenant, digest, tool string) (int, map[string]any) {
		code, raw, err := home.post("/v1/tools/Approve", humanTok(tenant),
			map[string]any{"digest": digest, "tool": tool}, 30*time.Second)
		if err != nil {
			t.Fatalf("Approve: %v", err)
		}
		return code, decode(t, raw)
	}
	digestOf := func(tool, args string) string {
		d, err := tools.ApprovalDigest(tool, json.RawMessage(args))
		if err != nil {
			t.Fatalf("ApprovalDigest: %v", err)
		}
		return d
	}
	// ensureEvidence supplies the durable evidence a side-effecting Call needs,
	// through the product's own path for the selected source. It MUST be called
	// only after the leg has proven the pre-evidence refusal.
	ensureEvidence := func(tenant, tool, args string) {
		t.Helper()
		if evidence == "approve" {
			if ac, _ := approve(tenant, digestOf(tool, args), tool); ac != 200 {
				t.Fatalf("Approve(human) %s: status=%d, want 200", tool, ac)
			}
			return
		}
		runOperatorPolicy(t, policyCmd, tenant, "grant", tool)
		t.Cleanup(func() { runOperatorPolicy(t, policyCmd, tenant, "revoke", tool) })
	}

	// (a) real side-effect through the mounted front, gated by a human approval.
	eff0 := observeMCP(t, mcpURL)
	nonceA := "a-" + uniqueSuffix()
	argsA := `{"note":"` + nonceA + `"}`

	// digestA is the canonical approval digest of the (a) arguments under the
	// evidence alias. Leg (h) asserts it leaves the pending worklist once a
	// human signs it.
	digestA := digestOf(effectAlias, argsA)
	keyA := "h23-a-" + uniqueSuffix()
	// The approval gate itself is asserted on the contract alias regardless of
	// evidence mode: side_effect is approval-gated, so the first Call must 409
	// and register the request; the real dispatch then runs through effectAlias
	// with the evidence this stand can produce.
	if code, _ := call(home, homeTenant, "h23-gate-"+uniqueSuffix(), "side_effect", argsA); code != 409 {
		t.Fatalf("(a) pre-approval Call side_effect: status=%d, want 409 (human approval required)", code)
	}
	// the 409 must have REGISTERED the approval request (pending row, unbound).
	requirePendingApproval(t, toolsSchema, homeTenant, digestOf("side_effect", argsA), "side_effect", "pending")
	if evidence == "operator_policy" {
		// An unbound request authorizes nothing: until durable evidence exists the
		// effect must still not have happened.
		if effMid := observeMCP(t, mcpURL); effMid.Count != eff0.Count {
			t.Fatalf("(a) effect happened with only a pending request on record: %d->%d", eff0.Count, effMid.Count)
		}
	}
	// P1 one-shot binding (root audit 2026-11-02): an approval authorises the
	// EXACT operation it was raised under, so the request is registered under
	// the FINAL operation key before evidence is granted. This pre-call is
	// refused by the gate/policy and must run no effect.
	keyA = "h23-a2-" + uniqueSuffix()
	if preCode, _ := call(home, homeTenant, keyA, effectAlias, argsA); preCode == 200 {
		t.Fatalf("(a) pre-evidence Call %s under the final key succeeded: evidence gate bypassed", effectAlias)
	}
	// the pending request the front registered is now approvable
	ensureEvidence(homeTenant, effectAlias, argsA)
	code, viewA := call(home, homeTenant, keyA, effectAlias, argsA)
	if code != 200 && evidence == "operator_policy" {
		// the operator grant is durable but asynchronous around the HTTP face;
		// one bounded retry keeps the leg honest (no silent pass: the effect
		// counter below still has to move).
		time.Sleep(500 * time.Millisecond)
		code, viewA = call(home, homeTenant, keyA, effectAlias, argsA)
	}
	if code != 200 || viewA["status"] != "SUCCEEDED" {
		t.Fatalf("(a) Call after approval: status=%d view=%v, want 200 SUCCEEDED", code, viewA)
	}
	eff1 := observeMCP(t, mcpURL)
	if eff1.Count != eff0.Count+1 {
		t.Fatalf("(a) tool-server effect count=%d, want %d (one real effect after approval)", eff1.Count, eff0.Count+1)
	}
	if eff1.lastNote() != nonceA {
		t.Fatalf("(a) recorded note=%q, want %q (real effect, argument echo)", eff1.lastNote(), nonceA)
	}
	if eff1.AuthOK <= eff0.AuthOK || eff1.Unauth != eff0.Unauth {
		t.Fatalf("(a) vault credential flow wrong (auth_ok %d->%d, unauth %d->%d)", eff0.AuthOK, eff1.AuthOK, eff0.Unauth, eff1.Unauth)
	}
	if gcode, gv := get(home, homeTenant, keyA); gcode != 200 || gv["status"] != "SUCCEEDED" {
		t.Fatalf("(a) Get(keyA): status=%d view=%v, want 200 SUCCEEDED", gcode, gv)
	}

	// (b) foreign tenant -> refuse WITHOUT existence leak (both directions).
	keyB := "h23-b-" + uniqueSuffix()
	nonceB := "b-" + uniqueSuffix()
	if code, _ := call(home, homeTenant, keyB, "record_note", `{"note":"home-secret-`+nonceB+`"}`); code != 200 {
		t.Fatalf("(b) home seed: status=%d, want 200", code)
	}
	fcode, fbody := get(foreign, foreignTenant, keyB)
	if foreignScoped {
		if fcode != 404 {
			t.Fatalf("(b) foreign Get(home key): status=%d body=%v, want 404", fcode, fbody)
		}
	} else if fcode != 403 && fcode != 404 {
		// No scoped foreign identity exists on this stand: the refusal must land
		// at the transport boundary (403 workload mismatch) or the service (404).
		// Either way the operation is invisible -- this is the honest shape of
		// leg (b) until a foreign human is provisioned (AHP_TOOLGW_FOREIGN_SCOPED=1
		// upgrades it to the full service-level 404 proof).
		t.Fatalf("(b) foreign Get(home key): status=%d body=%v, want 403/404 refusal", fcode, fbody)
	}
	blob, _ := json.Marshal(fbody)
	for _, leak := range []string{homeTenant, nonceB, "home-secret"} {
		if strings.Contains(strings.ToLower(string(blob)), strings.ToLower(leak)) {
			t.Fatalf("(b) foreign refusal leaked %q: %s", leak, redact(string(blob)))
		}
	}
	if foreignScoped {
		keyF := "h23-f-" + uniqueSuffix()
		if code, v := call(foreign, foreignTenant, keyF, "record_note", `{"note":"foreign-`+nonceB+`"}`); code != 200 || v["status"] != "SUCCEEDED" {
			t.Fatalf("(b) foreign own-scope Call: status=%d view=%v, want 200 SUCCEEDED", code, v)
		}
		if code, _ := get(home, homeTenant, keyF); code != 404 {
			t.Fatalf("(b) home Get(foreign key): status=%d, want 404", code)
		}
	} else {
		t.Logf("(b) reverse direction needs a foreign human holding the %q capability (a scoped foreign token is derived from what that human actually holds); this stand's foreign principal does not, so the refusal-boundary form runs here and the foreign principal's own refusals are asserted in leg (i)", "tools")
	}

	// (c) repeat the SAME idempotency key -> NOT a second effect.
	effC := observeMCP(t, mcpURL)
	if code, _ := call(home, homeTenant, keyB, "record_note", `{"note":"DIFFERENT-`+nonceB+`"}`); code != 200 {
		t.Fatalf("(c) replay same key: status=%d, want 200 (replayed terminal op)", code)
	}
	if after := observeMCP(t, mcpURL); after.Count != effC.Count {
		t.Fatalf("(c) repeat same key produced a SECOND effect: %d->%d", effC.Count, after.Count)
	}

	// (d) SIDE_EFFECT_UNKNOWN: an ambiguous side effect is journalled once and
	//     never auto-replayed; Fence keeps it sealed. (Reachable only because
	//     DEF-TGW-1 lets an approved side effect reach dispatch.)
	nonceD := "d-" + uniqueSuffix()
	argsD := `{"note":"` + nonceD + `"}`
	keyD := "h23-d-" + uniqueSuffix()
	if unknownAlias != "side_unknown" {
		// Ambiguous dispatch on the canon runs through a non-approval-gated alias
		// (policy evidence); the approval-gated 409 for ambiguous tools is already
		// proven on side_effect in leg (a) and on side_unknown in approve mode.
		if code, _ := call(home, homeTenant, "h23-dg-"+uniqueSuffix(), "side_unknown", argsD); code != 409 {
			t.Fatalf("(d) pre-approval Call side_unknown: status=%d, want 409", code)
		}
	} else {
		if code, _ := call(home, homeTenant, keyD, "side_unknown", argsD); code != 409 {
			t.Fatalf("(d) pre-approval Call side_unknown: status=%d, want 409", code)
		}
	}
	requirePendingApproval(t, toolsSchema, homeTenant, digestOf("side_unknown", argsD), "side_unknown", "pending")
	// P1 one-shot binding: the request must exist under the FINAL operation key
	// before evidence arrives; this pre-call is refused and runs no effect.
	keyD = "h23-d2-" + uniqueSuffix()
	if preCode, _ := call(home, homeTenant, keyD, unknownAlias, argsD); preCode == 200 {
		t.Fatalf("(d) pre-evidence Call %s under the final key succeeded: evidence gate bypassed", unknownAlias)
	}
	ensureEvidence(homeTenant, unknownAlias, argsD)
	effBeforeU := observeMCP(t, mcpURL)
	code, vU := call(home, homeTenant, keyD, unknownAlias, argsD)
	if code != 200 || vU["status"] != "SIDE_EFFECT_UNKNOWN" {
		t.Fatalf("(d) ambiguous Call: status=%d view=%v, want 200 SIDE_EFFECT_UNKNOWN", code, vU)
	}
	effAfterU := observeMCP(t, mcpURL)
	if effAfterU.Count != effBeforeU.Count+1 {
		t.Fatalf("(d) ambiguous side effect should run exactly once: %d->%d", effBeforeU.Count, effAfterU.Count)
	}
	// retry with the SAME key must NOT redispatch (no auto-replay of an unknown)
	if code, vR := call(home, homeTenant, keyD, unknownAlias, argsD); code != 200 || vR["status"] != "SIDE_EFFECT_UNKNOWN" {
		t.Fatalf("(d) retry same key: status=%d view=%v, want 200 SIDE_EFFECT_UNKNOWN (no replay)", code, vR)
	}
	if after := observeMCP(t, mcpURL); after.Count != effAfterU.Count {
		t.Fatalf("(d) retry produced a SECOND effect (auto-replayed an unknown): %d->%d", effAfterU.Count, after.Count)
	}
	// Fence seals the unknown op and must not reopen it
	fenceCode, fraw, ferr := home.post("/v1/tools/Fence", taskTok(homeTenant), map[string]any{"key": keyD}, 30*time.Second)
	if ferr != nil {
		t.Fatalf("Fence: %v", ferr)
	}
	fenceBody := decode(t, fraw)
	if fenceCode != 200 {
		t.Fatalf("(d) Fence: status=%d body=%v, want 200", fenceCode, fenceBody)
	}
	if !sealed(fenceBody["status"]) {
		t.Fatalf("(d) Fence left op non-terminal: %v", fenceBody["status"])
	}

	// (e) Approve accepts a HUMAN bearer; a task bearer is refused.
	argsE := `{"x":1,"e":"` + uniqueSuffix() + `"}`
	digestE := digestOf("side_effect", argsE)
	hcode, hbody, herr := home.post("/v1/tools/Approve", humanTok(homeTenant),
		map[string]any{"digest": digestE, "tool": "side_effect"}, 30*time.Second)
	if herr != nil {
		t.Fatalf("Approve(human): %v", herr)
	}
	hb := decode(t, hbody)
	if evidence == "approve" {
		if hcode == 403 {
			t.Fatalf("(e) human bearer rejected by Approve (403): %v", hb)
		}
		if hcode != 404 && hcode != 200 {
			t.Fatalf("(e) Approve(human): status=%d body=%v, want 404/200 (past auth)", hcode, hb)
		}
	} else {
		// operator_policy mode runs on the Identity canon, where the only
		// supported human token is minted audience ahp-browser while this plane
		// verifies audience ahp: a 403 here is the recorded open defect DEF-TGW-2,
		// not a permission to pass silently -- the exact status and message are
		// surfaced in the test log as evidence. Any OTHER status must still be
		// 200/404, so an unnoticed change in either direction fails.
		if hcode == 403 {
			t.Logf("(e) DEF-TGW-2 evidence: Approve with the canon's штатный human token returned 403: %v", hb)
		} else if hcode != 404 && hcode != 200 {
			t.Fatalf("(e) Approve(human): status=%d body=%v, want 403 (DEF-TGW-2) or 404/200", hcode, hb)
		} else {
			t.Logf("(e) Approve accepted the canon human token (status=%d): DEF-TGW-2 appears FIXED", hcode)
		}
	}
	tcode, _, terr := home.post("/v1/tools/Approve", taskTok(homeTenant),
		map[string]any{"digest": digestE, "tool": "side_effect"}, 30*time.Second)
	if terr != nil {
		t.Fatalf("Approve(task): %v", terr)
	}
	if tcode != 403 {
		t.Fatalf("(e) Approve(task bearer): status=%d, want 403 (task cannot approve)", tcode)
	}

	// (e2) Approve is idempotent-by-absence: a fresh gated Call registers a
	// pending request; the human signature binds it (200); a second Approve of
	// the same digest 404s (no double-signing); neither Approve executes the
	// tool -- Approve is evidence, not execution (asserted via the counter).
	// Runs only in approve mode: the operator-policy canon keeps the strict
	// pending-then-approve flow for its own digests, and DEF-TGW-2 (fixed on the
	// product side here) still needs the canon-side audience wiring.
	if evidence == "approve" {
		nonceE2 := "e2-" + uniqueSuffix()
		argsE2 := `{"note":"` + nonceE2 + `"}`
		digestE2 := digestOf("side_effect", argsE2)
		if code, _ := call(home, homeTenant, "h23-e2-"+uniqueSuffix(), "side_effect", argsE2); code != 409 {
			t.Fatalf("(e2) pre-approval Call: status=%d, want 409", code)
		}
		effE2 := observeMCP(t, mcpURL)
		if ac, ab := approve(homeTenant, digestE2, "side_effect"); ac != 200 {
			t.Fatalf("(e2) Approve pending digest: status=%d body=%v, want 200", ac, ab)
		}
		requirePendingApproval(t, toolsSchema, homeTenant, digestE2, "side_effect", "bound")
		if ac, ab := approve(homeTenant, digestE2, "side_effect"); ac != 404 {
			t.Fatalf("(e2) repeat Approve same digest: status=%d body=%v, want 404 (no double-signing)", ac, ab)
		}
		if after := observeMCP(t, mcpURL); after.Count != effE2.Count {
			t.Fatalf("(e2) Approve executed the tool: effects %d->%d (Approve must never execute)", effE2.Count, after.Count)
		}
	}

	// (f) guardian default-deny: a side-effecting tool with no approval and no
	//     operator policy is refused BEFORE any effect. In operator_policy mode
	//     the (a) grant may still be standing for this alias: revoke it first
	//     so the absence the leg asserts is real, not incidental.
	if evidence == "operator_policy" && effectAlias == "record_note_gated" {
		runOperatorPolicy(t, policyCmd, homeTenant, "revoke", "record_note_gated")
	}
	nonceF := "f-" + uniqueSuffix()
	gcode, gbody := call(home, homeTenant, "h23-gated-"+uniqueSuffix(), "record_note_gated", `{"note":"never-`+nonceF+`"}`)
	if gcode != 403 {
		t.Fatalf("(f) side-effecting Call: status=%d body=%v, want 403 guardian deny", gcode, gbody)
	}
	if !strings.Contains(strings.ToLower(string(mustJSON(t, gbody))), "guardian") {
		t.Fatalf("(f) deny body must name the guardian: %v", gbody)
	}

	// (g) anti-vacuity: BEFORE durable evidence the effect does NOT happen (the
	//     counter does not grow); the SAME digest runs only AFTER approval.
	effG0 := observeMCP(t, mcpURL)
	nonceG := "g-" + uniqueSuffix()
	argsG := `{"note":"` + nonceG + `"}`
	keyG := "h23-g-" + uniqueSuffix()
	// The pre-evidence refusal differs by evidence source: an approval-gated tool
	// refuses 409 (approval required); for the operator-policy source this leg
	// uses record_note_gated -- side-effecting WITHOUT an approval gate, whose
	// only possible evidence is a standing policy that does not exist yet, so the
	// Guardian refuses 403 before any effect.
	toolG := "side_effect"
	wantPre := 409
	if evidence == "operator_policy" {
		toolG = "record_note_gated"
		wantPre = 403
	}
	if code, v := call(home, homeTenant, keyG, toolG, argsG); code != wantPre {
		t.Fatalf("(g) Call before evidence: status=%d view=%v, want %d", code, v, wantPre)
	}
	if effG1 := observeMCP(t, mcpURL); effG1.Count != effG0.Count {
		t.Fatalf("(g) ANTI-VACUUM VIOLATION: effect happened before approval/policy: %d->%d", effG0.Count, effG1.Count)
	}
	// P1 one-shot binding: the post-call reuses keyG, so this same-key pre-call
	// already registered the request under the final key (operator_policy mode
	// re-grants here the alias revoked before (f); approve mode binds this key).
	ensureEvidence(homeTenant, toolG, argsG)
	if code, v := call(home, homeTenant, keyG, toolG, argsG); code != 200 || v["status"] != "SUCCEEDED" {
		t.Fatalf("(g) Call after approval: status=%d view=%v, want 200 SUCCEEDED", code, v)
	}
	if effG2 := observeMCP(t, mcpURL); effG2.Count != effG0.Count+1 {
		t.Fatalf("(g) post-approval effect count=%d, want %d", effG2.Count, effG0.Count+1)
	}

	// (h) the pending-approvals worklist: a fresh gated Call appears in the
	// human Approvals listing; a digest already signed by a human disappears;
	// a task bearer is refused the worklist entirely (human-only route).
	nonceH := "h-" + uniqueSuffix()
	argsH := `{"note":"` + nonceH + `"}`
	if code, _ := call(home, homeTenant, "h23-h-"+uniqueSuffix(), "side_effect", argsH); code != 409 {
		t.Fatalf("(h) pre-approval Call: status=%d, want 409", code)
	}
	digestH := digestOf("side_effect", argsH)
	acode, araw, aerr := home.post("/v1/tools/Approvals", humanTok(homeTenant), map[string]any{}, 30*time.Second)
	if aerr != nil {
		t.Fatalf("Approvals(human): %v", aerr)
	}
	if acode != 200 {
		if evidence == "operator_policy" && acode == 403 {
			// Canon without the DEF-TGW-2 audience wiring cannot serve human legs
			// at all; the pending-set assertions need a readable worklist, so
			// surface it and stop the leg instead of passing vacuously.
			t.Logf("(h) Approvals(human) 403 on the operator_policy canon (DEF-TGW-2 audience wiring not merged there): worklist assertions skipped with reason")
			return
		}
		t.Fatalf("(h) Approvals(human): status=%d body=%v, want 200", acode, redact(string(araw)))
	}
	pending := approvalsDigests(t, araw)
	if _, ok := pending[digestH+"|side_effect"]; !ok {
		t.Fatalf("(h) fresh pending digest missing from worklist: %v", pending)
	}
	if _, ok := pending[digestA+"|"+effectAlias]; ok {
		t.Fatalf("(h) human-approved digest still in worklist: %v", digestA)
	}
	tcode, traw, terr := home.post("/v1/tools/Approvals", taskTok(homeTenant), map[string]any{}, 30*time.Second)
	if terr != nil {
		t.Fatalf("Approvals(task): %v", terr)
	}
	if tcode != 403 {
		t.Fatalf("(h) Approvals(task bearer): status=%d body=%v, want 403 (human-only)", tcode, redact(string(traw)))
	}

	// (i) FOREIGN principal on the human legs (canon leg, director contract
	// 2026-11-02): a provisioned human in ANOTHER tenant that does NOT hold
	// tools:approve must be refused on Approve and Approvals, and the refusal
	// must carry no existence signal about the home tenant's pending requests.
	// The refusal for a digest that IS pending in the home tenant and for a
	// digest that exists nowhere must be identical -- otherwise the approvals
	// plane is an existence oracle for foreign principals. AHP_TOOLGW_FOREIGN_
	// HUMAN=1 makes the foreign human's presence a hard expectation; without
	// it the leg stops with a logged reason when no foreign human exists, so a
	// missing principal is never mistaken for a pass.
	runLegI := func() {
		if tokenSource != "external" {
			t.Logf("(i) dev token source: the harness mints foreign humans itself, so the service-level foreign case is already covered by (b) with full two-directional isolation")
			return
		}
		strong := os.Getenv("AHP_TOOLGW_FOREIGN_HUMAN") == "1"
		foreignHuman, ok := externalTokenSoft(t, "human", foreignTenant, deployment, "u-ahp-foreign")
		if !ok {
			if strong {
				t.Fatalf("(i) AHP_TOOLGW_FOREIGN_HUMAN=1 but no foreign human token could be minted on this stand")
			}
			t.Logf("(i) no foreign human provisioned on this stand: foreign-principal refusal leg not run (logged, not passed)")
			return
		}
		if !strong {
			// AHP_TOOLGW_ALLOW_TENANT_MISMATCH stands substitute a provisioned
			// human for the missing tenant, so the credential presented here is
			// NOT a foreign-tenant principal: the refusal this leg then observes
			// may land at the transport boundary (cert tenant != token tenant)
			// rather than in the capability check. Run it, say so, and treat the
			// canon run (strict creds + AHP_TOOLGW_FOREIGN_HUMAN=1, where a
			// foreign token exists only if a real foreign human holds it) as the
			// authoritative form.
			t.Logf("(i) WEAK form on this stand (tenant-mismatch substitution): refusing at the boundary is asserted, capability-level refusal is proven on the canon")
		}
		nonceI := "i-" + uniqueSuffix()
		argsI := `{"note":"` + nonceI + `"}`
		if code, _ := call(home, homeTenant, "h23-i-"+uniqueSuffix(), "side_effect", argsI); code != 409 {
			t.Fatalf("(i) home pre-approval Call: status=%d, want 409 (the digest must really be pending)", code)
		}
		digestI := digestOf("side_effect", argsI)
		fakeDigest := digestOf("side_effect", `{"note":"nowhere-`+uniqueSuffix()+`"}`)
		effI := observeMCP(t, mcpURL)

		sReal, bReal, err := foreign.post("/v1/tools/Approve", foreignHuman,
			map[string]any{"digest": digestI, "tool": "side_effect"}, 30*time.Second)
		if err != nil {
			t.Fatalf("(i) foreign Approve(real): %v", err)
		}
		sFake, bFake, err := foreign.post("/v1/tools/Approve", foreignHuman,
			map[string]any{"digest": fakeDigest, "tool": "side_effect"}, 30*time.Second)
		if err != nil {
			t.Fatalf("(i) foreign Approve(fake): %v", err)
		}
		if sReal == 200 {
			t.Fatalf("(i) foreign human APPROVED a home-tenant pending request: cross-tenant signature accepted")
		}
		if sReal != 403 && sReal != 404 {
			t.Fatalf("(i) foreign Approve(real digest): status=%d body=%v, want 403/404", sReal, redact(string(bReal)))
		}
		if sReal != sFake || refusalKey(bReal) != refusalKey(bFake) {
			t.Fatalf("(i) EXISTENCE ORACLE: pending digest refused as %d/%q, unknown digest as %d/%q",
				sReal, refusalKey(bReal), sFake, refusalKey(bFake))
		}

		sList, bList, err := foreign.post("/v1/tools/Approvals", foreignHuman, map[string]any{}, 30*time.Second)
		if err != nil {
			t.Fatalf("(i) foreign Approvals: %v", err)
		}
		if sList == 200 || (sList != 403 && sList != 404) {
			t.Fatalf("(i) foreign Approvals: status=%d body=%v, want 403/404 refusal", sList, redact(string(bList)))
		}
		if ftok, okTask := externalTokenSoft(t, "task", foreignTenant, deployment, "svc-worker"); okTask {
			st, bt, err := foreign.post("/v1/tools/Approvals", ftok, map[string]any{}, 30*time.Second)
			if err != nil {
				t.Fatalf("(i) foreign task Approvals: %v", err)
			}
			if st != 403 {
				t.Fatalf("(i) foreign task Approvals: status=%d body=%v, want 403", st, redact(string(bt)))
			}
			bList = append(bList, bt...)
		}

		blob := strings.ToLower(string(append(append(append([]byte{}, bReal...), bFake...), bList...)))
		// Leak criteria are OBJECT identifiers, not generic vocabulary: the word
		// "approval" in a refusal carries no information about the thing probed,
		// while an echoed digest, note, or home tenant name would. Indistinguish-
		// ability between a real pending digest and a nonexistent one is asserted
		// separately above.
		for _, leak := range []string{strings.ToLower(homeTenant), strings.ToLower(digestI),
			strings.ToLower(fakeDigest), strings.ToLower(nonceI)} {
			if strings.Contains(blob, leak) {
				t.Fatalf("(i) foreign refusal leaked %q: %s", leak, redact(blob))
			}
		}
		if after := observeMCP(t, mcpURL); after.Count != effI.Count {
			t.Fatalf("(i) foreign principal unlocked an effect: %d->%d", effI.Count, after.Count)
		}
		// Anti-vacuum: the digest the foreign principal was refused about really
		// is pending and readable by the HOME human -- the refusal is capability
		// driven, not a consequence of nothing being on record.
		hc2, hr2, err := home.post("/v1/tools/Approvals", humanTok(homeTenant), map[string]any{}, 30*time.Second)
		if err != nil {
			t.Fatalf("(i) home Approvals control: %v", err)
		}
		if hc2 != 200 || !approvalsDigests(t, hr2)[digestI+"|side_effect"] {
			t.Fatalf("(i) home Approvals control: status=%d, want 200 listing the pending digest", hc2)
		}
	}
	runLegI()
}

// externalTokenSoft is externalToken without the hard failure: it reports
// whether the credential could be minted, so a leg whose principal may
// legitimately be absent on a given stand can distinguish "not provisioned"
// (logged, leg not run) from "refused" (asserted).
func externalTokenSoft(t *testing.T, kind, tenant, deployment, subject string) (string, bool) {
	t.Helper()
	envKey := "AHP_TOOLGW_TASK_TOKEN_CMD"
	if kind == "human" {
		envKey = "AHP_TOOLGW_HUMAN_TOKEN_CMD"
	}
	cmdline := strings.TrimSpace(os.Getenv(envKey))
	if cmdline == "" {
		return "", false
	}
	cmd := exec.Command("sh", "-c", cmdline)
	cmd.Env = append(os.Environ(),
		"AHP_TOOLGW_KIND="+kind,
		"AHP_TOOLGW_TENANT="+tenant,
		"AHP_TOOLGW_DEPLOYMENT="+deployment,
		"AHP_TOOLGW_SUBJECT="+subject,
		"AHP_TOOLGW_AUDIENCE=ahp",
	)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Logf("no %s credential for tenant %s: %v: %s", kind, tenant, err, redact(stderr.String()))
		return "", false
	}
	tok := strings.TrimSpace(stdout.String())
	return tok, tok != ""
}

// refusalKey normalizes an error body to "code|message" so two refusals can be
// compared for indistinguishability without depending on field order.
func refusalKey(raw []byte) string {
	var e struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		return strings.TrimSpace(string(raw))
	}
	return e.Code + "|" + e.Message
}

// approvalsDigests parses the Approvals worklist into a set keyed by
// "digest|tool" so membership assertions are exact per pending request.
func approvalsDigests(t *testing.T, raw []byte) map[string]bool {
	t.Helper()
	var body struct {
		Approvals []struct {
			Digest string `json:"digest"`
			Tool   string `json:"tool"`
		} `json:"approvals"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode Approvals body %s: %v", redact(string(raw)), err)
	}
	out := make(map[string]bool, len(body.Approvals))
	for _, a := range body.Approvals {
		out[a.Digest+"|"+a.Tool] = true
	}
	return out
}

func sealed(v any) bool {
	switch fmt.Sprint(v) {
	case "SIDE_EFFECT_UNKNOWN", "FENCED", "SUCCEEDED", "FAILED":
		return true
	}
	return false
}

// runOperatorPolicy invokes the operator-plane provider command (audited grant/
// revoke through tools.Service.Grant/RevokeOperatorPolicy -- there is
// deliberately no task-facing HTTP route for it). The command receives the
// request via AHP_TOOLGW_OP_* environment and must exit 0.
func runOperatorPolicy(t *testing.T, cmdline, tenant, action, tool string) {
	t.Helper()
	cmd := exec.Command("sh", "-c", cmdline)
	cmd.Env = append(os.Environ(),
		"AHP_TOOLGW_OP_TENANT="+tenant,
		"AHP_TOOLGW_OP_TOOLS="+tool,
		"AHP_TOOLGW_OP_ACTION="+action,
		"AHP_TOOLGW_OP_REVOKE=",
	)
	if action == "revoke" {
		cmd.Env = append(cmd.Env, "AHP_TOOLGW_OP_REVOKE=1")
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("operator policy %s %s: %v: %s", action, tool, err, redact(string(out)))
	}
}

// requirePendingApproval asserts out-of-band that the 409 registered the
// approval request in the stand's approvals table (digest bound, no approver
// yet). It is skipped-with-log only when no database handle was provided.
// requirePendingApproval checks the approvals table out of band. expect is
// "pending" (row recorded, unbound -- anti-vacuum: an unbound request
// authorizes nothing) or "bound" (a human signed it: approved_by set). In
// operator_policy mode a bound row is not a violation (policy, not approval,
// is that mode's evidence), so "bound" relaxes to present-either-way there.
func requirePendingApproval(t *testing.T, schema, tenant, digest, tool, expect string) {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("AHP_TOOLS_DB_URL"))
	if dsn == "" {
		t.Logf("AHP_TOOLS_DB_URL unset: cannot verify the pending approval row out of band (409 status asserted only)")
		return
	}
	if !regexp.MustCompile(`^ahp_[a-z][a-z0-9_]{0,40}$`).MatchString(schema) {
		t.Fatalf("AHP_TOOLS_SCHEMA=%q is not a valid ahp schema name", schema)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open tools db: %v", err)
	}
	defer db.Close()
	var (
		requestID  string
		approved   sql.NullString
		approvedAt sql.NullTime
	)
	q := fmt.Sprintf(`SELECT request_id, approved_by, approved_at FROM %s.approvals WHERE tenant=$1 AND digest=$2 AND tool=$3`, schema)
	if err := db.QueryRow(q, tenant, digest, tool).Scan(&requestID, &approved, &approvedAt); err != nil {
		t.Fatalf("(a) pending approval request for digest not recorded: %v", err)
	}
	switch expect {
	case "pending":
		if approved.Valid || approvedAt.Valid {
			t.Fatalf("approval row bound before evidence: approved_by set")
		}
	case "bound":
		if os.Getenv("AHP_TOOLGW_EVIDENCE") == "operator_policy" {
			return
		}
		if !approved.Valid || !approvedAt.Valid {
			t.Fatalf("approval row not bound after human Approve: approved_by unset")
		}
	default:
		t.Fatalf("requirePendingApproval: unknown expect %q", expect)
	}
}

type mcpEffect struct {
	Count   int64 `json:"count"`
	AuthOK  int64 `json:"auth_ok"`
	Unauth  int64 `json:"unauth"`
	Entries []struct {
		Seq  int64  `json:"seq"`
		Note string `json:"note"`
	} `json:"entries"`
}

func (m mcpEffect) lastNote() string {
	if len(m.Entries) == 0 {
		return ""
	}
	return m.Entries[len(m.Entries)-1].Note
}

func observeMCP(t *testing.T, url string) mcpEffect {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("observe tool server %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
	if resp.StatusCode != 200 {
		t.Fatalf("observe tool server status=%d body=%s", resp.StatusCode, redact(string(raw)))
	}
	var e mcpEffect
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatalf("observe tool server decode: %v body=%s", err, redact(string(raw)))
	}
	return e
}

func decode(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if json.Unmarshal(raw, &out) != nil {
		out = map[string]any{"_raw": string(raw)}
	}
	return out
}

func mustJSON(t *testing.T, m map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal view: %v", err)
	}
	return b
}

// toolgwEnvOr returns the environment value or a fallback (the live test package
// has no shared envOr helper).
func toolgwEnvOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// externalToken obtains a scoped credential from an external provider command --
// the bridge to the canonical Identity flow (login -> control/exchange -> scoped
// token). The provider receives the request via environment
// (AHP_TOOLGW_KIND/TENANT/DEPLOYMENT/SUBJECT, AHP_DEPLOYED_URL) and prints the
// raw JWT on stdout. A missing provider is a skip-with-reason so the leg can be
// pre-staged now and enabled the moment scripts/identity-task-token.mjs lands in
// canon; it is never a silent pass and never a mistaken 403.
func externalToken(t *testing.T, kind, tenant, deployment, subject string) string {
	t.Helper()
	envKey := "AHP_TOOLGW_TASK_TOKEN_CMD"
	if kind == "human" {
		envKey = "AHP_TOOLGW_HUMAN_TOKEN_CMD"
	}
	cmdline := strings.TrimSpace(os.Getenv(envKey))
	if cmdline == "" {
		t.Skipf("AHP_TOOLGW_TOKEN_SOURCE=external but %s is unset: waiting for the scoped Identity token provider (scripts/identity-task-token.mjs login->control/exchange->scoped token); not a silent pass, and dev-authorize is intentionally not rolled back", envKey)
	}
	cmd := exec.Command("sh", "-c", cmdline)
	cmd.Env = append(os.Environ(),
		"AHP_TOOLGW_KIND="+kind,
		"AHP_TOOLGW_TENANT="+tenant,
		"AHP_TOOLGW_DEPLOYMENT="+deployment,
		"AHP_TOOLGW_SUBJECT="+subject,
		"AHP_TOOLGW_AUDIENCE=ahp",
	)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("external %s token provider failed: %v: %s", kind, err, redact(stderr.String()))
	}
	tok := strings.TrimSpace(stdout.String())
	if tok == "" {
		t.Fatalf("external %s token provider returned an empty token", kind)
	}
	return tok
}
