package httpapi

// Contract-code discipline of the memory plane's HTTP face (DEF-MEM-HTTP-CODE-1,
// foreman directive 2026-11-10). decision 2026-10-31-http-surface-no-false-200
// settled the shape of an HTTP 404 for the whole platform: the envelope carries
// the canonical code, byte-for-byte as declared in pkg/contracts ("NOT_FOUND").
// The internal layer already honours that (internal/memory/memory_test.go pins
// contracts.Code(err) == NOT_FOUND); this HTTP face was the one place answering
// a hand-typed lowercase "not_found", and a consumer forced to branch on codes
// reads that as a different vocabulary. The tests here pin the rule and keep the
// vocabulary from spreading: a table over the answered codes, plus a source scan
// that pins the legacy plane-specific codes so a NEW ad-hoc one cannot join it
// silently.

import (
	"encoding/json"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// answeredCode is one error the HTTP face put on the wire, observed as a client
// sees it: HTTP status plus the envelope code.
type answeredCode struct {
	name   string
	method string
	path   string
	kind   string // dev token kind
	body   string
	want   int // status the contract requires for this failure
}

// TestErrorEnvelopeCarriesContractCodes is the table regression: every failure
// this HTTP face answers must carry a code from the canonical vocabulary of
// pkg/contracts with the status that code implies. An answer outside the table
// fails with the observed code named.
func TestErrorEnvelopeCarriesContractCodes(t *testing.T) {
	h := setup(t)
	cases := []answeredCode{
		{"missing operation", http.MethodGet,
			"/v1/memory/operations/op-does-not-exist?scope=personal&project_id=review",
			"agent", "", http.StatusNotFound},
		{"no bearer", http.MethodGet, "/v1/memory/items?scope=personal&project_id=review",
			"", "", http.StatusUnauthorized},
		{"unknown JSON field", http.MethodPost, "/v1/memory/search",
			"agent", `{"scope":"personal","project_id":"review","query":"x","nope":1}`,
			http.StatusBadRequest},
		{"missing scope", http.MethodGet, "/v1/memory/operations/op-x",
			"agent", "", http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := h.request(t, c.method, c.path, c.kind, "review-key", c.body)
			var envelope struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("%s %s: body is not the error envelope: %v (%q)", c.method, c.path, err, rec.Body.String())
			}
			code := envelope.Error.Code
			if rec.Code != c.want {
				t.Errorf("%s %s: status=%d code=%q, want status %d", c.method, c.path, rec.Code, code, c.want)
			}
			// 401 keeps its own documented family; every other row must be a
			// canonical code from pkg/contracts, exact case.
			if rec.Code == http.StatusUnauthorized {
				if code != "authentication_required" {
					t.Errorf("%s %s: 401 code=%q, want the documented authentication_required", c.method, c.path, code)
				}
				return
			}
			if !isCanonicalContractCode(code) && !legacyPlaneCodes[code] {
				t.Errorf("%s %s: code=%q is neither canonical nor pinned plane debt (canonical set is %v)",
					c.method, c.path, code, canonicalContractCodes())
			}
			// The defect under repair: the missing-resource answer is held to the
			// canonical bar strictly - pinned debt never covers a 404.
			if c.want == http.StatusNotFound && code != contracts.ErrNotFound {
				t.Errorf("%s %s: a 404 must carry the canonical %q, got %q", c.method, c.path, contracts.ErrNotFound, code)
			}
		})
	}
}

// TestNoNewAdHocErrorCodes pins the emitted vocabulary at the source: every code
// literal this HTTP face can answer is either canonical or a member of the
// pinned legacy list. Adding a lowercase code here is a vocabulary decision, not
// a string edit: it must arrive as a contract change, not as a literal.
func TestNoNewAdHocErrorCodes(t *testing.T) {
	for _, file := range reachableFaceFiles {
		text := readSource(t, file)
		for _, literal := range emittedCodes(text) {
			if isCanonicalContractCode(literal) || legacyPlaneCodes[literal] {
				continue
			}
			t.Errorf("%s: answered code %q is neither canonical nor pinned legacy debt - a new ad-hoc vocabulary member needs a contract decision (DEF-MEM-HTTP-CODE-1)", file, literal)
		}
	}
	// The bar DEF-MEM-HTTP-CODE-1 leaves behind: no admitted debt may cover a 404.
	// A missing resource is answered with the canonical NOT_FOUND on every HTTP
	// surface of the platform (decision 2026-10-31-http-surface-no-false-200), so
	// the lowercase vocabulary is tolerated only in families where the platform has
	// no canonical code yet.
	for _, file := range reachableFaceFiles {
		for _, pair := range pairsOfCodes(readSource(t, file)) {
			if pair.status == http.StatusNotFound && pair.code != contracts.ErrNotFound {
				t.Errorf("%s: a 404 is answered with %q - the 404 family carries %q and admits no plane debt (DEF-MEM-HTTP-CODE-1 follow-up)", file, pair.code, contracts.ErrNotFound)
			}
		}
	}

	// The 404 family must not exist as a lowercase literal anywhere on this face:
	// a 404 is answered by the canonical NOT_FOUND of pkg/contracts, and any
	// re-literalised lowercase 404 code would return the original defect.
	for _, file := range reachableFaceFiles {
		if regexp.MustCompile(`"not_found"`).MatchString(readSource(t, file)) {
			t.Errorf("%s: literal \"not_found\" returned - the 404 envelope must carry contracts.ErrNotFound", file)
		}
	}
}

// readSource reads a source file of this package (test-time vocabulary scan).
func readSource(t *testing.T, file string) string {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	return string(raw)
}

// emittedCodes extracts every code this face can put into an error envelope:
// both memory.Fail(<status>, "<code>", ...) and the respond() fallback
// assignments code = "<code>".
func emittedCodes(text string) []string {
	out := []string{}
	seen := map[string]bool{}
	take := func(re *regexp.Regexp) {
		for _, m := range re.FindAllStringSubmatch(text, -1) {
			if !seen[m[1]] {
				seen[m[1]] = true
				out = append(out, m[1])
			}
		}
	}
	for _, pair := range pairsOfCodes(text) {
		if !seen[pair.code] {
			seen[pair.code] = true
			out = append(out, pair.code)
		}
	}
	take(regexp.MustCompile(`	code = "([a-z_]+)"`))
	return out
}

// reachableFaceFiles are the sources whose errors this HTTP face can put into the
// error envelope: its own handlers plus the typed errors that flow through
// respond()'s typed branch from the service, the list path and the AHP authority
// bridge (production wiring: cmd/ahp/mount_hindsight.go, ahpbridge/handler.go).
// Scanning only server.go would have been honest for one file and false for the
// face, so the pin covers the reachable set (DEF-MEM-HTTP-CODE-1 review C1).
var reachableFaceFiles = []string{
	"server.go",
	"../service.go",
	"../list.go",
	"../../ahpbridge/access.go",
}

// legacyPlaneCodes is DEF-MEM-HTTP-CODE-2, and the admitted scale of it: of the 46
// distinct codes this face can answer through respond(), exactly ONE was canonical
// before the 404 fix (contracts.ErrNotFound for a missing record); every other
// value is a plane-specific lowercase code, pinned here so that the vocabulary
// cannot grow while the migration is decided. This set may only SHRINK. Priority
// for promoting a code to pkg/contracts is by family exposure on the wire:
//  1. 404/403 answers that a caller must branch on: source_not_found (fixed here),
//     access_denied, wrong_project, wrong_scope, owner_mismatch, grant_revoked,
//     memory_action_denied, runtime_required, console_required, platform_required,
//     verified_principal_required, task_identity_required, invalid_identity/authority,
//     invalid_source_labels, memory_deleted/subject_deleted/tenant_deleted (410 family);
//  2. 400-family input refusals: invalid_* (search/source/turn/labels/correction/
//     page/scope/project/json/memory/memory_ref), project_required, global_setting,
//     source_limit (413), payload_expired (410);
//  3. 409-family write outcomes: turn_conflict, out_of_order, not_failed,
//     idempotency_conflict, memory_disabled, project_binding_conflict;
//  4. 5xx/internal and upstream: internal_error, memory_authority_missing,
//     generation_invariant, listing_unavailable, store_unavailable,
//     ahp_authority_unavailable, queue_limit (429), hindsight_* / upstream_*.
//
// Consumers that make this a wire contract, not a style choice: the Go SDK copies
// body.Error.Code verbatim (internal/memory/longterm/sdk/client.go:74) and the
// Python SDK raises MemoryError(status, error["code"], ...) verbatim
// (sdk/python/ahp_memory/client.py:67), whose test branches on "idempotency_conflict".
var legacyPlaneCodes = map[string]bool{}

func init() {
	for _, code := range []string{
		"access_denied", "ahp_authority_unavailable", "authentication_required",
		"console_required", "global_setting", "grant_revoked", "idempotency_conflict",
		"idempotency_key_required", "invalid_authority", "invalid_correction",
		"invalid_idle_days", "invalid_json", "invalid_labels", "invalid_memory",
		"invalid_memory_ref", "invalid_memory_source", "invalid_page", "invalid_project",
		"invalid_scope", "invalid_search", "invalid_source", "invalid_source_labels",
		"invalid_source_role", "invalid_sources", "invalid_turn", "listing_unavailable",
		"memory_action_denied", "memory_authority_missing", "memory_deleted",
		"memory_disabled", "not_failed", "out_of_order", "payload_expired",
		"platform_required", "project_deleted", "project_required", "queue_limit",
		"runtime_required", "source_limit", "subject_deleted",
		"task_identity_required", "turn_conflict", "verified_principal_required",
		"wrong_project", "wrong_scope",
	} {
		legacyPlaneCodes[code] = true
	}
}

func isCanonicalContractCode(code string) bool {
	for _, known := range canonicalContractCodes() {
		if code == known {
			return true
		}
	}
	return false
}

// emittedCode is one (status, code) pair this face can answer from a literal.
type emittedCode struct {
	status int
	code   string
}

// pairsOfCodes extracts the (status, code) pairs of every memory.Fail literal, so
// that family rules (a 404 must be canonical) can be enforced, not just vocabulary.
func pairsOfCodes(text string) []emittedCode {
	out := []emittedCode{}
	for _, m := range regexp.MustCompile(`Fail\(([0-9]{3}), "([a-z_]+)"`).FindAllStringSubmatch(text, -1) {
		status, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		out = append(out, emittedCode{status: status, code: m[2]})
	}
	return out
}

func canonicalContractCodes() []string {
	return []string{contracts.ErrNotFound, contracts.ErrConflict, contracts.ErrStaleEpoch,
		contracts.ErrForbidden, contracts.ErrInvalid, contracts.ErrKeyReuse,
		contracts.ErrLeaseLost, contracts.ErrUnavailable}
}
