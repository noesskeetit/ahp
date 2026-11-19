package space

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Tier-2 (decision 2026-10-30-exchange-scope-authority-model + FROZEN contract
// 2026-10-30T145405Z-tier2-authority-cancel-frozen-contract): each agent RUN needs its
// OWN runtime grant. The State run.ID is a RESOURCE id, NOT a grant; the ingress/authority
// token is used only to authenticate a request, list/read, verify ownership and authorize
// this Delegate — it is never presented as the run workload credential.
//
// This client speaks the AUTHORITATIVE Identity control-plane contract per the frozen
// ruling (E1 atomic delegate), it does NOT invent routes:
//
//	POST {roots}/{parentTask}/delegate  bearer=<attenuated parent task token>
//	    body{task,context,capabilities,ttl:<duration string>,max_depth?,max_fanout?,max_active?,allow_self?}
//	    -> 201 {grant, token}   (Identity creates the child Grant AND atomically returns the
//	                             minted child task token; Space does NOT do a separate Mint-of-child)
//	POST {roots}/{childTask}/cancel     bearer=<child task token>   -> 2xx (task-bound cancel of
//	                             the run's OWN grant subtree; operator cancel-subtree with a cert
//	                             allowlist is NEVER used by Space — a grant may cancel only itself)
//
// Identity binds the child's tenant/subject to the presenting parent's scope and its
// deployment to the parent deployment, and intersects the child's capabilities with the
// parent's grant — so Space never escalates: it can only delegate under its OWN (attenuated)
// ingress grant, and a cancel touches only the presented child grant, leaving siblings active.
const (
	IdentityRootsPath  = "/v1/identity/roots"
	delegateSuffix     = "/delegate"
	cancelSuffix       = "/cancel"
	defaultRunGrantTTL = 15 * time.Minute
)

// RunGrantSpec is a per-run grant request. Task is the SERVER-OWNED idempotent child task
// id (State's durable Run.TaskID after CreateRun presents this child token); Context is the
// thread; Capabilities are the run's requested caps (Identity intersects them with the
// parent — the subset is enforced by Identity, never trusted from here); TTL is a
// duration (serialized on the wire as a duration string, e.g. "15m").
type RunGrantSpec struct {
	Task         string
	Context      string
	Capabilities []string
	TTL          time.Duration
	MaxActive    int
	MaxFanout    int
	MaxDepth     int
	AllowSelf    bool
}

// RunGrantIssuer obtains a per-run (child) Identity grant under an attenuated parent grant
// and cancels a run's own grant. Implementations MUST be fail-closed: a failed Delegate
// never yields the parent token, and a run must never silently fall back to the shared
// ingress credential.
type RunGrantIssuer interface {
	// Delegate atomically creates a child Grant under parentTask and returns its minted
	// task token, presenting parentToken as bearer (transport.Task plane). parentTask MUST
	// be the attenuated task the parent token is bound to (from the human->task exchange),
	// never a broad authority or a browser value.
	Delegate(ctx context.Context, parentToken, parentTask string, spec RunGrantSpec) (string, error)
	// Cancel revokes the run's OWN child grant (and its subtree) via the task-bound
	// roots/{childTask}/cancel route, presenting the run's child token as bearer. A sibling
	// grant is untouched; revoke retry is idempotent; a non-2xx fails closed.
	Cancel(ctx context.Context, childToken, childTask string) error
}

type delegateBody struct {
	Task         string   `json:"task"`
	Context      string   `json:"context"`
	Capabilities []string `json:"capabilities,omitempty"`
	TTLOut       string   `json:"ttl"`
	MaxDepth     int      `json:"max_depth,omitempty"`
	MaxFanout    int      `json:"max_fanout,omitempty"`
	MaxActive    int      `json:"max_active,omitempty"`
	AllowSelf    bool     `json:"allow_self,omitempty"`
}

type grantRef struct {
	Task string `json:"task"`
}

// delegateResponse is the E1 atomic Delegate result: a child Grant plus its minted token.
type delegateResponse struct {
	Grant grantRef `json:"grant"`
	Token string   `json:"token"`
}

// IdentityDelegateIssuer is the production RunGrantIssuer. Delegate/Cancel use the bearer
// task plane (TLS to Identity). It holds no signing key (Identity signs).
type IdentityDelegateIssuer struct {
	RootsURL string // …/v1/identity/roots
	// HTTPClient is the mTLS client (Space workload cert) used for the bearer task-plane
	// delegate/cancel calls to Identity.
	HTTPClient *http.Client
	// Audience is optional; empty lets Identity use its default task audience.
	Audience string
}

// NewIdentityDelegateIssuer builds the issuer. identityBaseURL is the Identity service base
// (e.g. https://identity:8443); workloadClient is Space's mTLS client used for the bearer
// task-plane delegate/cancel calls.
func NewIdentityDelegateIssuer(identityBaseURL string, workloadClient *http.Client) (*IdentityDelegateIssuer, error) {
	if identityBaseURL == "" || workloadClient == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: delegate issuer needs identity url + mTLS client")
	}
	base := strings.TrimRight(identityBaseURL, "/")
	return &IdentityDelegateIssuer{
		RootsURL:   base + IdentityRootsPath,
		HTTPClient: workloadClient,
	}, nil
}

func (i *IdentityDelegateIssuer) Delegate(ctx context.Context, parentToken, parentTask string, spec RunGrantSpec) (string, error) {
	if parentToken == "" {
		return "", contracts.NewError(contracts.ErrForbidden, "space: delegate needs the parent task token")
	}
	if parentTask == "" {
		return "", contracts.NewError(contracts.ErrForbidden, "space: delegate needs the parent (attenuated) task id")
	}
	if spec.Task == "" {
		return "", contracts.NewError(contracts.ErrForbidden, "space: delegate needs a server-owned child task id")
	}
	ttl := spec.TTL
	if ttl <= 0 {
		ttl = defaultRunGrantTTL
	}
	// E1 atomic delegate: ONE call creates the child Grant AND returns its minted token.
	dreq := delegateBody{
		Task: spec.Task, Context: spec.Context, Capabilities: spec.Capabilities,
		TTLOut: ttl.String(), MaxDepth: spec.MaxDepth, MaxFanout: spec.MaxFanout,
		MaxActive: spec.MaxActive, AllowSelf: spec.AllowSelf,
	}
	dbody, _ := json.Marshal(dreq)
	durl := i.RootsURL + "/" + url.PathEscape(parentTask) + delegateSuffix
	dhttp, err := http.NewRequestWithContext(ctx, http.MethodPost, durl, bytes.NewReader(dbody))
	if err != nil {
		return "", err
	}
	dhttp.Header.Set("Content-Type", "application/json")
	dhttp.Header.Set("Authorization", "Bearer "+parentToken)
	dresp, err := i.client().Do(dhttp)
	if err != nil {
		// The dependency did not answer. That is not an entitlement statement about the caller,
		// and filing it as FORBIDDEN hides an outage inside an auth bucket.
		return "", opDelegate.outage(err)
	}
	defer dresp.Body.Close()
	ddata, _ := io.ReadAll(io.LimitReader(dresp.Body, 1<<20))
	// 201 is required; a bare 200 Grant (no atomic token) is rejected so we never fall back
	// to a parent-bearer Mint-of-child, which the frozen contract forbids.
	if dresp.StatusCode != http.StatusCreated {
		return "", opDelegate.refusal(dresp, ddata)
	}
	var out delegateResponse
	if err := json.Unmarshal(ddata, &out); err != nil {
		return "", contracts.NewError(contracts.ErrForbidden, "space: run delegate returned an unreadable body")
	}
	if out.Token == "" {
		return "", contracts.NewError(contracts.ErrForbidden, "space: run delegate returned no child token")
	}
	// The child the Identity store created MUST be the server-owned id we requested; a
	// divergence would let an over-privileged parent redirect the run to a foreign grant.
	if out.Grant.Task != "" && out.Grant.Task != spec.Task {
		return "", contracts.NewError(contracts.ErrForbidden, "space: run delegate returned an unexpected child task")
	}
	return out.Token, nil
}

func (i *IdentityDelegateIssuer) Cancel(ctx context.Context, childToken, childTask string) error {
	if childTask == "" {
		return contracts.NewError(contracts.ErrForbidden, "space: run cancel needs the durable child task ref")
	}
	if childToken == "" {
		return contracts.NewError(contracts.ErrForbidden, "space: run cancel needs the run's child token as bearer")
	}
	// Task-bound cancel of the run's OWN grant: POST roots/{childTask}/cancel with the
	// child token as bearer. Identity authorizes by lineage (the bearer's own grant), so a
	// sibling grant is untouched and no operator cert allowlist is involved.
	u := i.RootsURL + "/" + url.PathEscape(childTask) + cancelSuffix
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader([]byte("{}")))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+childToken)
	resp, err := i.client().Do(httpReq)
	if err != nil {
		return opRunCancel.outage(err)
	}
	defer resp.Body.Close()
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	// 2xx means accepted; anything else (or a still-live grant) fails closed so a cancel is
	// never reported as a false success. Identity's own status and code travel with the refusal:
	// "cancel rejected" without them is undiagnosable from Space's side, and the reasons measured
	// on the stand were genuinely different (a revoked grant vs a run credential that never named
	// a live grant at all). The token itself is never included.
	if resp.StatusCode/100 != 2 {
		return opRunCancel.refusal(resp, detail)
	}
	return nil
}

func (i *IdentityDelegateIssuer) client() *http.Client {
	return i.HTTPClient
}

// truncateOneLine collapses an upstream text to one bounded line. The bound is counted in RUNES: a
// byte-wise cut can land inside a two-byte character and leave half of it behind, which then reaches
// an operator as a replacement glyph - a clipped sentence is fine, a torn one reads as corruption.
func truncateOneLine(s string, max int) string {
	r := []rune(strings.Join(strings.Fields(s), " "))
	if len(r) <= max {
		return string(r)
	}
	return string(r[:max]) + "..."
}

// RunGrantController adds restart-safe control by a live, verified ancestor.
// It is optional so older issuers retain their exact-own cancellation contract.
type RunGrantController interface {
	ChildToken(context.Context, string, string) (string, error)
	CancelChild(context.Context, string, string, string) error
}

// ChildToken uses Identity's existing lineage-checked mint operation. The
// resulting token is bound to the child's persisted conversation context.
func (i *IdentityDelegateIssuer) ChildToken(ctx context.Context, parentToken, childTask string) (string, error) {
	body, _ := json.Marshal(map[string]string{"audience": i.Audience})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, i.RootsURL+"/"+url.PathEscape(childTask)+"/mint", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+parentToken)
	resp, err := i.client().Do(req)
	if err != nil {
		// An untyped error here reached the transport as INTERNAL/500, which reads as "Space broke" and
		// not as "Identity did not answer".
		return "", opChildMint.outage(err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", opChildMint.refusal(resp, payload)
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(payload, &out); err != nil || out.Token == "" {
		return "", contracts.NewError(contracts.ErrForbidden, "space: descendant mint returned no credential")
	}
	return out.Token, nil
}
func (i *IdentityDelegateIssuer) CancelChild(ctx context.Context, parentToken, parentTask, childTask string) error {
	if parentToken == "" || parentTask == "" || childTask == "" {
		return contracts.NewError(contracts.ErrForbidden, "space: parent authority and child reference required")
	}
	endpoint := i.RootsURL + "/" + url.PathEscape(parentTask) + "/children/" + url.PathEscape(childTask) + "/cancel"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader([]byte("{}")))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+parentToken)
	resp, err := i.client().Do(req)
	if err != nil {
		// Still fail closed - the acknowledgement is genuinely unknown - but say WHO is unavailable.
		return opChildCancel.outage(err)
	}
	defer resp.Body.Close()
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != http.StatusNoContent {
		return opChildCancel.refusal(resp, detail)
	}
	return nil
}

// upstreamStated reads the contracts envelope a dependency stated in its response body. Identity
// answers refusals through transport.WriteError, so a real entitlement denial arrives as
// {"code":"FORBIDDEN","message":"identity: ..."} and a capacity refusal as {"code":"CONFLICT",...};
// an unclassified server bug arrives as {"code":"INTERNAL","message":"internal server error"} and a
// gateway may answer 5xx with no envelope at all. Absent or unparsable stays empty - the caller must
// be able to tell "no envelope" from "an envelope that says nothing".
//
// Code and message are returned APART on purpose: the code becomes a stable machine token in
// contracts.ReasonOf, so a caller branches on identity:mint:http-403:forbidden instead of matching
// prose, while the message is only ever carried as bounded text (truncateOneLine). This is the single
// mechanism in this package for reading an upstream envelope - the older identityReason helper became
// its last caller-less duplicate once every seam started using upstreamStated.
func upstreamStated(body []byte) (code, message string) {
	var stated contracts.Error
	if json.Unmarshal(body, &stated) != nil {
		return "", ""
	}
	return strings.ToLower(strings.TrimSpace(stated.Code)), strings.TrimSpace(stated.Message)
}

// identityOp names ONE seam of this client, so a dependency's answer keeps its own identity on the way
// out. Each seam shares the same two facts about a dependency - it either did not answer at all, or it
// answered with a status and a body - so the mapping is written once and only the naming differs: slug is
// the stable token (identity:mint:http-403:forbidden), prose is what the caller was attempting.
//
// Measured reason this exists (density stand, root's repeat w1-repeats-20261110T025521Z: all 24 refusals
// were Identity HTTP 500 on POST /v1/identity/roots/{task}/delegate; that route logged 3626x201 and
// 75x500 with ZERO 403, so no quota branch fired): the old code threw the status and body away, so an
// internal Identity failure was reported as an entitlement refusal and the harness faithfully recorded
// failure_class=forbidden_entitlement. The cancel and mint seams had the same shape, one step worse - an
// untyped error there renders as INTERNAL/500, the anti-pattern fixed for the Registry in 354d27f7.
type identityOp struct {
	slug  string
	prose string
}

var (
	opDelegate    = identityOp{"delegate", "run delegate"}
	opRunCancel   = identityOp{"cancel", "run cancel"}
	opChildMint   = identityOp{"mint", "descendant token mint"}
	opChildCancel = identityOp{"child-cancel", "descendant cancellation"}
)

// upstreamProseRunes and retryAfterRunes bound what a dependency may say inside Space's own reply.
//
// upstreamProseRunes = 160 is not a new number: it is the inline limit this file already carried at
// df009e50 ("truncateOneLine(e.Message, 160)" and "truncateOneLine(string(body), 160)"), named so the
// prose budget and the retry hint below can be budgeted against each other instead of both clipping the
// same 243-rune reply. Budgeted that way, a refusal - hint included - stays inside the 243 runes the
// legs in rungrant_delegate_refusal_test.go pin.
//
// retryAfterRunes = 32 is new because the thing it bounds is new: Retry-After did not exist anywhere in
// this branch's base. It is neither number in the air - Retry-After takes exactly two legal forms
// (RFC 7231): a non-negative number of seconds, or an IMF-fixdate, whose longest ordinary rendering
// "Tue, 10 Nov 2026 03:52:00 GMT" is 29 runes. 32 therefore cannot truncate any legal value; it only
// bounds a broken upstream that sends prose where a date belongs.
//
// Both were deleted once by a patch that replaced the region they lived in, which is what the type check
// catches (review B-1).
const (
	upstreamProseRunes = 160
	retryAfterRunes    = 32
)

// outage classifies "the dependency did not answer". UNAVAILABLE, never FORBIDDEN and never an untyped
// error: a connection that never completed says nothing about the caller's rights, and filing it as an
// entitlement refusal hides an outage inside an auth bucket. Only a coarse bucket of the transport error
// is carried, so no URL, port or credential text can reach a reply.
func (o identityOp) outage(cause error) error {
	bucket := "unreachable"
	switch l := strings.ToLower(cause.Error()); {
	case strings.Contains(l, "timeout"), strings.Contains(l, "deadline"):
		bucket = "timeout"
	case strings.Contains(l, "refused"):
		bucket = "connection-refused"
	case strings.Contains(l, "reset"):
		bucket = "connection-reset"
	case strings.Contains(l, "eof"):
		bucket = "connection-closed"
	}
	return contracts.NewReasoned(contracts.ErrUnavailable, "identity:"+o.slug+":transport:"+bucket,
		"space: identity did not answer "+o.prose+" ("+bucket+")")
}

// refusal keeps the CLASS Identity chose instead of flattening every unexpected status into an anonymous
// FORBIDDEN. Deliberately narrow, and it changes no authorization decision:
//   - 5xx -> UNAVAILABLE. That says "the dependency failed", not "the cause is known and transient":
//     Identity does not classify or log its own 500 (internal/identity/delegate.go returns an untyped
//     error, so transport can only answer INTERNAL), so this makes the CLASS true and does not claim to
//     cure the underlying fault. A stated Retry-After travels in the text, because this seam renders no
//     headers.
//   - 409 with a CONFLICT envelope -> CONFLICT. Identity uses this contract for
//     decided transaction aborts, including an exhausted journal replay budget;
//     the caller may replay the same idempotent request unchanged.
//   - every other unexpected status -> the seam's existing FORBIDDEN class,
//     carrying Identity's own code and bounded message. A 401/403 cannot become
//     retryable just because its body claims CONFLICT; neither can an untyped 409.
//
// Nothing becomes a success here: every unexpected status still returns no token and fails closed.
func (o identityOp) refusal(resp *http.Response, body []byte) error {
	upCode, upMsg := upstreamStated(body)
	reason := "identity:" + o.slug + ":http-" + fmt.Sprint(resp.StatusCode)
	if upCode != "" {
		reason += ":" + upCode
	}
	message := fmt.Sprintf("space: identity refused the %s with HTTP %d", o.prose, resp.StatusCode)

	// The retry hint is paid for BEFORE the upstream prose is clipped, never appended after the clip.
	// Appending it was a real defect, caught by the executed leg rather than by review: an upstream that
	// both explains itself at length and names a retry moment produced a 248-rune reply against the 243
	// the R1/R2 legs pin - keeping the actionable half was only possible by breaking the promise about
	// the whole reply. Paying first restores the invariant the budget exists to hold, and it is a
	// computable one: hint cost moves out of the prose cost exactly, so a refusal WITH a hint is never
	// longer than the same refusal without one (both land on 232 runes for the delegate seam), and
	// Retry-After is never the part a verbose upstream deletes.
	hint := ""
	if resp.StatusCode >= http.StatusInternalServerError {
		if ra := strings.TrimSpace(resp.Header.Get("Retry-After")); ra != "" {
			// Retry-After has exactly two legal forms and the longest ordinary one (an IMF-fixdate) is
			// 29 runes, so this bound only ever clips a broken upstream.
			hint = " (retry after " + truncateOneLine(ra, retryAfterRunes) + ")"
			reason += ":retry-after"
		}
	}
	if upMsg != "" {
		// Bound the upstream sentence where it ENTERS the reply, not after composing it.
		budget := upstreamProseRunes - len([]rune(hint))
		if budget < 0 {
			// Unreachable for a legal Retry-After (the hint is at most 46 runes of the 160 available),
			// and pinned anyway: truncateOneLine slices r[:max], so a negative bound would PANIC inside
			// a refusal path - which is how a refusal would come back as an INTERNAL.
			budget = 0
		}
		message += ": " + truncateOneLine(upMsg, budget)
	}
	message += hint
	if resp.StatusCode >= http.StatusInternalServerError {
		return contracts.NewReasoned(contracts.ErrUnavailable, reason, message)
	}
	if resp.StatusCode == http.StatusConflict && upCode == "conflict" {
		return contracts.NewReasoned(contracts.ErrConflict, reason, message)
	}
	return contracts.NewReasoned(contracts.ErrForbidden, reason, message)
}
