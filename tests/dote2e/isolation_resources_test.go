package dote2e_test

// j9 resource breadth (director directive 2026-11-10). "A second user is denied
// the first user's data" is a statement about personal data, not about three
// endpoints: the leg used to gate a foreign artifact download, a foreign memory
// search and a foreign GetRun only, and it honestly declared the rest unproven.
// This file attempts every personal-resource kind the lane can reach without a new
// kind of credential - grant tree, AgentConfig, personal profile, tool history and
// HITL records, the note behind the tool result, threads, runs, the durable thread
// stream, chats and memory items - and classifies every answer by its CONTRACT CODE.
//
// Classification, which is the point of the leg:
//   - leaked              - the answer carries a marker owned by the first user, or
//     the platform ACCEPTED a foreign write against an owner object. That is a
//     PRODUCT BLOCKER: the leg fails and names the resource. It is never softened,
//     re-marked or re-scoped.
//   - denied              - HTTP 403/404 whose body carries the contract code
//     FORBIDDEN / NOT_FOUND. The gate branches on that code, never on status text
//     and never on an empty payload.
//   - empty_by_scope      - a 200 empty collection the contract DELIBERATELY answers
//     so a stranger cannot enumerate what exists. Recorded as its own fact, naming
//     where that was decided; never credited as a denial.
//   - denied_by_authority - a refusal whose cause the wire cannot distinguish: the
//     stranger's own principal simply lacks the entitlement (the lane's foreign
//     human holds no tools:approve). Recorded separately and NOT counted as proof of
//     data isolation.
//   - unproven            - attempted, and the answer is none of the above. Blocks
//     the claim.
//   - not_attempted       - the object or the capability was missing, so no claim at
//     all is made. A capability refusal is never laundered into isolation proof: the
//     stranger's actual capabilities are read from its own durable grant row.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	isoLeaked            = "leaked"
	isoDenied            = "denied"
	isoEmptyByScope      = "empty_by_scope"
	isoDeniedByAuthority = "denied_by_authority"
	isoUnproven          = "unproven"
	isoNotAttempted      = "not_attempted"
	// isoSentinelResource names the owner-control trap. It is a probe about the LEG,
	// not a kind of the owner's data, so it never enters the resource counters.
	isoSentinelResource = "owner_control_trap"
	// isoSentinelMissing is what the trap records when it did not run. It is
	// deliberately NOT an acceptable value anywhere: a guard that defaults to
	// "success" is a guard that can be deleted, and mutation M4 on 542c65af proved
	// this leg's trap could be removed with rc=0 (reviewer Descartes).
	isoSentinelMissing = "sentinel_did_not_run"
)

// isoProbe is one attempted access by the second user to one object of the first.
// It carries no payload - only the resource, the operation, the observed status,
// the contract code and the classification. Neither credentials nor user content
// ever reach the artifact.
type isoProbe struct {
	resource  string
	operation string
	status    int
	code      string
	outcome   string
	note      string
}

func (p isoProbe) recorded() map[string]any {
	row := map[string]any{"resource": p.resource, "operation": p.operation,
		"http": p.status, "contract_code": p.code, "outcome": p.outcome}
	if p.note != "" {
		row["note"] = p.note
	}
	return row
}

// isoTarget names one probe. write marks a mutation: for those ANY 2xx is a breach,
// because accepting the stranger's mutation of an owner object is the failure
// whether or not a byte came back. requires names the capability the stranger's
// grant must actually hold, so a capability refusal can never be counted as
// isolation. authorityOnly marks a route whose refusal cause the wire cannot
// distinguish from a missing personal entitlement.
type isoTarget struct {
	resource        string
	path            string
	body            any
	token           string
	ownerRef        string
	leak            func(payload []byte) bool
	write           bool
	requires        string
	authorityOnly   bool
	emptyIsContract bool
	decision        string
	// controlToken names a credential whose OWN successful read of this path proves
	// the route is live. Without it a 404 from an unrouted or mis-mounted path would
	// be credited as an isolation denial - the exact laundering this leg exists to
	// refuse. Only read-only probes carry one: an owner-side control for a write
	// would execute the owner's mutation.
	controlToken string
	// sentinel marks the trap probe: a route the platform does not mount at all. It
	// is never counted as a resource kind; it exists so that weakening the
	// owner-control gate changes a recorded field (see owner_control_sentinel).
	sentinel bool
}

// isoPost posts one probe and reads the contract code out of the reply. It mirrors
// instance.callWithStatus's envelope handling (flat {"code","message"} plus the
// Memory face's nested {"error":{...}}) while keeping the raw bytes, so a
// byte-stream answer (artifact download) is judged on the same terms as JSON.
func (s *instance) isoPost(client *http.Client, target isoTarget) (int, []byte, string) {
	raw, err := json.Marshal(target.body)
	if err != nil {
		return 0, nil, ""
	}
	request, err := http.NewRequest(http.MethodPost, s.Origin+target.path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, ""
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-AHP-Session", "1")
	if target.token != "" {
		request.Header.Set("Authorization", "Bearer "+target.token)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, ""
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return response.StatusCode, payload, ""
	}
	if response.StatusCode/100 == 2 {
		return response.StatusCode, payload, ""
	}
	var envelope struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Error   struct {
			Code    string `json:"code"`
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return response.StatusCode, payload, ""
	}
	return response.StatusCode, payload,
		firstNonEmpty(envelope.Code, envelope.Error.Code, envelope.Error.Reason)
}

// classifyIsolation applies the rules documented at the top of this file.
func classifyIsolation(target isoTarget, status int, code string, payload []byte) (string, string) {
	markerSeen := target.ownerRef != "" && bytes.Contains(payload, []byte(target.ownerRef))
	breach := target.leak != nil && target.leak(payload)
	refusal := (status == http.StatusForbidden || status == http.StatusNotFound) &&
		(code == contracts.ErrForbidden || code == contracts.ErrNotFound)
	switch {
	case markerSeen || breach:
		return isoLeaked, "owner data or an accepted owner mutation reached the second user"
	case target.write && status/100 == 2:
		return isoLeaked, "the platform accepted the second user's write against an owner object"
	case refusal && target.authorityOnly:
		return isoDeniedByAuthority, "refused, but the wire cannot distinguish a missing personal " +
			"entitlement from an owner-binding refusal: not counted as isolation proof"
	case refusal:
		return isoDenied, ""
	case status/100 == 2 && target.emptyIsContract:
		return isoEmptyByScope, "the contract answers an empty collection instead of naming the owner " +
			"object; decided in " + target.decision
	case status/100 == 2:
		return isoUnproven, fmt.Sprintf("a 200 without the owner marker is not a refusal the contract states (http %d)", status)
	default:
		return isoUnproven, fmt.Sprintf("no contract refusal code (http %d code %q)", status, code)
	}
}

// isoAttempt asks one question of the second user. When the probe carries an owner
// control, the SAME path is first read back through the first user's credential: if
// the route does not answer for the owner either, the stranger's answer is not
// evidence of anything and the kind is declared untried.
func (s *instance) isoAttempt(client *http.Client, target isoTarget) isoProbe {
	operation := strings.TrimPrefix(target.path, "/v1/")
	if target.controlToken != "" {
		control := isoTarget{resource: target.resource, path: target.path,
			body: target.body, token: target.controlToken}
		status, _, code := s.isoPost(s.HTTP, control)
		if status/100 != 2 {
			return isoProbe{resource: target.resource, operation: operation, status: status, code: code,
				outcome: isoNotAttempted,
				note: "the route did not answer for the owner either (control http " +
					fmt.Sprintf("%d", status) + " code " + fmt.Sprintf("%q", code) +
					"): the stranger's answer would prove nothing about isolation"}
		}
	}
	status, payload, code := s.isoPost(client, target)
	outcome, note := classifyIsolation(target, status, code, payload)
	return isoProbe{resource: target.resource, operation: operation,
		status: status, code: code, outcome: outcome, note: note}
}

// ownerPersonalData names the first user's objects this leg tries to reach. Every
// value is an identifier or a lane marker created by this run: no user content and
// no credential.
type ownerPersonalData struct {
	ownerTask    string
	contextID    string
	runID        string
	artifactID   string
	artifactMark string
	toolKey      string
	noteMark     string
	pendingKey   string
	memoryMark   string
	agentID      string
	agentRoute   string
	stamp        string
}

// isoCredentials performs the human -> task exchange without killing the run: the
// lane must be able to record "the stranger could not be given this credential"
// instead of dying on it. It returns the token and the task the token is ACTUALLY
// bound to (a narrowed exchange binds to the attenuated child task).
func (s *instance) isoCredentials(client *http.Client, human, task string, capabilities []string) (string, string, error) {
	var out struct {
		Token string `json:"token"`
		Task  string `json:"task"`
	}
	err := s.call(client, "/v1/identity/control/exchange", map[string]any{
		"human_token": human, "task": task, "context": task, "capabilities": capabilities}, human, &out)
	if err != nil {
		return "", "", err
	}
	if out.Token == "" || out.Task == "" {
		return "", "", fmt.Errorf("exchange answered without a token or task")
	}
	return out.Token, out.Task, nil
}

// grantCapabilities reads the stranger's OWN durable grant row, because a task
// token carries no capability claim: the grant row is the only truthful answer to
// "may this credential reach that route at all". Without it a capability refusal
// could be laundered into isolation proof.
func (s *instance) grantCapabilities(tenant, task string) ([]string, error) {
	for _, table := range []string{"ahp_identity.grants_child", "ahp_identity.grants_root"} {
		var capabilities []string
		err := s.Pool.QueryRow(context.Background(),
			`SELECT capabilities FROM `+table+` WHERE tenant=$1 AND task=$2`, tenant, task).Scan(&capabilities)
		if err == nil {
			return capabilities, nil
		}
	}
	return nil, fmt.Errorf("no durable grant row for task %q in tenant %q", task, truncate(tenant, 40))
}

const (
	isoMemoryDecision   = "internal/memory/ahpbridge/access.go (personal scope comes from the verified token, never from the request)"
	isoRegistryDecision = "internal/registry/management.go:44 (profile is read under the verified principal's scope)"
	isoToolsDecision    = "internal/tools/toolsapi/toolsapi.go:398 (the approvals worklist is this tenant's and human-only)"
	isoStateDecision    = "internal/stateapi/handlers.go:48 (every State read is bound to the verified principal)"
	isoSpaceDecision    = "internal/space/handler.go:557 (authorizeThread binds the thread to the principal before any frame is read)"
)

// isoCandidate is one probe declaration: the target, an optional reason this run
// could not attempt it, and the honest explanation shown when it was not attempted.
type isoCandidate struct {
	target   isoTarget
	blocking error
	why      string
}

// isoFixture carries every value the candidate table is built from. The table is a
// PURE function of it, so the owner-control trap can be asserted without a stand:
// a probe that is only inspectable by running the leg is a probe that can be
// deleted quietly (reviewer Descartes, mutation M4 on 542c65af).
type isoFixture struct {
	data           ownerPersonalData
	isoContext     string
	ownerToken     string
	ownerHuman     string
	foreignToken   string
	foreignHuman   string
	granted        map[string]bool
	errThread      error
	errRun         error
	errLineage     error
	ownerThreadID  string
	ownerRunID     string
	ownerRequestID string
	ownerDigest    string
	ownerMemoryRef string
}

// isoCandidates declares every kind of the owner's personal data this leg asks
// about. Adding a kind here is visible to TestIsolationSentinelTrapCannotVanish;
// deleting the trap from this list reddens that test without a stand.
func isoCandidates(f isoFixture) []isoCandidate {
	return []isoCandidate{
		{target: isoTarget{resource: "artifact_bytes", path: "/v1/artifacts/Download",
			body: map[string]any{"id": f.data.artifactID}, token: f.foreignToken,
			ownerRef: f.data.artifactMark, requires: "artifacts", controlToken: f.ownerToken}},
		{target: isoTarget{resource: "memory_personal_search", path: "/v1/memory/Search",
			body: map[string]any{"query": f.data.memoryMark, "scope": "personal"}, token: f.foreignToken,
			ownerRef: f.data.memoryMark, requires: "memory", controlToken: f.ownerToken,
			emptyIsContract: true, decision: isoMemoryDecision}},
		{target: isoTarget{resource: "memory_item_invalidate", path: "/v1/memory/Invalidate",
			body: map[string]any{"memory_ref": f.ownerMemoryRef, "scope": "personal",
				"reason": "dot-journey-isolation-probe-" + f.data.stamp,
				"op_key": f.data.ownerTask + "-iso-mem-" + f.data.stamp},
			token: f.foreignToken, write: true, requires: "memory"},
			blocking: mapMissing(f.ownerMemoryRef, "memory ref"),
			why:      "the owner's memory item never surfaced through the owner's own Search, so no ref was probed"},
		{target: isoTarget{resource: "run_record", path: "/v1/state/GetRun",
			body: map[string]any{"run_id": f.data.runID}, token: f.foreignToken,
			ownerRef: f.data.runID, requires: "state", controlToken: f.ownerToken}},
		{target: isoTarget{resource: "conversation_thread", path: "/v1/state/GetThread",
			body: map[string]any{"thread_id": f.ownerThreadID}, token: f.foreignToken,
			ownerRef: f.ownerThreadID, requires: "state", controlToken: f.ownerToken},
			blocking: f.errThread,
			why:      "this leg never created the owner thread"},
		{target: isoTarget{resource: "run_list_of_owner_thread", path: "/v1/state/ListRuns",
			body: map[string]any{"thread_id": f.ownerThreadID}, token: f.foreignToken,
			ownerRef: f.ownerRunID, requires: "state", controlToken: f.ownerToken,
			emptyIsContract: true, decision: isoStateDecision},
			blocking: f.errRun,
			why:      "this leg never created the owner run inside its thread"},
		{target: isoTarget{resource: "thread_durable_stream", path: "/v1/space/ReadStream",
			body: map[string]any{"thread_id": f.ownerThreadID, "after": 0}, token: f.foreignHuman,
			ownerRef: f.ownerThreadID, controlToken: f.ownerHuman},
			blocking: f.errThread,
			why:      "no owner thread, so no stream was attempted"},
		{target: isoTarget{resource: "chat_list", path: "/v1/space/ListChats",
			body: map[string]any{}, token: f.foreignHuman, ownerRef: f.ownerThreadID, controlToken: f.ownerHuman,
			emptyIsContract: true, decision: isoSpaceDecision}},
		{target: isoTarget{resource: "tool_operation_history", path: "/v1/tools/Get",
			body: map[string]any{"key": f.data.toolKey}, token: f.foreignToken,
			ownerRef: f.data.toolKey, requires: "tools", controlToken: f.ownerToken}},
		{target: isoTarget{resource: "tool_result_note", path: "/v1/tools/GetResult",
			body: map[string]any{"key": f.data.toolKey}, token: f.foreignToken,
			ownerRef: f.data.noteMark, requires: "tools", controlToken: f.ownerToken}},
		{target: isoTarget{resource: "hitl_worklist", path: "/u-ahp/v1/tools/Approvals",
			body: map[string]any{"limit": 50}, token: f.foreignHuman, ownerRef: f.ownerRequestID, controlToken: f.ownerHuman,
			emptyIsContract: true, decision: isoToolsDecision},
			blocking: mapMissing(f.ownerRequestID, "owner approval request id"),
			why:      "the owner's pending HITL request never surfaced, so nothing was looked for"},
		{target: isoTarget{resource: "hitl_approve_by_stranger", path: "/u-ahp/v1/tools/Approve",
			body: map[string]any{"request_id": f.ownerRequestID, "digest": f.ownerDigest,
				"tool": "record_note"}, token: f.foreignHuman, write: true, authorityOnly: true},
			blocking: mapMissing(f.ownerRequestID, "owner approval request id"),
			why:      "no owner request id, so no foreign approval was attempted"},
		{target: isoTarget{resource: "agent_config", path: "/v1/registry/GetConfig",
			body: map[string]any{"agent_id": f.data.agentID}, token: f.foreignToken,
			ownerRef: f.data.agentRoute, requires: "registry", controlToken: f.ownerToken}},
		// The personal profile layer is deliberately NOT probed on this stand. The
		// journey writes no profile document for the owner, so there is no marker of
		// the owner's to look for, and writing one mid-run would add a prompt layer to
		// the Dot's pinned snapshot and change leg j10 - another owner's leg. It is
		// declared untried rather than credited from the stranger's own empty answer.
		{target: isoTarget{resource: "agent_profile", path: "/v1/registry/GetProfile",
			body: map[string]any{}, token: f.foreignToken, requires: "registry",
			emptyIsContract: true, decision: isoRegistryDecision},
			blocking: fmt.Errorf("the owner wrote no personal profile document on this stand"),
			why: "no owner profile exists to look for, and creating one mid-run would " +
				"change the Dot's prompt layer for leg j10"},
		{target: isoTarget{resource: "grant_delegate_into_owner_tree",
			path: "/v1/identity/roots/" + f.data.ownerTask + "/delegate",
			body: map[string]any{"task": f.data.ownerTask + "-iso-child-" + f.data.stamp,
				"context": f.isoContext, "capabilities": []string{"state"}, "ttl": "60s"},
			token: f.foreignToken, write: true, requires: "identity:delegate"},
			blocking: f.errLineage,
			why:      "the lineage route was not proven live, so a 404 here would not be an ownership refusal"},
		{target: isoTarget{resource: "grant_cancel_owner_tree",
			path: "/v1/identity/roots/" + f.data.ownerTask + "/cancel",
			body: map[string]any{}, token: f.foreignToken, write: true, requires: "identity:cancel"},
			blocking: f.errLineage,
			why:      "the lineage route was not proven live, so a 404 here would not be an ownership refusal"},
		// The trap. State answers 404 NOT_FOUND for an operation it does not mount
		// (internal/stateapi/stateapi.go:137), which is exactly the answer this leg
		// refuses to book as an isolation denial. While the owner control is armed the
		// stranger is never asked and the outcome stays not_attempted; if the control
		// is ever removed the stranger's 404 becomes a "denied" and this recorded
		// outcome changes - the leg fails on that, so the gate cannot be weakened quietly.
		{target: isoTarget{resource: isoSentinelResource, path: "/v1/state/LaplaceControlTrap",
			body: map[string]any{}, token: f.foreignToken, controlToken: f.ownerToken, sentinel: true}},
	}
}

// isoSentinelVerify decides whether the owner-control trap RAN and whether the owner
// control kept the stranger away from it. Every state but "the control withheld the
// stranger" is an error, including the states produced by deleting the probe: no
// absence, zero value or unrecorded probe may read as a pass. The trap exists to make
// a removed owner control visible, so it must also make its own disappearance visible
// (reviewer Descartes, mutation M4 on 542c65af: removing the trap gave rc=0 because
// its default was the passing value).
func isoSentinelVerify(candidates []isoCandidate, probes []isoProbe) (isoProbe, error) {
	trap, traps := -1, 0
	for i, item := range candidates {
		if item.target.sentinel {
			traps++
			trap = i
		}
	}
	missing := isoProbe{resource: isoSentinelResource, outcome: isoSentinelMissing,
		note: "the trap probe did not run"}
	switch {
	case traps == 0:
		return missing, fmt.Errorf("the owner-control trap is gone from the candidate table: without it " +
			"a 404 from a route the platform does not mount is credited as an isolation denial and " +
			"nothing in the leg would notice")
	case traps > 1:
		return missing, fmt.Errorf("the leg declares %d sentinel probes; exactly one owner-control trap "+
			"is admissible, otherwise a passing trap can be hidden behind a decoy", traps)
	}
	switch {
	case candidates[trap].target.controlToken == "":
		return missing, fmt.Errorf("the trap carries no owner control: its answer would prove nothing " +
			"about whether the route is mounted")
	case candidates[trap].target.controlToken == candidates[trap].target.token:
		return missing, fmt.Errorf("the trap's owner control is the stranger's own credential: the " +
			"control proves nothing")
	case candidates[trap].target.write:
		return missing, fmt.Errorf("the trap must be read-only: an owner-side control for a write " +
			"would execute the owner's mutation")
	}
	for _, probe := range probes {
		if probe.resource != candidates[trap].target.resource {
			continue
		}
		switch probe.outcome {
		case isoNotAttempted:
			// The intended state: the route does not answer for the owner either, so the
			// stranger was never asked and no dead route is booked as a denial.
			return probe, nil
		case isoSentinelMissing:
			return probe, fmt.Errorf("the trap is declared and the control is armed, but the probe "+
				"recorded %q: the trap did not run", probe.outcome)
		default:
			return probe, fmt.Errorf("the stranger was asked %q (outcome %q, http %d code %q): the owner "+
				"control was bypassed and a dead route's answer is being credited as an isolation denial",
				candidates[trap].target.path, probe.outcome, probe.status, probe.code)
		}
	}
	return missing, fmt.Errorf("the trap is declared but its probe was never recorded: the leg cannot " +
		"claim the owner control held")
}

// runForeignIsolation attempts the personal-resource surface as the second user and
// returns the j9 leg. The owner's objects are created here (thread, run, memory
// item reference, pending HITL request) so every probe points at something that
// exists and belongs to the first user. Anything the lane could not create or was
// not entitled to attempt is declared not_attempted, never quietly dropped.
func (s *instance) runForeignIsolation(t *testing.T, ownerToken, ownerHuman,
	foreignToken, foreignTask, foreignHuman, foreignTenant, foreignSubject string,
	foreignCapabilities []string, data ownerPersonalData) map[string]any {
	t.Helper()

	// The first user's own thread and run: personal durable objects created by this
	// run and nameable by it.
	isoContext := data.contextID + "-iso-" + data.stamp
	var ownerThread struct {
		ID string `json:"id"`
	}
	errThread := s.call(s.HTTP, "/v1/state/CreateThread",
		map[string]any{"key": data.ownerTask + "-iso-thread", "context_id": isoContext}, ownerToken, &ownerThread)
	var ownerRun struct {
		ID string `json:"id"`
	}
	errRun := error(nil)
	if errThread == nil && ownerThread.ID != "" {
		errRun = s.call(s.HTTP, "/v1/state/CreateRun", map[string]any{
			"thread_id": ownerThread.ID,
			"key":       data.ownerTask + "-iso-run",
			"payload":   map[string]any{"input": "iso-private-run-input-" + data.stamp}}, ownerToken, &ownerRun)
	}
	if ownerRun.ID == "" && errRun == nil {
		errRun = fmt.Errorf("State answered without a run id")
	}

	// The owner's still-unapproved HITL request (written by j8) and one owner memory
	// reference: without them the corresponding probes have no real target.
	ownerRequestID, ownerDigest := s.ownerPendingApproval(t, ownerHuman, data.pendingKey)
	ownerMemoryRef := s.ownerMemoryRef(t, ownerToken, data.memoryMark)

	// Where the platform deliberately answers "nothing here" instead of naming an
	// object. Each is a decision with a location, recorded as a fact, never booked
	// as a denial.

	granted := map[string]bool{}
	for _, capability := range foreignCapabilities {
		granted[capability] = true
	}

	// Liveness of the Identity lineage routes, proved on the STRANGER's own tree.
	// Delegating a child under the stranger's own task is a legitimate operation on
	// its own object, touches nothing of the owner's, and answers 2xx only if the
	// route is really mounted - so a dead path can never be booked as "the owner's
	// tree refused the stranger".
	lineageLive := false
	lineageProbe := "not attempted: the stranger's grant lacks identity:delegate"
	if granted["identity:delegate"] {
		status, _, code := s.isoPost(s.Foreign, isoTarget{resource: "grant_delegate_own_tree",
			path: "/v1/identity/roots/" + foreignTask + "/delegate",
			body: map[string]any{"task": foreignTask + "-iso-child-" + data.stamp,
				"context": isoContext, "capabilities": []string{"state"}, "ttl": "60s"},
			token: foreignToken})
		lineageLive = status/100 == 2
		lineageProbe = "own-tree delegate http " + fmt.Sprintf("%d", status) +
			" code " + fmt.Sprintf("%q", code)
	}
	errLineage := error(nil)
	if !lineageLive {
		errLineage = fmt.Errorf("the lineage route did not answer for the stranger's own tree")
	}

	candidates := isoCandidates(isoFixture{data: data, isoContext: isoContext,
		ownerToken: ownerToken, ownerHuman: ownerHuman,
		foreignToken: foreignToken, foreignHuman: foreignHuman, granted: granted,
		errThread: errThread, errRun: errRun, errLineage: errLineage,
		ownerThreadID: ownerThread.ID, ownerRunID: ownerRun.ID,
		ownerRequestID: ownerRequestID, ownerDigest: ownerDigest,
		ownerMemoryRef: ownerMemoryRef})

	probes := []isoProbe{}
	notAttempted := []string{}
	ownerControls := []string{}
	// The default FAILS. Only a trap that ran and was withheld by the owner control
	// can replace it, so deleting the probe cannot be mistaken for a passing leg.
	sentinel := isoProbe{resource: isoSentinelResource, outcome: isoSentinelMissing,
		note: "the trap probe did not run: this is a leg failure, not a pass"}
	for _, item := range candidates {
		if item.target.sentinel {
			sentinel = s.isoAttempt(s.Foreign, item.target)
			probes = append(probes, sentinel)
			continue
		}
		if item.target.requires != "" && !granted[item.target.requires] {
			// A capability the stranger's grant does not hold would produce a refusal
			// that proves nothing about data isolation. Declared, not attempted.
			probes = append(probes, isoProbe{resource: item.target.resource,
				operation: strings.TrimPrefix(item.target.path, "/v1/"), outcome: isoNotAttempted,
				note: "the stranger's own durable grant lacks " + item.target.requires +
					"; a capability refusal would prove nothing about data isolation"})
			notAttempted = append(notAttempted, item.target.resource)
			continue
		}
		if item.blocking != nil {
			probes = append(probes, isoProbe{resource: item.target.resource,
				operation: strings.TrimPrefix(item.target.path, "/v1/"), outcome: isoNotAttempted,
				note: item.why + " (" + truncate(errText(item.blocking), 90) + ")"})
			notAttempted = append(notAttempted, item.target.resource)
			continue
		}
		probe := s.isoAttempt(s.Foreign, item.target)
		probes = append(probes, probe)
		if item.target.controlToken != "" && probe.outcome != isoNotAttempted {
			ownerControls = append(ownerControls, probe.resource)
		}
		if probe.outcome == isoNotAttempted {
			// The owner-side control said the route is not live for the owner either.
			notAttempted = append(notAttempted, probe.resource)
		}
	}

	// Durable fact beside the wire answers: no grant row inside the owner's tenant
	// may carry the stranger's subject. This is the grant kind measured at its
	// source, not inferred from a refusal.
	foreignGrants, grantErr := s.foreignSubjectGrants(foreignSubject)

	// The trap is verified AFTER the probes ran, from the declared table and the
	// recorded answers, so both its disappearance and its bypass change a recorded
	// field. Nothing here is inferred from the absence of a symptom.
	sentinelProbe, sentinelErr := isoSentinelVerify(candidates, probes)
	if sentinelErr != nil {
		sentinel = sentinelProbe
		sentinel.note = sentinelErr.Error()
	}

	denied, leaked, unproven, empty, authority, attempted := []string{}, []string{}, []string{}, []string{}, []string{}, 0
	// Missing evidence is never read as a zero: a failed grant probe would otherwise
	// look exactly like "the stranger holds no grant row in the owner's tenant".
	grantRows := foreignGrants
	if grantErr != nil {
		grantRows = -1
	}
	codes := map[string]string{}
	for _, probe := range probes {
		if probe.resource == isoSentinelResource {
			// The trap is a statement about the leg, not a kind of the owner's data: it
			// never enters the breadth or denial counts, in either direction.
			codes[probe.resource] = probe.code
			continue
		}
		switch probe.outcome {
		case isoDenied:
			denied = append(denied, probe.resource)
		case isoLeaked:
			leaked = append(leaked, probe.resource)
		case isoUnproven:
			unproven = append(unproven, probe.resource)
		case isoEmptyByScope:
			empty = append(empty, probe.resource)
		case isoDeniedByAuthority:
			authority = append(authority, probe.resource)
		}
		if probe.outcome != isoNotAttempted {
			attempted++
		}
		codes[probe.resource] = probe.code
	}
	outcomeOf := func(resource string) (int, string, bool) {
		for _, probe := range probes {
			if probe.resource == resource {
				return probe.status, probe.outcome, probe.outcome == isoDenied
			}
		}
		return 0, isoNotAttempted, false
	}
	artifactStatus, artifactOutcome, artifactDenied := outcomeOf("artifact_bytes")
	runStatus, runOutcome, runDenied := outcomeOf("run_record")
	_, memoryOutcome, memoryDenied := outcomeOf("memory_personal_search")

	recorded := make([]map[string]any, 0, len(probes))
	for _, probe := range probes {
		recorded = append(recorded, probe.recorded())
	}
	leg := map[string]any{
		// The three gates this leg always had, now derived from the contract code
		// instead of "an error happened" / "the list came back empty".
		"artifact_denied": artifactDenied, "artifact_http": artifactStatus,
		"artifact_outcome": artifactOutcome,
		"run_denied":       runDenied, "run_http": runStatus, "run_outcome": runOutcome,
		"memory_empty_or_denied":   memoryDenied || memoryOutcome == isoEmptyByScope,
		"memory_outcome":           memoryOutcome,
		"resources_attempted":      attempted,
		"denied_resource_count":    len(denied),
		"denied_resources":         denied,
		"leaked_resources":         leaked,
		"unproven_resources":       unproven,
		"empty_by_scope_resources": empty,
		"authority_refusals":       authority,
		"not_attempted_resources":  notAttempted,
		"contract_codes":           codes,
		"probes":                   recorded,
		"foreign_grant_task":       foreignTask,
		"foreign_capabilities":     foreignCapabilities,
		"foreign_subject":          foreignSubject,
		"foreign_tenant_mismatch":  foreignTenant != fmt.Sprint(s.Scope["tenant"]),
		// A stranger subject holding a grant row inside the owner's tenant is a
		// breach in the durable record, whatever any endpoint answered.
		"foreign_subject_grant_rows":          grantRows,
		"foreign_subject_grant_probe_failure": truncate(errText(grantErr), 120),
		// Route liveness beside the refusals: what the stranger's own credential can
		// do on its own tree, and the same route's answer on the owner's tree.
		"grant_lineage_route_live":  lineageLive,
		"grant_lineage_route_probe": truncate(lineageProbe, 120),
		"owner_controls_passed":     ownerControls,
		"owner_control_route":       len(ownerControls),
		// The trap's answer, and whether it ran at all. "not_attempted" together with
		// _ran=true means the owner control is armed; "sentinel_did_not_run" means the
		// trap itself is gone or never executed, and both of those are leg failures. No
		// acceptable value is reachable without the probe having executed.
		"owner_control_sentinel":      sentinel.outcome,
		"owner_control_sentinel_http": sentinel.status,
		"owner_control_sentinel_ran":  sentinelErr == nil,
		"owner_control_sentinel_note": truncate(sentinel.note, 220),
	}
	if sentinelErr != nil {
		leg["owner_control_sentinel_failure"] = truncate(sentinelErr.Error(), 260)
	}
	if len(leaked) > 0 {
		leg["breach_note"] = "PRODUCT BLOCKER: the second user reached owner data on " +
			strings.Join(leaked, ", ") + ". This leg does not soften the gate: report the resource, " +
			"the http status and the contract code from probes[] to that plane's owner."
	}
	if len(unproven) > 0 {
		leg["unproven_note"] = "attempted but not a contract refusal (" + strings.Join(unproven, ", ") +
			"): recorded so coverage cannot be claimed from an empty or unexpected answer"
	}
	if len(authority) > 0 {
		leg["authority_note"] = "refused for a reason the wire cannot distinguish from a missing " +
			"personal entitlement (" + strings.Join(authority, ", ") + "): recorded, not counted as isolation"
	}
	if grantErr != nil {
		leg["grant_fact_note"] = "the durable grant probe itself failed, so the grant fact is unproven this run"
	}
	return leg
}

// ownerPendingApproval reads the OWNER's own HITL worklist and returns the request
// id and digest of the call that is still unapproved. An empty answer means "the
// lane could not name the object", which becomes not_attempted - never a denial
// claimed against nothing.
func (s *instance) ownerPendingApproval(t *testing.T, ownerHuman, pendingKey string) (string, string) {
	t.Helper()
	for attempt := 0; attempt < 5; attempt++ {
		var worklist struct {
			Approvals []map[string]any `json:"approvals"`
		}
		if err := s.call(s.HTTP, "/u-ahp/v1/tools/Approvals",
			map[string]any{"limit": 50}, ownerHuman, &worklist); err != nil {
			return "", ""
		}
		for _, item := range worklist.Approvals {
			opKey, _ := item["operation_key"].(string)
			if strings.Contains(opKey, pendingKey) {
				return field(item, "request_id", "id"), field(item, "digest")
			}
		}
		time.Sleep(400 * time.Millisecond)
	}
	return "", ""
}

// ownerMemoryRef reads one owner memory reference through the owner's own personal
// search, so the invalidate probe points at an item that really exists.
func (s *instance) ownerMemoryRef(t *testing.T, ownerToken, marker string) string {
	t.Helper()
	for attempt := 0; attempt < 3; attempt++ {
		var personal struct {
			Facts []memoryFact `json:"facts"`
		}
		if err := s.call(s.HTTP, "/v1/memory/Search",
			map[string]any{"query": marker, "scope": "personal"}, ownerToken, &personal); err == nil {
			for _, fact := range personal.Facts {
				if fact.MemoryRef != "" {
					return fact.MemoryRef
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	return ""
}

// foreignSubjectGrants counts durable grant rows inside the OWNER's tenant that
// carry the stranger's subject. Zero is the only acceptable answer, and a failed
// probe is reported as failed instead of being read as zero.
func (s *instance) foreignSubjectGrants(foreignSubject string) (int, error) {
	var total int
	for _, table := range []string{"ahp_identity.grants_root", "ahp_identity.grants_child"} {
		var count int
		if err := s.Pool.QueryRow(context.Background(),
			`SELECT count(*) FROM `+table+` WHERE tenant=$1 AND subject=$2`,
			s.Scope["tenant"], foreignSubject).Scan(&count); err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func mapMissing(value, name string) error {
	if value != "" {
		return nil
	}
	return fmt.Errorf("no %s", name)
}

// legStrings reads one of the leg's resource lists back in the same process, where
// it is still a []string. Only after a JSON round trip would it be []any, and the
// coverage gates accept both shapes.
func legStrings(value any) []string {
	switch list := value.(type) {
	case []string:
		return list
	case []any:
		out := []string{}
		for _, entry := range list {
			if text, ok := entry.(string); ok {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

// isoTestFixture is the smallest input the REAL candidate table accepts: every value
// is a lane placeholder, so the table - and the owner-control trap inside it - can be
// inspected with no stand, no credential and no user data. Without this the trap was
// only observable by running the live leg, which is how mutation M4 survived a review.
func isoTestFixture() isoFixture {
	return isoFixture{
		data: ownerPersonalData{ownerTask: "task-owner", contextID: "ctx-owner", runID: "run-owner",
			artifactID: "art-owner", artifactMark: "mark-artifact", toolKey: "tool-owner",
			noteMark: "mark-note", pendingKey: "pending-owner", memoryMark: "mark-memory",
			agentID: "agent-owner", agentRoute: "lane-agent", stamp: "stamp"},
		isoContext: "ctx-owner-iso-stamp",
		ownerToken: "owner-token", ownerHuman: "owner-human",
		foreignToken: "stranger-token", foreignHuman: "stranger-human",
		granted: map[string]bool{"artifacts": true, "memory": true, "state": true,
			"tools": true, "registry": true, "identity:delegate": true, "identity:cancel": true},
		ownerThreadID: "thread-owner", ownerRunID: "run-owner",
		ownerRequestID: "req-owner", ownerDigest: "digest-owner", ownerMemoryRef: "memref-owner",
	}
}

// isoSentinelCandidate returns the one trap the table declares, failing the test when
// the table declares anything other than exactly one.
func isoSentinelCandidate(t *testing.T, candidates []isoCandidate) isoCandidate {
	t.Helper()
	found := []isoCandidate{}
	for _, item := range candidates {
		if item.target.sentinel {
			found = append(found, item)
		}
	}
	if len(found) != 1 {
		t.Fatalf("the candidate table must declare exactly one owner-control trap, found %d: "+
			"the trap is what keeps a dead route from being booked as an isolation denial", len(found))
	}
	return found[0]
}

// TestIsolationSentinelTrapCannotVanish is the regression the reviewer's mutation M4
// demanded: deleting the trap probe must redden the lane WITHOUT a stand. It pins the
// declaration (exactly one trap, read-only, distinct owner control), the breadth
// invariant as an inequality, and every way the trap can stop protecting the leg -
// gone, never run, control bypassed, control defanged.
func TestIsolationSentinelTrapCannotVanish(t *testing.T) {
	candidates := isoCandidates(isoTestFixture())
	trap := isoSentinelCandidate(t, candidates)
	if trap.target.write {
		t.Fatal("the trap must be read-only: an owner control for a write would execute the " +
			"owner's mutation")
	}
	if !strings.HasPrefix(trap.target.path, "/v1/state/") {
		t.Fatalf("the trap must sit on a route State does not mount, got %q", trap.target.path)
	}
	kinds := map[string]bool{}
	for _, item := range candidates {
		if item.target.sentinel {
			continue
		}
		if kinds[item.target.resource] {
			t.Fatalf("resource kind %q is declared twice: the breadth count would double-count one question",
				item.target.resource)
		}
		kinds[item.target.resource] = true
	}
	if len(kinds) < 12 {
		t.Fatalf("the requirement is broader than three endpoints; the table declares %d kinds", len(kinds))
	}

	// The trap never ran: absence must not verify.
	if _, err := isoSentinelVerify(candidates, nil); err == nil {
		t.Fatal("a declared trap with no recorded answer must fail the leg: absence is not proof " +
			"that the owner control held")
	}
	// M4 - the trap entry is deleted from the table.
	gone := []isoCandidate{}
	for _, item := range candidates {
		if !item.target.sentinel {
			gone = append(gone, item)
		}
	}
	if _, err := isoSentinelVerify(gone, nil); err == nil {
		t.Fatal("removing the owner-control trap must redden the leg: a guard that passes when it " +
			"is deleted guards nothing")
	}
	// M4 variant - the entry stays but its probe never reaches the record.
	if _, err := isoSentinelVerify(candidates, []isoProbe{
		{resource: "artifact_bytes", outcome: isoDenied}}); err == nil {
		t.Fatal("a trap whose probe never reached the record must fail the leg")
	}
	// M2 - the owner control is bypassed and the stranger is asked a dead route.
	bypassed := isoProbe{resource: isoSentinelResource, outcome: isoDenied, status: 404,
		code: string(contracts.ErrNotFound)}
	if _, err := isoSentinelVerify(candidates, []isoProbe{bypassed}); err == nil {
		t.Fatalf("a trap that answered for the stranger must fail the leg: the answer came from a "+
			"route the platform does not mount (%q)", trap.target.path)
	}
	// A trap whose "owner control" is the stranger's own credential is decoration.
	defanged := isoCandidates(func() isoFixture {
		fixture := isoTestFixture()
		fixture.ownerToken = fixture.foreignToken
		return fixture
	}())
	if _, err := isoSentinelVerify(defanged, []isoProbe{
		{resource: isoSentinelResource, outcome: isoNotAttempted}}); err == nil {
		t.Fatal("a trap with no distinct owner control proves nothing and must fail the leg")
	}
	// The state the leg actually wants: the control withheld the stranger.
	if _, err := isoSentinelVerify(candidates, []isoProbe{
		{resource: isoSentinelResource, outcome: isoNotAttempted}}); err != nil {
		t.Fatalf("a trap withheld by the owner control must verify, got %v", err)
	}
}

// TestIsolationOwnerControlGateIsNotDecorative is the unit half of the trap. It pins
// three things at once: a route that does not answer for the OWNER is recorded as
// not_attempted (never as a denial); the very same answer, asked without the control,
// WOULD have been booked as a denial - which is the mutation the trap in
// runForeignIsolation exists to make observable; and the control does not blind the
// leg to a real refusal once the owner can use the route.
func TestIsolationOwnerControlGateIsNotDecorative(t *testing.T) {
	const ownerToken, strangerToken = "owner-token", "stranger-token"
	dead := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"stateapi: unknown route"}`))
	})
	deadServer := httptest.NewServer(dead)
	defer deadServer.Close()
	stand := &instance{Origin: deadServer.URL, HTTP: deadServer.Client(), Foreign: deadServer.Client(),
		Scope: map[string]any{"tenant": "trap-tenant"}}
	// The trap's own declaration comes from the table the live leg runs, so this unit
	// test cannot drift away from it: drop or move the entry and there is no trap left
	// for this test to reason about.
	target := isoSentinelCandidate(t, isoCandidates(isoTestFixture())).target
	target.token, target.controlToken = strangerToken, ownerToken
	probe := stand.isoAttempt(stand.Foreign, target)
	if probe.outcome != isoNotAttempted {
		t.Fatalf("a route that does not answer for the owner must be %q, got %q: the owner control is bypassed",
			isoNotAttempted, probe.outcome)
	}
	status, payload, code := stand.isoPost(stand.Foreign, target)
	if outcome, _ := classifyIsolation(target, status, code, payload); outcome != isoDenied {
		t.Fatalf("the trap works only because a dead route answers refusal-shaped, got %q", outcome)
	}

	live := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") == "Bearer "+ownerToken {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"runs":[]}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"thread not found"}`))
	})
	liveServer := httptest.NewServer(live)
	defer liveServer.Close()
	armed := &instance{Origin: liveServer.URL, HTTP: liveServer.Client(), Foreign: liveServer.Client(),
		Scope: map[string]any{"tenant": "trap-tenant"}}
	real := isoTarget{resource: "conversation_thread", path: "/v1/state/GetThread",
		body: map[string]any{"thread_id": "th-owner"}, token: strangerToken, controlToken: ownerToken}
	if answered := armed.isoAttempt(armed.Foreign, real); answered.outcome != isoDenied {
		t.Fatalf("a contract refusal on a route the owner can use must stay %q, got %q",
			isoDenied, answered.outcome)
	}
}
