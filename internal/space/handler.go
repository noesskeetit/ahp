package space

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	maxBodyBytes = 4 << 20
)

// Handler is the Space API HTTP endpoint: POST /v1/space/{Operation}.
type Handler struct {
	coordinator *Coordinator
	auth        func(http.Handler) http.Handler
	ops         map[string]func(http.ResponseWriter, *http.Request)
	// requireScopedTask, when true, forbids the Space edge from using the raw
	// Authorization bearer as the State task token: every State call MUST use the
	// scoped Task token resolved by the auth chain (browser human->task exchange or
	// a verified service token injected into context). This is the production gate
	// that keeps a browser human JWT (aud ahp-web) from ever reaching State.
	requireScopedTask bool
}

// HandlerConfig wires the HTTP handler.
type HandlerConfig struct {
	Coordinator *Coordinator
	Auth        func(http.Handler) http.Handler
	// RequireScopedTask enables the production human->task guarantee (see Handler).
	RequireScopedTask bool
}

// NewHandler creates the Space HTTP handler.
func NewHandler(cfg HandlerConfig) *Handler {
	h := &Handler{
		coordinator:       cfg.Coordinator,
		auth:              cfg.Auth,
		requireScopedTask: cfg.RequireScopedTask,
	}
	if h.auth == nil {
		h.auth = func(next http.Handler) http.Handler { return next }
	}
	h.ops = map[string]func(http.ResponseWriter, *http.Request){
		"AcceptTurn":     h.acceptTurn,
		"DispatchTurn":   h.dispatchTurn,
		"CancelTurn":     h.cancelTurn,
		"Steer":          h.steer,
		"SubmitHITL":     h.submitHITL,
		"RespondHITL":    h.respondHITL,
		"GetHITL":        h.getHITL,
		"EmitOutbox":     h.emitOutbox,
		"SaveCheckpoint": h.saveCheckpoint,
		"Recover":        h.recover,
		"GetTurn":        h.getTurn,
		"ListTurns":      h.listTurns,
		"ReadStream":     h.readStream,
		"ListRunHistory": h.listRunHistory,
		"ListChats":      h.listChats,
		"DeleteChat":     h.deleteChat,
	}
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// State the correlation id before anything decides the outcome, and ABOVE the auth wrapper: the
	// rejection that happens first - unknown route, no verified principal - is the one a reader most
	// needs to find, and a caller can only quote an id it was handed. Everything downstream reads this
	// same value back out of the context, including the run-grant refusal logged before any run row
	// exists (logRunGrantRefusal), so one token joins the reply, the log record and the caller's report.
	requestID := resolveRequestID(r.Header.Get(requestIDHeader))
	w.Header().Set(requestIDHeader, requestID)
	r = r.WithContext(WithRequestID(r.Context(), requestID))

	// SSE tail of a thread's durable stream is the one GET route on the Space edge
	// (PDF rev 3.2: the browser consumes an SSE replay cursor). It is a read-only
	// projection of ReadStream; every other route stays POST /v1/space/{Operation}.
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/space/stream/") {
		h.auth(http.HandlerFunc(h.serveStream)).ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodPost {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "space: unknown route"))
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/v1/space/")
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "space: unknown route"))
		return
	}
	fn, ok := h.ops[rest]
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "space: unknown operation "+rest))
		return
	}
	h.auth(http.HandlerFunc(fn)).ServeHTTP(w, r)
}

func (h *Handler) acceptTurn(w http.ResponseWriter, r *http.Request) {
	var req AcceptTurnRequest
	if !readJSON(w, r, &req) {
		return
	}
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	resp, err := h.coordinator.AcceptTurn(r.Context(), token, scope, req)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) dispatchTurn(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.coordinator.DispatchTurn(r.Context(), token, scope); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (h *Handler) cancelTurn(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TurnID string `json:"turn_id"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	_, err := h.authorizeTurn(r, req.TurnID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// Tier-2: pass the AUTHORIZED caller scope so CancelTurn revokes THIS run's child grant
	// subtree by its durable child task ref over the workload cancel-subtree (Identity binds
	// the revoke to the grant scope; a revoke failure is surfaced as an error, never a false
	// 200, and a sibling run's grant is never touched).
	callerToken, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.coordinator.CancelTurn(r.Context(), req.TurnID, callerToken); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// The cancel itself is done: the run's credential is revoked and its row closed, so the
	// answer stays 200 when the terminal journal write failed - a journal failure must not
	// resurrect a credential the user just killed. What it must not do is say nothing. The
	// measured hole (forum 20261101T104800Z §1 item 4) was exactly this: {"ok":true} about a
	// thread whose durable transcript never got its terminal event, with the failure visible
	// only in the server's log and to a client that reads State directly.
	durable := h.coordinator.CancelTerminalFrameDurable(req.TurnID)
	resp := map[string]any{"ok": true, "terminal_frame_durable": durable}
	if !durable {
		resp["note"] = "run canceled and its grant revoked, but this turn's terminal frame is not in the thread's durable stream: a reader of State sees the transcript stop without a terminal event"
	}
	transport.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) steer(w http.ResponseWriter, r *http.Request) {
	var req SteerRequest
	if !readJSON(w, r, &req) {
		return
	}
	if _, err := h.authorizeTurn(r, req.TurnID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.coordinator.Steer(r.Context(), token, req); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (h *Handler) submitHITL(w http.ResponseWriter, r *http.Request) {
	var req struct {
		HITLSubmitRequest
		RunID string `json:"run_id"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if _, err := h.authorizeTurn(r, req.TurnID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	hitl, err := h.coordinator.SubmitHITL(r.Context(), token, req.HITLSubmitRequest, req.RunID, "", 1)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, hitl)
}

func (h *Handler) respondHITL(w http.ResponseWriter, r *http.Request) {
	var req struct {
		HITLRespondRequest
		ThreadID string `json:"thread_id"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if _, err := h.authorizeThread(r, req.ThreadID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.coordinator.RespondHITL(r.Context(), token, req.HITLRespondRequest, req.ThreadID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// getHITL reads one gate as the thread's durable record states it, so an approval card can still ask
// what became of its gate after a runner restart - a question only the call that created the gate
// could answer before, while the decision itself was durable the whole time.
func (h *Handler) getHITL(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ThreadID string `json:"thread_id"`
		HITLID   string `json:"hitl_id"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if _, err := h.authorizeThread(r, req.ThreadID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	gate, err := h.coordinator.GetHITL(r.Context(), token, req.ThreadID, req.HITLID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, gate)
}

func (h *Handler) emitOutbox(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OutboxEmitRequest
		ThreadID string `json:"thread_id"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if _, err := h.authorizeTurn(r, req.TurnID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	entry, err := h.coordinator.EmitOutbox(r.Context(), token, req.OutboxEmitRequest, req.ThreadID, "", 1)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, entry)
}

func (h *Handler) saveCheckpoint(w http.ResponseWriter, r *http.Request) {
	var req CheckpointRequest
	if !readJSON(w, r, &req) {
		return
	}
	if _, err := h.authorizeTurn(r, req.TurnID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	resp, err := h.coordinator.SaveCheckpoint(r.Context(), token, req)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) recover(w http.ResponseWriter, r *http.Request) {
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	result, err := h.coordinator.Recover(r.Context(), token)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// Visibility is subject-scoped: a subject must not observe which turns other
	// subjects have (even as unknown) via the recovery report. The state mutation
	// above is worker-global; only the response is filtered to the caller scope.
	if scope, perr := principal(r); perr == nil {
		result.Recovered = filterTurnsScope(result.Recovered, scope)
		result.Unknown = filterTurnsScope(result.Unknown, scope)
	}
	transport.WriteJSON(w, http.StatusOK, result)
}

func (h *Handler) getTurn(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TurnID string `json:"turn_id"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	t, err := h.turnForRead(r, req.TurnID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, t)
}

// listRunHistory returns the durable run history of an owned thread (restart-safe
// via State). Thread ownership is enforced by authorizeThread (foreign -> 404); the
// task token bounds scope server-side. NOTE: thread-ownership uses the coordinator's
// in-memory thread-scope map, so facade access after a full restart (with no prior
// accept in this process) additionally requires State thread enumeration (ListThreads)
// — until then this is fail-closed (404), never a leak.
func (h *Handler) listRunHistory(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ThreadID string `json:"thread_id"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if _, err := h.authorizeThread(r, req.ThreadID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	runs, err := h.coordinator.ListRunHistory(r.Context(), token, req.ThreadID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

// listChats returns one newest-first page of the authenticated subject's own
// threads (durable, restart-safe). Scope is taken from the verified principal and
// enforced server-side by State; the browser never sends subject/tenant.
func (h *Handler) listChats(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Cursor string `json:"cursor"`
		Limit  int    `json:"limit"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if _, err := principal(r); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	chats, next, hasMore, err := h.coordinator.ListChats(r.Context(), token, req.Cursor, req.Limit)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"threads": chats, "next_cursor": next, "has_more": hasMore})
}

// deleteChat deletes the caller's own chat (durable thread + everything State
// cascades from it). Order matters twice over:
//
//   - AUTHORIZATION FIRST, on the same no-leak rule as every other thread op
//     (foreign or unknown thread => 404, never 403), so the delete can never be used
//     to probe whether some other subject owns a thread id;
//   - then the scope-bound task token, so State re-checks tenant/deployment/subject
//     itself and the thread id in the body carries no authority.
//
// A chat with a run still in flight is refused with 409 rather than deleted out from
// under it; see Coordinator.DeleteChat for why that refusal is a security property
// (the intent journal is the duplicate-effect barrier) and not a UX preference.
func (h *Handler) deleteChat(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ThreadID string `json:"thread_id"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if _, err := h.authorizeThread(r, req.ThreadID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.coordinator.DeleteChat(r.Context(), token, req.ThreadID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"deleted": true, "thread_id": req.ThreadID})
}

// listTurns answers the caller's own turns. The in-memory execution table is authoritative
// for whatever this process is running; when it cannot answer (empty, or a thread this
// process never executed) the durable Run rows in State are rebuilt by turn_history.go and
// merged in, live copy winning per id. Before this, an as-runner restart dropped the whole
// turn index and this endpoint answered 200 with turns=[] while the threads stayed visible
// through ListChats - the user lost the visible history of work that State still had.
func (h *Handler) listTurns(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	threadID, cursor, limit := turnListRequest(r)
	live := h.coordinator.listTurns()

	scoped := make([]Turn, 0, len(live))
	byID := make(map[string]Turn, len(live))
	for _, t := range live {
		if t.Scope == scope {
			scoped = append(scoped, t)
			byID[t.ID] = t
		}
	}
	resp := map[string]any{"turns": scoped}

	if threadID != "" {
		// Same authz as ReadStream/ListRunHistory: the thread must belong to the verified
		// principal, then State is read with the caller's own scope-bound task token.
		if _, err := h.authorizeThread(r, threadID); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		token, err := h.stateToken(r)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		durable, err := h.coordinator.HydrateTurnsFromState(r.Context(), token, threadID, scope)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		merged := make([]Turn, 0, len(durable)+len(byID))
		for _, t := range durable {
			if lt, ok := byID[t.ID]; ok {
				merged = append(merged, lt) // live record wins: it has steps/checkpoint
				delete(byID, t.ID)
				continue
			}
			merged = append(merged, t)
		}
		for _, t := range live {
			if t.Scope == scope && t.ThreadID == threadID {
				if _, stillOurs := byID[t.ID]; stillOurs {
					merged = append(merged, t)
				}
			}
		}
		resp["turns"] = merged
		transport.WriteJSON(w, http.StatusOK, resp)
		return
	}

	// Unscoped view (the SPA’s "my turns" screen: POST with an empty body).
	// The contract for ListTurns {} is the caller's turn list, not "the turns this process
	// happens to remember", so the durable rows are read on EVERY such call - bounded, with the
	// caller's own scope-bound task token, merged over the live index. Rebuilding only when the
	// live table came back empty (the first version of this fix) still lied in the case the
	// deputy measured at 02:26Z on the deployed pin: two warm turns from 02:16/02:20 made the
	// live table non-empty, so cold history stayed invisible AND the answer carried no
	// durable_partial to explain itself. Silence about missing data is the defect; an empty or
	// short list without a flag is the same defect in a warmer costume.
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	walk, err := h.coordinator.HydrateRecentTurnsFromState(r.Context(), token, cursor, scope)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	merged := mergeTurnPages(scoped, walk.Turns, byID)
	partial := walk.Partial
	if limit > 0 && len(merged) > limit {
		merged = merged[:limit]
		partial = true // rows are knowingly held back: say so instead of truncating quietly
	}
	resp["turns"] = merged
	if partial {
		resp["durable_partial"] = true
		resp["has_more"] = true
	}
	if walk.NextCursor != "" {
		resp["next_cursor"] = walk.NextCursor
	}
	transport.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) readStream(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ThreadID string `json:"thread_id"`
		After    int64  `json:"after"`
		Limit    int    `json:"limit"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if _, err := h.authorizeThread(r, req.ThreadID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	events, err := h.coordinator.state.ReadStream(r.Context(), token, req.ThreadID, req.After, req.Limit)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (c *Coordinator) listTurns() []Turn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Turn, 0, len(c.turns))
	for _, t := range c.turns {
		out = append(out, *t)
	}
	return out
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	// FIX3 (decision 2026-10-30-exchange-scope-authority-model): enforce maxBodyBytes
	// by reading AT MOST maxBodyBytes+1 bytes; over-cap => 413. This never truncates a
	// body into the strict decoder (which could accept a valid JSON prefix and thereby
	// hide smuggled trailing bytes). The EXACT bytes are then strict-decoded by the
	// authoritative transport reader (DisallowUnknownFields + dup-key/trailing/empty).
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "read body"))
		return false
	}
	if int64(len(body)) > maxBodyBytes {
		transport.WriteJSON(w, http.StatusRequestEntityTooLarge,
			contracts.Error{Code: "BODY_TOO_LARGE", Message: "request body exceeds limit"})
		return false
	}
	r.Body = io.NopCloser(bytes.NewReader(body)) // exact bytes, no truncation
	if err := transport.ReadJSON(w, r, v, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return false
	}
	return true
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	return strings.TrimPrefix(h, "Bearer ")
}

func principal(r *http.Request) (contracts.Scope, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden, "no verified principal")
	}
	return p.Scope, nil
}

// authorizeTurn verifies that the authenticated principal owns the given turn.
// It returns a not-found (not forbidden) error when the turn is absent or owned
// by a different scope, so the Space API never leaks the existence of another
// subject's turns. Durable isolation is additionally enforced by the State API
// via the task-token principal; this guards the coordinator's local turn map.
func (h *Handler) authorizeTurn(r *http.Request, turnID string) (contracts.Scope, error) {
	scope, err := principal(r)
	if err != nil {
		return scope, err
	}
	t := h.coordinator.getTurn(turnID)
	if t == nil || t.Scope != scope {
		return scope, contracts.NewError(contracts.ErrNotFound, "turn not found")
	}
	return scope, nil
}

// stateToken returns the scope-bound State Task token to use for the downstream
// State call. It is resolved server-side by the auth chain (browser human->task
// exchange via ExchangeMiddleware, or a verified service token injected via
// WithScopedTask) and read from context. In production RequireScopedTask is on, so
// the raw Authorization bearer is NEVER used and a browser human JWT can never be
// forwarded to State. When it is off (internal service callers and the offline test
// double), the bearer is already a valid scoped Task token and is used directly.
func (h *Handler) stateToken(r *http.Request) (string, error) {
	if tok, ok := ScopedTaskFromContext(r.Context()); ok {
		return tok, nil
	}
	if h.requireScopedTask {
		return "", contracts.NewError(contracts.ErrForbidden, "space: scoped task token not resolved")
	}
	return bearerToken(r), nil
}

// authorizeThread verifies that the authenticated principal owns the given
// thread, using the same not-found-on-cross-scope semantics as authorizeTurn.
func (h *Handler) authorizeThread(r *http.Request, threadID string) (contracts.Scope, error) {
	scope, err := principal(r)
	if err != nil {
		return scope, err
	}
	owner, ok := h.coordinator.scopeOfThread(threadID)
	if ok {
		if owner != scope {
			return scope, contracts.NewError(contracts.ErrNotFound, "thread not found")
		}
		return scope, nil
	}
	// In-memory miss (e.g. after a process restart that dropped the turn table):
	// fall back to the durable, scope-isolated State GetThread. The caller's own
	// scope already came from the verified principal; GetThread is filtered by the
	// presented task token's tenant/deployment/subject, so it resolves the thread
	// ONLY within this caller's scope. A foreign or unknown thread stays not-found
	// (404), preserving fail-closed isolation across restarts.
	sToken, terr := h.stateToken(r)
	if terr != nil {
		return scope, contracts.NewError(contracts.ErrNotFound, "thread not found")
	}
	if err := h.coordinator.VerifyThreadOwnership(r.Context(), sToken, threadID); err != nil {
		return scope, contracts.NewError(contracts.ErrNotFound, "thread not found")
	}
	return scope, nil
}

// filterTurnsScope returns only the turns owned by the given scope.
func filterTurnsScope(turns []Turn, scope contracts.Scope) []Turn {
	out := make([]Turn, 0, len(turns))
	for _, t := range turns {
		if t.Scope == scope {
			out = append(out, t)
		}
	}
	return out
}
