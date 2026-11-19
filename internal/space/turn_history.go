package space

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Durable turn history for the read side (the "restart keeps the history" requirement).
//
// Coordinator.turns is an IN-MEMORY execution index. The durable fact of a turn is State's
// Run row: AcceptTurn creates it with turn.ID = run.ID and the payload
// {"type":"turn","input":...,"max_steps":...,"agent_config":...}. Before this file the only
// production writer into that table was the orphan sweep, which claims runs that are STILL
// claimable - so every already-finished turn disappeared from ListTurns/GetTurn the moment
// the runner restarted (200 with turns=[], then 404 "turn not found"), while ListChats kept
// answering because it reads State directly. This is the read-side counterpart of the durable
// fallback authorizeThread already documents, and it changes no contract: every read below
// goes through the caller's OWN scope-bound State task token, exactly like ListRunHistory,
// so State itself keeps a foreign caller from ever seeing another tenant's rows.
//
// Design limits, stated rather than hidden:
//   - This index is READ-ONLY. Mutating operations (CancelTurn, Steer, SubmitHITL) keep
//     answering NOT_FOUND for a turn this process does not execute: a cold row cannot
//     reconstruct the run's own child grant, and a false 200 from a credential-less cancel
//     would be worse than an honest 404.
//   - A cold row carries no Steps/Checkpoint reconstruction; the durable stream
//     (ReadStream) stays the source for step detail. Status/thread/input/max_steps are real.
//   - The unscoped ListTurns (the SPA posts an empty body) fans out over the first page of
//     the caller's own threads, bounded by turnHistoryThreads and turnHistoryBudget. When the
//     budget cuts the fan-out short, the answer says so with durable_partial=true instead of
//     silently looking complete.

// turnHistoryIndex is the rebuilt, read-only view of turns this process did not create.
type turnHistoryIndex struct {
	mu       sync.RWMutex
	byID     map[string]*Turn
	byThread map[string][]*Turn
}

func newTurnHistoryIndex() *turnHistoryIndex {
	return &turnHistoryIndex{byID: map[string]*Turn{}, byThread: map[string][]*Turn{}}
}

func (h *turnHistoryIndex) get(turnID string) (Turn, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	t, ok := h.byID[turnID]
	if !ok {
		return Turn{}, false
	}
	return *t, true
}

func (h *turnHistoryIndex) ofThread(threadID string) ([]Turn, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	rows, ok := h.byThread[threadID]
	if !ok {
		return nil, false
	}
	out := make([]Turn, 0, len(rows))
	for _, t := range rows {
		out = append(out, *t)
	}
	return out, true
}

// forgetThread drops every cached row of a thread that no longer exists durably.
// Without it a deleted chat keeps answering: byThread would keep serving the whole
// (terminal) page to ListTurns and byID would keep serving GetTurn for turns whose
// run rows State has already cascaded away. The durable rows are gone for good, so
// any cached answer about them is a lie by construction, not a stale-but-harmless
// optimization.
func (h *turnHistoryIndex) forgetThread(threadID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, t := range h.byThread[threadID] {
		delete(h.byID, t.ID)
	}
	delete(h.byThread, threadID)
}

// store caches what was read from State under two separate rules, because the two caches
// answer different questions - and a stale answer here would trade the reported bug for a
// quieter one:
//
//   - a turn is cached BY ID only when its status is terminal. A non-terminal turn is still
//     moving (the orphan sweep claims it within seconds), so pinning "running" would let
//     GetTurn keep answering with a status that stopped being true long before any restart.
//   - a thread's page is cached only when pageComplete asserts the slice IS the whole thread
//     and every turn in it is terminal. A single-turn GetTurn must never become the cached
//     answer to ListTurns for that thread - it would hide every other turn of the user's own
//     thread, which is a data-loss bug wearing a performance hat.
func (h *turnHistoryIndex) store(threadID string, turns []Turn, pageComplete bool) {
	allTerminal := len(turns) > 0
	for i := range turns {
		if !turns[i].Status.IsTerminal() {
			allTerminal = false
			break
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if allTerminal {
		for i := range turns {
			cp := turns[i]
			h.byID[cp.ID] = &cp
		}
	}
	if pageComplete && allTerminal {
		rows := make([]*Turn, 0, len(turns))
		for i := range turns {
			cp := turns[i]
			rows = append(rows, &cp)
		}
		h.byThread[threadID] = rows
	}
}

// turnRunPayload is the AcceptTurn run payload, read back from State.
type turnRunPayload struct {
	AgentInstallationID  string          `json:"agent_installation_id,omitempty"`
	AgentTarget          *AgentTargetRef `json:"agent_target,omitempty"`
	ProjectID            string          `json:"project_id,omitempty"`
	ProjectMemoryProfile string          `json:"project_memory_profile,omitempty"`
	Type                 string          `json:"type"`
	Input                json.RawMessage `json:"input"`
	MaxSteps             int             `json:"max_steps"`
	AgentConfig          *AgentConfigRef `json:"agent_config,omitempty"`
	// FailureCause is the refusing plane's verbatim sentence, recorded on a terminal
	// release (state.RunPayloadFailureCause). A turn payload that never carried one
	// decodes to "" - nothing is invented for it here.
	FailureCause string `json:"failure_cause,omitempty"`
}

// spaceTurnStatus names a durable State run status in Space's own turn vocabulary.
//
// The two vocabularies agree on pending/running/done/failed/canceled, and a value outside either
// is surfaced unchanged: an invented status is a lie about a durable fact. A parked run is the one
// place they genuinely differ. State says `waiting` plus the reason for the wait (wait_for); Space
// names the state after the gate it opened, and SubmitHITL sets `awaiting_hitl` in memory - which
// is the status a reader keys an approval card on. A projection that copies the run status through
// verbatim therefore answers one thing while the runner still remembers the turn and another thing
// after a restart, and the second answer is a word Space never published.
//
// Reading the reason is what makes this a projection rather than a rename: only a wait whose target
// IS the HITL answer becomes `awaiting_hitl`. A run parked on any other wait target keeps `waiting`,
// because Space did not open that gate, and calling it a HITL request nobody submitted would invent
// a gate the platform never asked for.
//
// `awaiting_steering` is deliberately NOT derived here. Steering records an inbox event and never
// parks the run, so no durable run state is evidence of a steering gate, and a cold read of a
// steering turn says `running` because that is State's own fact. Closing that warm/cold split needs
// a durable marker for the gate first; it is reported as a gap rather than worked around by guess.
func spaceTurnStatus(run runResponse) TurnStatus {
	if run.Status == runStatusWaiting && run.WaitFor == hitlWaitFor {
		return TurnAwaiting
	}
	return TurnStatus(run.Status)
}

// turnFromRun maps one durable Run row back into the turn the Space API answers with.
// The second result is false when the row is not a turn (an inbox or other run payload):
// a thread's run list is not the same set as its turns, and pretending otherwise would
// invent turns that a user never asked for.
//
// Status is State's status expressed in Space's vocabulary (see spaceTurnStatus): every status the
// two planes agree on, and every status outside both, is surfaced unchanged rather than
// approximated, because an invented status is a lie about a durable fact.
func turnFromRun(run runResponse, scope contracts.Scope) (Turn, bool) {
	if strings.TrimSpace(run.ID) == "" || strings.TrimSpace(run.ThreadID) == "" {
		return Turn{}, false
	}
	var p turnRunPayload
	if len(run.Payload) > 0 {
		if err := json.Unmarshal(run.Payload, &p); err != nil {
			return Turn{}, false
		}
	}
	if p.Type != "turn" {
		return Turn{}, false
	}
	created, hasCreated := turnIDTime(run.ID)
	turn := Turn{
		AgentInstallationID:  p.AgentInstallationID,
		AgentTarget:          p.AgentTarget,
		ProjectID:            p.ProjectID,
		ProjectMemoryProfile: p.ProjectMemoryProfile,
		ID:                   run.ID,
		ThreadID:             run.ThreadID,
		Scope:                scope, // scope-bound token: State returned this row to THIS caller only
		Input:                p.Input,
		Status:               spaceTurnStatus(run),
		MaxSteps:             p.MaxSteps,
		AgentConfig:          p.AgentConfig,
		RunID:                run.ID,
		RunTask:              run.TaskID, // resource identifier, never a credential
	}
	// A run-row cause is the run's own durable record of why a plane refused it, so a
	// cold read starts from it. The transcript keeps precedence in HydrateTurnFromState:
	// the frame the run emitted while executing outranks the release-time note, and a
	// run that recorded neither is reported as exactly that.
	if run.Status == string(TurnFailed) && p.FailureCause != "" {
		turn.FailureReason = p.FailureCause
	}
	if hasCreated {
		turn.CreatedAt = created
		// State's published Run contract carries no mutation instant, so a rebuilt turn
		// cannot report when it last changed. Restamping it with the time of THIS read would
		// be invented data and would reorder the history on every refresh; an id that follows
		// the product convention at least states the real creation instant, which is what the
		// history list is sorted and grouped by. The exact completion instant is a gap in the
		// State Run contract (checkpoints and stream events do carry created_at) and is
		// reported as such rather than worked around here with one extra read per turn.
		turn.UpdatedAt = created
	}
	return turn, true
}

// turnIDTime recovers the creation instant encoded in the sortable-UTC prefix that
// contracts.NewID gives every identifier (see idTimeLayout in pkg/contracts). It is the
// product's own convention, not a guess about the payload: an id that does not match the
// layout exactly reports false, so no timestamp is manufactured for it.
const idTimeLayout = "20060102T150405.000000000Z"

func turnIDTime(id string) (time.Time, bool) {
	if len(id) <= len(idTimeLayout) || id[len(idTimeLayout)] != '_' {
		return time.Time{}, false
	}
	t, err := time.Parse(idTimeLayout, id[:len(idTimeLayout)])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// HydrateTurnsFromState rebuilds the turns of ONE thread from State and merges them into the
// read-only history index. token MUST be the caller's own scope-bound State task token.
func (c *Coordinator) HydrateTurnsFromState(ctx context.Context, token, threadID string, scope contracts.Scope) ([]Turn, error) {
	if strings.TrimSpace(threadID) == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: thread_id required")
	}
	if cached, ok := c.turnHistory.ofThread(threadID); ok {
		return cached, nil
	}
	runs, err := c.state.ListRuns(ctx, token, threadID)
	if err != nil {
		return nil, fmt.Errorf("hydrate turns: %w", err)
	}
	out := make([]Turn, 0, len(runs))
	for _, r := range runs {
		if t, ok := turnFromRun(r, scope); ok {
			out = append(out, t)
		}
	}
	c.turnHistory.store(threadID, out, true)
	return out, nil
}

// HydrateTurnFromState rebuilds ONE turn by id. It returns (nil, nil) when State reports the
// row as genuinely absent (NOT_FOUND), which stays a 404 to the caller; every other failure
// is returned as an error so a read that did not happen is never answered as "no turns".
func (c *Coordinator) HydrateTurnFromState(ctx context.Context, token, turnID string, scope contracts.Scope) (*Turn, error) {
	if strings.TrimSpace(turnID) == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: turn_id required")
	}
	if t, ok := c.turnHistory.get(turnID); ok {
		return &t, nil
	}
	run, err := c.state.GetRun(ctx, token, turnID)
	if err != nil {
		if contracts.Code(err) == contracts.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("hydrate turn: %w", err)
	}
	if run == nil {
		return nil, nil
	}
	t, ok := turnFromRun(*run, scope)
	if !ok {
		return nil, nil
	}
	// D8's rule, applied to the path D8 left open. A turn this process no longer remembers was
	// rebuilt from State's Run row, which carries a STATUS and never carried a cause - so without
	// this the reader gets `status=failed` with no explanation again, one restart after the bug
	// it was supposed to have. The cause is read where it actually lives: the durable transcript,
	// the same frames the SSE reader sends, under the CALLER's own task token. It is never copied
	// into the Run row, because two places that say why a run died are two places that can
	// disagree - the argument this file already makes about status.
	if t.Status == TurnFailed {
		cause, found, cerr := c.transcriptCause(ctx, token, t.ThreadID, t.ID)
		switch {
		case cerr != nil:
			// An unreadable transcript proves nothing about the cause, so it states nothing NEW:
			// claiming "the record carries no cause" on a failed read would be an invented
			// negative about durable data. The status above is still State's own fact, and a
			// run-row cause already read from that same fact is kept, not thrown away.
			slog.Warn("space: cold turn cause could not be read from the durable transcript",
				"turn_id", t.ID, "thread_id", t.ThreadID, "err", cerr.Error())
		case found:
			// The transcript frame wins over the run-row note: it is what the run itself
			// emitted while executing, and the two never coexist for one failure anyway
			// (a preflight refusal writes no frame precisely because it wrote the note).
			t.FailureReason = cause
		default:
			// No cause frame and no run-row note: a turn refused before Core ever started
			// writes no stream frame at all, on purpose, and a release that recorded no cause
			// recorded nothing. Saying so keeps the failed turn from going silent again
			// without pretending to quote a cause that was never recorded - but a turn whose
			// Run row DOES carry the plane's sentence is not nameless, and gets it verbatim.
			if t.FailureReason == "" {
				t.FailureReason = coldCauseNotInTranscript
			}
		}
	}
	// A GetTurn page is one row, not the thread: pageComplete=false keeps it out of the
	// thread cache so the next ListTurns still reads the whole thread.
	c.turnHistory.store(t.ThreadID, []Turn{t}, false)
	return &t, nil
}

// coldCauseNotInTranscript is what a cold read of a failed turn reports when the durable
// transcript really does hold no cause frame for it. It is a statement about the record, not a
// paraphrase of a cause: the two must stay tellable apart, because the live projection names
// causes it quoted from the run.
const coldCauseNotInTranscript = "space: durable transcript carries no cause frame for this turn"

// transcriptCause rebuilds the cause a failed turn emitted from the thread's durable stream.
//
// Why a scan and not a field: the stream event row carries a key, a kind and a payload, and no
// turn id, so the only honest way to attribute a frame to a turn is the writer's own key form -
// space.StepKey is turnID + ":" + step, the same prefix durableStepTranscript already reads back for
// the same reason. Kind must be the error kind because a turn also owns terminal `status` frames
// (the worker's and the cancel's), and a cancel frame is a user decision, not a failure cause.
//
// Bounded the same way the high-water read is: one page at a time, hard cap on frames examined.
// The FIRST error frame wins, matching the live rule - the run stops at its first error, so that
// frame is the one that killed the turn.
//
// Running out of scan budget is reported as an error, NOT as "no cause frame": a thread longer
// than the bound proves nothing about what its transcript holds, and an answer built on a
// truncated read would be a claim about durable data that this read never verified.
var errTranscriptScanCapped = errors.New("space: cause scan reached its bound before the thread's transcript was drained")

func (c *Coordinator) transcriptCause(ctx context.Context, token, threadID, turnID string) (string, bool, error) {
	const page = stateStreamPageSize
	const maxScan = 200000
	prefix := turnID + ":"
	scanned := 0
	after := int64(0)
	for {
		evs, err := c.state.ReadStream(ctx, token, threadID, after, page)
		if err != nil {
			return "", false, err // cannot read the transcript: prove nothing either way
		}
		for _, ev := range evs {
			scanned++
			after = ev.Sequence
			if ev.Kind != string(StepError) || !strings.HasPrefix(ev.Key, prefix) {
				continue
			}
			var frame struct {
				Content json.RawMessage `json:"content"`
			}
			cause := ""
			if json.Unmarshal(ev.Payload, &frame) == nil {
				cause = stepErrorCause(frame.Content)
			}
			if cause == "" {
				// Not the shape Core writes. Reported verbatim rather than dropped: an
				// unreadable cause still beats an absent one, exactly as stepErrorCause says.
				cause = stepErrorCause(ev.Payload)
			}
			if cause == "" {
				continue
			}
			return cause, true, nil
		}
		if len(evs) < page {
			return "", false, nil // fully drained: the transcript really holds no error frame here
		}
		if scanned >= maxScan {
			return "", false, errTranscriptScanCapped
		}
	}
}

// TurnHistoryWalk is one bounded pass of the unscoped history rebuild. The counters are part
// of what this function returns on purpose: "how many of the caller's threads did we actually
// read, and how long did that take" is exactly what the caller needs in order to decide
// whether the answer it is about to send is honest, and that cannot be inferred from the turn
// list itself (a short list is indistinguishable from a walk that never ran).
type TurnHistoryWalk struct {
	Turns         []Turn
	Partial       bool
	NextCursor    string
	ThreadsWalked int
	// ThreadsSkipped counts pages State offered for threads this process knows belong to
	// another subject. It must stay 0 against a scope-correct State server; a non-zero count
	// means the product refused a cross-scope merge that the server should never have offered.
	ThreadsSkipped int
	Elapsed        time.Duration
}

// HydrateRecentTurnsFromState answers the UNSCOPED ListTurns: the caller's own turns, live and
// cold alike, rebuilt from State with the caller's own scope-bound task token. cursor resumes a
// previous bounded walk in the same cursor form ListChats already uses; an empty cursor starts
// at the newest threads. Bounded twice on purpose - turnHistoryThreads threads per page and
// turnHistoryBudget wall time - because this is a browser read and State latency is not ours to
// promise. Partial=true states that the walk did not cover everything the caller has;
// NextCursor states where to continue. A walk that was cut short is never reported as an empty
// history, which is the exact failure this file exists to prevent.
func (c *Coordinator) HydrateRecentTurnsFromState(ctx context.Context, token, cursor string, scope contracts.Scope) (TurnHistoryWalk, error) {
	started := time.Now()
	walk := TurnHistoryWalk{}
	limit := c.turnHistoryThreads
	if limit <= 0 {
		walk.Elapsed = time.Since(started)
		return walk, nil // durable read disabled by configuration
	}
	budget := c.turnHistoryBudget
	if budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	threads, next, hasMore, err := c.state.ListThreads(ctx, token, cursor, limit)
	if err != nil {
		return TurnHistoryWalk{}, fmt.Errorf("hydrate recent turns: %w", err)
	}
	// A handed-back cursor is treated as "more exists" even when has_more lied: over-flagging
	// partial is a cosmetic cost, under-flagging it is the silent-incomplete-list defect.
	walk.NextCursor = next
	walk.Partial = hasMore || next != ""
	for _, th := range threads {
		if ctx.Err() != nil {
			walk.Partial = true
			break
		}
		// Defense in depth, not the primary line: the contract says State scopes ListThreads by
		// the presenting task token, and scopedState proves it holds. But this walk merges rows
		// into a BROWSER's history, so a server that handed back a thread this very process
		// executed for a different subject must still not succeed - the in-memory thread table
		// is exactly the knowledge the scope of a cold row is checked against.
		if owner, known := c.scopeOfThread(th.ID); known && owner != scope {
			walk.ThreadsSkipped++
			continue
		}
		page, perr := c.HydrateTurnsFromState(ctx, token, th.ID, scope)
		if perr != nil {
			if ctx.Err() != nil {
				walk.Partial = true
				break
			}
			return TurnHistoryWalk{}, perr
		}
		walk.ThreadsWalked++
		walk.Turns = append(walk.Turns, page...)
	}
	if ctx.Err() != nil {
		walk.Partial = true
	}
	walk.Elapsed = time.Since(started)
	return walk, nil
}

// mergeTurnPages joins the live execution index with rebuilt durable rows for one scope.
// The live record wins wherever both know the same turn (only it carries Steps/Checkpoint),
// a durable row is never dropped just because the process did not create it, and ordering is
// newest-first by the creation instant the id already states. Rows whose id does not carry a
// date stay at the end in the order they arrived: inventing a timestamp to make a list look
// ordered would be worse than an obviously unscheduled tail.
func mergeTurnPages(live []Turn, durable []Turn, liveByID map[string]Turn) []Turn {
	seen := make(map[string]bool, len(durable)+len(live))
	out := make([]Turn, 0, len(durable)+len(live))
	for _, t := range durable {
		if seen[t.ID] {
			continue
		}
		seen[t.ID] = true
		if lt, ok := liveByID[t.ID]; ok {
			out = append(out, lt)
			continue
		}
		out = append(out, t)
	}
	for _, t := range live {
		if seen[t.ID] {
			continue
		}
		seen[t.ID] = true
		out = append(out, t)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i].CreatedAt.IsZero(), out[j].CreatedAt.IsZero()
		if a != b {
			return !a // dated rows before undated ones
		}
		if a && b {
			return false // both undated: keep arrival order
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out
}

// turnListRequest decodes the OPTIONAL body of ListTurns. The contract allows an empty body
// (docs/space-components-api-contract-2026-10-30.md: ListTurns {} -> []Turn), so a missing or
// oversized body is not an error - it is simply the unscoped "my turns" view. cursor is the
// same thread-page cursor ListChats publishes; limit caps how many merged rows the caller
// wants to see, and a list trimmed by it is reported as partial rather than truncated quietly.
func turnListRequest(r *http.Request) (threadID, cursor string, limit int) {
	if r.Body == nil {
		return "", "", 0
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil || len(body) == 0 || int64(len(body)) > maxBodyBytes {
		return "", "", 0
	}
	var req struct {
		ThreadID string `json:"thread_id"`
		Cursor   string `json:"cursor"`
		Limit    int    `json:"limit"`
	}
	if json.Unmarshal(body, &req) != nil {
		return "", "", 0
	}
	if req.Limit < 0 {
		return strings.TrimSpace(req.ThreadID), strings.TrimSpace(req.Cursor), 0
	}
	return strings.TrimSpace(req.ThreadID), strings.TrimSpace(req.Cursor), req.Limit
}

// isNotFound reports whether err carries the shared NOT_FOUND code, through any %w wrapper.
func isNotFound(err error) bool {
	var ce *contracts.Error
	if errors.As(err, &ce) {
		return ce.Code == contracts.ErrNotFound
	}
	return contracts.Code(err) == contracts.ErrNotFound
}

// turnForRead resolves the turn GetTurn answers with. A turn this process actually executed
// keeps its exact previous behavior (authorizeTurn, then the live snapshot with steps and
// checkpoint). Only a NOT_FOUND from the in-memory table falls back to the durable Run row,
// and that cold hit is still checked against thread ownership through the same
// authorizeThread path ReadStream/ListRunHistory use - so a restart cannot turn into a
// cross-scope read. Any OTHER authorization error is returned unchanged: a refusal is never
// downgraded into a durable lookup, and a genuinely absent turn stays a 404.
func (h *Handler) turnForRead(r *http.Request, turnID string) (Turn, error) {
	if _, err := h.authorizeTurn(r, turnID); err == nil {
		snapshot, _ := h.coordinator.TurnSnapshot(turnID)
		// A terminal local snapshot, or a turn THIS process is running, is authoritative
		// and cheap - serve it directly. But a NON-terminal snapshot this process is NOT
		// running can be stale: another worker may have taken the run to a terminal State
		// knows about while this face's snapshot stayed `pending` (DEF-H30-x). Re-read the
		// durable record once; if it is terminal, that truth wins over the local pending,
		// and the face the user is watching converges on what the runner and State say.
		if snapshot.Status.IsTerminal() || h.coordinator.IsExecuting(turnID) {
			return snapshot, nil
		}
		if durable := h.hydrateDurableTurn(r, turnID); durable != nil && durable.Status.IsTerminal() {
			h.coordinator.foldTerminalSnapshot(*durable)
			return *durable, nil
		}
		return snapshot, nil
	} else if !isNotFound(err) {
		return Turn{}, err
	}
	scope, err := principal(r)
	if err != nil {
		return Turn{}, err
	}
	token, err := h.stateToken(r)
	if err != nil {
		return Turn{}, err
	}
	t, err := h.coordinator.HydrateTurnFromState(r.Context(), token, turnID, scope)
	if err != nil {
		return Turn{}, err
	}
	if t == nil {
		return Turn{}, contracts.NewError(contracts.ErrNotFound, "turn not found")
	}
	if _, err := h.authorizeThread(r, t.ThreadID); err != nil {
		return Turn{}, err
	}
	if live, ok := h.coordinator.TurnSnapshot(turnID); ok {
		return live, nil // this process claimed it while we were reading
	}
	return *t, nil
}

// hydrateDurableTurn reads the durable Run row for a turn this process knows only
// in-memory, so turnForRead can converge on a terminal a different worker wrote. Any
// error or absence yields nil: a read that cannot prove a newer durable truth must keep
// serving the local snapshot, never turn a stale-pending into an error. Ownership was
// already established by authorizeTurn (scope match) before this is called, and the read
// is under the caller's own scope-bound token, so it stays in-scope.
func (h *Handler) hydrateDurableTurn(r *http.Request, turnID string) *Turn {
	scope, err := principal(r)
	if err != nil {
		return nil
	}
	token, err := h.stateToken(r)
	if err != nil {
		return nil
	}
	t, err := h.coordinator.HydrateTurnFromState(r.Context(), token, turnID, scope)
	if err != nil {
		return nil
	}
	return t
}
