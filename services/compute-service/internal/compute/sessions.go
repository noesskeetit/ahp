package compute

// MOCK session facade: PTY sessions and browser workers (PDF rev3.2 s.29-30 lists
// "команды, PTY, окружения, браузерные воркеры" as Compute's own objects, and s.36/s.48
// require their effects to be lease-guarded and auditable like everything else).
//
// Why this file exists. The audit finding was precise: "polling GetCommand is not a
// browser/PTY". A caller that has to model an interactive session as a stream of
// one-shot commands has no session to hold, no input to send, and no honest way to be
// told that the platform cannot give it one. This facade supplies the session-shaped
// surface - open / input / output / close - with the SAME enforcement a command gets:
// the scope comes from the verified identity, the Computer must belong to the caller,
// the activity lease (pty / browser) is held for the life of the session, and the PTY
// budget is the same quota CheckPTYBudget already applies.
//
// What that lease pins, measured rather than assumed: the DURABLE activity lease is what
// the export guard reads, so a live session stops its Computer from being exported - the
// same guarantee a running command has, and taken from the same call the command path
// uses (Store.AcquireLease, internal/compute/runner.go). It is NOT what Runner.Sleep
// checks: the sleep gate counts the Redis lease service (LeaseService.HoldingCount), and
// neither this facade nor the existing PTY command path appears there. A Computer with a
// live PTY can therefore be slept today. That asymmetry is reported as DEF-COMPUTE-7
// rather than fixed here, because closing it changes the sleep contract for real commands
// as well and that belongs to the plane owner, not to a mock facade.
//
// What it is NOT, stated where a reader will not skip it: this service is the MOCK
// implementation. It runs NO program, opens NO pseudoterminal, starts NO browser and
// touches NO network. Input bytes are recorded and answered with a labelled simulated
// line that says in its own text that nothing executed; there is no code path here that
// could interpret them. It is not evidence about isolation, escape resistance, latency
// or density, and it does not implement PTYBackend on any backend (that absence stays
// pinned by backend_mock_test.go).
//
// Session transcripts live in this process's memory and end when it does. That is a
// property of the mock, not of the requirement: the real facade must put the transcript
// where the command ledger puts output - durably - and until it does, nothing here may
// be presented as a durable session record.

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// SessionKind is which kind of interactive worker a session is.
type SessionKind string

const (
	// SessionPTY is an interactive terminal session.
	SessionPTY SessionKind = "pty"
	// SessionBrowser is a browser worker session (PDF: a separate realm).
	SessionBrowser SessionKind = "browser"
)

func (k SessionKind) leaseKind() ActivityLeaseKind {
	if k == SessionBrowser {
		return LeaseBrowser
	}
	return LeasePTY
}

// SessionStatus is the lifecycle of a session.
type SessionStatus string

const (
	// SessionOpen accepts input and produces output.
	SessionOpen SessionStatus = "open"
	// SessionClosed is terminal: input is refused, output stays readable.
	SessionClosed SessionStatus = "closed"
)

// maxMockSessions bounds the registry. A facade that grows with every open and frees
// only on a caller's Close is a memory leak with an API in front of it.
const maxMockSessions = 64

// maxSessionChunks bounds one session's retained transcript, oldest-first eviction.
const maxSessionChunks = 256

// maxSessionInputBytes bounds one input write, so a "session" cannot be used to
// accumulate unbounded data in a control-plane process.
const maxSessionInputBytes = 64 << 10

// SessionSpec describes the session a caller asked for. Every field is RECORDED, never
// acted on: the mock has no shell to launch and no browser to point at a URL.
type SessionSpec struct {
	Shell string `json:"shell"`
	Cwd   string `json:"cwd"`
	Cols  uint16 `json:"cols"`
	Rows  uint16 `json:"rows"`
	// URL is recorded for a browser session and never fetched.
	URL string `json:"url"`
}

// Session is the observable state of one session. MockMarker travels with it because
// the nature of the answer is part of the answer.
type Session struct {
	ID         string        `json:"id"`
	ComputerID string        `json:"computer_id"`
	Kind       SessionKind   `json:"kind"`
	Status     SessionStatus `json:"status"`
	Spec       SessionSpec   `json:"spec"`
	OutputSeq  int64         `json:"output_seq"`
	CreatedAt  time.Time     `json:"created_at"`
	UpdatedAt  time.Time     `json:"updated_at"`
	// ExecutedOnHost is false and stays false by construction: this facade has no
	// execution path. It is a field rather than a comment because a caller can then
	// assert on it, and a test does.
	ExecutedOnHost bool   `json:"executed_on_host"`
	MockMarker     string `json:"mock_marker"`
}

// SessionChunk is one piece of session output, ordered by Seq so a reader can poll
// without re-reading (or guessing) what it already has.
type SessionChunk struct {
	Seq  int64     `json:"seq"`
	Text string    `json:"text"`
	At   time.Time `json:"at"`
}

// SessionOutput is a poll result: the chunks after a cursor plus the session state.
type SessionOutput struct {
	Session
	Chunks []SessionChunk `json:"chunks"`
	// Truncated says the oldest chunks have already been evicted from the bounded
	// transcript, so "after N" may be older than what is still kept. Saying it beats
	// silently answering with less.
	Truncated bool  `json:"truncated"`
	FirstSeq  int64 `json:"first_seq"`
}

type mockSession struct {
	Session
	chunks  []SessionChunk
	first   int64 // seq of chunks[0]
	evicted int64 // how many chunks were dropped from the front
	holder  string
}

// SessionService is the bounded in-process registry of MOCK sessions.
type SessionService struct {
	store *Store

	mu      sync.Mutex
	byKey   map[string]*mockSession
	created time.Time
}

// NewMockSessionService wires the facade to the durable store, which is what makes the
// Computer ownership check and the activity leases real rather than decorative.
func NewMockSessionService(store *Store) *SessionService {
	return &SessionService{store: store, byKey: map[string]*mockSession{}, created: time.Now().UTC()}
}

// MockMarker lets the mounting layer disclose the facade on every answer, the same way
// the mock backend does.
func (s *SessionService) MockMarker() string { return MockMarker }

func sessionKey(scope contracts.Scope, computerID, id string) string {
	return scope.Tenant + "\x00" + scope.Deployment + "\x00" + scope.Subject + "\x00" + computerID + "\x00" + id
}

// Open starts a simulated session on a Computer the caller owns.
//
// The checks that run here are the ones a real session would need, and they are not
// simulated: the Computer is read under the caller's scope (a foreign Computer is
// NOT_FOUND, exactly as elsewhere in Compute), the PTY budget is the same quota the
// runner enforces, and the pty/browser activity lease is held for the life of the
// session, which is what holds the export guard - see the note above for what it does
// not hold.
func (s *SessionService) Open(ctx context.Context, scope contracts.Scope, computerID string, kind SessionKind, spec SessionSpec) (Session, error) {
	if kind != SessionPTY && kind != SessionBrowser {
		return Session{}, invalid(`session kind must be "pty" or "browser"`)
	}
	if _, err := s.store.GetComputer(ctx, scope, computerID); err != nil {
		return Session{}, err
	}
	if kind == SessionPTY {
		active, err := s.store.ActiveLeases(ctx, scope, computerID)
		if err != nil {
			return Session{}, err
		}
		computer, err := s.store.GetComputer(ctx, scope, computerID)
		if err != nil {
			return Session{}, err
		}
		if err := CheckPTYBudget(computer.Quota, active); err != nil {
			return Session{}, err
		}
	}
	id := contracts.NewID()
	holder := "session-" + string(kind) + "-" + id
	if _, err := s.store.AcquireLease(ctx, scope, computerID, kind.leaseKind(), holder, defaultSessionTTL); err != nil {
		return Session{}, err
	}
	now := time.Now().UTC()
	sess := &mockSession{Session: Session{
		ID: id, ComputerID: computerID, Kind: kind, Status: SessionOpen, Spec: spec,
		CreatedAt: now, UpdatedAt: now, MockMarker: MockMarker,
	}, holder: holder}
	sess.chunks = append(sess.chunks, SessionChunk{Seq: 1, At: now,
		Text: openLine(kind, spec)})
	sess.OutputSeq = 1
	s.mu.Lock()
	if len(s.byKey) >= maxMockSessions {
		s.mu.Unlock()
		_ = s.store.ReleaseLease(ctx, scope, computerID, kind.leaseKind(), holder)
		return Session{}, contracts.NewError(contracts.ErrConflict,
			"compute: the session facade is at its limit; close a session before opening another")
	}
	s.byKey[sessionKey(scope, computerID, id)] = sess
	s.mu.Unlock()
	return sess.Snapshot(), nil
}

// Input records caller input and appends the simulated answer. Nothing interprets the
// bytes; the only text this can produce is the labelled line below.
func (s *SessionService) Input(ctx context.Context, scope contracts.Scope, computerID, sessionID, input string) (Session, error) {
	return s.append(ctx, scope, computerID, sessionID, func(sess *mockSession, at time.Time) string {
		if sess.Kind == SessionBrowser {
			return fmt.Sprintf("[mock] browser session %s recorded input at %s; no browser exists here, nothing navigated, nothing executed.",
				sess.ID, at.UTC().Format(time.RFC3339Nano))
		}
		return fmt.Sprintf("[mock] terminal session %s recorded %d input byte(s) at %s; no shell is attached to this session, nothing executed.",
			sess.ID, len(input), at.UTC().Format(time.RFC3339Nano))
	})
}

// BrowserAction records one browser-worker action (navigate / click / fill / snapshot /
// close). The action and its target are kept for the audit trail; the answer states that
// no browser and no network were involved.
func (s *SessionService) BrowserAction(ctx context.Context, scope contracts.Scope, computerID, sessionID, action, target string) (Session, error) {
	switch action {
	case "navigate", "click", "fill", "snapshot", "screenshot", "evaluate", "close":
	default:
		return Session{}, invalid(`browser action must be one of navigate, click, fill, snapshot, screenshot, evaluate, close`)
	}
	return s.append(ctx, scope, computerID, sessionID, func(sess *mockSession, at time.Time) string {
		return fmt.Sprintf("[mock] browser session %s recorded action=%s target=%q at %s; no browser, no network request and no code executed.",
			sess.ID, action, target, at.UTC().Format(time.RFC3339Nano))
	})
}

func (s *SessionService) append(ctx context.Context, scope contracts.Scope, computerID, sessionID string, line func(*mockSession, time.Time) string) (Session, error) {
	key := sessionKey(scope, computerID, sessionID)
	s.mu.Lock()
	sess, ok := s.byKey[key]
	if !ok {
		s.mu.Unlock()
		return Session{}, notFound("session")
	}
	if sess.Status != SessionOpen {
		out := sess.Snapshot()
		s.mu.Unlock()
		return Session{}, contracts.NewError(contracts.ErrConflict,
			"compute: session "+out.ID+" is "+string(out.Status)+"; input to a closed session is refused, not swallowed")
	}
	at := time.Now().UTC()
	sess.chunks = append(sess.chunks, SessionChunk{Seq: sess.OutputSeq + 1, At: at, Text: line(sess, at)})
	sess.OutputSeq++
	sess.UpdatedAt = at
	truncateFront(sess)
	out := sess.Snapshot()
	s.mu.Unlock()
	return out, nil
}

// Output polls the transcript after a cursor. Reading is allowed on a closed session:
// the point of a transcript is that it survives the session it came from.
func (s *SessionService) Output(ctx context.Context, scope contracts.Scope, computerID, sessionID string, after int64) (SessionOutput, error) {
	key := sessionKey(scope, computerID, sessionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byKey[key]
	if !ok {
		return SessionOutput{}, notFound("session")
	}
	out := SessionOutput{Session: sess.Snapshot(), FirstSeq: sess.first, Truncated: sess.evicted > 0}
	for _, c := range sess.chunks {
		if c.Seq > after {
			out.Chunks = append(out.Chunks, c)
		}
	}
	return out, nil
}

// Close ends a session and releases its activity lease, so the Computer becomes
// quiescent again as far as the ledger is concerned: the export guard stops counting it.
func (s *SessionService) Close(ctx context.Context, scope contracts.Scope, computerID, sessionID string) (Session, error) {
	key := sessionKey(scope, computerID, sessionID)
	s.mu.Lock()
	sess, ok := s.byKey[key]
	if !ok {
		s.mu.Unlock()
		return Session{}, notFound("session")
	}
	out := sess.Snapshot()
	if sess.Status == SessionClosed {
		s.mu.Unlock()
		return out, nil // idempotent: a second close is the same fact, not a new effect
	}
	at := time.Now().UTC()
	sess.Status = SessionClosed
	sess.UpdatedAt = at
	sess.chunks = append(sess.chunks, SessionChunk{Seq: sess.OutputSeq + 1, At: at,
		Text: "[mock] session closed; its activity lease is released and the transcript above is all there was."})
	sess.OutputSeq++
	truncateFront(sess)
	// The answer describes the session as it now is. Answering with the snapshot taken
	// before the close would tell the caller "open" about a session it just closed - a
	// contradiction the caller would have to poll past, and exactly the kind of answer
	// that makes a client re-issue a close and read a second effect into it.
	out = sess.Snapshot()
	holder := sess.holder
	kind := sess.Kind
	s.mu.Unlock()
	if err := s.store.ReleaseLease(ctx, scope, computerID, kind.leaseKind(), holder); err != nil {
		return out, err
	}
	return out, nil
}

func (m *mockSession) Snapshot() Session {
	out := m.Session
	out.MockMarker = MockMarker
	return out
}

// truncateFront keeps the transcript bounded, and records how much it dropped so a
// reader can be told rather than quietly given less.
func truncateFront(sess *mockSession) {
	for len(sess.chunks) > maxSessionChunks {
		sess.chunks = sess.chunks[1:]
		sess.first++
		sess.evicted++
	}
}

func openLine(kind SessionKind, spec SessionSpec) string {
	if kind == SessionBrowser {
		return "[mock] browser session opened. Nothing was launched and no address was fetched; the URL above is recorded, not visited. This facade executes nothing." +
			" realm=" + strconv.Quote(spec.URL)
	}
	return fmt.Sprintf("[mock] terminal session opened (%dx%d). No shell was started and no bytes will be executed; this transcript is simulated.",
		spec.Cols, spec.Rows)
}

// defaultSessionTTL bounds the activity lease a session holds, in line with the PTY
// lease the runner takes: a session that is never closed still stops pinning the
// Computer once its lease expires.
var defaultSessionTTL = 30 * time.Minute
