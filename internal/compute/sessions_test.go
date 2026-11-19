package compute

// The PTY / browser facade is the answer to a finding that was specific: "polling
// GetCommand is not a browser/PTY". Adding the routes is only half of it - the bar the
// customer set when MOCK was accepted is that a mock must never be able to read as a
// real session. So these tests pin three things and nothing else:
//
//  1. DISCLOSURE on every answer of the facade, including a poll of a closed session;
//  2. NO EXECUTION, measured by effect rather than by wording - input that would create
//     a file on a real shell is fed in, and the file is then looked for on disk;
//  3. the enforcement a session has to carry is not simulated: the Computer must be the
//     caller's (a foreign scope gets NOT_FOUND, not a "not authorised" that leaks
//     existence), the durable activity lease is held for the session's whole life - which
//     is what pins sleep and export - and closing releases it, twice idempotently.
//
// What is deliberately NOT claimed here: that a real PTY or browser works, that anything
// is isolated, or that a transcript survives the process. Sessions live in this process's
// memory; that is stated in sessions.go and it is the reason a real facade still has to
// put a transcript where the command ledger puts output.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func sessionFixture(t *testing.T) (*SessionService, *Store, contracts.Scope, string) {
	t.Helper()
	s, scope := tenantComputeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	c, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfileEphemeral}, "sessions-"+t.Name())
	if err != nil {
		t.Fatal(err)
	}
	return NewMockSessionService(s), s, scope, c.ID
}

func TestMockSessionDisclosesAndExecutesNothing(t *testing.T) {
	svc, s, scope, computerID := sessionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sess, err := svc.Open(ctx, scope, computerID, SessionPTY, SessionSpec{Shell: "/bin/sh", Cols: 80, Rows: 24})
	if err != nil {
		t.Fatal(err)
	}
	// The spec the caller described comes back in the shape the wire promises. A session
	// body is API surface: {"Shell":..} next to every other DTO's snake_case is a defect
	// a client finds, not one it is told about.
	if raw, mErr := json.Marshal(sess.Spec); mErr != nil {
		t.Fatal(mErr)
	} else if string(raw) != `{"shell":"/bin/sh","cwd":"","cols":80,"rows":24,"url":""}` {
		t.Fatalf("session spec wire shape: %s", raw)
	}
	if sess.MockMarker != MockMarker {
		t.Fatalf("an opened session did not disclose itself: %q", sess.MockMarker)
	}
	if sess.ExecutedOnHost {
		t.Fatal("the mock session claims it executed something on the host")
	}
	if sess.Status != SessionOpen || sess.Kind != SessionPTY {
		t.Fatalf("session = %+v", sess)
	}

	// The execution claim is tested by effect. These bytes are exactly what a shell would
	// receive; if any path in this facade ever handed them to a shell, the file appears.
	probe := filepath.Join(t.TempDir(), "executed-on-the-host")
	if _, err := svc.Input(ctx, scope, computerID, sess.ID, "touch "+probe+"\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Input(ctx, scope, computerID, sess.ID, "echo pwned > "+probe+"\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(probe); !os.IsNotExist(err) {
		t.Fatalf("the mock executed caller input on the host: %v", err)
	}

	out, err := svc.Output(ctx, scope, computerID, sess.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if out.MockMarker != MockMarker {
		t.Fatal("a poll did not disclose itself")
	}
	if len(out.Chunks) != 3 {
		t.Fatalf("transcript = %+v, want open + two recorded inputs", out.Chunks)
	}
	for _, chunk := range out.Chunks {
		if !strings.HasPrefix(chunk.Text, "[mock]") {
			t.Fatalf("chunk %d is not labelled as simulated: %q", chunk.Seq, chunk.Text)
		}
	}
	if !strings.Contains(out.Chunks[1].Text, "nothing executed") {
		t.Fatalf("the answer to caller input does not say that nothing happened: %q", out.Chunks[1].Text)
	}
	if strings.Contains(out.Chunks[1].Text, "pwned") {
		t.Fatalf("the facade echoed command output it cannot have produced: %q", out.Chunks[1].Text)
	}

	// A cursor is what makes polling usable; re-reading with the cursor must be empty.
	rest, err := svc.Output(ctx, scope, computerID, sess.ID, out.OutputSeq)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest.Chunks) != 0 {
		t.Fatalf("the cursor re-delivered output: %+v", rest.Chunks)
	}

	// The session holds a durable activity lease; that row, not a comment, is what holds
	// the export guard. (Sleep is gated elsewhere and does not see it - DEF-COMPUTE-7.)
	active, err := s.ActiveLeases(ctx, scope, computerID)
	if err != nil {
		t.Fatal(err)
	}
	if active < 1 {
		t.Fatalf("a live session left no activity lease: %d", active)
	}
	if _, err := s.beginExportGuard(ctx, scope, computerID); contracts.Code(err) != contracts.ErrConflict ||
		contracts.ReasonOf(err) != "export_source_busy" {
		t.Fatalf("a live session did not hold the export guard: %v", err)
	}

	closed, err := svc.Close(ctx, scope, computerID, sess.ID)
	if err != nil {
		t.Fatal(err)
	}
	// The answer describes the session as it is after the call, not as it was before it.
	if closed.Status != SessionClosed {
		t.Fatalf("Close answered about a session it had not closed: %+v", closed)
	}
	if again, err := svc.Close(ctx, scope, computerID, sess.ID); err != nil || again.Status != SessionClosed {
		t.Fatalf("closing twice must be the same fact, not a new effect: %+v err=%v", again, err)
	}
	if active, err = s.ActiveLeases(ctx, scope, computerID); err != nil || active != 0 {
		t.Fatalf("closing left the Computer pinned: %d leases, err=%v", active, err)
	}
	guard, err := s.beginExportGuard(ctx, scope, computerID)
	if err != nil {
		t.Fatalf("the export guard is still held after the session closed: %v", err)
	}
	guard.close()

	// Closing refuses input rather than swallowing it, and the transcript stays readable:
	// an operator has to be able to see what the session was told.
	if _, err := svc.Input(ctx, scope, computerID, sess.ID, "ls\n"); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("input to a closed session was not refused: %v", err)
	}
	if after, err := svc.Output(ctx, scope, computerID, sess.ID, 0); err != nil || after.MockMarker != MockMarker {
		t.Fatalf("a closed session's transcript stopped answering: %+v err=%v", after, err)
	}
}

func TestMockSessionFacadeRefusesAnotherTenantsComputer(t *testing.T) {
	svc, _, _, computerID := sessionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A different tenant, same facade, same Computer id: it must not be findable, which is
	// the same answer the caller gets for an id that does not exist.
	foreignScope := contracts.Scope{Tenant: contracts.NewID(), Deployment: "compute", Subject: "mallory"}
	if _, err := svc.Open(ctx, foreignScope, computerID, SessionPTY, SessionSpec{}); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("a foreign scope opened a session on someone else's Computer: %v", err)
	}
	// And an id this facade never issued is refused the same way, so the two cases are
	// indistinguishable from the outside.
	if _, err := svc.Input(ctx, foreignScope, computerID, "no-such-session", "x"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("unknown session: %v", err)
	}
}

func TestMockBrowserFacadeRecordsAndRefusesInvention(t *testing.T) {
	svc, _, scope, computerID := sessionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sess, err := svc.Open(ctx, scope, computerID, SessionBrowser, SessionSpec{URL: "https://example.invalid/"})
	if err != nil {
		t.Fatal(err)
	}
	if sess.MockMarker != MockMarker {
		t.Fatal("a browser session did not disclose itself")
	}
	acted, err := svc.BrowserAction(ctx, scope, computerID, sess.ID, "navigate", "https://example.invalid/login")
	if err != nil {
		t.Fatal(err)
	}
	if acted.OutputSeq != 2 {
		t.Fatalf("the recorded action produced no transcript entry: %+v", acted)
	}
	out, err := svc.Output(ctx, scope, computerID, sess.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	last := out.Chunks[len(out.Chunks)-1].Text
	if !strings.Contains(last, "no browser, no network request and no code executed") {
		t.Fatalf("the browser answer does not say what it did not do: %q", last)
	}
	// Invention is judged on the disclosure clause, not on a bare substring: the
	// honest answer embeds a session id and an RFC3339Nano wall-clock time whose
	// characters (the "200" inside a 20:0x time, a target URL containing "html") are
	// not page content. Matching "200"/"html" anywhere made the verdict depend on the
	// wall clock (false failure at 20:04:31 / 20:09:02). See browserFacadeInventsPage.
	if browserFacadeInventsPage(last) {
		t.Fatalf("the facade invented a page it cannot have fetched: %q", last)
	}
	// An action outside the contract is refused rather than recorded as a shrug.
	if _, err := svc.BrowserAction(ctx, scope, computerID, sess.ID, "download", "/etc/shadow"); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("an unknown browser action was accepted: %v", err)
	}
	if _, err := svc.Close(ctx, scope, computerID, sess.ID); err != nil {
		t.Fatal(err)
	}
}

func TestMockSessionFacadeIsBounded(t *testing.T) {
	svc, _, scope, computerID := sessionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// A facade that only frees memory when a caller remembers to Close is a leak with an
	// API in front of it, so the registry refuses at its limit.
	var first string
	for i := 0; i < maxMockSessions; i++ {
		sess, err := svc.Open(ctx, scope, computerID, SessionBrowser, SessionSpec{})
		if err != nil {
			t.Fatalf("session %d of %d: %v", i+1, maxMockSessions, err)
		}
		if i == 0 {
			first = sess.ID
		}
	}
	if _, err := svc.Open(ctx, scope, computerID, SessionBrowser, SessionSpec{}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("the registry grew past its limit instead of refusing: %v", err)
	}

	// One session's transcript is bounded the same way, and a reader is TOLD rather than
	// quietly given less.
	// The sequence keeps counting what was evicted: seq is the caller's cursor, and a
	// bounded transcript must not restart it or leave a gap the reader cannot see.
	want := int64(1) // the open line
	for i := 0; i < maxSessionChunks+40; i++ {
		if _, err := svc.Input(ctx, scope, computerID, first, fmt.Sprintf("line %d\n", i)); err != nil {
			t.Fatal(err)
		}
		want++
	}
	out, err := svc.Output(ctx, scope, computerID, first, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Truncated || out.FirstSeq != 41 {
		t.Fatalf("the transcript was not bounded as promised: truncated=%v first_seq=%d", out.Truncated, out.FirstSeq)
	}
	if int64(len(out.Chunks)) != maxSessionChunks || out.OutputSeq != want {
		t.Fatalf("bounded transcript lost the sequence: %d chunks kept, seq %d, want %d/%d",
			len(out.Chunks), out.OutputSeq, maxSessionChunks, want)
	}
}

// browserFacadeInventsPage decides whether a browser-facade answer claims a real
// fetched page. The honest mock answer is
//
//	[mock] browser session <id> recorded action=<a> target=<t> at <time>; no browser, no network request and no code executed.
//
// It embeds a session id and an RFC3339Nano wall-clock time. Those carry
// characters that are not page content -- the "200" inside a 20:0x time, the "html"
// of an https target -- so judging invention by a bare "200"/"html" substring over
// the whole line makes the verdict depend on the wall clock. The decision is instead
// taken on the disclosure clause that follows "; ": a genuine fetch would surface
// concrete page/HTTP markers there, and the honest clause never contains them. If the
// honest clause itself is absent the answer is not the trustworthy disclosure and is
// treated as invention (the check is NOT simply removed).
func browserFacadeInventsPage(answer string) bool {
	const honest = "no browser, no network request and no code executed"
	_, clause, found := strings.Cut(answer, "; ")
	if !found || !strings.Contains(clause, honest) {
		return true
	}
	lower := strings.ToLower(clause)
	for _, marker := range []string{
		"status=200", "200 ok", "http/1.1", "http/2", "http/3",
		"<html", "content-length", "cookie", "text/html",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

// TestMockBrowserFacadeInventionCheckIsIndependentOfIDsAndTimes is the RED/GREEN
// regression for the wall-clock defect: it drives browserFacadeInventsPage over
// synthetic ids and times -- including the exact values that made the old substring
// rule fail (20:04:31, 20:09:02) and ids/times that literally contain "200", "html",
// "200 OK", "<html>", "cookie" -- and asserts the verdict is identical (no invention)
// for every honest disclosure; then it asserts that a real fabricated page, carrying
// concrete markers in its disclosure clause, is still caught. The fix is therefore
// provably not "drop the check".
func TestMockBrowserFacadeInventionCheckIsIndependentOfIDsAndTimes(t *testing.T) {
	const clause = "no browser, no network request and no code executed."
	honest := func(id, action, target, at string) string {
		return fmt.Sprintf("[mock] browser session %s recorded action=%s target=%q at %s; %s", id, action, target, at, clause)
	}

	// (a) Honest answers: identical verdict (NOT invention) regardless of what the id
	//     or the time happen to spell. These embed every benign string the old rule
	//     misread, in both the id and the time field.
	benignIDs := []string{
		"sess-200431",
		"20261109T200902.123456789Z_abc",
		"id-200-html",
		"id-<html>-200-OK",
		"2026-11-09T20:10:25.9Z",
		"id-cookie-<html>-content-length",
	}
	benignTimes := []string{
		"20:04:31",
		"2026-11-09T20:09:02.000000Z",
		"1970-01-01T200000.000Z",
		"200 OK <html> cookie",
	}
	for _, id := range benignIDs {
		for _, at := range benignTimes {
			line := honest(id, "navigate", "https://example.invalid/login", at)
			if browserFacadeInventsPage(line) {
				t.Errorf("honest answer misjudged as invention (id=%q at=%q): %q", id, at, line)
			}
		}
	}

	// (b) A fabricated page: the concrete markers the honest clause never carries.
	//     Must be caught even when the id and time are perfectly clean.
	fabricated := []string{
		`[mock] browser session x recorded action=navigate target="https://x" at 20:00:00; status=200 <html><body>hi</body></html>`,
		`[mock] browser session x recorded action=snapshot target="" at 20:00:00; HTTP/1.1 200 OK content-length: 42 <html>`,
		`[mock] browser session x recorded action=navigate target="https://x" at 20:00:00; cookie: session=abc; <html>`,
		`[mock] browser session x recorded action=navigate target="https://x" at 20:00:00; 200 OK text/html`,
		`[mock] browser session x recorded action=navigate target="https://x" at 20:00:00; <!DOCTYPE html> ...`,
		// The honest clause is gone entirely -- the answer was replaced by a page body:
		"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\n<html>hi</html>",
	}
	for i, line := range fabricated {
		if !browserFacadeInventsPage(line) {
			t.Errorf("fabricated page #%d was not caught: %q", i, line)
		}
	}

	// (c) The real product disclosure (built exactly as sessions.go formats it) is
	//     judged not-invention, proving the tightened rule keeps passing the honest
	//     facade it was written to protect.
	real := honest("20261109T201025.000000001Z_deadbeef", "navigate", "https://example.invalid/login", "2026-11-09T20:10:25.000000001Z")
	if browserFacadeInventsPage(real) {
		t.Errorf("the honest product disclosure was flagged as invention: %q", real)
	}
}
