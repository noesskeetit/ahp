package memory

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type testAccess struct {
	revoked   bool
	bindError error
	bound     []string
}

func (a *testAccess) Authorize(context.Context, string, AccessRequest) (Identity, error) {
	return Identity{}, nil
}
func (a *testAccess) Revalidate(context.Context, Identity, AccessRequest) error {
	if a.revoked {
		return Fail(403, "revoked", "revoked")
	}
	return nil
}
func (a *testAccess) Bind(_ context.Context, _ Identity, labels []string) error {
	a.bound = labels
	return a.bindError
}

type fakeEngine struct {
	mu         sync.Mutex
	jobs       map[string]Job
	states     map[string]string
	facts      map[string][]NativeFact
	configured map[string]Bank
	fail       Scope
	lostAck    bool
	zero       bool
	failRead   bool
	deleted    []string
	committed  map[string]int
}

func newEngine() *fakeEngine {
	return &fakeEngine{jobs: map[string]Job{}, states: map[string]string{}, facts: map[string][]NativeFact{}, configured: map[string]Bank{}, committed: map[string]int{}}
}
func (f *fakeEngine) Configure(_ context.Context, b Bank) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.configured[b.ID] = b
	return nil
}
func (f *fakeEngine) Retain(_ context.Context, b Bank, j Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail == b.Scope {
		return Fail(503, "temporary", "temporary")
	}
	f.jobs[j.ID] = j
	f.states[j.ID] = "completed"
	if !f.zero {
		f.facts[b.ID] = append(f.facts[b.ID], NativeFact{ID: newID(), Text: j.Content, Type: "world", Metadata: j.Metadata})
		f.committed[j.ID] = 1
	} else {
		f.committed[j.ID] = 0
	}
	if f.lostAck {
		f.lostAck = false
		return Fail(502, "lost_ack", "lost acknowledgement")
	}
	return nil
}
func (f *fakeEngine) Status(_ context.Context, bank, id string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if state, ok := f.states[id]; ok {
		return state, nil
	}
	return "not_found", nil
}
func (f *fakeEngine) Cancel(_ context.Context, bank, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.states[id] == "pending" || f.states[id] == "processing" {
		f.states[id] = "cancelled"
	}
	return nil
}
func (f *fakeEngine) Recall(_ context.Context, b Bank, _ string, _ int) ([]NativeFact, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failRead {
		return nil, Fail(503, "read_unavailable", "unavailable")
	}
	return append([]NativeFact(nil), f.facts[b.ID]...), nil
}
func (f *fakeEngine) Correct(context.Context, Bank, Job) error { return nil }

// RetainFactsCommitted makes the fake a RetainStatsEngine: it reports the count
// the fake recorded for that retain (0 when the engine extracted nothing), so
// tests can assert the durable Operation surfaces an observable extraction
// outcome rather than a silent success.
func (f *fakeEngine) RetainFactsCommitted(_ context.Context, _, id string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.committed[id], nil
}

func (f *fakeEngine) DeleteBank(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.facts, id)
	f.deleted = append(f.deleted, id)
	return nil
}

type fixture struct {
	s   *Service
	e   *fakeEngine
	a   *testAccess
	now time.Time
	id  Identity
}

func setup(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{e: newEngine(), a: &testAccess{}, now: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
	f.s = New(NewMemoryStore(), f.e, f.a)
	f.s.Now = func() time.Time { return f.now }
	f.s.Config.Poll = time.Millisecond
	f.s.Config.PurgeGrace = time.Second
	f.id = Identity{TenantID: "tenant-a", UserID: "user-a", ActorID: "runtime-a", ActorKind: "runtime", GrantID: "grant-a", TaskID: "task-a", ExpiresAt: f.now.Add(365 * 24 * time.Hour)}
	return f
}
func (f *fixture) turn(project string, seq int64, allowed ...Scope) TurnRequest {
	return TurnRequest{ProjectID: project, SessionID: "session-1", Sequence: seq, Sanitized: true, Sources: []Source{{Ref: fmt.Sprintf("source-%s-%d", project, seq), Role: "assistant", Text: fmt.Sprintf("%s finding %d", project, seq), At: f.now, AllowedScopes: allowed}}}
}
func (f *fixture) ingest(t *testing.T, req TurnRequest) {
	t.Helper()
	if _, err := f.s.Ingest(context.Background(), f.id, fmt.Sprintf("turn-%s-%d", req.ProjectID, req.Sequence), req); err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) step(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := f.s.WorkOwner(context.Background(), f.id.Owner()); err != nil {
			t.Fatal(err)
		}
		f.now = f.now.Add(time.Millisecond)
	}
}
func records[T any](t *testing.T, f *fixture, kind string) []T {
	t.Helper()
	var out []T
	err := f.s.Store.Within(context.Background(), f.id.Owner(), func(tx Tx) error { var err error; out, err = list[T](tx, kind); return err })
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestCadenceTimerIsNotResetByLaterTurns(t *testing.T) {
	f := setup(t)
	f.ingest(t, f.turn("alpha", 1, Personal, Project))
	f.now = f.now.Add(time.Minute)
	f.ingest(t, f.turn("alpha", 2, Personal, Project))
	f.step(t, 1)
	if len(f.e.jobs) != 0 {
		t.Fatal("too early")
	}
	f.now = f.now.Add(time.Minute)
	f.step(t, 1)
	if len(f.e.jobs) != 1 {
		t.Fatalf("want project retain at first pending deadline, got %d", len(f.e.jobs))
	}
	for _, j := range f.e.jobs {
		if j.Scope != Project || !strings.Contains(j.Content, "finding 1") || !strings.Contains(j.Content, "finding 2") {
			t.Fatal(j)
		}
	}
	f.now = f.now.Add(8 * time.Minute)
	f.step(t, 4)
	personal := 0
	for _, j := range f.e.jobs {
		if j.Scope == Personal {
			personal++
		}
	}
	if personal != 1 {
		t.Fatalf("want one personal window, got %d", personal)
	}
}
func TestFiveTurnsTriggerProjectWithoutWaiting(t *testing.T) {
	f := setup(t)
	for n := int64(1); n <= 5; n++ {
		f.ingest(t, f.turn("alpha", n, Personal, Project))
	}
	f.step(t, 1)
	if len(f.e.jobs) != 1 {
		t.Fatal("batch did not trigger")
	}
	for _, j := range f.e.jobs {
		if j.Scope != Project {
			t.Fatal("personal batch triggered too early")
		}
	}
}
func TestSourceRestrictionAndAssistantKnowledge(t *testing.T) {
	f := setup(t)
	req := f.turn("alpha", 1, Project)
	req.Sources[0].Text = "project-only verified discovery"
	req.Sources[0].Labels = []string{"project-restricted"}
	req.Sources = append(req.Sources, Source{Ref: "preference", Role: "user", Text: "I prefer concise Russian answers", At: f.now, AllowedScopes: []Scope{Personal, Project}})
	req.Flush = true
	f.ingest(t, req)
	f.step(t, 6)
	if len(f.e.jobs) != 2 {
		t.Fatal("both branches should execute")
	}
	for _, j := range f.e.jobs {
		if j.Scope == Personal && strings.Contains(j.Content, "project-only") {
			t.Fatal("restricted input reached personal retain")
		}
		if j.Scope == Project && !strings.Contains(j.Content, "verified discovery") {
			t.Fatal("assistant discovery was lost")
		}
	}
}
func TestAcceptedDoesNotAdvanceCursorAndZeroFactsIsSuccess(t *testing.T) {
	f := setup(t)
	f.e.zero = true
	req := f.turn("alpha", 1, Project)
	req.Flush = true
	f.ingest(t, req)
	f.step(t, 1)
	cs := records[Cursor](t, f, "cursor")
	if cs[0].ProcessedSequence != 0 {
		t.Fatal("accepted is not indexed")
	}
	f.step(t, 1)
	cs = records[Cursor](t, f, "cursor")
	if cs[0].ProcessedSequence != 1 || len(cs[0].Pending) != 0 {
		t.Fatal("successful empty extraction must advance cursor")
	}
}
func TestIdempotencyAndDuplicateTurn(t *testing.T) {
	f := setup(t)
	req := f.turn("alpha", 1, Personal, Project)
	f.ingest(t, req)
	f.ingest(t, req)
	if _, err := f.s.Ingest(context.Background(), f.id, "different-key", req); err != nil {
		t.Fatal(err)
	}
	for _, c := range records[Cursor](t, f, "cursor") {
		if len(c.Pending) != 1 {
			t.Fatal("duplicate pending turn")
		}
	}
	req.Sources[0].Text = "different"
	_, err := f.s.Ingest(context.Background(), f.id, "turn-alpha-1", req)
	assertCode(t, err, "idempotency_conflict")
	_, err = f.s.Ingest(context.Background(), f.id, "new-key", req)
	assertCode(t, err, "turn_conflict")
}
func TestConcurrentIdempotencyCreatesOneJob(t *testing.T) {
	f := setup(t)
	req := RememberRequest{Target: Personal, Text: "I prefer Russian", SourceRefs: []string{"user-command"}}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for n := 0; n < 20; n++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := f.s.Remember(context.Background(), f.id, "same", req); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(records[Job](t, f, "job")) != 1 {
		t.Fatal("duplicate jobs")
	}
}
func TestFailureOfPersonalDoesNotRollbackProject(t *testing.T) {
	f := setup(t)
	f.e.fail = Personal
	f.s.Config.MaxAttempts = 1
	req := f.turn("alpha", 1, Personal, Project)
	req.Flush = true
	f.ingest(t, req)
	f.step(t, 8)
	states := map[Scope]string{}
	for _, j := range records[Job](t, f, "job") {
		states[j.Scope] = j.State
	}
	if states[Personal] != "failed" || states[Project] != "succeeded" {
		t.Fatal(states)
	}
}
func TestLostAcknowledgementUsesSameUpstreamOperation(t *testing.T) {
	f := setup(t)
	f.e.lostAck = true
	op, err := f.s.Remember(context.Background(), f.id, "remember", RememberRequest{ProjectID: "alpha", Target: Project, Text: "Use gRPC", SourceRefs: []string{"decision"}})
	if err != nil {
		t.Fatal(err)
	}
	f.step(t, 1)
	f.now = f.now.Add(4 * time.Second)
	f.step(t, 2)
	if len(f.e.jobs) != 1 {
		t.Fatal("ambiguous acknowledgement created another operation")
	}
	out, err := f.s.GetOperation(context.Background(), f.id, op.ID, "alpha", Project)
	if err != nil || out.State != "succeeded" {
		t.Fatal(out, err)
	}
}
func TestDisableAndReenableDoNotBackfill(t *testing.T) {
	f := setup(t)
	f.ingest(t, f.turn("alpha", 1, Project))
	off, on := false, true
	if _, err := f.s.UpdateSettings(context.Background(), f.id, "disable", SettingsRequest{ProjectID: "alpha", ProjectEnabled: &off}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.UpdateSettings(context.Background(), f.id, "enable", SettingsRequest{ProjectID: "alpha", ProjectEnabled: &on}); err != nil {
		t.Fatal(err)
	}
	req := f.turn("alpha", 2, Project)
	req.Flush = true
	f.ingest(t, req)
	f.step(t, 4)
	for _, j := range f.e.jobs {
		if strings.Contains(j.Content, "finding 1") || !strings.Contains(j.Content, "finding 2") {
			t.Fatal("disabled interval was replayed")
		}
	}
}
func TestClearIsolatesNewGenerationAndPurgesOld(t *testing.T) {
	f := setup(t)
	req := RememberRequest{ProjectID: "alpha", Target: Project, Text: "old decision", SourceRefs: []string{"old"}}
	if _, err := f.s.Remember(context.Background(), f.id, "old", req); err != nil {
		t.Fatal(err)
	}
	f.step(t, 2)
	old := records[Bank](t, f, "bank")[0]
	if _, err := f.s.Clear(context.Background(), f.id, "clear", ClearRequest{ProjectID: "alpha", Scope: Project}); err != nil {
		t.Fatal(err)
	}
	req.Text = "new decision"
	if _, err := f.s.Remember(context.Background(), f.id, "new", req); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(2 * time.Second)
	f.step(t, 5)
	banks := records[Bank](t, f, "bank")
	if len(banks) != 2 {
		t.Fatal(banks)
	}
	for _, b := range banks {
		if b.ID == old.ID && b.State != "deleted" {
			t.Fatal("old bank not purged")
		}
		if b.State == "active" && (b.ID == old.ID || b.Generation != 2) {
			t.Fatal("physical bank reused")
		}
	}
	out, err := f.s.Search(context.Background(), f.id, SearchRequest{ProjectID: "alpha", Scope: "project", Query: "decision"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Facts) != 1 || out.Facts[0].Text != "new decision" {
		t.Fatal(out)
	}
}
func TestInactivityExpiresBeforeNewActivityCanReviveBank(t *testing.T) {
	f := setup(t)
	if _, err := f.s.Remember(context.Background(), f.id, "p", RememberRequest{ProjectID: "alpha", Target: Project, Text: "old", SourceRefs: []string{"p"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Remember(context.Background(), f.id, "u", RememberRequest{Target: Personal, Text: "personal", SourceRefs: []string{"u"}}); err != nil {
		t.Fatal(err)
	}
	f.step(t, 6)
	f.now = f.now.Add(30*24*time.Hour + time.Second)
	out, err := f.s.Search(context.Background(), f.id, SearchRequest{ProjectID: "alpha", Scope: "project", Query: "old"})
	if err != nil || len(out.Facts) != 0 {
		t.Fatal(out, err)
	}
	if _, err := f.s.Remember(context.Background(), f.id, "new", RememberRequest{ProjectID: "alpha", Target: Project, Text: "new", SourceRefs: []string{"new"}}); err != nil {
		t.Fatal(err)
	}
	counts := map[Scope]int{}
	for _, b := range records[Bank](t, f, "bank") {
		counts[b.Scope]++
	}
	if counts[Personal] != 1 || counts[Project] != 2 {
		t.Fatal(counts)
	}
}
func TestProjectDeletionPreservesPersonalAndRejectsResurrection(t *testing.T) {
	f := setup(t)
	req := f.turn("alpha", 1, Personal, Project)
	req.Flush = true
	f.ingest(t, req)
	f.step(t, 6)
	if _, err := f.s.DeleteProject(context.Background(), f.id, "delete", "alpha"); err != nil {
		t.Fatal(err)
	}
	_, err := f.s.Remember(context.Background(), f.id, "late", RememberRequest{ProjectID: "alpha", Target: Project, Text: "late", SourceRefs: []string{"late"}})
	assertCode(t, err, "project_deleted")
	out, err := f.s.Search(context.Background(), f.id, SearchRequest{Scope: "personal", Query: "preferences"})
	if err != nil || len(out.Facts) != 1 {
		t.Fatal(out, err)
	}
}
func TestForeignUserAndWrongProjectCannotCorrectFact(t *testing.T) {
	f := setup(t)
	if _, err := f.s.Remember(context.Background(), f.id, "a", RememberRequest{ProjectID: "alpha", Target: Project, Text: "decision", SourceRefs: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	f.step(t, 2)
	bank := records[Bank](t, f, "bank")[0]
	fact := f.e.facts[bank.ID][0]
	ref := bank.ID + "." + fact.ID
	other := f.id
	other.UserID = "other"
	_, err := f.s.Correct(context.Background(), other, "attack", ref, CorrectRequest{Scope: Project, ProjectID: "alpha", Text: "bad"}, false)
	if !errors.Is(err, ErrMissing) {
		t.Fatal(err)
	}
	_, err = f.s.Correct(context.Background(), f.id, "attack", ref, CorrectRequest{Scope: Project, ProjectID: "beta", Text: "bad"}, false)
	assertCode(t, err, "wrong_project")
}
func TestBindMustSucceedBeforeReturningRetrievedText(t *testing.T) {
	f := setup(t)
	f.id.Labels = []string{"private-data"}
	if _, err := f.s.Remember(context.Background(), f.id, "a", RememberRequest{Target: Personal, Text: "sensitive", SourceRefs: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	f.step(t, 2)
	f.a.bindError = Fail(403, "bind_denied", "denied")
	out, err := f.s.Search(context.Background(), f.id, SearchRequest{Scope: "personal", Query: "sensitive"})
	assertCode(t, err, "bind_denied")
	if len(out.Facts) > 0 {
		t.Fatal("returned text without bound labels")
	}
	if len(f.a.bound) != 1 || f.a.bound[0] != "private-data" {
		t.Fatal(f.a.bound)
	}
}
func TestRevokedGrantNeverWrites(t *testing.T) {
	f := setup(t)
	f.a.revoked = true
	if _, err := f.s.Remember(context.Background(), f.id, "a", RememberRequest{Target: Personal, Text: "test", SourceRefs: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	f.step(t, 1)
	if len(f.e.jobs) > 0 {
		t.Fatal("revoked grant was used")
	}
	if records[Job](t, f, "job")[0].State != "failed" {
		t.Fatal("revocation must be terminal")
	}
}
func TestStaleLeaseCannotCommit(t *testing.T) {
	f := setup(t)
	if _, err := f.s.Remember(context.Background(), f.id, "a", RememberRequest{Target: Personal, Text: "test", SourceRefs: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	old, ok, err := f.s.claim(context.Background(), f.id.Owner())
	if err != nil || !ok {
		t.Fatal(err)
	}
	f.now = f.now.Add(f.s.Config.Lease + time.Second)
	current, ok, err := f.s.claim(context.Background(), f.id.Owner())
	if err != nil || !ok {
		t.Fatal(err)
	}
	if err := f.s.finish(context.Background(), f.id.Owner(), old, "succeeded", nil); err != nil {
		t.Fatal(err)
	}
	j := records[Job](t, f, "job")[0]
	if j.LeaseToken != current.LeaseToken || j.State == "succeeded" {
		t.Fatal("stale worker committed")
	}
}
func TestStoreTransactionRollsBack(t *testing.T) {
	store := NewMemoryStore()
	sentinel := errors.New("rollback")
	err := store.Within(context.Background(), "owner", func(tx Tx) error {
		if err := tx.Put("x", "id", true); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	err = store.Within(context.Background(), "owner", func(tx Tx) error { var v bool; return tx.Get("x", "id", &v) })
	if !errors.Is(err, ErrMissing) {
		t.Fatal("failed transaction persisted")
	}
}

func TestLateOldSourceDoesNotRepopulateClearedBank(t *testing.T) {
	f := setup(t)
	req := f.turn("alpha", 1, Project)
	req.Flush = true
	f.ingest(t, req)
	f.step(t, 2)
	oldAt := f.now.Add(-time.Second)
	if _, err := f.s.Clear(context.Background(), f.id, "clear", ClearRequest{ProjectID: "alpha", Scope: Project}); err != nil {
		t.Fatal(err)
	}
	late := f.turn("alpha", 2, Project)
	late.Flush = true
	late.Sources[0].At = oldAt
	late.Sources[0].Text = "late old history"
	f.ingest(t, late)
	f.step(t, 4)
	if len(f.e.jobs) != 1 {
		t.Fatal("old source was re-ingested after clear")
	}
	f.now = f.now.Add(time.Second)
	fresh := f.turn("alpha", 3, Project)
	fresh.Flush = true
	f.ingest(t, fresh)
	f.step(t, 4)
	if len(f.e.jobs) != 2 {
		t.Fatal("fresh source was not ingested")
	}
}
func TestUnchangedSettingDoesNotDiscardPendingMaterial(t *testing.T) {
	f := setup(t)
	f.ingest(t, f.turn("alpha", 1, Project))
	on := true
	if _, err := f.s.UpdateSettings(context.Background(), f.id, "same", SettingsRequest{ProjectID: "alpha", ProjectEnabled: &on}); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(2 * time.Minute)
	f.step(t, 2)
	if len(f.e.jobs) != 1 {
		t.Fatal("unchanged setting discarded material")
	}
}
func TestFailedPayloadExpiresAndReleasesCursor(t *testing.T) {
	f := setup(t)
	f.e.fail = Project
	f.s.Config.MaxAttempts = 1
	req := f.turn("alpha", 1, Project)
	req.Flush = true
	f.ingest(t, req)
	f.step(t, 1)
	job := records[Job](t, f, "job")[0]
	f.now = f.now.Add(f.s.Config.PayloadRetention + time.Second)
	f.step(t, 2)
	jobs := records[Job](t, f, "job")
	if !jobs[0].PayloadExpired || jobs[0].Content != "" || len(jobs[0].Sources) != 0 {
		t.Fatal("failed payload retained past retention")
	}
	if records[Cursor](t, f, "cursor")[0].JobID != "" {
		t.Fatal("expired failed job blocks later material")
	}
	if len(records[Turn](t, f, "turn")[0].Request.Sources) != 0 {
		t.Fatal("source text retained after failed payload expired")
	}
	_, err := f.s.Retry(context.Background(), f.id, "retry", job.ID, "alpha", Project)
	assertCode(t, err, "payload_expired")
}
func TestTurnWithAllMemoryDisabledKeepsOnlyReceipt(t *testing.T) {
	f := setup(t)
	off := false
	if _, err := f.s.UpdateSettings(context.Background(), f.id, "off", SettingsRequest{PersonalEnabled: &off}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.UpdateSettings(context.Background(), f.id, "off-p", SettingsRequest{ProjectID: "alpha", ProjectEnabled: &off}); err != nil {
		t.Fatal(err)
	}
	f.ingest(t, f.turn("alpha", 1, Personal, Project))
	turns := records[Turn](t, f, "turn")
	if len(turns) != 1 || len(turns[0].Request.Sources) != 0 {
		t.Fatal("unneeded transcript payload retained")
	}
}
func TestNewServiceResumesPendingWindowFromStore(t *testing.T) {
	f := setup(t)
	f.ingest(t, f.turn("alpha", 1, Project))
	store := f.s.Store
	f.s = New(store, f.e, f.a)
	f.s.Now = func() time.Time { return f.now }
	f.s.Config.Poll = time.Millisecond
	f.now = f.now.Add(2 * time.Minute)
	f.step(t, 2)
	if len(f.e.jobs) != 1 {
		t.Fatal("pending work was process-local")
	}
}

func TestStatusTracksCleanupWithoutRenewingActivity(t *testing.T) {
	f := setup(t)
	if _, err := f.s.Remember(context.Background(), f.id, "remember", RememberRequest{ProjectID: "alpha", Target: Project, Text: "decision", SourceRefs: []string{"s"}}); err != nil {
		t.Fatal(err)
	}
	activity := records[ProjectState](t, f, "project")[0].LastActivity
	f.now = f.now.Add(time.Hour)
	status, err := f.s.Status(context.Background(), f.id, Project, "alpha")
	if err != nil || status.State != "active" || status.PendingOperations != 1 {
		t.Fatal(status, err)
	}
	if !records[ProjectState](t, f, "project")[0].LastActivity.Equal(activity) {
		t.Fatal("status read renewed inactivity deadline")
	}
	if _, err := f.s.Clear(context.Background(), f.id, "clear", ClearRequest{ProjectID: "alpha", Scope: Project}); err != nil {
		t.Fatal(err)
	}
	status, err = f.s.Status(context.Background(), f.id, Project, "alpha")
	if err != nil || status.State != "deleting" || !status.CleanupPending {
		t.Fatal(status, err)
	}
	f.now = f.now.Add(2 * time.Second)
	f.step(t, 2)
	status, err = f.s.Status(context.Background(), f.id, Project, "alpha")
	if err != nil || status.State != "deleted" || status.CleanupPending {
		t.Fatal(status, err)
	}
}
func TestReadOutageReturnsExplicitDegradation(t *testing.T) {
	f := setup(t)
	if _, err := f.s.Remember(context.Background(), f.id, "remember", RememberRequest{ProjectID: "alpha", Target: Project, Text: "decision", SourceRefs: []string{"s"}}); err != nil {
		t.Fatal(err)
	}
	f.step(t, 2)
	f.e.failRead = true
	out, err := f.s.Search(context.Background(), f.id, SearchRequest{ProjectID: "alpha", Scope: "project", Query: "decision"})
	if err != nil || len(out.Facts) != 0 || len(out.Degraded) != 1 || out.Degraded[0] != Project {
		t.Fatal(out, err)
	}
}

// row 17: a completed retain must expose the upstream extraction outcome on the
// durable Operation, so "Remember succeeded" is never a silent promise that the
// fact was stored. A non-empty input whose content the extractor committed zero
// facts must surface FactsCommitted=0 (observable), while success semantics stay
// unchanged - a correct extract-nothing (runtime noise) is still a success.
func TestRememberOperationObservesFactsCommitted(t *testing.T) {
	f := setup(t)
	op, err := f.s.Remember(context.Background(), f.id, "remember", RememberRequest{Target: Personal, Text: "I prefer Russian answers", SourceRefs: []string{"pref"}})
	if err != nil {
		t.Fatal(err)
	}
	f.step(t, 2)
	out, err := f.s.GetOperation(context.Background(), f.id, op.ID, "", Personal)
	if err != nil {
		t.Fatal(err)
	}
	if out.State != "succeeded" {
		t.Fatalf("state=%s want succeeded", out.State)
	}
	if out.FactsCommitted == nil {
		t.Fatal("a completed retain must expose an observable facts_committed")
	}
	if *out.FactsCommitted != 1 {
		t.Fatalf("facts_committed=%d want 1", *out.FactsCommitted)
	}
}

func TestRememberNonEmptyInputObservesZeroFactsNotSilent(t *testing.T) {
	f := setup(t)
	f.e.zero = true
	op, err := f.s.Remember(context.Background(), f.id, "remember", RememberRequest{Target: Personal, Text: "The zeno canary value is plumbed through Hindsight live on the handbook instance", SourceRefs: []string{"noise"}})
	if err != nil {
		t.Fatal(err)
	}
	f.step(t, 2)
	out, err := f.s.GetOperation(context.Background(), f.id, op.ID, "", Personal)
	if err != nil {
		t.Fatal(err)
	}
	if out.State != "succeeded" {
		t.Fatalf("a correct extract-nothing must stay succeeded, got %s", out.State)
	}
	if out.FactsCommitted == nil {
		t.Fatal("payload loss must be observable, not a silent success: facts_committed is nil")
	}
	if *out.FactsCommitted != 0 {
		t.Fatalf("facts_committed=%d want 0 (non-empty input that committed nothing)", *out.FactsCommitted)
	}
}
