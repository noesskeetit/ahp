package memory

import (
	"context"
	"testing"
	"time"
)

type configureBoundaryEngine struct {
	*fakeEngine
	atConfigure func()
}

func (e *configureBoundaryEngine) Configure(ctx context.Context, b Bank) error {
	if err := e.fakeEngine.Configure(ctx, b); err != nil {
		return err
	}
	e.atConfigure()
	return nil
}
func TestReviewAuthorityChangesDuringConfigure(t *testing.T) {
	for _, change := range []string{"revoke", "expiry"} {
		t.Run(change, func(t *testing.T) {
			f := setup(t)
			e := &configureBoundaryEngine{fakeEngine: f.e}
			e.atConfigure = func() {
				if change == "revoke" {
					f.a.revoked = true
				} else {
					f.now = f.id.ExpiresAt.Add(time.Second)
				}
			}
			f.s.Engine = e
			_, err := f.s.Remember(context.Background(), f.id, "review-key", RememberRequest{Target: Personal, Text: "sensitive review payload", SourceRefs: []string{"review-source"}})
			if err != nil {
				t.Fatal(err)
			}
			f.step(t, 1)
			e.mu.Lock()
			n := len(e.jobs)
			e.mu.Unlock()
			if n != 0 {
				t.Fatalf("Retain submitted %d sensitive job(s) after %s completed during Configure", n, change)
			}
		})
	}
}

type configureBoundaryAccess struct {
	Access
	failure error
}

func (a *configureBoundaryAccess) Revalidate(ctx context.Context, id Identity, request AccessRequest) error {
	if a.failure != nil {
		return a.failure
	}
	return a.Access.Revalidate(ctx, id, request)
}

func TestPreRetainAuthorityUnavailableRetriesWithoutDispatch(t *testing.T) {
	f := setup(t)
	access := &configureBoundaryAccess{Access: f.a}
	engine := &configureBoundaryEngine{fakeEngine: f.e}
	engine.atConfigure = func() { access.failure = Fail(503, "authority_unavailable", "temporary Identity failure") }
	f.s.Access = access
	f.s.Engine = engine
	op, err := f.s.Remember(context.Background(), f.id, "transient", RememberRequest{Target: Personal, Text: "sensitive payload", SourceRefs: []string{"source"}})
	if err != nil {
		t.Fatal(err)
	}
	f.step(t, 1)
	if len(f.e.jobs) != 0 {
		t.Fatal("payload dispatched while Identity was unavailable")
	}
	jobs := records[Job](t, f, "job")
	if len(jobs) != 1 || jobs[0].ID != op.ID || jobs[0].State != "queued" || jobs[0].Failures != 1 {
		t.Fatalf("temporary failure became terminal or lost receipt: %+v", jobs)
	}
	access.failure = nil
	engine.atConfigure = func() {}
	f.now = f.now.Add(3 * time.Second)
	f.step(t, 2)
	jobs = records[Job](t, f, "job")
	if len(f.e.jobs) != 1 || jobs[0].ID != op.ID || jobs[0].State != "succeeded" {
		t.Fatalf("authorized retry did not retain the original receipt: %+v", jobs)
	}
}
