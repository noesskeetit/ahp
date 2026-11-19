package registry

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http/httptest"
	"strings"
)

// These tests use real PostgreSQL; HTTP invocation here is a contract fixture,
// with verified-principal context supplied by the test rather than TLS.
func TestDotProfileOverlayPinsRevisionsAndCannotExpandPolicy(t *testing.T) {
	h, _ := newHTTPFixture(t)
	tenant := "profile-" + httpSuffix(t)
	call := func(subject, op, body string, want int) map[string]json.RawMessage {
		t.Helper()
		fn := h.routes[op]
		if fn == nil {
			t.Fatalf("missing operation %s", op)
		}
		r := httptest.NewRequest("POST", "/v1/registry/"+op, strings.NewReader(body))
		r = r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: contracts.Scope{Tenant: tenant, Subject: subject, Deployment: "space"}}))
		w := httptest.NewRecorder()
		fn(w, r)
		if w.Code != want {
			t.Fatalf("%s status=%d want=%d body=%s", op, w.Code, want, w.Body.String())
		}
		var out map[string]json.RawMessage
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return out
	}
	call("admin", "PutConfig", `{"agent_id":"agents-space","expected_revision":0,"data":{"instructions":"tenant rule","model_route":"fast","max_steps":8,"policy":{"allowed_model_routes":["fast"],"allowed_skills":["writing"],"allowed_tools":["save_artifact"],"allow_personal_instructions":true,"allowed_computer_policies":["disabled"]}}}`, 200)
	call("admin", "PutModelRoute", `{"route":"fast","expected_revision":0,"document":{"model":"model-v1"}}`, 200)
	call("admin", "PutSkill", `{"skill_id":"writing","expected_revision":0,"level":"user","bundle":{"instructions":"skill v1"}}`, 200)
	call("alice", "PutProfile", `{"expected_revision":0,"data":{"instructions":"concise","model_route":"fast","max_steps":4,"skills":["writing"]}}`, 200)
	call("alice", "PutProfile", `{"expected_revision":1,"data":{"policy":{"allowed_tools":["shell"]}}}`, 400)
	call("alice", "PutProfile", `{"expected_revision":1,"data":{"max_steps":9}}`, 403)
	call("alice", "PutProfile", `{"expected_revision":0,"data":{"instructions":"stale"}}`, 409)
	call("alice", "PutProfile", `{"subject":"bob","expected_revision":1,"data":{}}`, 400)
	out := call("bob", "GetProfile", `{}`, 200)
	var bob struct{ Revision int64 }
	_ = json.Unmarshal(out["profile"], &bob)
	if bob.Revision != 0 {
		t.Fatal("other user received Alice overlay")
	}
	first := call("alice", "PinDotConfig", `{"key":"run-one","context_id":"conversation"}`, 200)
	call("admin", "PutModelRoute", `{"route":"fast","expected_revision":1,"document":{"model":"model-v2"}}`, 200)
	call("admin", "PutSkill", `{"skill_id":"writing","expected_revision":1,"level":"user","bundle":{"instructions":"skill v2"}}`, 200)
	call("alice", "PutProfile", `{"expected_revision":1,"data":{"instructions":"verbose","model_route":"fast","max_steps":3,"skills":["writing"]}}`, 200)
	replay := call("alice", "PinDotConfig", `{"key":"run-one","context_id":"conversation"}`, 200)
	if string(first["snapshot"]) != string(replay["snapshot"]) {
		t.Fatal("old run changed after profile/route/skill updates")
	}
	next := call("alice", "PinDotConfig", `{"key":"run-two","context_id":"conversation"}`, 200)
	if !strings.Contains(string(next["snapshot"]), "model-v2") || !strings.Contains(string(next["snapshot"]), "skill v2") || !strings.Contains(string(next["snapshot"]), "verbose") {
		t.Fatalf("new run missing changed config: %s", next["snapshot"])
	}
	if !strings.Contains(string(first["snapshot"]), "model-v1") || !strings.Contains(string(first["snapshot"]), "skill v1") || !strings.Contains(string(first["snapshot"]), "tenant rule") {
		t.Fatalf("old snapshot incomplete: %s", first["snapshot"])
	}
	if err := h.store.FenceSubject(context.Background(), tenant, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := h.store.DeleteSubject(context.Background(), tenant, "alice"); err != nil {
		t.Fatal(err)
	}
	call("alice", "PutProfile", `{"expected_revision":0,"data":{}}`, 403)
}

func TestDotConfigurationMissingFailsClearlyAndDeletionErasesPrivateSettings(t *testing.T) {
	h, _ := newHTTPFixture(t)
	s := h.store
	ctx := context.Background()
	scope := contracts.Scope{Tenant: "erase-dot-" + httpSuffix(t), Subject: "alice", Deployment: "space"}
	if _, err := s.PinDotConfig(ctx, scope, "run", "context"); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("unconfigured tenant silently starts: %v", err)
	}
	if _, err := s.PutConfig(ctx, scope.Tenant, DotAgentID, 0, json.RawMessage(`{"max_steps":8,"policy":{}}`)); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("unpinned default model accepted: %v", err)
	}
	if _, err := s.PutConfig(ctx, scope.Tenant, DotAgentID, 0, json.RawMessage(`{"model_route":"fast","max_steps":8,"policy":{"allowed_model_routes":["fast"],"allow_personal_skills":true}}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutModelRoute(ctx, scope.Tenant, "fast", 0, json.RawMessage(`{"model":"test"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutScopedSkill(ctx, scope, true, "private", "user", 0, json.RawMessage(`{"instructions":"private content"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutProfile(ctx, scope, 0, json.RawMessage(`{"personal_skills":["private"]}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PinDotConfig(ctx, scope, "run", "context"); err != nil {
		t.Fatal(err)
	}
	if err := s.FenceSubject(ctx, scope.Tenant, scope.Subject); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSubject(ctx, scope.Tenant, scope.Subject); err != nil {
		t.Fatal(err)
	}
	p, err := s.GetProfile(ctx, scope)
	if err != nil || p.Revision != 0 {
		t.Fatalf("profile survived erasure: %+v %v", p, err)
	}
	if _, err := s.GetScopedSkill(ctx, scope.Tenant, scope.Subject, "private", 0); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("personal skill survived: %v", err)
	}
	if _, err := s.PinDotConfig(ctx, scope, "run", "context"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("pin survived subject fence: %v", err)
	}
	// Tenant-wide erasure also covers scoped skills that are shared with employees.
	other := scope
	other.Subject = "admin"
	if _, err := s.PutScopedSkill(ctx, other, false, "shared", "user", 0, json.RawMessage(`{"instructions":"tenant content"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.FenceTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	skills, err := s.ListScopedSkills(ctx, scope.Tenant, "")
	if err != nil || len(skills) != 0 {
		t.Fatalf("tenant skill survived: %+v %v", skills, err)
	}
}

func TestDotSkillCatalogDoesNotExposeHigherLevelBundles(t *testing.T) {
	h, _ := newHTTPFixture(t)
	ctx := context.Background()
	scope := contracts.Scope{Tenant: "skill-level-" + httpSuffix(t), Subject: "admin", Deployment: "space"}
	if _, err := h.store.PutScopedSkill(ctx, scope, false, "restricted", SkillLevelAdmin, 0, json.RawMessage(`{"instructions":"admin-only bundle"}`)); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/v1/registry/GetSkill", strings.NewReader(`{"skill_id":"restricted"}`))
	scope.Subject = "ordinary-user"
	r = r.WithContext(transport.WithPrincipal(ctx, transport.Principal{Scope: scope}))
	w := httptest.NewRecorder()
	h.routes["GetSkill"](w, r)
	if w.Code != 404 {
		t.Fatalf("user read administrative skill: status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestModelRouteDeleteRecreatePreservesCASGeneration(t *testing.T) {
	h, _ := newHTTPFixture(t)
	s := h.store
	ctx := context.Background()
	tenant := "route-cas-" + httpSuffix(t)
	old, err := s.PutModelRoute(ctx, tenant, "route", 0, json.RawMessage(`{"model":"one"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteModelRoute(ctx, tenant, "route", old.Revision); err != nil {
		t.Fatal(err)
	}
	recreated, err := s.PutModelRoute(ctx, tenant, "route", 0, json.RawMessage(`{"model":"two"}`))
	if err != nil {
		t.Fatal(err)
	}
	if recreated.Revision <= old.Revision {
		t.Fatalf("route revision went backwards across recreation: before=%d after=%d", old.Revision, recreated.Revision)
	}
	if _, err = s.PutModelRoute(ctx, tenant, "route", old.Revision, json.RawMessage(`{"model":"stale"}`)); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("stale pre-delete editor changed new route: %v", err)
	}
}

func TestDotConcurrentPinReturnsSameSnapshot(t *testing.T) {
	h, _ := newHTTPFixture(t)
	s := h.store
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	scope := contracts.Scope{Tenant: "pin-race-" + httpSuffix(t), Subject: "alice", Deployment: "space"}
	if _, err := s.PutModelRoute(ctx, scope.Tenant, "fast", 0, json.RawMessage(`{"model":"one"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutConfig(ctx, scope.Tenant, DotAgentID, 0, json.RawMessage(`{"model_route":"fast","max_steps":4,"computer_policy":"disabled","policy":{"allowed_model_routes":["fast"],"allowed_computer_policies":["disabled"]}}`)); err != nil {
		t.Fatal(err)
	}
	barrier, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Rollback(context.Background())
	var barrierPID int
	if err := barrier.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&barrierPID); err != nil {
		t.Fatal(err)
	}
	if _, err := barrier.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "dot-pin/"+scope.Tenant+"/"+scope.Subject+"/"+scope.Deployment+"/same-key"); err != nil {
		t.Fatal(err)
	}
	type result struct {
		snapshot DotSnapshot
		err      error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			snapshot, err := s.PinDotConfig(ctx, scope, "same-key", "conversation")
			results <- result{snapshot, err}
		}()
	}
	// Both transactions have acquired their RR snapshot before the held key
	// lock. Inspect actual PostgreSQL waiters instead of relying on a sleep.
	for {
		var waiters int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks w JOIN pg_locks b
ON w.locktype=b.locktype AND w.database=b.database AND w.classid=b.classid AND w.objid=b.objid AND w.objsubid=b.objsubid
WHERE b.pid=$1 AND b.locktype='advisory' AND b.granted AND NOT w.granted`, barrierPID).Scan(&waiters); err != nil {
			t.Fatal(err)
		}
		if waiters == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("two concurrent pins did not reach the advisory barrier")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := barrier.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent admission: first=%v second=%v", first.err, second.err)
	}
	a, _ := json.Marshal(first.snapshot)
	b, _ := json.Marshal(second.snapshot)
	if string(a) != string(b) {
		t.Fatalf("same-key snapshots differ: %s vs %s", a, b)
	}
}

func TestDotPinObservesErasureCommittedWhileWaiting(t *testing.T) {
	h, _ := newHTTPFixture(t)
	s := h.store
	for _, fence := range []string{"tenant", "subject"} {
		t.Run(fence, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			scope := contracts.Scope{Tenant: "pin-fence-" + httpSuffix(t), Subject: "alice", Deployment: "space"}
			if _, err := s.PutModelRoute(ctx, scope.Tenant, "fast", 0, json.RawMessage(`{"model":"one"}`)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.PutConfig(ctx, scope.Tenant, DotAgentID, 0, json.RawMessage(`{"model_route":"fast","max_steps":4,"policy":{"allowed_model_routes":["fast"]}}`)); err != nil {
				t.Fatal(err)
			}
			barrier, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer barrier.Rollback(context.Background())
			var pid int
			if err := barrier.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
				t.Fatal(err)
			}
			key := tenantFenceKey(scope.Tenant)
			if fence == "subject" {
				key = "ahp_registry/subject-erasure/" + scope.Tenant + "/" + scope.Subject
			}
			if _, err := barrier.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, key); err != nil {
				t.Fatal(err)
			}
			if fence == "tenant" {
				_, err = barrier.Exec(ctx, `INSERT INTO ahp_registry.erased_tenants(tenant) VALUES($1)`, scope.Tenant)
			} else {
				_, err = barrier.Exec(ctx, `INSERT INTO ahp_registry.erased_subjects(tenant,subject) VALUES($1,$2)`, scope.Tenant, scope.Subject)
			}
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				_, err := s.PinDotConfig(ctx, scope, "key", "conversation")
				result <- err
			}()
			for {
				var waiting bool
				if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("pin did not wait on erasure fence")
				case <-time.After(5 * time.Millisecond):
				}
			}
			if err := barrier.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-result; contracts.Code(err) != contracts.ErrForbidden {
				t.Fatalf("pin after committed %s erasure: %v", fence, err)
			}
		})
	}
}

func TestDotPinCanceledWaitReleasesLocksWithSingleConnection(t *testing.T) {
	h, _ := newHTTPFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := h.store.pool.Config().Copy()
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := New(pool)
	scope := contracts.Scope{Tenant: "pin-cancel-" + httpSuffix(t), Subject: "alice", Deployment: "space"}
	if _, err := s.PutModelRoute(ctx, scope.Tenant, "fast", 0, json.RawMessage(`{"model":"one"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutConfig(ctx, scope.Tenant, DotAgentID, 0, json.RawMessage(`{"model_route":"fast","max_steps":4,"policy":{"allowed_model_routes":["fast"]}}`)); err != nil {
		t.Fatal(err)
	}
	barrier, err := h.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Rollback(context.Background())
	var pid int
	if err := barrier.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := barrier.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "dot-pin/"+scope.Tenant+"/"+scope.Subject+"/"+scope.Deployment+"/key"); err != nil {
		t.Fatal(err)
	}
	pinCtx, stopPin := context.WithCancel(ctx)
	defer stopPin()
	result := make(chan error, 1)
	go func() {
		_, err := s.PinDotConfig(pinCtx, scope, "key", "conversation")
		result <- err
	}()
	for {
		var waiting bool
		if err := h.store.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("pin did not wait on key lock")
		case <-time.After(5 * time.Millisecond):
		}
	}
	stopPin()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled pin: %v", err)
	}
	for _, key := range []string{tenantFenceKey(scope.Tenant), "ahp_registry/subject-erasure/" + scope.Tenant + "/" + scope.Subject} {
		// PostgreSQL may observe a cancelled connection's close after the client
		// returns. Bound actual lock acquisition instead of an immediate try.
		if _, err := barrier.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, key); err != nil {
			t.Fatalf("cancelled pin leaked erasure lock: %v", err)
		}
	}
	if err := barrier.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PinDotConfig(ctx, scope, "key", "conversation"); err != nil {
		t.Fatalf("single connection did not recover after cancellation: %v", err)
	}
}
