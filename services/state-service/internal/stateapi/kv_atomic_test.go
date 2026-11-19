package stateapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func rev7APIEnv(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("real PG URL required; no shared fallback")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("URL DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	maint, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("maintenance connection failed")
	}
	t.Cleanup(maint.Close)
	name := fmt.Sprintf("ahp_rev7_http_%d", time.Now().UnixNano())
	if _, err = maint.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal("own database creation failed; no fallback")
	}
	t.Cleanup(func() {
		c, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if _, e := maint.Exec(c, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()); e != nil {
			t.Error("own database cleanup failed")
		}
	})
	u.Path = "/" + name
	t.Setenv("AHP_TEST_DATABASE_URL", u.String())
	return newEnv(t)
}
func TestRev7AtomicKVOverMTLS(t *testing.T) {
	e := rev7APIEnv(t)
	option, err := state.WithKVNamespacePolicies([]state.KVNamespacePolicy{
		{Tenant: e.tenant, Deployment: e.deployment, Namespace: []string{"adk-user"}, Owner: "subject"},
		{Tenant: e.tenant, Deployment: e.deployment, Namespace: []string{"adk-app"}, Owner: "deployment"},
	})
	if err != nil {
		t.Fatal(err)
	}
	option(e.store)
	if err = e.store.Migrate(e.ctx); err != nil {
		t.Fatal(err)
	}
	marshal := func(v any) string {
		b, x := json.Marshal(v)
		if x != nil {
			t.Fatal(x)
		}
		return string(b)
	}
	own := func(subject string) (string, map[string]any) {
		token, x := e.signer.Sign(auth.Claims{Audience: testAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment, Subject: subject, Task: "task-" + contracts.NewID(), Root: "root", Context: "context", Grant: "grant", Depth: 0}, time.Minute)
		if x != nil {
			t.Fatal(x)
		}
		st, th := e.call(t, "CreateThread", token, marshal(map[string]any{"context_id": subject, "key": "thread-" + subject}))
		mustStatus(t, st, http.StatusOK, th)
		st, run := e.call(t, "CreateRun", token, marshal(map[string]any{"thread_id": th["id"], "key": "run-" + subject, "payload": map[string]any{}}))
		mustStatus(t, st, http.StatusOK, run)
		st, claim := e.call(t, "ClaimRun", "", marshal(map[string]any{"run_id": run["id"], "worker": "worker-" + subject, "ttl_ms": 30000}))
		mustStatus(t, st, http.StatusOK, claim)
		l := claim["lease"].(map[string]any)
		return claim["task_token"].(string), map[string]any{"run_id": run["id"], "worker": l["worker"], "epoch": l["epoch"]}
	}
	alice, af := own("alice")
	bob, _ := own("bob")
	items := []any{
		map[string]any{"namespace": []string{"adk-user"}, "key": "state", "expected_owner": "subject", "expected_version": 0, "value": map[string]any{"favorite": "blue"}},
		map[string]any{"namespace": []string{"adk-app"}, "key": "state", "expected_owner": "deployment", "expected_version": 0, "value": map[string]any{"theme": "dark"}},
	}
	payload := marshal(map[string]any{"fence": af, "batch": map[string]any{"operation_key": "native-event-1", "items": items}})
	st, body := e.call(t, "PutKVBatch", alice, payload)
	mustStatus(t, st, http.StatusOK, body)
	keys := []any{map[string]any{"namespace": []string{"adk-user"}, "key": "state", "expected_owner": "subject"}, map[string]any{"namespace": []string{"adk-app"}, "key": "state", "expected_owner": "deployment"}}
	st, body = e.call(t, "GetKVBatch", bob, marshal(map[string]any{"keys": keys}))
	mustStatus(t, st, http.StatusOK, body)
	values := body["items"].([]any)
	if len(values) != 2 || values[0] != nil || values[1] == nil {
		t.Fatal("private user/global app scope mismatch")
	}
	if values[1].(map[string]any)["value"].(map[string]any)["theme"] != "dark" {
		t.Fatal("shared value missing")
	}
	st, body = e.call(t, "GetKVBatch", bob, marshal(map[string]any{"keys": keys, "subject": "alice"}))
	mustStatus(t, st, http.StatusBadRequest, body)
	st, body = e.call(t, "GetKVBatch", "", marshal(map[string]any{"keys": keys}))
	mustStatus(t, st, http.StatusForbidden, body)
	if body["code"] != "FORBIDDEN" {
		t.Fatal("missing task authority must be denied")
	}
	st, body = e.call(t, "PutKVBatch", bob, payload)
	mustStatus(t, st, http.StatusNotFound, body)
	if body["code"] != "NOT_FOUND" {
		t.Fatal("foreign writer must be hidden as NOT_FOUND")
	}
}
