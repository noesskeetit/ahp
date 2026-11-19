package stateapi

import (
	"fmt"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"net/http"
	"testing"
	"time"
)

func TestTaskCannotPerformAdministrativeStateOperations(t *testing.T) {
	e := newEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject}
	th, err := e.store.CreateThread(e.ctx, scope, "root-admin-boundary", "create")
	if err != nil {
		t.Fatal(err)
	}
	token := e.signTask(t, "ordinary-task")
	for _, op := range []string{"SetLegalhold", "RemoveLegalhold", "ExportThread", "DeleteSubject", "ControlDeleteSubject", "ControlDeleteTenant"} {
		t.Run(op, func(t *testing.T) {
			body := fmt.Sprintf(`{"thread_id":%q}`, th.ID)
			if op == "SetLegalhold" {
				body = fmt.Sprintf(`{"thread_id":%q,"reason":"task injected"}`, th.ID)
			}
			if op == "DeleteSubject" {
				body = `{}`
			}
			code, got := e.call(t, op, token, body)
			if code != http.StatusForbidden {
				t.Errorf("ordinary task got %d for %s: %v", code, op, got)
			}
		})
	}
}

func (e *env) adminExport(t *testing.T, threadID string) (int, map[string]any) {
	return adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject, "ExportThread", fmt.Sprintf(`{"thread_id":%q}`, threadID), "state-admin")
}
func (p *laneWorkload) adminExport(t *testing.T, threadID string) (int, map[string]any) {
	return adminCall(t, p.env, p.client, p.tenant, p.deployment, p.subject, "ExportThread", fmt.Sprintf(`{"thread_id":%q}`, threadID), "state-admin")
}
func adminCall(t *testing.T, e *env, client *http.Client, tenant, deployment, subject, op, input, actor string) (int, map[string]any) {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{Kind: auth.KindHuman, Tenant: tenant, Subject: actor, Audience: testAudience}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"target":{"deployment":%q,"subject":%q},"input":%s}`, deployment, subject, input)
	return doCall(t, client, e.srv.URL+"/v1/state-admin/"+op, http.MethodPost, tok, body)
}

func TestAdministrativeStateRouteRequiresHumanPermissionAndTenant(t *testing.T) {
	e := newEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject}
	th, err := e.store.CreateThread(e.ctx, scope, "admin-positive", "create")
	if err != nil {
		t.Fatal(err)
	}
	input := fmt.Sprintf(`{"thread_id":%q,"reason":"retention"}`, th.ID)
	code, body := adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject, "SetLegalhold", input, "ordinary-human")
	mustStatus(t, code, http.StatusForbidden, body)
	task := e.signTask(t, "task-admin-forbidden")
	code, body = doCall(t, e.srv.Client(), e.srv.URL+"/v1/state-admin/SetLegalhold", http.MethodPost, task, fmt.Sprintf(`{"target":{"deployment":%q,"subject":%q},"input":%s}`, e.deployment, testSubject, input))
	mustStatus(t, code, http.StatusForbidden, body)
	code, body = adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject, "SetLegalhold", input, "state-admin")
	mustStatus(t, code, http.StatusOK, body)
	code, body = e.call(t, "DeleteThread", task, fmt.Sprintf(`{"thread_id":%q}`, th.ID))
	if code == http.StatusOK {
		t.Fatal("hold did not prevent task deletion")
	}
	foreign := e.foreignWorkload(t, testSubject)
	code, body = adminCall(t, e, foreign.client, foreign.tenant, e.deployment, testSubject, "ExportThread", fmt.Sprintf(`{"thread_id":%q}`, th.ID), "state-admin")
	mustStatus(t, code, http.StatusNotFound, body)
	code, body = adminCall(t, e, foreign.client, e.tenant, e.deployment, testSubject, "ExportThread", fmt.Sprintf(`{"thread_id":%q}`, th.ID), "state-admin")
	mustStatus(t, code, http.StatusForbidden, body)
	code, body = adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject, "RemoveLegalhold", fmt.Sprintf(`{"thread_id":%q}`, th.ID), "state-admin")
	mustStatus(t, code, http.StatusOK, body)
	code, body = e.adminExport(t, th.ID)
	mustStatus(t, code, http.StatusOK, body)
	code, body = adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject, "DeleteSubject", `{}`, "state-admin")
	mustStatus(t, code, http.StatusOK, body)
	if _, err = e.store.GetThread(e.ctx, scope, th.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("admin deletion did not remove thread: %v", err)
	}
}

func TestTenantDeletionRequiresSeparateCapability(t *testing.T) {
	e := newEnv(t)
	code, body := adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject, "ControlDeleteTenant", `{"key":"tenant"}`, "state-admin")
	mustStatus(t, code, http.StatusForbidden, body)
}
