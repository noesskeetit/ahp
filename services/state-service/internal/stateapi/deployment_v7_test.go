package stateapi

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/auth"
	"net/http"
	"testing"
)

func TestCommonDeploymentNewProfilesRequireHumanAndTransferCapability(t *testing.T) {
	e := rev7APIEnv(t)
	old := e.handler.(*Handler)
	handler, err := NewWithAdmin(e.store, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, old.issue, AdminConfig{Audience: testAudience, Authorize: func(context.Context, auth.Claims, string) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	e.srv.Config.Handler = handler
	for _, op := range []string{"ExportDeploymentV6", "ImportDeploymentV6", "ExportDeploymentV7", "ImportDeploymentV7"} {
		status, body := doCall(t, e.srv.Client(), e.srv.URL+"/v1/state-admin/"+op, http.MethodPost, e.signTask(t, "task"), `{"target":{"deployment":"`+e.deployment+`"},"input":{}}`)
		if status != http.StatusForbidden {
			t.Fatal("versioned operation not protected by administrative human route", op, status, body["code"])
		}
	}
}
