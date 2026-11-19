package gateway_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestVerifiedGrantBindingIsDurableAndIgnoresSpoofedMetadata(t *testing.T) {
	e := newEnv(t)
	iss := newIssuer(t)
	svc := e.service(t, echoExecutor(), func(cfg *gateway.Config) { cfg.TaskVerifier = iss.verifier; cfg.TaskAudience = iss.audience })
	claims := taskClaims(e, "actual-grant-task", "actual-root", 1)
	claims.Context = "actual-grant-context-" + e.sfx
	ts := attestFront(t, svc, claims.Scope(), slog.Default())
	token := iss.token(t, claims, 5*time.Minute)
	req := sendParams(t, "binding-"+e.sfx, "hello", "")
	req.Metadata = map[string]any{gateway.MetaGrantTask: "forged-task", gateway.MetaGrantContext: "forged-context", gateway.MetaGrantDeployment: "forged-deployment", "caller-note": "keep"}
	req.Message.Metadata = map[string]any{gateway.MetaGrantTask: "forged-message-task", gateway.MetaGrantContext: "forged-message-context", gateway.MetaGrantDeployment: "forged-message-deployment"}
	got := rpcBearer(t, ts, req, token, 1)
	if got.Error != nil {
		t.Fatalf("signed task rejected: %+v", got.Error)
	}
	var response struct {
		Task a2a.Task `json:"task"`
	}
	if err := json.Unmarshal(got.Result, &response); err != nil {
		t.Fatal(err)
	}
	store, err := gateway.NewStore(e.pool, e.schema)
	if err != nil {
		t.Fatal(err)
	}
	row, err := store.Task(context.Background(), claims.Scope(), string(response.Task.ID))
	if err != nil {
		t.Fatal(err)
	}
	if row.ContextID == claims.Context {
		t.Fatal("fixture did not distinguish gateway conversation from Identity context")
	}
	expected := map[string]string{gateway.MetaGrantTask: claims.Task, gateway.MetaGrantContext: claims.Context, gateway.MetaGrantDeployment: claims.Deployment}
	for key, want := range expected {
		if row.Metadata[key] != want {
			t.Fatalf("durable %s=%v, want verified %s", key, row.Metadata[key], want)
		}
	}
	if row.Metadata["caller-note"] != "keep" {
		t.Fatal("ordinary metadata was lost")
	}
	// A replay cannot replace the authoritative reference with caller metadata.
	req.Metadata[gateway.MetaGrantContext] = "another-forgery"
	replay := rpcBearer(t, ts, req, token, 2)
	if replay.Error != nil {
		t.Fatalf("replay rejected: %+v", replay.Error)
	}
	row, err = store.Task(context.Background(), claims.Scope(), string(response.Task.ID))
	if err != nil || row.Metadata[gateway.MetaGrantContext] != claims.Context {
		t.Fatalf("replay rewrote binding: %+v %v", row.Metadata, err)
	}
}

func TestIncompleteVerifiedGrantCannotBecomeDurableMetadata(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)
	for _, field := range []string{"context", "task", "deployment"} {
		t.Run(field, func(t *testing.T) {
			claims := taskClaims(e, "parent", "root", 0)
			switch field {
			case "context":
				claims.Context = ""
			case "task":
				claims.Task = ""
			case "deployment":
				claims.Deployment = ""
			}
			_, err := svc.SendMessage(attestCtx(claims), e.scope, e.caller, sendParams(t, "incomplete-"+field+e.sfx, "hello", ""))
			if contracts.Code(err) != contracts.ErrForbidden || contracts.ReasonOf(err) != gateway.ReasonAttestationUnverifiable {
				t.Fatalf("incomplete %s accepted or misdiagnosed: %v", field, err)
			}
		})
	}
}
