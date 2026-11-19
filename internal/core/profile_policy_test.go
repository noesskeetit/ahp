package core

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	artifacts "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

func TestDotPolicyGuardsSpecialToolExecution(t *testing.T) {
	a := &outputArtifacts{}
	_, err := a.Put(context.Background(), artifacts.PutRequest{Data: []byte("data")})
	if err != nil {
		t.Fatal(err)
	}
	r := NewRuntime(RuntimeConfig{Artifacts: a, ToolAllowed: func(string) bool { return false }})
	req := &space.CoreRequest{ContextID: "ctx", Scope: contracts.Scope{Tenant: "tenant", Subject: "subject", Deployment: "dep"}}
	raw, err := r.executeTool(context.Background(), req, nil, ToolCall{Name: ToolReadOutputChunk, Args: json.RawMessage(`{"id":"artifact","version":"v1","offset":0,"length":4}`)})
	if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("disabled special tool executed: result=%s err=%v", raw, err)
	}
	for _, def := range r.toolDefs() {
		if def.Name == ToolReadOutputChunk {
			t.Fatal("disabled special tool advertised")
		}
	}
}
