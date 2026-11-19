package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	artifacts "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

type outputArtifacts struct {
	ref     artifacts.ArtifactRef
	data    []byte
	fail    bool
	revoked bool
}

func (a *outputArtifacts) Put(_ context.Context, in artifacts.PutRequest) (artifacts.ArtifactRef, error) {
	if a.fail {
		return artifacts.ArtifactRef{}, errors.New("lost upload acknowledgement")
	}
	a.data = append([]byte(nil), in.Data...)
	s := sha256.Sum256(a.data)
	a.ref = artifacts.ArtifactRef{ID: "artifact", Version: "v1", SHA256: hex.EncodeToString(s[:]), Size: int64(len(a.data)), Tenant: "tenant", Deployment: "dep", Subject: "subject", ContextID: "ctx", TaskID: "run"}
	return a.ref, nil
}
func (a *outputArtifacts) Get(context.Context, string) (artifacts.ArtifactRef, error) {
	if a.revoked {
		return artifacts.ArtifactRef{}, contracts.NewError(contracts.ErrForbidden, "revoked")
	}
	return a.ref, nil
}
func (a *outputArtifacts) ReadRange(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	if a.revoked {
		return nil, contracts.NewError(contracts.ErrForbidden, "revoked")
	}
	end := offset + length
	if end > int64(len(a.data)) {
		end = int64(len(a.data))
	}
	return a.data[offset:end], nil
}

type payloadModel struct {
	calls int
	data  json.RawMessage
	tool  bool
	last  *ModelRequest
}

func (m *payloadModel) Generate(_ context.Context, req *ModelRequest) (*ModelResponse, error) {
	m.calls++
	m.last = req
	if m.tool && m.calls == 1 {
		return &ModelResponse{Content: json.RawMessage(`"tool"`), ToolCalls: []ToolCall{{ID: "call", Name: "large", Args: json.RawMessage(`{}`)}}}, nil
	}
	return &ModelResponse{Content: m.data}, nil
}

type payloadTool struct {
	calls int
	data  json.RawMessage
}

func (t *payloadTool) Execute(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	t.calls++
	return t.data, nil
}
func payloadRun() *space.CoreRequest {
	return &space.CoreRequest{RunID: "run", ThreadID: "thread", ContextID: "ctx", TaskToken: "token", Input: json.RawMessage(`"go"`), MaxSteps: 3, Scope: contracts.Scope{Tenant: "tenant", Deployment: "dep", Subject: "subject"}}
}
func TestLargeCoreOutputsAreBoundedReferences(t *testing.T) {
	raw, _ := json.Marshal(string(bytes.Repeat([]byte("й"), 800000)))
	for _, tool := range []bool{false, true} {
		t.Run(map[bool]string{true: "tool", false: "final"}[tool], func(t *testing.T) {
			objects := &outputArtifacts{}
			model := &payloadModel{data: raw, tool: tool}
			exec := &payloadTool{data: raw}
			registry := NewToolRegistry(exec)
			registry.Register(ToolDef{Name: "large"})
			rt := NewRuntime(RuntimeConfig{Model: model, Tools: registry, Artifacts: objects})
			out := make(chan space.StepResult, 16)
			if err := rt.Run(context.Background(), payloadRun(), out); err != nil {
				t.Fatal(err)
			}
			close(out)
			var refs int
			for frame := range out {
				if len(frame.Content) > 64<<10 {
					t.Fatalf("unbounded state output: %d", len(frame.Content))
				}
				var doc map[string]any
				_ = json.Unmarshal(frame.Content, &doc)
				if doc["artifact"] != nil {
					refs++
				}
			}
			if refs == 0 || !bytes.Equal(objects.data, raw) {
				t.Fatal("large content was truncated or no reference emitted")
			}
			if tool && exec.calls != 1 {
				t.Fatal("tool dispatch count")
			}
			args, _ := json.Marshal(map[string]any{"id": objects.ref.ID, "version": objects.ref.Version, "offset": 0, "length": 32})
			chunk, err := rt.readOutputChunk(context.Background(), payloadRun(), args)
			if err != nil || len(chunk) == 0 {
				t.Fatal(err)
			}
			objects.revoked = true
			if _, err = rt.readOutputChunk(context.Background(), payloadRun(), args); err == nil {
				t.Fatal("revoked output readable")
			}
		})
	}
}
func TestLargeToolUploadFailureStopsBeforeAnotherDispatch(t *testing.T) {
	raw, _ := json.Marshal(string(bytes.Repeat([]byte("x"), 2<<20)))
	exec := &payloadTool{data: raw}
	registry := NewToolRegistry(exec)
	registry.Register(ToolDef{Name: "large"})
	model := &payloadModel{data: json.RawMessage(`"done"`), tool: true}
	rt := NewRuntime(RuntimeConfig{Model: model, Tools: registry, Artifacts: &outputArtifacts{fail: true}})
	if err := rt.Run(context.Background(), payloadRun(), make(chan space.StepResult, 16)); contracts.ReasonOf(err) != "SIDE_EFFECT_UNKNOWN" {
		t.Fatalf("need unknown outcome: %v", err)
	}
	if exec.calls != 1 || model.calls != 1 {
		t.Fatalf("continued after unknown: tools=%d model=%d", exec.calls, model.calls)
	}
}
