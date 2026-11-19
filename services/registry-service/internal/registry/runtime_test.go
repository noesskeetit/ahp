package registry_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Wire regression only: actual image evidence is exercised by the container
// acceptance command, never inferred from these two in-process HTTP endpoints.
func TestImmutableRuntimeRoutingAndUnavailability(t *testing.T) {
	// A SYNTHETIC stand, elected by name: this test proves immutable digest
	// routing and binding immutability, not signature verification, and it must
	// not depend on an attestation being believed just because it is non-empty.
	store, tenant := newSyntheticStore(t)
	ctx := context.Background()
	agent := "runtime-" + randSuffix(t)
	m1 := manifest(t, "runtime-one")
	m1.Components[0].Image = "example/agent@sha256:" + strings.Repeat("a", 64)
	m2 := m1
	m2.Components = append([]registry.Component(nil), m1.Components...)
	m2.Components[0].Image = "example/agent@sha256:" + strings.Repeat("b", 64)
	v1 := publish(t, store, agent, m1)
	v2 := publish(t, store, agent, m2)
	if v1.Digest == v2.Digest {
		t.Fatal("different image must produce different version digest")
	}
	inst, err := store.Install(ctx, tenant, agent, v1.ID, "dev", "runtime-"+randSuffix(t), registry.Capabilities{SelfTasks: 1})
	if err != nil {
		t.Fatal(err)
	}
	var calls [2]atomic.Int64
	bindings := []registry.RuntimeBinding{}
	for i, v := range []registry.Version{v1, v2} {
		i, v := i, v
		b := registry.RuntimeBinding{Tenant: tenant, Deployment: "target-" + v.ID, Environment: "dev", AgentID: agent, Digest: v.Digest, Image: "example/agent@sha256:" + strings.Repeat(string(rune('a'+i)), 64), Attestation: "test protocol binding (not deployment proof)"}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/readyz" {
				_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": agent, "version_digest": v.Digest, "image": b.Image})
				return
			}
			if r.Header.Get("Authorization") != "Bearer fixture-child-token" {
				t.Error("target did not receive child token")
			}
			calls[i].Add(1)
			var request struct {
				ID     json.RawMessage `json:"id"`
				Params struct {
					Message struct {
						Context string `json:"contextId"`
					} `json:"message"`
				} `json:"params"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"id": "task-" + v.ID, "contextId": request.Params.Message.Context}})
		}))
		t.Cleanup(srv.Close)
		b.Endpoint = srv.URL + "/a2a"
		b.ReadyURL = srv.URL + "/readyz"
		bindings = append(bindings, b)
		if err := store.RegisterRuntime(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	disp, err := registry.NewRoutedA2AInvokeDispatcher(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	disp.SetRuntimeTokenIssuer(protocolTestIssuer{})
	for i, v := range []registry.Version{v1, v2} {
		if _, err := disp.Invoke(ctx, registry.InvokeRequest{Tenant: tenant, TaskID: "grant-" + v.ID, AgentID: agent, InstallationID: inst.ID, Environment: "dev", VersionID: v.ID, VersionDigest: v.Digest, Input: json.RawMessage(`"work"`)}); err != nil {
			t.Fatal(err)
		}
		if calls[i].Load() != 1 {
			t.Fatalf("version %d routed incorrectly", i)
		}
	}
	legacy := publish(t, store, "legacy-"+agent, manifest(t, "legacy-runtime"))
	legacyBinding := bindings[0]
	legacyBinding.AgentID = legacy.AgentID
	legacyBinding.Digest = legacy.Digest
	legacyBinding.Deployment = "legacy-" + v1.ID
	// A metadata-only version (no image declared in its manifest) still gets a
	// binding on the synthetic stand, and invoking it stays refused because no
	// runtime is bound to its digest.
	if err := store.RegisterRuntime(ctx, legacyBinding); err != nil {
		t.Fatal(err)
	}
	if _, err := disp.Invoke(ctx, registry.InvokeRequest{Tenant: tenant, AgentID: legacy.AgentID, Environment: "dev", VersionID: legacy.ID, VersionDigest: legacy.Digest}); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("metadata-only version invoked: %v", err)
	}
	changed := bindings[0]
	changed.Endpoint = bindings[1].Endpoint
	changed.ReadyURL = bindings[1].ReadyURL
	if err := store.RegisterRuntime(ctx, changed); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("immutable binding mutated: %v", err)
	}
	if _, err := disp.Invoke(ctx, registry.InvokeRequest{Tenant: "foreign", AgentID: agent, Environment: "dev", VersionID: v1.ID, VersionDigest: v1.Digest}); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("foreign/no runtime must fail closed: %v", err)
	}
	if _, err := store.Pin(ctx, tenant, inst.ID, "pin-"+agent); err != nil {
		t.Fatal(err)
	}
	another, err := store.Install(ctx, tenant, agent, v1.ID, "stage", "stage-"+randSuffix(t), registry.Capabilities{SelfTasks: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pin(ctx, tenant, another.ID, "pin-"+agent); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("cross-installation pin reused: %v", err)
	}
	if _, err := store.ResolveConfig(ctx, tenant, another.ID, "pin-"+agent); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("cross-installation resolve reused: %v", err)
	}
}

type protocolTestIssuer struct{}

func (protocolTestIssuer) IssueRuntimeToken(context.Context, registry.InvokeRequest, registry.RuntimeBinding) (string, error) {
	return "fixture-child-token", nil
}
