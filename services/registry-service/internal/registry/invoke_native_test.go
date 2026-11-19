package registry_test

import (
	"context"
	"encoding/json"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Actual pinned native A2A server and Registry PG. Token/admission are explicit
// fixtures; this does not claim native State persistence, inference or mTLS.
func TestRegistryOneShotNativeA2AUsesOriginalGrantAfterAliasAndDrain(t *testing.T) {
	store, tenant := newSyntheticStore(t)
	agent := "invoke-native-" + randSuffix(t)
	m := manifest(t, "invoke-native")
	m.Components[0].Image = "example/native@sha256:" + strings.Repeat("f", 64)
	v := publish(t, store, agent, m)
	inst, err := store.Install(t.Context(), tenant, agent, v.ID, "dev", "invoke-"+randSuffix(t), m.Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	grant := "invoke-grant-" + randSuffix(t)
	scope := contracts.Scope{Tenant: tenant, Deployment: "space", Subject: "owner"}
	if _, err := store.PinForSubject(t.Context(), scope, inst.ID, grant); err != nil {
		t.Fatal(err)
	}
	var ready atomic.Bool
	ready.Store(true)
	var executions atomic.Int32
	release := make(chan struct{})
	wire := a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(a2asrv.AgentExecutorFunc(func(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			executions.Add(1)
			if ec.Message.Role != a2a.MessageRoleUser || ec.Message.Parts[0].Text() != "hello native" || ec.ContextID == "" {
				t.Error("one-shot invocation lost native role, input or context")
			}
			if !yield(a2a.NewSubmittedTask(ec, ec.Message), nil) {
				return
			}
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			yield(a2a.NewStatusUpdateEvent(ec, a2a.TaskStateCompleted, nil), nil)
		}
	})))
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			if !ready.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": agent, "version_digest": v.Digest, "image": m.Components[0].Image})
			return
		}
		if r.Header.Get("Authorization") != "Bearer fixture-target-token" {
			t.Error("runtime received a parent or missing credential")
		}
		wire.ServeHTTP(w, r)
	}))
	defer srv.Close()
	defer close(release)
	b := registry.RuntimeBinding{Tenant: tenant, Deployment: "native-invoke", Environment: "dev", AgentID: agent, Digest: v.Digest, Image: m.Components[0].Image, Attestation: "synthetic protocol fixture", Endpoint: srv.URL + "/a2a", ReadyURL: srv.URL + "/readyz"}
	if err := store.RegisterRuntime(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	d, err := registry.NewRoutedA2AInvokeDispatcher(store, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	issuer := &agentWireIssuer{}
	d.SetRuntimeTokenIssuer(issuer)
	req := registry.InvokeRequest{Scope: scope, Tenant: tenant, AgentID: agent, VersionID: v.ID, VersionDigest: v.Digest, InstallationID: inst.ID, Environment: "dev", TaskID: grant, Input: json.RawMessage(`"hello native"`)}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	remote, err := d.Invoke(ctx, req)
	if err != nil || remote == "" || remote == grant {
		t.Fatalf("one-shot native Invoke did not return an opaque task while execution waits: remote=%q error=%v", remote, err)
	}
	if err := store.AliasTaskPin(t.Context(), tenant, inst.ID, grant, remote); err != nil {
		t.Fatal(err)
	}
	if err := store.AliasTaskPin(t.Context(), tenant, inst.ID, grant, remote); err != nil {
		t.Fatal("lost alias acknowledgement could not replay the same mapping", err)
	}
	competing := "other-grant-" + randSuffix(t)
	if _, err := store.PinForSubject(t.Context(), scope, inst.ID, competing); err != nil {
		t.Fatal(err)
	}
	if err := store.AliasTaskPin(t.Context(), tenant, inst.ID, competing, remote); contracts.Code(err) != contracts.ErrConflict {
		t.Fatal("another invocation replaced the existing runtime mapping", err)
	}
	if _, err := store.PinnedVersion(t.Context(), tenant, competing); err != nil {
		t.Fatal("failed alias deleted the other invocation reservation", err)
	}
	// A native task id is not its Identity grant. Observation must resolve the
	// original grant from the owned durable alias, including on a fresh router.
	req.Observe, req.TaskID = true, remote
	ready.Store(false)
	restarted, err := registry.NewRoutedA2AInvokeDispatcher(store, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	restarted.SetRuntimeTokenIssuer(issuer)
	raw, err := restarted.Observe(ctx, req)
	if err != nil {
		t.Fatal("observation lost the task or original grant when readiness closed", err)
	}
	var task a2a.Task
	if json.Unmarshal(raw, &task) != nil || string(task.ID) != remote || task.ContextID == "" || task.Status.State != a2a.TaskStateSubmitted {
		t.Fatalf("native observed task changed: %s", raw)
	}
	if len(issuer.calls) != 2 || issuer.calls[1].TaskID != grant || !issuer.calls[1].Observe || executions.Load() != 1 {
		t.Fatalf("observation reissued authority for the runtime id or reran work: calls=%d graphs=%d", len(issuer.calls), executions.Load())
	}
	for _, change := range []string{"subject", "installation"} {
		foreign := req
		if change == "subject" {
			foreign.Scope.Subject = "other-owner"
		} else {
			foreign.InstallationID = "other-installation"
		}
		if _, err := restarted.Observe(ctx, foreign); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("foreign %s resolved the invocation grant: %v", change, err)
		}
	}
	if len(issuer.calls) != 2 {
		t.Fatal("foreign observation reached token issuance")
	}
}
