//go:build ahp_data

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestCheckpointArtifactOwnerUsesLiveIdentityThroughStateHTTP(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	source := state.ReferenceSource(pool, "ahp_state")
	artifacts.WithReferenceKeeper(artifacts.ReferenceScanFunc(func(ctx context.Context, tenant string, arts []artifacts.ArtifactQuery) (map[string]bool, error) {
		queries := make([]references.Artifact, len(arts))
		for i, a := range arts {
			queries[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
		}
		out, err := source.InspectReferences(ctx, tenant, nil, queries)
		return out.Referenced, err
	}))(art)
	artifacts.WithCheckpointSettlement(st.CheckpointOperationSettled)(art)
	st.SetCheckpointArtifacts(checkpointArtifactOwner{Store: art, identity: ids})
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("checkpoint-owner", "key", priv)
	verifier := auth.NewVerifier("checkpoint-owner", map[string]ed25519.PublicKey{"key": pub})
	mint := func(task, contextID string, capabilities []string) string {
		t.Helper()
		if _, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: task, Context: contextID, Deployment: scope.Deployment, Capabilities: capabilities, TTL: time.Minute}); err != nil {
			t.Fatal(err)
		}
		token, err := ids.MintTask(ctx, scope, task, signer, "ahp", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	contextID := scope.Tenant + "-context"
	token := mint("task", contextID, []string{"state", "artifacts"})
	missingCapability := mint("no-artifacts", contextID, []string{"state"})
	foreignContext := mint("other-context-task", scope.Tenant+"-other-context", []string{"state", "artifacts"})
	thread, err := st.CreateThread(ctx, scope, contextID, "thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := st.CreateRun(ctx, scope, thread.ID, "task", "run", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if run.ID == run.TaskID {
		t.Fatal("must exercise different RunID and TaskID")
	}
	lease, err := st.ClaimRun(ctx, scope.Tenant, scope.Deployment, "worker", time.Minute)
	if err != nil || lease == nil {
		t.Fatal(err)
	}
	id := sealArtifact(t, ctx, art, scope, artifacts.Binding{ContextID: contextID, TaskID: "task"}, "owner authority checkpoint")
	ref, err := art.Get(ctx, scope, id)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, _ := json.Marshal(references.CheckpointArtifact{ID: ref.ID, Tenant: ref.Tenant, Deployment: ref.Deployment, Subject: ref.Subject, ContextID: ref.ContextID, TaskID: ref.TaskID, Version: ref.Version, SHA256: ref.SHA256, Size: ref.Size, Labels: ref.Labels})
	artifacts.WithGrantLabels(dataArtifactGrantLabels(ids))(art)
	handler, err := stateapi.New(st, verifier, ids.Authorize, func(context.Context, contracts.Scope, string) (string, error) { return token, nil })
	if err != nil {
		t.Fatal(err)
	}
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	defer server.Close()
	client := server.Client()
	tr := client.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	client.Transport = tr
	call := func(token, id string, want int) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"key": id, "fence": state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, "checkpoint": state.Checkpoint{ID: id, Version: 1, Metadata: descriptor, MetadataEncoding: state.CheckpointArtifactEncoding}})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/state/PutCheckpoint", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != want {
			t.Fatalf("%s status=%d want=%d", id, resp.StatusCode, want)
		}
	}
	call(missingCapability, "missing-artifact-capability", http.StatusForbidden)
	call(foreignContext, "wrong-context", http.StatusForbidden)
	call(token, "accepted", http.StatusOK)
	if err := ids.CancelSubtree(ctx, scope, "task"); err != nil {
		t.Fatal(err)
	}
	call(token, "revoked", http.StatusForbidden)
	checkpoints, err := st.ListCheckpoints(ctx, scope, thread.ID, "", 10)
	if err != nil || len(checkpoints) != 1 || checkpoints[0].ID != "accepted" {
		t.Fatalf("refused owner writes persisted: %+v %v", checkpoints, err)
	}
}
