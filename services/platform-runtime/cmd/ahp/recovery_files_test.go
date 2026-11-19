//go:build ahp_data

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// A checkpoint that names a file is only worth recovering if the file comes
// back with it. What existed before this leg proved the metadata survives
// (internal/state checkpoint round-trips) and that the artifact owner admits a
// descriptor at write time (TestCheckpointArtifactOwnerUsesLiveIdentityThrough-
// StateHTTP). Neither of them reopened a run and read its bytes.
//
// So this leg runs the whole lifecycle over real PostgreSQL and real object
// storage: a task seals a file, its run records the descriptor twice the way an
// SDK does it -- once as checkpoint metadata, once as a channel part -- the
// worker then dies (the run goes back to pending with its lease cleared), and a
// NEW State process over the same database takes the run, recovers both
// descriptors and reads the bytes through the artifact owner. The assertions are
// on the file, not about it: the recovered digest equals the digest of the bytes
// actually streamed, and those bytes equal what was written.
//
// The last block is what stops this being a tautology. A recovered descriptor
// that resolved to a cached or copied payload would pass every check above, so
// after the recovery the artifact is deleted and the same read must fail. The
// recovered run really does depend on the artifact owner holding the object.
func TestRecoveredRunReadsTheFileItsCheckpointNames(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	source := state.ReferenceSource(pool, "ahp_state")
	artifacts.WithReferenceKeeper(artifacts.ReferenceScanFunc(
		func(ctx context.Context, tenant string, arts []artifacts.ArtifactQuery) (map[string]bool, error) {
			queries := make([]references.Artifact, len(arts))
			for i, a := range arts {
				queries[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
			}
			out, err := source.InspectReferences(ctx, tenant, nil, queries)
			return out.Referenced, err
		}))(art)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	artifacts.WithCheckpointSettlement(st.CheckpointOperationSettled)(art)
	st.SetCheckpointArtifacts(checkpointArtifactOwner{Store: art, identity: ids})

	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("checkpoint-owner", "key", priv)
	verifier := auth.NewVerifier("checkpoint-owner", map[string]ed25519.PublicKey{"key": pub})
	contextID := scope.Tenant + "-context"
	if _, err := ids.CreateRoot(ctx, scope, identity.RootRequest{
		Task: "task", Context: contextID, Deployment: scope.Deployment,
		Capabilities: []string{"state", "artifacts"}, TTL: 10 * time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	token, err := ids.MintTask(ctx, scope, "task", signer, "ahp", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := stateapi.New(st, verifier, ids.Authorize,
		func(context.Context, contracts.Scope, string) (string, error) { return token, nil })
	if err != nil {
		t.Fatal(err)
	}
	artFace, err := artifactsapi.New(art, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	front := http.NewServeMux()
	front.Handle("/v1/state/", handler)
	front.Handle("/v1/artifacts/", artFace)
	server := httptest.NewUnstartedServer(front)
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	defer server.Close()
	client := server.Client()
	tr := client.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	client.Transport = tr

	thread, err := st.CreateThread(ctx, scope, contextID, "thread-recovered")
	if err != nil {
		t.Fatal(err)
	}
	run, err := st.CreateRun(ctx, scope, thread.ID, "task", "run-recovered", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := st.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "worker-before-restart", run.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("ClaimRunFor: lease=%v err=%v", lease, err)
	}

	// The file is written before the grant-label derivation is wired in, in the
	// same order the accepted owner leg uses: writing an artifact needs the
	// task's own principal, and the derivation is a property of the reads that
	// follow.
	body := "the file this run produced before it died"
	fileID := sealArtifact(t, ctx, art, scope,
		artifacts.Binding{ContextID: contextID, TaskID: "task"}, body)
	ref, err := art.Get(ctx, scope, fileID)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := json.Marshal(references.CheckpointArtifact{
		ID: ref.ID, Tenant: ref.Tenant, Deployment: ref.Deployment, Subject: ref.Subject,
		ContextID: ref.ContextID, TaskID: ref.TaskID, Version: ref.Version,
		SHA256: ref.SHA256, Size: ref.Size, Labels: ref.Labels,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifacts.WithGrantLabels(dataArtifactGrantLabels(ids))(art)

	put := func(name string, cp state.Checkpoint) {
		t.Helper()
		buf, err := json.Marshal(map[string]any{
			"key": name, "checkpoint": cp,
			"fence": state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch},
		})
		if err != nil {
			t.Fatal(err)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
			server.URL+"/v1/state/PutCheckpoint", bytes.NewReader(buf))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status=%d body=%s", name, resp.StatusCode, response)
		}
	}
	// Both shapes an SDK uses: the descriptor as checkpoint metadata, and as the
	// value of a channel part. The part is the one a real turn's file lives in.
	put("cp-meta", state.Checkpoint{ID: "cp-meta", Namespace: "main", Version: 1,
		Metadata: descriptor, MetadataEncoding: state.CheckpointArtifactEncoding})
	put("cp-part", state.Checkpoint{ID: "cp-part", Namespace: "main", Version: 2, Parent: "cp-meta",
		Parts: []state.Part{{Channel: "files", Version: "v1",
			Encoding: state.CheckpointArtifactEncoding, Data: descriptor}}})

	// The worker dies. Not a terminal release: the run goes back to pending with
	// its lease cleared, which is what a crashed process leaves behind.
	if err := st.Release(ctx, scope, run.ID, lease.Worker, lease.Epoch, state.RunPending); err != nil {
		t.Fatalf("release back to pending: %v", err)
	}

	// A new process: new State store, new owner wiring, same database.
	recovered := state.New(pool)
	recovered.SetCheckpointArtifacts(checkpointArtifactOwner{Store: art, identity: ids})
	resumed, err := recovered.ClaimRunFor(ctx, scope.Tenant, scope.Deployment,
		"worker-after-restart", run.ID, time.Minute)
	if err != nil || resumed == nil {
		t.Fatalf("the restarted worker cannot take the run back: lease=%v err=%v", resumed, err)
	}
	if resumed.Epoch <= lease.Epoch {
		t.Fatalf("resuming must fence the previous worker: epoch %d <= %d", resumed.Epoch, lease.Epoch)
	}
	if again, err := recovered.GetRun(ctx, scope, run.ID); err != nil || again.ID != run.ID ||
		again.ThreadID != thread.ID {
		t.Fatalf("the recovered run is not the run that died: %+v err=%v", again, err)
	}

	// The descriptors survived, in both places they were written.
	metadata, err := recovered.GetCheckpoint(ctx, scope, thread.ID, "main", "cp-meta")
	if err != nil {
		t.Fatalf("GetCheckpoint: %v", err)
	}
	var fromMetadata references.CheckpointArtifact
	if err := json.Unmarshal(metadata.Metadata, &fromMetadata); err != nil {
		t.Fatalf("the recovered metadata is not an artifact descriptor: %v (%s)", err, metadata.Metadata)
	}
	part, err := recovered.GetPartEncoded(ctx, scope, thread.ID, "main", "files", "v1")
	if err != nil {
		t.Fatalf("GetPartEncoded: %v", err)
	}
	if part.Encoding != state.CheckpointArtifactEncoding {
		t.Fatalf("the recovered part lost its descriptor encoding: %q", part.Encoding)
	}
	var fromPart references.CheckpointArtifact
	if err := json.Unmarshal(part.Data, &fromPart); err != nil {
		t.Fatalf("the recovered part is not an artifact descriptor: %v (%s)", err, part.Data)
	}
	for _, recorded := range []references.CheckpointArtifact{fromMetadata, fromPart} {
		if recorded.ID != fileID || recorded.SHA256 != ref.SHA256 || recorded.Size != ref.Size {
			t.Fatalf("the recovered descriptor is not the file the run wrote: %+v want id=%s sha=%s",
				recorded, fileID, ref.SHA256)
		}
	}

	// The file itself, streamed by the artifact face to the recovered run's own
	// task credential over mTLS -- the path a resumed worker takes, and the one
	// where the label/authority gate really applies. Hashed, not trusted.
	call := func(op, path string, body any) (int, []byte) {
		t.Helper()
		buf, _ := json.Marshal(body)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+path, bytes.NewReader(buf))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		defer resp.Body.Close()
		payload, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("%s: read body: %v", op, err)
		}
		return resp.StatusCode, payload
	}
	download := func(op, id string) (int, []byte) {
		t.Helper()
		return call(op, "/v1/artifacts/Download", map[string]any{"id": id})
	}
	status, content := download("recovered read", fromPart.ID)
	if status != http.StatusOK {
		t.Fatalf("the recovered run cannot read its own file: status=%d body=%s", status, content)
	}
	sum := sha256.Sum256(content)
	if got := hex.EncodeToString(sum[:]); got != fromPart.SHA256 {
		t.Fatalf("the recovered bytes are not the recorded digest: bytes=%s descriptor=%s",
			got, fromPart.SHA256)
	}
	if string(content) != body {
		t.Fatalf("the recovered run read someone else's file: %q", content)
	}

	// A live checkpoint is a durable reference: ordinary artifact deletion
	// must refuse while a recovered worker still needs these bytes.
	if status, payload := call("protected delete", "/v1/artifacts/Delete", map[string]any{"id": fileID, "key": "delete-live-checkpoint"}); status != http.StatusConflict {
		t.Fatalf("live recovery reference did not protect its file: status=%d body=%s", status, payload)
	}
	// Anti-tautology after explicitly releasing the owning thread's references:
	// the real owner tombstones its object, and the same download must fail.
	if status, payload := call("delete owning thread", "/v1/state/DeleteThread", map[string]any{"thread_id": thread.ID}); status != http.StatusOK {
		t.Fatalf("release recovery references: status=%d body=%s", status, payload)
	}
	if del, payload := call("delete", "/v1/artifacts/Delete", map[string]any{"id": fileID, "key": "del-after-recovery"}); del != http.StatusOK {
		t.Fatalf("Delete: status=%d body=%s", del, payload)
	}

	if status, payload := download("read after deletion", fromPart.ID); status == http.StatusOK {
		t.Fatalf("the recovered descriptor resolved after the artifact was deleted: %s", payload)
	} else if status != http.StatusNotFound {
		t.Fatalf("a deleted artifact must read as absent, got status=%d body=%s", status, payload)
	}
}
