//go:build ahp_data

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DATA lifecycle boundary, pinned with a reproduction rather than a claim.
//
// RESUME.md records the gap as: "cross-context raw StateImport retains
// ArtifactRef.sourcecontext; target token cannot read, so retention proof !=
// import hydration". The two halves are different questions and this leg keeps
// them apart:
//
//   - retention/GC: does the imported conversation's data still EXIST? Yes -
//     State re-materialises the thread and the artifact row is untouched, so
//     nothing about import breaks retention;
//   - hydration: can the imported conversation actually READ the bytes its
//     checkpoints name? No. Read admission carries a conversation gate
//     (internal/artifacts/read_scope.go): a task credential reads the artifacts
//     written into its own context and no others. State import copies the
//     checkpoint payloads verbatim - the artifact reference inside them still
//     names the SOURCE context, and State deliberately leaves artifact
//     authority to its owner (internal/state/control.go:346-357).
//
// So an exported thread imported into a new context replays its structure but
// refuses its own artifact reads - until the Artifacts owner widens them itself.
// The third half below is that operation (GrantContext/RevokeContext in
// internal/artifacts): consent comes from the conversation the bytes belong to,
// the widening is durable and attributable, the binding context never moves,
// and it is reversible. What remains forbidden is the shortcut - rewriting the
// reference at import time, or letting the importer name a context - which is
// the label laundering RESUME forbids.
func TestCrossContextImportThenOwnerGrantHydratesReads(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	// The lane database outlives one test binary, so every identifier carries a
	// per-run nonce: this leg has to pass on the second run against the same live
	// database, not once against a freshly created one.
	nonce := contracts.NewID()
	sourceCtx := "ctxsrc-" + nonce
	targetCtx := "ctxdst-" + nonce
	body := "the bytes the imported conversation cannot read"

	// Seal inside the source conversation, as a task of that conversation would.
	srcCallers := artifacts.WithCallerContext(ctx, sourceCtx)
	id := sealArtifact(t, ctx, e.seed, e.scope,
		artifacts.Binding{ContextID: sourceCtx, TaskID: "tasksrc-" + nonce}, body)
	ref, err := e.seed.Get(srcCallers, e.scope, id)
	if err != nil {
		t.Fatalf("the source conversation must read its own artifact: %v", err)
	}

	// The source thread's checkpoint names the artifact, the way a real
	// LangGraph checkpoint does.
	thread, err := e.state.CreateThread(ctx, e.scope, sourceCtx, "thread-src-"+nonce)
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	payload, err := json.Marshal(map[string]any{"artifact_id": ref.ID, "digest": ref.SHA256})
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.state.CreateRun(ctx, e.scope, thread.ID, "tasksrc-"+nonce, "runsrc-"+nonce, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	lease, err := e.state.ClaimRunFor(ctx, e.scope.Tenant, e.scope.Deployment, "w-src", run.ID, time.Minute)
	if err != nil {
		t.Fatalf("ClaimRunFor: %v", err)
	}
	if _, err := e.state.PutCheckpoint(ctx, e.scope,
		state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch},
		state.Checkpoint{ID: "cp-src-" + nonce, Namespace: "main", Version: 1, Metadata: json.RawMessage(payload)},
		"cp-src-key-"+nonce); err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}

	exp, err := e.state.ExportThread(ctx, e.scope, thread.ID)
	if err != nil {
		t.Fatalf("ExportThread: %v", err)
	}
	imported, err := e.state.ImportThread(ctx, e.scope, exp, targetCtx)
	if err != nil {
		t.Fatalf("ImportThread into a new context: %v", err)
	}

	// Half one: the import is real and the data is there. The copied checkpoint
	// still names the artifact - durable, in the imported thread's own row.
	reimported, err := e.state.ExportThread(ctx, e.scope, imported.ID)
	if err != nil {
		t.Fatalf("ExportThread of the imported thread: %v", err)
	}
	if len(reimported.Checkpoints) == 0 {
		t.Fatal("the import materialised no checkpoints; this leg would prove nothing")
	}
	if !bytes.Contains([]byte(reimported.Checkpoints[0].Metadata), []byte(ref.ID)) {
		t.Fatalf("the imported checkpoint must still name the artifact %s: %s", ref.ID, reimported.Checkpoints[0].Metadata)
	}
	if _, err := e.seed.Get(srcCallers, e.scope, id); err != nil {
		t.Fatalf("retention half of the boundary: the artifact must survive the import: %v", err)
	}

	// Half two: the imported conversation cannot hydrate those bytes. A caller
	// whose context claim is the imported conversation is refused by the
	// artifact owner, and is refused with a reason, not a silent empty read.
	dstCallers := artifacts.WithCallerContext(ctx, targetCtx)
	if _, err := e.seed.Get(dstCallers, e.scope, id); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("imported context reading the source artifact: err=%v (%T), want FORBIDDEN", err, err)
	}

	// Half three: the seam closes the only way that is not laundering. The
	// conversation that OWNS the bytes grants them to the imported conversation,
	// through the artifact owner's own operation; State is not involved, and the
	// artifact's binding context does not move.
	if err := e.seed.GrantContext(srcCallers, e.scope, id, targetCtx, "grant-"+targetCtx); err != nil {
		t.Fatalf("the owning conversation granting its artifact to the imported one: %v", err)
	}
	hydrated, err := e.seed.Get(dstCallers, e.scope, id)
	if err != nil {
		t.Fatalf("hydration: the imported conversation still cannot read after the owner granted: %v", err)
	}
	if hydrated.SHA256 != ref.SHA256 {
		t.Fatalf("the imported conversation read different bytes: %+v", hydrated)
	}

	// The provenance an operator needs: the durable grant names the source, and
	// the artifact still belongs to the conversation it was written in.
	grant, err := e.seed.GetContextGrant(srcCallers, e.scope, id, targetCtx)
	if err != nil {
		t.Fatalf("the grant is not durable: %v", err)
	}
	if grant.SourceContext != sourceCtx {
		t.Fatalf("grant lost its immutable source: %+v", grant)
	}
	var binding string
	if err := e.pool.QueryRow(ctx, `SELECT COALESCE(context_id,'') FROM ahp_artifacts.artifacts WHERE id=$1`, id).
		Scan(&binding); err != nil {
		t.Fatalf("read binding: %v", err)
	}
	if binding != sourceCtx {
		t.Fatalf("the grant rewrote the artifact's binding context: %q", binding)
	}

	// And it is reversible: revoking takes the imported conversation back to
	// refusal, while the owning conversation keeps reading.
	if err := e.seed.RevokeContext(srcCallers, e.scope, id, targetCtx, "revoke-"+targetCtx); err != nil {
		t.Fatalf("RevokeContext: %v", err)
	}
	if _, err := e.seed.Get(dstCallers, e.scope, id); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("after revoke the imported conversation must be refused again: %v", err)
	}
	if _, err := e.seed.Get(srcCallers, e.scope, id); err != nil {
		t.Fatalf("revoking a grant must not disturb the owning conversation: %v", err)
	}
}
