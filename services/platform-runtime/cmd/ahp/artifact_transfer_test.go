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
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
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
	"github.com/ahp-platform/ahp/pkg/labels"
	artifactclient "github.com/ahp-platform/ahp/sdk/go/artifacts"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type recoveryFaultCopier struct {
	delegate artifacts.TransferCopier
	mode     atomic.Int32
	calls    atomic.Int64
	after    atomic.Pointer[func() error]
}

func (c *recoveryFaultCopier) CopyExact(ctx context.Context, bucket, source, version, target string) error {
	c.calls.Add(1)
	if c.mode.Load() == 1 {
		return errors.New("injected unavailable S3 before dispatch")
	}
	if err := c.delegate.CopyExact(ctx, bucket, source, version, target); err != nil {
		return err
	}
	if after := c.after.Load(); after != nil {
		if err := (*after)(); err != nil {
			return err
		}
	}
	if c.mode.Load() == 2 {
		return errors.New("injected lost S3 ACK")
	}
	return nil
}

// This is real PG + S3 + Identity + mTLS. Only the human admin role decision
// is a synthetic IAM fixture; no corporate IAM or model E2E is asserted.
func TestCheckpointRecoveryRealOwners(t *testing.T) {
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Fatal("real PG required; no skip")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, art, scope := cascadeEnv(t, ctx)
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := configureArtifactTransfer(art, ids); err != nil {
		t.Fatal(err)
	}
	endpoint, e := url.Parse(os.Getenv("AHP_S3_ENDPOINT"))
	if e != nil {
		t.Fatal(e)
	}
	copyDelegate, e := artifacts.NewTransferCopier(endpoint.Host, minio.Options{Creds: credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""), Secure: endpoint.Scheme == "https"})
	if e != nil {
		t.Fatal(e)
	}
	copyFault := &recoveryFaultCopier{delegate: copyDelegate}
	artifacts.WithTransferCopier(copyFault)(art)
	artifacts.WithGrantLabels(dataArtifactGrantLabels(ids))(art)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	st.SetCheckpointArtifacts(checkpointArtifactOwner{Store: art, identity: ids})
	artifacts.WithCheckpointSettlement(st.CheckpointOperationSettled)(art)
	artifacts.WithReferenceKeeper(artifacts.ReferenceScanFunc(func(ctx context.Context, tenant string, items []artifacts.ArtifactQuery) (map[string]bool, error) {
		queries := make([]references.Artifact, len(items))
		for i, a := range items {
			queries[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
		}
		result, err := state.ReferenceSource(pool, "ahp_state").InspectReferences(ctx, tenant, nil, queries)
		return result.Referenced, err
	}))(art)
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("recovery", "k", priv)
	verifier := auth.NewVerifier("recovery", map[string]ed25519.PublicKey{"k": pub})
	newTask := func(task, contextID, subject string) (string, identity.Grant) {
		t.Helper()
		s := scope
		s.Subject = subject
		g, err := ids.CreateRoot(ctx, s, identity.RootRequest{Task: task, Context: contextID, Deployment: s.Deployment, Capabilities: []string{"state", "artifacts"}, TTL: 10 * time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		token, err := ids.MintTask(ctx, s, task, signer, "ahp", 10*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token, g
	}
	sourceContext := scope.Tenant + "-source"
	targetContext := scope.Tenant + "-target"
	sourceToken, sourceGrant := newTask("source-task", sourceContext, scope.Subject)
	targetToken, targetGrant := newTask("target-task", targetContext, scope.Subject)
	_, err := ids.AddLabels(ctx, scope, sourceGrant.Task, labels.Set{Workspaces: []string{"source-stored"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ids.AddLabels(ctx, scope, targetGrant.Task, labels.Set{Workspaces: []string{"target-current"}})
	if err != nil {
		t.Fatal(err)
	}
	sh, err := stateapi.NewWithAdmin(st, verifier, ids.Authorize, func(_ context.Context, s contracts.Scope, task string) (string, error) {
		return ids.MintTask(ctx, s, task, signer, "ahp", 10*time.Minute)
	}, stateapi.AdminConfig{Audience: "human", Authorize: func(_ context.Context, c auth.Claims, _ string) error {
		if c.Kind != auth.KindHuman || c.Subject != "admin" {
			return contracts.NewError(contracts.ErrForbidden, "admin required")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ah, err := artifactsapi.New(art, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/state/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/state/PutCheckpoint" {
			sh.ServeHTTP(w, r)
			return
		}
		recorder := httptest.NewRecorder()
		sh.ServeHTTP(recorder, r)
		if recorder.Code >= 400 {
			t.Log("checkpoint refusal", recorder.Body.String())
		}
		for k, v := range recorder.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(recorder.Body.Bytes())
	}))
	mux.Handle("/v1/state-admin/", sh)
	mux.Handle("/v1/artifacts/", ah)
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	httpClient := srv.Client()
	tr := httpClient.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	httpClient.Transport = tr
	stateClient := func(token string) *sdkstate.Client {
		c, err := sdkstate.New(sdkstate.Config{BaseURL: srv.URL, HTTPClient: httpClient, TokenSource: func(context.Context) (string, error) { return token, nil }})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	artifactClient := func(token string) *artifactclient.Client {
		c, err := artifactclient.New(artifactclient.Config{BaseURL: srv.URL, HTTPClient: httpClient, TokenSource: func(context.Context) (string, error) { return token, nil }})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	source, target := stateClient(sourceToken), stateClient(targetToken)
	th, err := source.CreateThread(ctx, sourceContext, "source-thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := source.CreateRun(ctx, th.ID, "run", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := source.ClaimRunFor(ctx, "recovery-worker", 120000, run.ID)
	if err != nil || claim == nil {
		t.Fatal("claim", err)
	}
	source = stateClient(claim.TaskToken)
	f := sdkstate.Fence{RunID: run.ID, Worker: "recovery-worker", Epoch: claim.Lease.Epoch}
	large := bytes.Repeat([]byte{0, 0xff, 0x61, 0x62}, 120000)
	metadata, _ := json.Marshal(map[string]string{"serializer": "jsonplus", "large": string(bytes.Repeat([]byte("m"), 350000))})
	cp := sdkstate.Checkpoint{ID: "recovery-cp", ThreadID: th.ID, Namespace: "recover", Version: 1, Metadata: metadata, Parts: []sdkstate.CheckpointPart{{Channel: "value", Version: "v1", Data: large}}}
	if _, err = source.PutCheckpoint(ctx, f, cp, "cp"); err != nil {
		t.Fatal(err)
	}
	if err = source.PutWrites(ctx, f, "recover", cp.ID, []sdkstate.PendingWrite{{Task: "pending-task", Index: -1, Channel: "value", Data: large}}); err != nil {
		t.Fatal(err)
	}
	_, err = ids.AddLabels(ctx, scope, sourceGrant.Task, labels.Set{Workspaces: []string{"source-current"}})
	if err != nil {
		t.Fatal(err)
	}
	// A second real checkpoint has a distinct immutable artifact label that
	// the source reader grant does not carry. Export orders A, B, then pending A.
	refB, err := artifactClient(sourceToken).Put(ctx, artifactclient.PutRequest{Key: "source-B", Name: "checkpoint.bin", MIME: "application/octet-stream", Data: []byte("distinct checkpoint B"), Labels: labels.Set{Workspaces: []string{"source-B-only"}}})
	if err != nil {
		t.Fatal(err)
	}
	refBDoc, _ := json.Marshal(map[string]any{"id": refB.ID, "version": refB.Version, "sha256": refB.SHA256, "size": refB.Size, "tenant": refB.Tenant, "deployment": refB.Deployment, "subject": refB.Subject, "context_id": refB.ContextID, "task_id": refB.TaskID, "labels": refB.Labels})
	extra := sdkstate.Checkpoint{ID: "mixed-labels-cp", ThreadID: th.ID, Namespace: "recover", Version: 2, Metadata: json.RawMessage(`{}`), Parts: []sdkstate.CheckpointPart{{Channel: "extra", Version: "v1", Encoding: "ahp-artifact-v1", Data: refBDoc}}}
	putBody, _ := json.Marshal(map[string]any{"fence": f, "checkpoint": extra, "key": "mixed-labels-cp"})
	req, _ := http.NewRequestWithContext(ctx, "POST", srv.URL+"/v1/state/PutCheckpoint", bytes.NewReader(putBody))
	req.Header.Set("Authorization", "Bearer "+claim.TaskToken)
	req.Header.Set("Content-Type", "application/json")
	putResult, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	putResult.Body.Close()
	if putResult.StatusCode != 200 {
		t.Fatal("mixed source checkpoint rejected", putResult.StatusCode)
	}
	adminToken, err := signer.Sign(auth.Claims{Audience: "human", Kind: auth.KindHuman, Tenant: scope.Tenant, Subject: "admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sdkstate.NewAdmin(sdkstate.Config{BaseURL: srv.URL, HTTPClient: httpClient, TokenSource: func(context.Context) (string, error) { return adminToken, nil }})
	if err != nil {
		t.Fatal(err)
	}
	exp, err := admin.ExportThread(ctx, sdkstate.AdminTarget{Deployment: scope.Deployment, Subject: scope.Subject}, th.ID)
	if err != nil {
		t.Fatal("export", err)
	}
	taskAdmin, _ := sdkstate.NewAdmin(sdkstate.Config{BaseURL: srv.URL, HTTPClient: httpClient, TokenSource: func(context.Context) (string, error) { return sourceToken, nil }})
	if _, err = taskAdmin.ExportThread(ctx, sdkstate.AdminTarget{Deployment: scope.Deployment, Subject: scope.Subject}, th.ID); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("task escaped admin boundary", err)
	}
	if _, err = target.ImportThreadTo(ctx, exp, targetContext); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("raw foreign-context reference accepted", err)
	}
	badRaw, _ := json.Marshal(exp)
	var badExport sdkstate.ControlExport
	_ = json.Unmarshal(badRaw, &badExport)
	var badDigest map[string]any
	_ = json.Unmarshal(badExport.Checkpoints[0].Metadata, &badDigest)
	badDigest["sha256"] = strings.Repeat("0", 64)
	badExport.Checkpoints[0].Metadata, _ = json.Marshal(badDigest)
	beforeDigest := copyFault.calls.Load()
	if _, err = target.RehomeExport(ctx, source, badExport, artifactclient.TransferTarget{Deployment: scope.Deployment, ContextID: targetContext, TaskID: targetGrant.Task}, "bad-digest"); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatal("tampered digest accepted", err)
	}
	if copyFault.calls.Load() != beforeDigest {
		t.Fatal("tampered digest dispatched copy")
	}
	var occupied int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM ahp_state.threads WHERE tenant=$1 AND context_id=$2`, scope.Tenant, targetContext).Scan(&occupied); err != nil || occupied != 0 {
		t.Fatal("refused import left a thread", occupied, err)
	}
	crossLanguageKey := "restore<&>\u2028\u2029"
	prepared, err := target.RehomeExport(ctx, source, exp, artifactclient.TransferTarget{Deployment: scope.Deployment, ContextID: targetContext, TaskID: targetGrant.Task}, crossLanguageKey)
	if err != nil {
		t.Fatal("rehome", err)
	}
	repeated, err := target.RehomeExport(ctx, source, exp, artifactclient.TransferTarget{Deployment: scope.Deployment, ContextID: targetContext, TaskID: targetGrant.Task}, crossLanguageKey)
	if err != nil {
		t.Fatal("rehome replay", err)
	}
	if !bytes.Equal(prepared.Checkpoints[0].Parts[0].Data, repeated.Checkpoints[0].Parts[0].Data) {
		t.Fatal("replay changed immutable target version")
	}
	restored, err := target.ImportThreadTo(ctx, prepared, targetContext)
	if err != nil {
		t.Fatal("import prepared", err)
	}
	fresh := stateClient(targetToken)
	got, err := fresh.GetPart(ctx, restored.ID, "recover", "value", "v1")
	if err != nil || !bytes.Equal(got, large) {
		t.Fatal("fresh client byte recovery failed", err)
	}
	header, err := fresh.GetCheckpoint(ctx, restored.ID, "recover", cp.ID)
	if err != nil || !bytes.Equal(header.Metadata, metadata) {
		t.Fatal("metadata recovery", err)
	}
	pending, err := fresh.GetWrites(ctx, restored.ID, "recover", cp.ID)
	if err != nil || len(pending) != 1 || !bytes.Equal(pending[0].Data, large) {
		t.Fatal("pending recovery", err)
	}
	current, found, err := ids.ContextLabels(ctx, scope, targetContext)
	if err != nil || !found || len(current.Workspaces) != 4 {
		t.Fatalf("target journal was not tainted: %+v found=%v err=%v", current, found, err)
	}
	// The reader issuing transfer need not be the historical artifact creator.
	readerToken, readerGrant := newTask("reader-task", sourceContext, scope.Subject)
	var ref artifactclient.ArtifactRef
	if err = json.Unmarshal(exp.Checkpoints[0].Parts[0].Data, &ref); err != nil {
		t.Fatal(err)
	}
	reader := artifactClient(readerToken)
	recipient := artifactClient(targetToken)
	grant, err := reader.TransferGrant(ctx, ref, artifactclient.TransferTarget{Deployment: scope.Deployment, ContextID: targetContext, TaskID: targetGrant.Task})
	if err != nil {
		t.Fatal(err)
	}
	if err = ids.CancelSubtree(ctx, scope, readerGrant.Task); err != nil {
		t.Fatal(err)
	}
	if _, err = recipient.CopyFromTransferGrant(ctx, grant.Grant, "revoked-reader"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("revoked issuing reader remained valid")
	}
	// Recipient task/context and subject cannot be substituted, even with ticket.
	otherToken, otherGrant := newTask("other-task", scope.Tenant+"-other", scope.Subject)
	grant, err = artifactClient(sourceToken).TransferGrant(ctx, ref, artifactclient.TransferTarget{Deployment: scope.Deployment, ContextID: targetContext, TaskID: targetGrant.Task})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = artifactClient(otherToken).CopyFromTransferGrant(ctx, grant.Grant, "other-target"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("target substitution accepted", otherGrant.Task)
	}
	mallory, _ := newTask("mallory-task", scope.Tenant+"-mallory", "mallory")
	if _, err = artifactClient(mallory).CopyFromTransferGrant(ctx, grant.Grant, "other-subject"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("cross-subject transfer accepted")
	}
	foreignScope := scope
	foreignScope.Tenant += "-foreign"
	foreignGrant, e := ids.CreateRoot(ctx, foreignScope, identity.RootRequest{Task: "foreign-task", Context: foreignScope.Tenant + "-context", Deployment: foreignScope.Deployment, Capabilities: []string{"artifacts"}, TTL: 10 * time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	foreignToken, e := ids.MintTask(ctx, foreignScope, foreignGrant.Task, signer, "ahp", 10*time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	foreignCert := ca.clientCert(t, auth.WorkloadID(foreignScope))
	foreignTransport := tr.Clone()
	foreignTransport.TLSClientConfig = tr.TLSClientConfig.Clone()
	foreignTransport.TLSClientConfig.Certificates = []tls.Certificate{foreignCert}
	foreignHTTP := *httpClient
	foreignHTTP.Transport = foreignTransport
	foreignClient, e := artifactclient.New(artifactclient.Config{BaseURL: srv.URL, HTTPClient: &foreignHTTP, TokenSource: func(context.Context) (string, error) { return foreignToken, nil }})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = foreignClient.CopyFromTransferGrant(ctx, grant.Grant, "foreign-tenant"); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("valid foreign workload crossed tenant boundary")
	}
	makeUnknown := func(token string, dest identity.Grant, key string, mode int32) artifactclient.TransferGrant {
		t.Helper()
		ticket, e := artifactClient(sourceToken).TransferGrant(ctx, ref, artifactclient.TransferTarget{Deployment: scope.Deployment, ContextID: dest.Context, TaskID: dest.Task})
		if e != nil {
			t.Fatal(e)
		}
		copyFault.mode.Store(mode)
		_, e = artifactClient(token).CopyFromTransferGrant(ctx, ticket.Grant, key)
		copyFault.mode.Store(0)
		if contracts.ReasonOf(e) != "transfer_outcome_unknown" {
			t.Fatalf("SDK lost UNKNOWN reason: code=%s reason=%s err=%v", contracts.Code(e), contracts.ReasonOf(e), e)
		}
		return ticket
	}
	unknown := makeUnknown(targetToken, targetGrant, "go-lost-ack", 2)
	copyCount := copyFault.calls.Load()
	resolved, e := recipient.CopyFromTransferGrant(ctx, unknown.Grant, "go-lost-ack")
	if e != nil || resolved.SHA256 != ref.SHA256 || copyFault.calls.Load() != copyCount {
		t.Fatal("Go UNKNOWN reconcile redispatched or changed digest", e)
	}
	missing := makeUnknown(targetToken, targetGrant, "go-missing-outcome", 1)
	copyCount = copyFault.calls.Load()
	for i := 0; i < 2; i++ {
		if _, e = recipient.CopyFromTransferGrant(ctx, missing.Grant, "go-missing-outcome"); contracts.ReasonOf(e) != "transfer_outcome_unknown" {
			t.Fatal("Go missing outcome lost reason", e)
		}
	}
	if copyFault.calls.Load() != copyCount {
		t.Fatal("Go missing outcome redispatched")
	}
	assertUnpublished := func(reader identity.Grant, token string) {
		t.Helper()
		var id string
		if err := pool.QueryRow(ctx, `SELECT target_id FROM ahp_artifacts.checkpoint_transfer_operations WHERE source_identity->>'grant_id'=$1`, reader.ID).Scan(&id); err != nil {
			t.Fatal(err)
		}
		client := artifactClient(token)
		if _, err := client.Get(ctx, id); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatal("refused transfer target readable", err)
		}
		if _, err := client.Download(ctx, id); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatal("refused transfer bytes readable", err)
		}
		if _, err := client.Seal(ctx, id); contracts.Code(err) != contracts.ErrForbidden {
			t.Fatal("ordinary Seal published refused transfer", err)
		}
	}
	goRaceToken, goRaceReader := newTask("go-publication-reader", sourceContext, scope.Subject)
	goRaceTicket, err := artifactClient(goRaceToken).TransferGrant(ctx, ref, artifactclient.TransferTarget{Deployment: scope.Deployment, ContextID: targetContext, TaskID: targetGrant.Task})
	if err != nil {
		t.Fatal(err)
	}
	goRevoke := func() error {
		copyFault.after.Store(nil)
		return ids.CancelSubtree(ctx, scope, goRaceReader.Task)
	}
	copyFault.after.Store(&goRevoke)
	if _, err := recipient.CopyFromTransferGrant(ctx, goRaceTicket.Grant, "go-revoke-after-copy"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("source revoke before publication accepted", err)
	}
	assertUnpublished(goRaceReader, targetToken)
	// HTTP authority data is verified by the production composition adapter.
	if python := os.Getenv("AHP_RECOVERY_PYTHON"); python != "" {
		pyToken, pyGrant := newTask("python-task", scope.Tenant+"-python", scope.Subject)
		pyReaderToken, pyReaderGrant := newTask("python-reader", sourceContext, scope.Subject)
		pyRevoked, e := artifactClient(pyReaderToken).TransferGrant(ctx, ref, artifactclient.TransferTarget{Deployment: scope.Deployment, ContextID: pyGrant.Context, TaskID: pyGrant.Task})
		if e != nil {
			t.Fatal(e)
		}
		if e = ids.CancelSubtree(ctx, scope, pyReaderGrant.Task); e != nil {
			t.Fatal(e)
		}
		pyUnknown := makeUnknown(pyToken, pyGrant, "python-lost-ack", 2)
		pyMissing := makeUnknown(pyToken, pyGrant, "python-missing-outcome", 1)
		pyRaceToken, pyRaceReader := newTask("python-publication-reader", sourceContext, scope.Subject)
		pyRaceTicket, e := artifactClient(pyRaceToken).TransferGrant(ctx, ref, artifactclient.TransferTarget{Deployment: scope.Deployment, ContextID: pyGrant.Context, TaskID: pyGrant.Task})
		if e != nil {
			t.Fatal(e)
		}
		// Switch languages while the first Rehome blob is UNKNOWN, not merely
		// after a completed snapshot. Python must reconcile that same object.
		crossUnknownKey := "unknown<&>\u2028\u2029"
		copyFault.mode.Store(2)
		_, e = target.RehomeExport(ctx, source, exp, artifactclient.TransferTarget{Deployment: scope.Deployment, ContextID: targetContext, TaskID: targetGrant.Task}, crossUnknownKey)
		copyFault.mode.Store(0)
		if contracts.ReasonOf(e) != "transfer_outcome_unknown" {
			t.Fatal("Go Rehome did not surface UNKNOWN", e)
		}
		var metadataRef artifactclient.ArtifactRef
		if e = json.Unmarshal(exp.Checkpoints[0].Metadata, &metadataRef); e != nil {
			t.Fatal(e)
		}
		innerKey, _ := contracts.Digest([]string{"rehome-v1", crossUnknownKey, metadataRef.ID, metadataRef.Version, metadataRef.SHA256})
		ownerKey, _ := contracts.Digest([]string{"checkpoint-transfer-v1", scope.Tenant, scope.Deployment, scope.Subject, targetContext, targetGrant.Task, innerKey})
		var crossUnknownID string
		if e = pool.QueryRow(ctx, `SELECT target_id FROM ahp_artifacts.checkpoint_transfer_operations WHERE operation=$1`, ownerKey).Scan(&crossUnknownID); e != nil {
			t.Fatal(e)
		}
		beforePython := copyFault.calls.Load()
		dir := t.TempDir()
		write := func(name string, data []byte) string {
			p := filepath.Join(dir, name)
			if err := os.WriteFile(p, data, 0600); err != nil {
				t.Fatal(err)
			}
			return p
		}
		cert := tr.TLSClientConfig.Certificates[0]
		keyDER, e := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
		if e != nil {
			t.Fatal(e)
		}
		serverDER := srv.TLS.Certificates[0].Certificate[0]
		hand := map[string]any{"base_url": srv.URL, "source_token": sourceToken, "target_token": pyToken, "admin_token": adminToken, "source_thread": th.ID, "target_context": pyGrant.Context, "target_task": pyGrant.Task, "deployment": scope.Deployment, "subject": scope.Subject, "ca_file": write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})), "cert_file": write("client.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})), "key_file": write("key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))}
		foreignKey, e := x509.MarshalPKCS8PrivateKey(foreignCert.PrivateKey)
		if e != nil {
			t.Fatal(e)
		}
		hand["foreign_cert_file"] = write("foreign-client.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: foreignCert.Certificate[0]}))
		hand["foreign_key_file"] = write("foreign-key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: foreignKey}))
		hand["foreign_token"] = foreignToken
		hand["other_token"] = otherToken
		hand["mallory_token"] = mallory
		hand["revoked_grant"] = pyRevoked.Grant
		hand["unknown_grant"] = pyUnknown.Grant
		hand["missing_grant"] = pyMissing.Grant
		hand["shared_target_token"] = targetToken
		hand["shared_target_context"] = targetContext
		hand["shared_target_task"] = targetGrant.Task
		hand["shared_recovery_key"] = crossLanguageKey
		hand["shared_prepared"] = prepared
		hand["shared_unknown_key"] = crossUnknownKey
		hand["shared_unknown_id"] = crossUnknownID
		hand["publication_race_grant"] = pyRaceTicket.Grant
		pyRevoke := func() error {
			copyFault.after.Store(nil)
			return ids.CancelSubtree(ctx, scope, pyRaceReader.Task)
		}
		copyFault.after.Store(&pyRevoke)
		raw, _ := json.Marshal(hand)
		handfile := write("recovery.json", raw)
		command := exec.CommandContext(ctx, python, "../../sdk/python/ahp_state/tests/recovery_live.py", handfile)
		command.Env = append(os.Environ(), "PYTHONPATH=../../sdk/python")
		if output, e := command.CombinedOutput(); e != nil {
			t.Fatalf("Python recovery failed: %v\n%s", e, output)
		} else {
			t.Log(string(output))
		}
		assertUnpublished(pyRaceReader, pyToken)
		// The positive Python snapshot has exactly three unique source blobs;
		// the publication race copies once then refuses without publishing.
		// Cross-language UNKNOWN resumes the existing first blob and copies only
		// the two not-yet-dispatched blobs under that other recovery key.
		// All pre-dispatch negatives and reconciliation dispatch zero extra copies.
		if copyFault.calls.Load() != beforePython+6 {
			t.Fatalf("Python unexpected copy count delta=%d", copyFault.calls.Load()-beforePython)
		}
	}
	t.Log("Go real PG/MinIO/Identity/mTLS PASS: admin export; task-admin/raw-import/digest/revoked-reader/target/subject/tenant denied; revoke after Copy before publication denied with HTTP Get/Download/Seal rejection; mixed-label A-B-A stable replay; exact metadata/parts/pending; labels journal; lost-ACK reconcile and missing-outcome UNKNOWN without redispatch")
}
