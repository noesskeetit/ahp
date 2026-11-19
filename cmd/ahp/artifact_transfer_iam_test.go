//go:build ahp_data

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	artifactclient "github.com/ahp-platform/ahp/sdk/go/artifacts"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// A live task grant alone is insufficient: the source deployment's IAM rule
// can be revoked during physical Copy while the recipient remains authorized.
func TestSyntheticTransferResourceRevokeBeforePublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, art, sourceScope := cascadeEnv(t, ctx)
	policy := identity.NewSyntheticResourcePolicy(pool)
	owner := &server{pool: pool, iam: policy}
	ids := owner.identityStore()
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	targetScope := sourceScope
	targetScope.Deployment = "transfer-target"
	for _, resource := range []identity.Resource{{Kind: "tenant", ID: sourceScope.Tenant}, {Kind: "deployment", ID: sourceScope.Deployment}, {Kind: "deployment", ID: targetScope.Deployment}} {
		if _, err := policy.PutRule(ctx, identity.ResourceRule{Tenant: sourceScope.Tenant, Subject: sourceScope.Subject, Resource: resource, Action: "artifacts", Allowed: true}, 0); err != nil {
			t.Fatal(err)
		}
	}
	verifier, signer := devIdentityAuth()
	newTask := func(scope contracts.Scope, suffix string) (identity.Grant, string) {
		t.Helper()
		g, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: scope.Tenant + suffix, Context: scope.Tenant + suffix, Deployment: scope.Deployment, Capabilities: []string{"artifacts"}, TTL: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		token, err := ids.MintTask(ctx, scope, g.Task, signer, "ahp", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return g, token
	}
	source, sourceToken := newTask(sourceScope, "-source")
	target, targetToken := newTask(targetScope, "-target")
	id := sealArtifact(t, ctx, art, sourceScope, artifacts.Binding{ContextID: source.Context, TaskID: source.Task}, "immutable checkpoint")
	ref, err := art.Get(ctx, sourceScope, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureArtifactTransfer(art, ids); err != nil {
		t.Fatal(err)
	}
	artifacts.WithGrantLabels(dataArtifactGrantLabels(ids))(art)
	endpoint, _ := url.Parse(os.Getenv("AHP_S3_ENDPOINT"))
	delegate, err := artifacts.NewTransferCopier(endpoint.Host, minio.Options{Creds: credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""), Secure: endpoint.Scheme == "https"})
	if err != nil {
		t.Fatal(err)
	}
	copier := &recoveryFaultCopier{delegate: delegate}
	artifacts.WithTransferCopier(copier)(art)
	handler, err := artifactsapi.New(art, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	front := httptest.NewUnstartedServer(handler)
	front.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	front.StartTLS()
	defer front.Close()
	client := func(scope contracts.Scope, token string) *artifactclient.Client {
		t.Helper()
		httpClient := *front.Client()
		tr := httpClient.Transport.(*http.Transport).Clone()
		tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
		httpClient.Transport = tr
		t.Cleanup(tr.CloseIdleConnections)
		out, err := artifactclient.New(artifactclient.Config{BaseURL: front.URL, HTTPClient: &httpClient, TokenSource: func(context.Context) (string, error) { return token, nil }})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	from, to := client(sourceScope, sourceToken), client(targetScope, targetToken)
	ticket, err := from.TransferGrant(ctx, artifactclient.ArtifactRef{ID: ref.ID, Version: ref.Version, SHA256: ref.SHA256}, artifactclient.TransferTarget{Deployment: targetScope.Deployment, ContextID: target.Context, TaskID: target.Task})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := to.CopyFromTransferGrant(ctx, ticket.Grant, "allowed"); err != nil {
		t.Fatal("configured IAM transfer refused", err)
	}
	revoke := func() error {
		copier.after.Store(nil)
		_, err := policy.PutRule(ctx, identity.ResourceRule{Tenant: sourceScope.Tenant, Subject: sourceScope.Subject, Resource: identity.Resource{Kind: "deployment", ID: sourceScope.Deployment}, Action: "artifacts", Allowed: false}, 1)
		return err
	}
	copier.after.Store(&revoke)
	if _, err := to.CopyFromTransferGrant(ctx, ticket.Grant, "resource-revoke"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("source resource revoke before publication bypassed: %v", err)
	}
	// The task grant itself remains live: this proves the new resource gate.
	if _, err := ids.Evaluate(ctx, sourceScope, source.Task, "artifacts"); err != nil {
		t.Fatal("test unexpectedly revoked the task grant", err)
	}
	var pending string
	if err := pool.QueryRow(ctx, `SELECT target_id FROM ahp_artifacts.checkpoint_transfer_operations WHERE source_identity->>'grant_id'=$1 AND status='dispatched'`, source.ID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if _, err := to.Get(ctx, pending); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("denied transfer became visible", err)
	}
	if _, err := to.Download(ctx, pending); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("denied transfer bytes became readable", err)
	}
	if _, err := to.Seal(ctx, pending); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("public Seal bypassed source resource denial", err)
	}
	if _, err := to.CopyFromTransferGrant(ctx, ticket.Grant, "resource-revoke"); contracts.Code(err) != contracts.ErrForbidden || copier.calls.Load() != 2 {
		t.Fatal("revoked retry dispatched another copy", copier.calls.Load(), err)
	}
}
