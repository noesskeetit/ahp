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
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Exercises the production authorizer through real HTTPS/mTLS, signed human
// credentials and Identity sessions/JTIs. The successful request runs the real
// deletion coordinator; no administrative permission callback is replaced.
func TestStateAdminProductionAuthorityHTTP(t *testing.T) {
	for _, name := range []string{"AHP_TEST_DATABASE_URL", "AHP_S3_ENDPOINT", "AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD"} {
		if os.Getenv(name) == "" {
			t.Fatalf("%s required for real administrative boundary regression", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, art, _ := cascadeEnv(t, ctx)
	ids, st, catalog := identity.New(pool), state.New(pool), registry.New(pool)
	for _, migrate := range []func(context.Context) error{ids.Migrate, st.Migrate, catalog.Migrate} {
		if err := migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Data-owner fixtures are local stores; admission itself uses the production
	// State Admin authority below, backed by actual Identity grants and sessions.
	dataAuth := func(context.Context, contracts.Scope, string, string) error { return nil }
	mem, err := memory.New(pool, art, dataAuth, nil)
	if err != nil {
		t.Fatal(err)
	}
	ws, err := workspace.New(pool, art, dataAuth)
	if err != nil {
		t.Fatal(err)
	}
	for _, migrate := range []func(context.Context) error{mem.Migrate, ws.Migrate} {
		if err := migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("admin-authority-test", "test", private)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("admin-authority-test", map[string]ed25519.PublicKey{"test": public})
	issue := func(context.Context, contracts.Scope, string) (string, error) {
		return "", contracts.NewError(contracts.ErrUnavailable, "test does not issue task credentials")
	}
	handler, err := stateapi.NewWithAdmin(st, verifier, ids.Authorize, issue, stateapi.AdminConfig{
		Audience: "ahp", Authorize: stateAdminAuthority(ids, []string{"control"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	composed := &server{cascade: withMemoryDeletion(artifactStoreCascade{store: art}, mem, ws)}
	stopped, stop := context.WithCancel(ctx)
	stop()
	handler = composed.withDeletionCascade(handler, st, verifier, ControlDeletionConfig{
		ConsumerContext: stopped, TenantOwners: tenantTestOwners(t, pool),
	})
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca.pem) {
		t.Fatal("invalid test CA")
	}
	front := httptest.NewUnstartedServer(handler)
	front.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	front.StartTLS()
	defer front.Close()

	for _, tc := range []struct {
		name        string
		permissions []string
		mutation    string
		legacy      bool
		want        int
	}{
		{"both_permissions_delete", []string{stateapi.AdminCapability, stateapi.TenantDeleteCapability}, "", false, http.StatusOK},
		{"ordinary_admin_cannot_delete_tenant", []string{stateapi.AdminCapability}, "", false, http.StatusForbidden},
		{"dedicated_permission_requires_admin", []string{stateapi.TenantDeleteCapability}, "", false, http.StatusForbidden},
		{"permission_match_is_exact", []string{stateapi.AdminCapability, stateapi.TenantDeleteCapability + ":other"}, "", false, http.StatusForbidden},
		{"revoked_jti", []string{stateapi.AdminCapability, stateapi.TenantDeleteCapability}, "revoke", false, http.StatusForbidden},
		{"wrong_workload", []string{stateapi.AdminCapability, stateapi.TenantDeleteCapability}, "workload", false, http.StatusForbidden},
		{"cross_tenant_jti", []string{stateapi.AdminCapability, stateapi.TenantDeleteCapability}, "tenant", false, http.StatusForbidden},
		{"cross_subject_jti", []string{stateapi.AdminCapability, stateapi.TenantDeleteCapability}, "subject", false, http.StatusForbidden},
		{"certificate_tenant_mismatch", []string{stateapi.AdminCapability, stateapi.TenantDeleteCapability}, "certificate", false, http.StatusForbidden},
		{"legacy_admin_still_allowed", []string{stateapi.AdminCapability}, "", true, http.StatusOK},
		{"legacy_requires_admin", []string{stateapi.TenantDeleteCapability}, "", true, http.StatusForbidden},
		{"legacy_revoked_jti", []string{stateapi.AdminCapability}, "revoke", true, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope := contracts.Scope{Tenant: "admin-" + contracts.NewID(), Deployment: "control", Subject: "admin"}
			user := identity.BrowserUser{UserID: "user-" + contracts.NewID(), Tenant: scope.Tenant, Subject: scope.Subject, DisplayName: "Synthetic administrator"}
			if err := ids.SeedBrowserUser(ctx, user, user.UserID, "test-password-long-enough", []string{"admin"}, tc.permissions); err != nil {
				t.Fatal(err)
			}
			session, err := ids.StartBrowserSession(ctx, user.UserID)
			if err != nil {
				t.Fatal(err)
			}
			jti := contracts.NewID()
			if err = ids.BindBrowserToken(ctx, jti, session); err != nil {
				t.Fatal(err)
			}
			claims := auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: scope.Subject, JTI: jti}
			switch tc.mutation {
			case "revoke":
				if err = ids.RevokeBrowserSession(ctx, session.ID); err != nil {
					t.Fatal(err)
				}
			case "workload":
				scope.Deployment = "untrusted-agent"
			case "tenant":
				scope.Tenant = "victim-" + contracts.NewID()
				claims.Tenant = scope.Tenant // Valid signature/certificate, wrong live JTI tenant.
			case "subject":
				claims.Subject = "other-subject"
			}
			thread, err := st.CreateThread(ctx, scope, "context", "thread")
			if err != nil {
				t.Fatal(err)
			}
			certScope := scope
			if tc.mutation == "certificate" {
				certScope.Tenant = "foreign-" + contracts.NewID()
			}
			tr := front.Client().Transport.(*http.Transport).Clone()
			tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(certScope))}
			defer tr.CloseIdleConnections()
			client := &http.Client{Transport: tr}
			token, err := signer.Sign(claims, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			op, input := "ControlDeleteTenant", map[string]string{"key": "delete"}
			if tc.legacy {
				op, input = "SetLegalhold", map[string]string{"thread_id": thread.ID, "reason": "fixture"}
			}
			raw, err := json.Marshal(map[string]any{"target": map[string]string{"deployment": scope.Deployment, "subject": scope.Subject}, "input": input})
			if err != nil {
				t.Fatal(err)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, front.URL+"/v1/state-admin/"+op, bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var body map[string]any
			if err = json.NewDecoder(response.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != tc.want {
				t.Fatalf("%s: status=%d body=%v want=%d", op, response.StatusCode, body, tc.want)
			}
			if tc.want == http.StatusOK && !tc.legacy {
				if body["state"] != "completed" {
					t.Fatalf("authorized deletion did not complete: %v", body)
				}
				if _, err = st.GetThread(ctx, scope, thread.ID); contracts.Code(err) != contracts.ErrNotFound {
					t.Fatalf("authorized deletion left thread: %v", err)
				}
				if _, err = ids.BrowserTokenActive(ctx, jti); err == nil {
					t.Fatal("completed deletion left the initiating JTI active")
				}
				return
			}
			if _, err = st.GetThread(ctx, scope, thread.ID); err != nil {
				t.Fatalf("non-deleting request changed target thread: %v", err)
			}
			if _, err = st.GetControlDeletion(ctx, scope.Tenant, claims.Subject, "delete"); contracts.Code(err) != contracts.ErrNotFound {
				t.Fatalf("unpermitted tenant deletion was admitted: %v", err)
			}
			held, err := st.TenantHeld(ctx, scope.Tenant)
			if err != nil || held != (tc.legacy && tc.want == http.StatusOK) {
				t.Fatalf("legacy hold outcome: held=%v err=%v", held, err)
			}
		})
	}
}
