//go:build ahp_data

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/memory/memoryapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// TestMemoryFaceMountedOffline proves the Memory API is really mounted in the
// tagged binary and answers with the memory package's OWN contract codes, not
// the browser edge's "route not found"/"no route or asset". It constructs the
// real stores and the real handler WITHOUT migrating (pgx connects lazily,
// minio dials only on object I/O), registers them through the same helper the
// binary uses, and drives the dispatcher -- so it needs no database or object
// store and runs anywhere.
func TestMemoryFaceMountedOffline(t *testing.T) {
	t.Setenv("AHP_S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("AHP_S3_BUCKET", "mem-mount-offline")
	t.Setenv("AHP_S3_REGION", "us-east-1")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://mem:mem@127.0.0.1:59999/mem?sslmode=disable")
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	s := &server{pool: pool}
	artStore, wsStore, verifier, err := s.newDataPlanes(ctx)
	if err != nil {
		t.Fatalf("newDataPlanes: %v", err)
	}
	// The same authorize the composed front hands the mount: dev mode accepts,
	// identity mode checks live grants. The assertions below never depend on it
	// accepting -- they stop at the transport guard, which is upstream of it.
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	store, err := newMemoryStore(pool, artStore, wsStore, authorize)
	if err != nil {
		t.Fatalf("newMemoryStore: %v", err)
	}
	handler, err := memoryapi.New(store, verifier, authorize)
	if err != nil {
		t.Fatalf("memoryapi.New: %v", err)
	}
	mux := http.NewServeMux()
	mountMemoryHandlers(mux, handler)

	post := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	t.Run("unknown op answers the package contract 404", func(t *testing.T) {
		rec := post("/v1/memory/Bogus")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 (body=%s)", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "memoryapi: unknown route") {
			t.Fatalf("body = %q, want the memory package's own unknown-route code", rec.Body.String())
		}
	})

	t.Run("nested path is rejected by the package", func(t *testing.T) {
		rec := post("/v1/memory/Read/extra")
		if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "memoryapi: unknown route") {
			t.Fatalf("nested = %d %q", rec.Code, rec.Body.String())
		}
	})

	t.Run("non-POST is rejected by the package", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/memory/Search", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "memoryapi: unknown route") {
			t.Fatalf("GET = %d %q", rec.Code, rec.Body.String())
		}
	})

	// A REAL operation token reaches the transport guard: that proves the route
	// is dispatched rather than swallowed as unknown, and that the guard (task
	// token bound to the mTLS workload, capability "memory") stands in front of
	// the store. Without a certificate the guard refuses, and the refusal is the
	// plane's answer -- NOT the package's unknown route, NOT the edge's 404.
	t.Run("known op is dispatched to the transport guard", func(t *testing.T) {
		for _, op := range []string{"Propose", "Search", "Read", "Get", "Review", "AddLabels", "Delete", "DeleteSubject"} {
			rec := post("/v1/memory/" + op)
			body := rec.Body.String()
			if strings.Contains(body, "memoryapi: unknown route") {
				t.Fatalf("%s was not recognized as an operation (body=%s)", op, body)
			}
			if strings.Contains(body, "route not found") || strings.Contains(body, "no route or asset") {
				t.Fatalf("edge fallthrough leaked into a mounted route (body=%s)", body)
			}
			if rec.Code != http.StatusForbidden {
				t.Fatalf("%s = status %d, want 403 from the transport guard (body=%s)", op, rec.Code, body)
			}
		}
	})
}

// TestMemoryStoreAuthorizeFailsClosed pins the ACL adapter the mount installs.
// The Memory package delegates its fine-grained decisions (who may review a
// proposal, label, delete, erase a subject) to this callback; if the adapter
// were a permissive allowlist, any memory-capable token could activate
// proposals. Every path that cannot name a verified principal of the exact
// scope has to be refused before the authority is consulted.
func TestMemoryStoreAuthorizeFailsClosed(t *testing.T) {
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	calls := []string{}
	authority := func(_ context.Context, _ auth.Claims, capability string) error {
		calls = append(calls, capability)
		return nil
	}
	authz := memoryStoreAuthorize(authority)

	t.Run("unknown action never reaches the authority", func(t *testing.T) {
		ctx := transport.WithPrincipal(context.Background(), transport.Principal{Scope: scope})
		for _, action := range []string{"", "memory", "state", "artifacts:delete", "memory:review:admin", "admin"} {
			err := authz(ctx, scope, action, "memory:x")
			if err == nil {
				t.Fatalf("action %q was permitted", action)
			}
			if contracts.Code(err) != contracts.ErrForbidden {
				t.Fatalf("action %q code = %s, want forbidden", action, contracts.Code(err))
			}
		}
		if len(calls) != 0 {
			t.Fatalf("authority consulted for out-of-set actions: %v", calls)
		}
	})

	t.Run("no principal is refused", func(t *testing.T) {
		err := authz(context.Background(), scope, "memory:review", "memory:x")
		if err == nil || contracts.Code(err) != contracts.ErrForbidden {
			t.Fatalf("err = %v, want forbidden", err)
		}
	})

	t.Run("foreign scope is refused", func(t *testing.T) {
		other := contracts.Scope{Tenant: "t2", Deployment: "d1", Subject: "s1"}
		ctx := transport.WithPrincipal(context.Background(), transport.Principal{Scope: other})
		err := authz(ctx, scope, "memory:delete", "memory:x")
		if err == nil || contracts.Code(err) != contracts.ErrForbidden {
			t.Fatalf("cross-scope borrow err = %v, want forbidden", err)
		}
	})

	t.Run("nil authority is refused", func(t *testing.T) {
		ctx := transport.WithPrincipal(context.Background(), transport.Principal{Scope: scope})
		err := memoryStoreAuthorize(nil)(ctx, scope, "memory:label", "memory:x")
		if err == nil || contracts.Code(err) != contracts.ErrForbidden {
			t.Fatalf("nil authority err = %v, want forbidden", err)
		}
	})

	t.Run("in-set action delegates the exact capability", func(t *testing.T) {
		claims := auth.Claims{Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject}
		ctx := transport.WithPrincipal(context.Background(),
			transport.Principal{Claims: claims, Scope: claims.Scope()})
		for _, action := range []string{"memory:review", "memory:label", "memory:delete", "memory:delete-subject"} {
			before := len(calls)
			if err := authz(ctx, scope, action, "memory:x"); err != nil {
				t.Fatalf("%s: %v", action, err)
			}
			if len(calls) != before+1 || calls[len(calls)-1] != action {
				t.Fatalf("%s: authority saw %v", action, calls[before:])
			}
		}
	})

	t.Run("malformed scope is refused before anything else", func(t *testing.T) {
		ctx := transport.WithPrincipal(context.Background(), transport.Principal{Scope: scope})
		bad := contracts.Scope{Tenant: "", Deployment: "d1", Subject: "s1"}
		if err := authz(ctx, bad, "memory:review", "memory:x"); err == nil {
			t.Fatal("an invalid scope was permitted")
		}
	})
}

func TestMemoryOperationAuthorityRequiresLiveMatchingPrincipal(t *testing.T) {
	scope := contracts.Scope{Tenant: "t", Deployment: "d", Subject: "alice"}
	principal := transport.Principal{Scope: scope, Claims: auth.Claims{Subject: "alice"}}
	ctx := transport.WithPrincipal(context.Background(), principal)
	revoked := false
	calls := 0
	authority := memoryStoreAuthorize(func(_ context.Context, claims auth.Claims, capability string) error {
		calls++
		if revoked || claims.Subject != "alice" || capability != "memory" {
			return contracts.NewError(contracts.ErrForbidden, "live operation grant denied")
		}
		return nil
	})
	if err := authority(ctx, scope, "memory", "memory:operation"); err != nil {
		t.Fatalf("live operation authority refused: %v", err)
	}
	revoked = true
	if err := authority(ctx, scope, "memory", "memory:operation"); contracts.Code(err) != contracts.ErrForbidden || calls != 2 {
		t.Fatalf("revocation not checked: calls=%d err=%v", calls, err)
	}
	for _, test := range []struct {
		ctx      context.Context
		scope    contracts.Scope
		resource string
	}{
		{context.Background(), scope, "memory:operation"},
		{ctx, contracts.Scope{Tenant: "t", Deployment: "other", Subject: "alice"}, "memory:operation"},
		{ctx, scope, "memory:erase"},
	} {
		if err := authority(test.ctx, test.scope, "memory", test.resource); contracts.Code(err) != contracts.ErrForbidden {
			t.Fatalf("operation authority failed open: %v", err)
		}
	}
	if calls != 2 {
		t.Fatal("invalid principal/resource reached authority")
	}
	if err := memoryStoreAuthorize(nil)(ctx, scope, "memory", "memory:operation"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("missing authority allowed operation: %v", err)
	}
}

// TestTryMountMemoryFailsLoudWithoutObjectStore proves the mount never half
// mounts: with the object store unnamed it returns an error (main.go then logs
// and keeps serving the other planes) instead of registering a face whose
// evidence checks could never be verified.
func TestTryMountMemoryFailsLoudWithoutObjectStore(t *testing.T) {
	t.Setenv("AHP_MEMORY_BACKEND", "legacy")
	t.Setenv("AHP_S3_ENDPOINT", "")
	pool, err := pgxpool.New(context.Background(), "postgres://mem:mem@127.0.0.1:59999/mem?sslmode=disable")
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	s := &server{pool: pool}
	mux := http.NewServeMux()
	err = s.tryMountMemory(context.Background(), mux, nil, nil, authorityDevMode, nil)
	if err == nil {
		t.Fatal("mount succeeded without AHP_S3_ENDPOINT")
	}
	if !strings.Contains(err.Error(), "AHP_S3_ENDPOINT") {
		t.Fatalf("err = %v, want it to name the missing variable", err)
	}
	// Nothing was registered, so the prefix still belongs to whatever else is
	// mounted (the SPA fallback), never to a half-wired memory face.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/memory/Read", strings.NewReader("{}")))
	if strings.Contains(rec.Body.String(), "memoryapi:") {
		t.Fatalf("a memory answer leaked from a failed mount (body=%s)", rec.Body.String())
	}
}
