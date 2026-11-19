//go:build ahp_data

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestWS1WorkspaceRoutesMounted proves the workspace API is actually mounted in
// the data-plane binary and answers with the workspace PACKAGE's own contract
// codes, not the browser edge's "route not found"/"no route or asset". It
// constructs the real stores and handlers WITHOUT migrating (pgx connects
// lazily, minio dials only on object I/O), mounts them on a mux exactly as the
// binary does, and drives the dispatcher — so it needs no database or object
// store and runs anywhere.
func TestWS1WorkspaceRoutesMounted(t *testing.T) {
	t.Setenv("AHP_S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("AHP_S3_BUCKET", "ws1-bucket")
	t.Setenv("AHP_S3_REGION", "us-east-1")

	ctx := context.Background()
	// A parseable-but-unroutable pool: pgxpool.New does not open a connection and
	// nothing here queries it, so this stands in for the real pool at construction.
	pool, err := pgxpool.New(ctx, "postgres://ws1:ws1@127.0.0.1:59999/ws1?sslmode=disable")
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	s := &server{pool: pool}
	artStore, wsStore, verifier, err := s.newDataPlanes(ctx)
	if err != nil {
		t.Fatalf("newDataPlanes: %v", err)
	}
	artifactsHandler, workspaceHandler, err := dataHandlers(artStore, wsStore, verifier)
	if err != nil {
		t.Fatalf("dataHandlers: %v", err)
	}
	if artifactsHandler == nil || workspaceHandler == nil {
		t.Fatal("nil handler from dataHandlers")
	}

	mux := http.NewServeMux()
	mountDataHandlers(mux, artifactsHandler, workspaceHandler)

	post := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	t.Run("unknown op answers the package contract 404", func(t *testing.T) {
		rec := post("/v1/workspace/head") // "head" is NOT an operation token
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 (body=%s)", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "workspaceapi: unknown route") {
			t.Fatalf("body = %q, want the workspace package's own unknown-route code", rec.Body.String())
		}
	})

	t.Run("nested path is rejected by the package", func(t *testing.T) {
		rec := post("/v1/workspace/a/b")
		if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "workspaceapi: unknown route") {
			t.Fatalf("nested = %d %q", rec.Code, rec.Body.String())
		}
	})

	t.Run("non-POST is rejected by the package", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/workspace/GetHead", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "workspaceapi: unknown route") {
			t.Fatalf("GET = %d %q", rec.Code, rec.Body.String())
		}
	})

	// A REAL operation token reaches the transport guard (proving the route is
	// dispatched, not swallowed as unknown). Without a workload certificate it is
	// refused by the transport, and that refusal is a plane answer, NOT the
	// package's "unknown route" and NOT the browser edge's "route not found".
	t.Run("known op is dispatched to the transport guard", func(t *testing.T) {
		rec := post("/v1/workspace/GetHead") // canonical operation token
		body := rec.Body.String()
		if strings.Contains(body, "workspaceapi: unknown route") {
			t.Fatalf("GetHead was not recognized as an operation (body=%s)", body)
		}
		if strings.Contains(body, "route not found") || strings.Contains(body, "no route or asset") {
			t.Fatalf("edge fallthrough leaked into a mounted route (body=%s)", body)
		}
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403 from the transport guard (body=%s)", rec.Code, body)
		}
	})
}
