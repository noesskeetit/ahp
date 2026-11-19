package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func iscope(tenant string) contracts.Scope {
	return contracts.Scope{Tenant: tenant, Deployment: "d1", Subject: "s1"}
}

// TestStubIdentityTreeIsolation verifies the in-memory Identity client hides
// another tenant's tasks (no existence oracle) and reconstructs the graph.
func TestStubIdentityTreeIsolation(t *testing.T) {
	stub := NewStubIdentityClient()
	tenantA := iscope("tenant-a")
	tenantB := iscope("tenant-b")

	stub.AddTask(tenantA, "root", "", "working")
	stub.AddTask(tenantA, "child-1", "root", "working")
	stub.AddTask(tenantA, "grand", "child-1", "completed")
	stub.AddTask(tenantB, "secret", "", "working")

	tree, err := stub.TaskTree(context.Background(), tenantA, "root")
	if err != nil {
		t.Fatalf("TaskTree: %v", err)
	}
	if tree.Count() != 3 {
		t.Fatalf("expected 3 nodes, got %d", tree.Count())
	}
	if !tree.IsDescendant("root", "grand") {
		t.Fatal("grand must be a descendant of root")
	}
	if tree.IsDescendant("root", "root") {
		t.Fatal("a task is never its own descendant")
	}
	if tree.Find("secret") != nil {
		t.Fatal("tenant-b task must be invisible to tenant-a")
	}
	if _, err := stub.TaskTree(context.Background(), tenantA, "secret"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant id should be NotFound, got %v", err)
	}
}

func TestStubIdentityNotFound(t *testing.T) {
	stub := NewStubIdentityClient()
	if _, err := stub.TaskTree(context.Background(), iscope("t"), "missing"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func newIdentityTestClient(t *testing.T, base string, cfg IdentityClientConfig, tokens TokenSource) *HTTPIdentityClient {
	t.Helper()
	cfg.Base = base
	c, err := newHTTPIdentityClientWithHTTP(cfg, http.DefaultClient, tokens)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return c
}

func httpStub(t *testing.T, gotAuth *string, handler http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*gotAuth = r.Header.Get("Authorization")
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func writeTree(w http.ResponseWriter, tree any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tree)
}

func TestIdentityHTTPValidTree(t *testing.T) {
	var gotAuth string
	url := httpStub(t, &gotAuth, func(w http.ResponseWriter, r *http.Request) {
		writeTree(w, map[string]any{
			"tenant":     "tenant-a",
			"rootTaskId": "root",
			"task": map[string]any{
				"taskId":   "root",
				"state":    "working",
				"children": []any{map[string]any{"taskId": "child-1", "state": "working"}},
			},
		})
	})
	c := newIdentityTestClient(t, url, IdentityClientConfig{}, TokenSourceFunc(func(context.Context) (string, error) {
		return "task-token-xyz", nil
	}))
	tree, err := c.TaskTree(context.Background(), iscope("tenant-a"), "root")
	if err != nil {
		t.Fatalf("TaskTree: %v", err)
	}
	if tree.Count() != 2 || !tree.IsDescendant("root", "child-1") {
		t.Fatalf("bad tree: %+v", tree)
	}
	if gotAuth != "Bearer task-token-xyz" {
		t.Fatalf("bearer not forwarded: %q", gotAuth)
	}
}

func TestIdentityHTTPStatusMapping(t *testing.T) {
	cases := []struct {
		status int
		want   string
	}{
		{http.StatusNotFound, contracts.ErrNotFound},
		{http.StatusUnauthorized, contracts.ErrForbidden},
		{http.StatusForbidden, contracts.ErrForbidden},
		{http.StatusInternalServerError, ""},
	}
	for _, tc := range cases {
		var auth string
		url := httpStub(t, &auth, func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "nope", tc.status)
		})
		c := newIdentityTestClient(t, url, IdentityClientConfig{}, nil)
		_, err := c.TaskTree(context.Background(), iscope("tenant-a"), "root")
		if err == nil {
			t.Fatalf("status %d: want error", tc.status)
		}
		if tc.want != "" {
			if contracts.Code(err) != tc.want {
				t.Fatalf("status %d: want code %s got %v", tc.status, tc.want, err)
			}
		} else if !errors.Is(err, ErrIdentityTree) {
			t.Fatalf("status %d: want ErrIdentityTree got %v", tc.status, err)
		}
	}
}

func TestIdentityHTTPRejectsCrossTenantTree(t *testing.T) {
	var auth string
	url := httpStub(t, &auth, func(w http.ResponseWriter, r *http.Request) {
		writeTree(w, map[string]any{
			"tenant":     "evil-tenant",
			"rootTaskId": "root",
			"task":       map[string]any{"taskId": "root"},
		})
	})
	c := newIdentityTestClient(t, url, IdentityClientConfig{}, nil)
	if _, err := c.TaskTree(context.Background(), iscope("tenant-a"), "root"); !errors.Is(err, ErrIdentityTree) {
		t.Fatalf("cross-tenant tree must be ErrIdentityTree, got %v", err)
	}
}

func TestIdentityHTTPRejectsForgedRoot(t *testing.T) {
	var auth string
	url := httpStub(t, &auth, func(w http.ResponseWriter, r *http.Request) {
		writeTree(w, map[string]any{
			"tenant":     "tenant-a",
			"rootTaskId": "someone-else",
			"task":       map[string]any{"taskId": "someone-else"},
		})
	})
	c := newIdentityTestClient(t, url, IdentityClientConfig{}, nil)
	if _, err := c.TaskTree(context.Background(), iscope("tenant-a"), "root"); !errors.Is(err, ErrIdentityTree) {
		t.Fatalf("forged root must be ErrIdentityTree, got %v", err)
	}
}

func TestIdentityHTTPRejectsOverdeepTree(t *testing.T) {
	var auth string
	url := httpStub(t, &auth, func(w http.ResponseWriter, r *http.Request) {
		deep := map[string]any{"taskId": "leaf"}
		for i := 0; i < 70; i++ {
			deep = map[string]any{"taskId": "n" + contracts.NewID(), "children": []any{deep}}
		}
		writeTree(w, map[string]any{"tenant": "tenant-a", "rootTaskId": "root", "task": deep})
	})
	c := newIdentityTestClient(t, url, IdentityClientConfig{MaxDepth: 10, MaxNodes: 100000}, nil)
	if _, err := c.TaskTree(context.Background(), iscope("tenant-a"), "root"); !errors.Is(err, ErrIdentityTree) {
		t.Fatalf("over-deep tree must be ErrIdentityTree, got %v", err)
	}
}

func TestIdentityHTTPRejectsNodeCrossTenant(t *testing.T) {
	var auth string
	url := httpStub(t, &auth, func(w http.ResponseWriter, r *http.Request) {
		writeTree(w, map[string]any{
			"tenant":     "tenant-a",
			"rootTaskId": "root",
			"task": map[string]any{
				"taskId":   "root",
				"children": []any{map[string]any{"taskId": "c", "tenant": "tenant-b"}},
			},
		})
	})
	c := newIdentityTestClient(t, url, IdentityClientConfig{}, nil)
	if _, err := c.TaskTree(context.Background(), iscope("tenant-a"), "root"); !errors.Is(err, ErrIdentityTree) {
		t.Fatalf("cross-tenant node must be ErrIdentityTree, got %v", err)
	}
}
