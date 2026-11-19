package emil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// A legacy peer cannot receive the reserved bound. A configured tree must
// refuse this peer before any Send; otherwise a child spends outside the tree.
func TestLegacyPeerCannotBypassConfiguredTree(t *testing.T) {
	var sends atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == AgentCardPath {
			_, _ = w.Write([]byte(`{"name":"legacy","version":"1.0","skills":[]}`))
			return
		}
		sends.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"id":"unbudgeted-child","status":{"state":"completed"}}}`))
	}))
	defer srv.Close()
	a, err := New(Config{BaseURL: srv.URL, RunAuthorityFor: runAuthority(&fakeBudget{granted: 1}, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.Delegate(context.Background(), "work"); err == nil {
		t.Error("legacy delegation with configured tree succeeded")
	}
	if sends.Load() != 0 {
		t.Fatalf("legacy peer received %d unbudgeted sends", sends.Load())
	}
}
