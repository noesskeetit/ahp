//go:build unix

package journal

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestUnixInjectorUsesLocalSocketWithoutGuestCredentials(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "ahp-sdki-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "state.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	boot := RuntimeBootstrap{
		Scope:     contracts.Scope{Tenant: "t", Deployment: "dep", Subject: "subject"},
		SessionID: "s", Harness: "go", LaunchTurnSeq: 1, Generation: 2, ExecutionID: "execution",
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/runtime/bootstrap", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("guest bootstrap sent credentials")
		}
		_ = json.NewEncoder(w).Encode(boot)
	})
	mux.HandleFunc("POST /v1/state/sessions/s/vm:claim", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("guest state request sent credentials")
		}
		var in VMTurnClaimRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.ExecutionID != boot.ExecutionID {
			t.Errorf("claim changed on transport: %+v (%v)", in, err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"session_id":"s","epoch":2}`))
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	t.Cleanup(func() { _ = srv.Close() })
	go func() { _ = srv.Serve(listener) }()

	got, err := ReadRuntimeBootstrap(t.Context(), path)
	if err != nil || got != boot {
		t.Fatalf("bootstrap = %+v, %v", got, err)
	}
	client, err := NewUnixInjectorClient(path)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := client.ClaimVMTurn(t.Context(), boot.SessionID, VMTurnClaimRequest{
		ExecutionID: boot.ExecutionID, TurnSeq: boot.LaunchTurnSeq, Generation: boot.Generation, TTLMilliseconds: 1000,
	})
	if err != nil || claimed.Epoch != boot.Generation {
		t.Fatalf("claim = %+v, %v", claimed, err)
	}
}

func TestUnixInjectorRejectsNonAbsoluteSocketPath(t *testing.T) {
	if _, err := NewUnixInjectorClient("socket"); err == nil {
		t.Fatal("accepted a relative host socket address")
	}
	if _, err := ReadRuntimeBootstrap(context.Background(), "https://example.invalid/x"); err == nil {
		t.Fatal("accepted a network URL as a local socket address")
	}
}
