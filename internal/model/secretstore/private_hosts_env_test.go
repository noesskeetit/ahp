package secretstore

// F-1: the AHP_LLM_PRIVATE_HOSTS opt-in must apply on EVERY scoped-secret read
// path, not only QwenFromEnv. The production execbridge resolver is
// vaultSecretResolver -> Client.ReadQwen, so the worker seam resolves private
// hosts through ReadQwen. These legs pin that env is honoured there.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func vaultWithRecord(t *testing.T, record map[string]any) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": record}})
	}))
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, "tok")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestF1ReadQwenAppliesPrivateHostsEnv(t *testing.T) {
	// Route record declares NO private_hosts; only the environment registers it.
	c := vaultWithRecord(t, map[string]any{
		"api_key": "k", "base_url": "http://172.18.0.6:8080/v1", "model": "m",
	})
	t.Setenv("AHP_LLM_PRIVATE_HOSTS", "172.18.0.6:8080")
	sec, err := c.ReadQwen(context.Background(), DefaultQwenPath)
	if err != nil {
		t.Fatal(err)
	}
	if !containsHost(sec.PrivateHosts, "172.18.0.6:8080") {
		t.Fatalf("ReadQwen must apply AHP_LLM_PRIVATE_HOSTS (execbridge seam): got %v", sec.PrivateHosts)
	}
}

func TestF1ReadQwenMergesRecordAndEnv(t *testing.T) {
	c := vaultWithRecord(t, map[string]any{
		"api_key": "k", "base_url": "http://provider.internal/v1", "model": "m",
		"private_hosts": []any{"provider.internal"},
	})
	t.Setenv("AHP_LLM_PRIVATE_HOSTS", "172.18.0.6:8080")
	sec, err := c.ReadQwen(context.Background(), DefaultQwenPath)
	if err != nil {
		t.Fatal(err)
	}
	if !containsHost(sec.PrivateHosts, "provider.internal") || !containsHost(sec.PrivateHosts, "172.18.0.6:8080") {
		t.Fatalf("ReadQwen must merge record + env private_hosts, got %v", sec.PrivateHosts)
	}
}

func TestF1EmptyEnvAndRecordKeepsBehavior(t *testing.T) {
	c := vaultWithRecord(t, map[string]any{
		"api_key": "k", "base_url": "https://qwen.example/v1", "model": "m",
	})
	t.Setenv("AHP_LLM_PRIVATE_HOSTS", "")
	sec, err := c.ReadQwen(context.Background(), DefaultQwenPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(sec.PrivateHosts) != 0 {
		t.Fatalf("empty env + empty record must leave PrivateHosts nil, got %v", sec.PrivateHosts)
	}
}

func containsHost(list []string, want string) bool {
	for _, h := range list {
		if h == want {
			return true
		}
	}
	return false
}
