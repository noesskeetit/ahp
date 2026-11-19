package secretstore

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestReadQwenParsesKVv2(t *testing.T) {
	var gotToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-Vault-Token")
		if r.URL.Path != "/v1/"+DefaultQwenPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"data": map[string]any{
				"api_key":  "secret-value-do-not-log",
				"base_url": "https://qwen.example/v1",
				"model":    "model-run-tqmbu-road",
			}},
		})
	}))
	defer srv.Close()

	c, err := New(srv.URL, "tok-123")
	if err != nil {
		t.Fatal(err)
	}
	sec, err := c.ReadQwen(context.Background(), DefaultQwenPath)
	if err != nil {
		t.Fatal(err)
	}
	if gotToken != "tok-123" {
		t.Fatalf("token header not forwarded")
	}
	if sec.BaseURL != "https://qwen.example/v1" || sec.Model != "model-run-tqmbu-road" {
		t.Fatalf("bad parse: %+v", sec)
	}
	if sec.String() != "QwenSecret{redacted}" {
		t.Fatalf("String leaked: %s", sec.String())
	}
}

func TestReadQwenMissingFieldFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"data": map[string]any{"api_key": "x", "base_url": "https://y"}},
		})
	}))
	defer srv.Close()
	c, _ := New(srv.URL, "t")
	if _, err := c.ReadQwen(context.Background(), DefaultQwenPath); err == nil {
		t.Fatal("expected validation error for missing model")
	}
}

func TestNotConfigured(t *testing.T) {
	if _, err := New("", ""); err != ErrNotConfigured {
		t.Fatalf("want ErrNotConfigured, got %v", err)
	}
}

// TestQwenFromEnvLive reads the real scoped secret when the operator has
// provisioned a Vault token in the environment. It asserts only on presence
// and shape, never printing the credential value.
func TestQwenFromEnvLive(t *testing.T) {
	if firstEnv(EnvVaultToken, EnvVaultTokenAlt) == "" {
		t.Skip("no vault token in environment")
	}
	sec, err := QwenFromEnv(context.Background())
	if err != nil {
		t.Fatalf("live qwen secret: %v", err)
	}
	if err := sec.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(sec.APIKey) < 8 {
		t.Fatalf("api_key unexpectedly short")
	}
	_ = os.Getenv("PATH")
}
