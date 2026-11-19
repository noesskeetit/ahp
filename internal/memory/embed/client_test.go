package embed

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// codeOf resolves the contract class of an error the way callers see it.
func codeOf(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
	var e *contracts.Error
	if !errors.As(err, &e) {
		t.Fatalf("error %q is not a contracts error", err)
	}
	return e.Code
}

func writeKeyFile(t *testing.T, mode os.FileMode) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(p, []byte("sk-secret-value-do-not-log\n"), mode); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestNotConfiguredIsNotAnError(t *testing.T) {
	t.Setenv("AHP_MEMORY_EMBEDDING_BASE_URL", "")
	b, err := FromEnv()
	if err != nil {
		t.Fatalf("an absent embedder must be a clean no, got %v", err)
	}
	if b != nil {
		t.Fatal("expected a nil backend when no base URL is configured")
	}
}

func TestConfigurationIsRefusedPiecewise(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string // substring of the refusal
	}{
		{"no model", map[string]string{
			"AHP_MEMORY_EMBEDDING_BASE_URL": "https://example.invalid/v1",
			"AHP_MEMORY_EMBEDDING_API_KEY":  "k", "AHP_MEMORY_EMBEDDING_DIM": "384"}, "MODEL"},
		{"no dimension", map[string]string{
			"AHP_MEMORY_EMBEDDING_BASE_URL": "https://example.invalid/v1",
			"AHP_MEMORY_EMBEDDING_MODEL":    "m", "AHP_MEMORY_EMBEDDING_API_KEY": "k"}, "DIM"},
		{"dimension not a number", map[string]string{
			"AHP_MEMORY_EMBEDDING_BASE_URL": "https://example.invalid/v1",
			"AHP_MEMORY_EMBEDDING_MODEL":    "m", "AHP_MEMORY_EMBEDDING_API_KEY": "k",
			"AHP_MEMORY_EMBEDDING_DIM": "wide"}, "DIM"},
		{"dimension out of range", map[string]string{
			"AHP_MEMORY_EMBEDDING_BASE_URL": "https://example.invalid/v1",
			"AHP_MEMORY_EMBEDDING_MODEL":    "m", "AHP_MEMORY_EMBEDDING_API_KEY": "k",
			"AHP_MEMORY_EMBEDDING_DIM": "20000"}, "DIM"},
		{"no credential", map[string]string{
			"AHP_MEMORY_EMBEDDING_BASE_URL": "https://example.invalid/v1",
			"AHP_MEMORY_EMBEDDING_MODEL":    "m", "AHP_MEMORY_EMBEDDING_DIM": "384"}, "credential"},
		{"two credentials", map[string]string{
			"AHP_MEMORY_EMBEDDING_BASE_URL": "https://example.invalid/v1",
			"AHP_MEMORY_EMBEDDING_MODEL":    "m", "AHP_MEMORY_EMBEDDING_DIM": "384",
			"AHP_MEMORY_EMBEDDING_API_KEY": "k", "AHP_MEMORY_EMBEDDING_KEY_FILE": "/tmp/also-set"}, "credential"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range []string{
				"AHP_MEMORY_EMBEDDING_BASE_URL", "AHP_MEMORY_EMBEDDING_MODEL",
				"AHP_MEMORY_EMBEDDING_DIM", "AHP_MEMORY_EMBEDDING_API_KEY",
				"AHP_MEMORY_EMBEDDING_KEY_FILE"} {
				t.Setenv(k, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := FromEnv()
			if err == nil {
				t.Fatal("an incomplete embedding configuration must be refused, not half-mounted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refusal %q must name the missing %q", err, tc.want)
			}
		})
	}
}

// A secret readable by anyone on the box is not a secret: the key file is
// refused unless it is a regular file restricted to its owner.
func TestKeyFileMustBeOwnerReadableOnly(t *testing.T) {
	t.Setenv("AHP_MEMORY_EMBEDDING_BASE_URL", "https://example.invalid/v1")
	t.Setenv("AHP_MEMORY_EMBEDDING_MODEL", "m")
	t.Setenv("AHP_MEMORY_EMBEDDING_DIM", "4")
	t.Setenv("AHP_MEMORY_EMBEDDING_API_KEY", "")
	t.Setenv("AHP_MEMORY_EMBEDDING_KEY_FILE", writeKeyFile(t, 0o644))
	_, err := FromEnv()
	if err == nil || !strings.Contains(err.Error(), "0600") {
		t.Fatalf("a group-readable key file must be refused with the required mode, got %v", err)
	}

	t.Setenv("AHP_MEMORY_EMBEDDING_KEY_FILE", writeKeyFile(t, 0o600))
	b, err := FromEnv()
	if err != nil {
		t.Fatalf("a 0600 key file must be accepted: %v", err)
	}
	if b.APIKey() != "sk-secret-value-do-not-log" {
		t.Fatal("key file content was not read")
	}
	if strings.Contains(b.Describe(), "sk-secret") {
		t.Fatal("Describe() leaked the credential")
	}
}

func TestEmbedSendsOpenAICompliantRequestAndKeepsOrder(t *testing.T) {
	var gotAuth, gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		// Answer in reverse order: the client must restore input order. The
		// answer count follows the request, because a client that tolerates a
		// mismatched count would silently mis-assign vectors between texts.
		input, _ := gotBody["input"].([]any)
		data := make([]any, 0, len(input))
		for i := len(input) - 1; i >= 0; i-- {
			vec := make([]float64, 4)
			vec[0] = float64(i)
			vec[1] = 1
			data = append(data, map[string]any{"index": i, "embedding": vec})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer srv.Close()

	b, err := newWithHTTP(Config{BaseURL: srv.URL, APIKey: "k", Model: "bge-test", Dimensions: 4, Timeout: 2 * time.Second}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	got, err := b.EmbedBatch(context.Background(), []string{"first", "second"})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/v1/embeddings" {
		t.Fatalf("path %q, want the OpenAI-compatible /v1/embeddings", gotPath)
	}
	if gotAuth != "Bearer k" {
		t.Fatal("bearer auth header missing")
	}
	if gotBody["model"] != "bge-test" {
		t.Fatalf("model not sent: %v", gotBody)
	}
	if len(got) != 2 || got[0][0] != 0 || got[1][0] != 1 {
		t.Fatalf("input order not restored: %v", got)
	}
	one, err := b.Embed(context.Background(), "solo")
	if err != nil || len(one) != 4 {
		t.Fatalf("Embed: %v %v", one, err)
	}
}

func TestEmbedRefusesAnswersItCannotTrust(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"wrong width", `{"data":[{"index":0,"embedding":[1,2,3]}]}`},
		{"missing data", `{"data":[]}`},
		{"fewer answers than inputs", `{"data":[{"index":0,"embedding":[1,2,3,4]}]}`},
		{"not a number", `{"data":[{"index":0,"embedding":[1,2,3,"four"]}]}`},
		{"not json", `nope`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			b, err := newWithHTTP(Config{BaseURL: srv.URL, APIKey: "k", Model: "m", Dimensions: 4, Timeout: 2 * time.Second}, srv.Client())
			if err != nil {
				t.Fatal(err)
			}
			_, err = b.EmbedBatch(context.Background(), []string{"a", "b"})
			if codeOf(t, err) != contracts.ErrUnavailable {
				t.Fatalf("an unusable embedder answer must be UNAVAILABLE, got %v", err)
			}
		})
	}
}

// The dependency being down is a dependency failure, never a fake success and
// never the caller's fault; the credential must not travel into the message.
func TestTransportFailureIsUnavailableAndStaysSecretFree(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal boom"))
	}))
	defer srv.Close()
	b, err := newWithHTTP(Config{BaseURL: srv.URL, APIKey: "super-secret-k", Model: "m", Dimensions: 4, Timeout: 2 * time.Second}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Embed(context.Background(), "x")
	if codeOf(t, err) != contracts.ErrUnavailable {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "super-secret-k") {
		t.Fatal("credential leaked into the error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("the dependency status must be reported, got %q", err)
	}
}
