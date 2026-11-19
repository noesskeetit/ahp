//go:build ahp_data

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The mount line is the operator's only view of what backs Memory search, so it
// must distinguish "no embedder configured" from "embedder configured" and must
// never carry the credential.
func TestMemoryEmbedderLogDistinguishesAbsentFromWired(t *testing.T) {
	for _, k := range []string{
		"AHP_MEMORY_EMBEDDING_BASE_URL", "AHP_MEMORY_EMBEDDING_MODEL",
		"AHP_MEMORY_EMBEDDING_DIM", "AHP_MEMORY_EMBEDDING_API_KEY",
		"AHP_MEMORY_EMBEDDING_KEY_FILE"} {
		t.Setenv(k, "")
	}
	t.Setenv("AHP_MEMORY_SEARCH_MODE", "lexical")
	opts, desc, err := memoryEmbedderFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(opts) != 0 || !strings.Contains(desc, "absent") {
		t.Fatalf("absent configuration must mount lexical-only, got %q", desc)
	}

	t.Setenv("AHP_MEMORY_SEARCH_MODE", "hybrid")
	t.Setenv("AHP_MEMORY_EMBEDDING_BASE_URL", "https://embed.invalid/v1")
	t.Setenv("AHP_MEMORY_EMBEDDING_MODEL", "bge-m3")
	t.Setenv("AHP_MEMORY_EMBEDDING_DIM", "1024")
	t.Setenv("AHP_MEMORY_EMBEDDING_REVISION", "test-config-v1")
	t.Setenv("AHP_MEMORY_EMBEDDING_API_KEY", "never-log-this")
	policy := filepath.Join(t.TempDir(), "policy.json")
	if err := os.WriteFile(policy, []byte(`{"version":1,"providers":[{"name":"embed","audience":{"kind":"external"},"secret_ref":"AHP_MEMORY_EMBEDDING_KEY_FILE","models":["bge-m3"]}],"routes":[{"name":"allowed","when":{"tenants":["synthetic"]},"then":{"provider":"embed","model":"bge-m3"}}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AHP_MEMORY_EMBEDDING_PROVIDER", "embed")
	t.Setenv("AHP_MEMORY_EMBEDDING_POLICY_FILE", policy)
	opts, desc, err = memoryEmbedderFromEnv()
	if err != nil {
		t.Fatalf("a complete configuration must wire the embedder: %v", err)
	}
	if len(opts) != 2 {
		t.Fatalf("expected the embedding option, got %d options", len(opts))
	}
	if !strings.Contains(desc, "bge-m3") || !strings.Contains(desc, "1024") {
		t.Fatalf("the log line must name model and width, got %q", desc)
	}
	if strings.Contains(desc, "never-log-this") {
		t.Fatal("the credential reached the log line")
	}
}

// Half-configured embedding is a refused mount, not a silently lexical plane: an
// operator who set a base URL and forgot the dimension asked for semantic search
// and must be told, because the pgvector column width cannot be guessed.
func TestMemoryEmbedderHalfConfigFailsClosed(t *testing.T) {
	for _, k := range []string{
		"AHP_MEMORY_EMBEDDING_BASE_URL", "AHP_MEMORY_EMBEDDING_MODEL",
		"AHP_MEMORY_EMBEDDING_DIM", "AHP_MEMORY_EMBEDDING_API_KEY",
		"AHP_MEMORY_EMBEDDING_KEY_FILE"} {
		t.Setenv(k, "")
	}
	t.Setenv("AHP_MEMORY_SEARCH_MODE", "hybrid")
	t.Setenv("AHP_MEMORY_EMBEDDING_BASE_URL", "https://embed.invalid/v1")
	t.Setenv("AHP_MEMORY_EMBEDDING_MODEL", "bge-m3")
	// The credential is present so the refusal under test is about the one
	// field that genuinely cannot be defaulted: the pgvector column width.
	t.Setenv("AHP_MEMORY_EMBEDDING_API_KEY", "never-log-this")
	_, _, err := memoryEmbedderFromEnv()
	if err == nil {
		t.Fatal("a missing dimension must refuse the mount")
	}
	if !strings.Contains(err.Error(), "AHP_MEMORY_EMBEDDING_DIM") {
		t.Fatalf("the refusal must name the variable, got %q", err)
	}
}
