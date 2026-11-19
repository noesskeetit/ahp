package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The profiling LLM phase must take its provider/model identity from the same
// config point production routing loads -- never from a literal in this file
// (identity-ledger defect, DEF-CONFIG-COPIES). These tests pin that rule on a
// fixture source table: derived identity = default route's provider and its
// routed model; a configured AHP_LLM_MODEL that the route does not serve is
// refused instead of silently retargeting the measurement.
func writeFixtureSource(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "providers.json")
	table := map[string]any{
		"version": 1,
		"providers": []any{map[string]any{
			"name": "unitrun", "class": "standard",
			"audience":   map[string]any{"kind": "external", "allow_external": false},
			"secret_ref": "unit/ref", "models": []string{"unit-model-1"},
		}},
		"routes": []any{map[string]any{
			"name": "default",
			"when": map[string]any{"tenants": []string{"*"}},
			"then": map[string]any{"provider": "unitrun"},
		}},
	}
	raw, err := json.MarshalIndent(table, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestProfilingIdentityDerivedFromConfigPoint(t *testing.T) {
	source := writeFixtureSource(t)
	t.Setenv("AHP_PROFILING_PROVIDERS_SOURCE", source)
	t.Setenv("AHP_LLM_MODEL", "")
	out := filepath.Join(t.TempDir(), "prof-providers.json")
	if err := writeProfilingProviders(out); err != nil {
		t.Fatalf("writeProfilingProviders: %v", err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Providers []struct {
			Name      string   `json:"name"`
			SecretRef string   `json:"secret_ref"`
			Models    []string `json:"models"`
		} `json:"providers"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("fixture unreadable: %v", err)
	}
	if len(got.Providers) != 1 || got.Providers[0].Name != "unitrun" || got.Providers[0].SecretRef != "unit/ref" ||
		len(got.Providers[0].Models) != 1 || got.Providers[0].Models[0] != "unit-model-1" {
		t.Fatalf("identity not derived from the config point: %s", raw)
	}
	// The literal of the source identity appears exactly once per field value
	// and nowhere else; crucially this test proves no hardcoded default exists:
	// a source naming a DIFFERENT model changes the fixture, and removing the
	// source is an error, not a default.
	t.Setenv("AHP_LLM_MODEL", "someone-elses-model")
	if err := writeProfilingProviders(out); err == nil || !strings.Contains(err.Error(), "not routed through provider") {
		t.Fatalf("foreign AHP_LLM_MODEL accepted: %v", err)
	}
	t.Setenv("AHP_LLM_MODEL", "unit-model-1")
	if err := writeProfilingProviders(out); err != nil {
		t.Fatalf("agreed AHP_LLM_MODEL refused: %v", err)
	}
	t.Setenv("AHP_PROFILING_PROVIDERS_SOURCE", filepath.Join(t.TempDir(), "absent.json"))
	if err := writeProfilingProviders(out); err == nil {
		t.Fatal("missing source accepted: the profiler must have no silent identity default")
	}
}
