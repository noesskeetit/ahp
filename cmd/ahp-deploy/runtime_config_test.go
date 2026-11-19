package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// Operator-owned attachment: references only, never secret values in a manifest.
func TestRuntimeConfigurationSecretIsMountedAsEnvironment(t *testing.T) {
	m := mustParse(t, pdfManifest)
	if err := json.Unmarshal([]byte(`{"config_secrets":{"entry":"entry-runtime"}}`), &m.Runtime); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, o := range m.Generate() {
		if o["kind"] != "Deployment" {
			continue
		}
		spec := o["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		c := spec["containers"].([]any)[0].(map[string]any)
		b, err := json.Marshal(c["envFrom"])
		if err != nil {
			t.Fatal(err)
		}
		if c["name"] == "entry" {
			found = true
			if string(b) != `[{"secretRef":{"name":"entry-runtime"}}]` {
				t.Fatalf("missing operator config reference: %s", b)
			}
		} else if string(b) != "null" {
			t.Fatalf("configuration leaked to a different component: %s", b)
		}
	}
	if !found {
		t.Fatal("entry deployment absent")
	}
}

func TestRuntimeConfigurationRefusesUnknownComponentOrInvalidSecret(t *testing.T) {
	for _, raw := range []string{`{"config_secrets":{"ghost":"ghost-runtime"}}`, `{"config_secrets":{"entry":"../runtime"}}`, `{"config_secrets":{"entry":""}}`} {
		m := mustParse(t, pdfManifest)
		if err := json.Unmarshal([]byte(raw), &m.Runtime); err != nil {
			t.Fatal(err)
		}
		if errs := m.ValidateRuntime(); len(errs) == 0 || !strings.Contains(strings.Join(errs, " "), "config_secrets") {
			t.Fatalf("unvalidated operator configuration: %s, %v", raw, errs)
		}
		if objects := m.Generate(); len(objects) > 0 {
			t.Fatal("invalid configuration generated objects")
		}
	}
}
