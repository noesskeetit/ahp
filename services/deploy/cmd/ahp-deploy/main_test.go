package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// pdfManifest is the authoritative §6.6 example, transcribed to JSON.
const pdfManifest = `{
  "kind": "agent",
  "system": "hr-assistant",
  "visibility": "catalog",
  "audience": "shared",
  "components": [
    {"name": "entry", "a2a": true},
    {"name": "worker", "replicas": {"min": 0, "max": 20}}
  ],
  "state": {
    "partition": "subject",
    "shared": ["policy_cache"],
    "compat": "forward",
    "drain": "7d",
    "own_tables": true,
    "delete_hook": "/ahp/delete-subject"
  },
  "capabilities": {
    "models": ["default-chat"],
    "tools": ["hr-api.read", "hr-api.write"],
    "memory": ["agent"],
    "compute": ["ephemeral"],
    "calls": ["hr-reader"],
    "self_tasks": {"max_fanout": 8}
  },
  "limits": {"depth": 3, "task_timeout": "2h", "active_tasks": 200}
}`

func mustParse(t *testing.T, s string) *Manifest {
	t.Helper()
	var m Manifest
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("parse: %v", err)
	}
	m.Runtime = Runtime{Images: map[string]string{"entry": "example/entry@sha256:" + strings.Repeat("a", 64), "worker": "example/worker@sha256:" + strings.Repeat("b", 64)}, TLSSecret: "agent-tls", DrainHooks: map[string]string{"entry": "ahp-agent", "worker": "ahp-agent"}}
	return &m
}

func TestValidPDFManifest(t *testing.T) {
	m := mustParse(t, pdfManifest)
	if errs := m.Validate(); len(errs) != 0 {
		t.Fatalf("expected valid manifest, got errors: %v", errs)
	}
	objs := m.Generate()
	kinds := map[string]int{}
	for _, o := range objs {
		kinds[o["kind"].(string)]++
	}
	for _, want := range []string{"Namespace", "ServiceAccount", "NetworkPolicy", "StateScope", "Role", "RoleBinding"} {
		if kinds[want] != 1 {
			t.Errorf("expected exactly one %s, got %d", want, kinds[want])
		}
	}
	if len(objs) != 12 {
		t.Errorf("expected 12 objects, got %d", len(objs))
	}
	// namespace determinism.
	ns := objs[0]["metadata"].(map[string]any)["name"]
	if ns != "ahp-hr-assistant" {
		t.Errorf("namespace = %v, want ahp-hr-assistant", ns)
	}
	// capability ceiling annotation must carry the tools list (R6.6-03).
	var role map[string]any
	for _, o := range objs {
		if o["kind"] == "Role" {
			role = o
		}
	}
	ann := role["metadata"].(map[string]any)["annotations"].(map[string]string)
	if ann["ahp.github.io/cap-tools"] != "hr-api.read,hr-api.write" {
		t.Errorf("cap-tools annotation = %q", ann["ahp.github.io/cap-tools"])
	}
	if ann["ahp.github.io/capability"] != "ceiling" {
		t.Errorf("capability ceiling marker missing")
	}
}

func TestInvalidVisibilityRefused(t *testing.T) {
	m := mustParse(t, pdfManifest)
	m.Visibility = "public" // not in system|tenant|catalog
	errs := m.Validate()
	if len(errs) == 0 {
		t.Fatal("expected refusal for invalid visibility")
	}
	if !contains(errs, "visibility") {
		t.Errorf("expected visibility error, got %v", errs)
	}
}

func TestOwnTablesRequiresDeleteHook(t *testing.T) {
	m := mustParse(t, pdfManifest)
	m.State.DeleteHook = "" // own_tables stays true
	errs := m.Validate()
	if !contains(errs, "delete_hook") {
		t.Errorf("expected delete_hook required error, got %v", errs)
	}
}

func TestSingleA2AEntryPoint(t *testing.T) {
	// zero entries
	m := mustParse(t, pdfManifest)
	m.Components[0].A2A = false
	if !contains(m.Validate(), "a2a") {
		t.Errorf("expected single-a2a error when zero entries")
	}
	// two entries
	m2 := mustParse(t, pdfManifest)
	m2.Components[1].A2A = true
	if !contains(m2.Validate(), "a2a") {
		t.Errorf("expected single-a2a error when two entries")
	}
}

func TestMissingFieldRefused(t *testing.T) {
	m := mustParse(t, pdfManifest)
	m.State.Drain = ""
	m.Limits.Depth = 0
	m.Capability.SelfTasks.MaxFanout = 0
	errs := m.Validate()
	for _, want := range []string{"drain", "depth", "max_fanout"} {
		if !contains(errs, want) {
			t.Errorf("expected error mentioning %q, got %v", want, errs)
		}
	}
}

func TestEmitIsMultiDocYAMLStream(t *testing.T) {
	m := mustParse(t, pdfManifest)
	objs := m.Generate()
	var sb strings.Builder
	if err := emit(&sb, objs); err != nil {
		t.Fatalf("emit: %v", err)
	}
	docs := strings.Split(strings.TrimSpace(sb.String()), "\n---\n")
	if len(docs) != len(objs) {
		t.Fatalf("expected %d docs, got %d", len(objs), len(docs))
	}
	// every doc must be parseable JSON (=> valid YAML scalar/flow too).
	for _, d := range docs {
		var o map[string]any
		if err := json.Unmarshal([]byte(d), &o); err != nil {
			t.Errorf("doc not valid JSON: %v", err)
		}
	}
}

func contains(errs []string, substr string) bool {
	for _, e := range errs {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

func TestWorkloadImagesAndSecurityAreConcrete(t *testing.T) {
	m := mustParse(t, pdfManifest)
	objects := m.Generate()
	deployments := 0
	services := 0
	crds := 0
	for _, o := range objects {
		switch o["kind"] {
		case "CustomResourceDefinition":
			crds++
		case "Service":
			services++
		case "Deployment":
			deployments++
			spec := o["spec"].(map[string]any)
			pod := spec["template"].(map[string]any)["spec"].(map[string]any)
			container := pod["containers"].([]any)[0].(map[string]any)
			image := container["image"].(string)
			if !strings.Contains(image, "@sha256:") {
				t.Fatalf("mutable image %s", image)
			}
			if pod["automountServiceAccountToken"] != false {
				t.Fatal("automatic token mount enabled")
			}
			sec := container["securityContext"].(map[string]any)
			if sec["allowPrivilegeEscalation"] != false || sec["readOnlyRootFilesystem"] != true {
				t.Fatal("weak container security")
			}
			for _, key := range []string{"resources", "startupProbe", "readinessProbe", "livenessProbe", "lifecycle"} {
				if container[key] == nil {
					t.Fatalf("missing %s", key)
				}
			}
		}
	}
	if deployments != 2 || services != 2 || crds != 1 {
		t.Fatalf("missing workloads: deployments=%d services=%d crds=%d", deployments, services, crds)
	}
	delete(m.Runtime.Images, "entry")
	if len(m.ValidateRuntime()) == 0 || len(m.Generate()) != 0 {
		t.Fatal("missing immutable image rendered partial objects")
	}
}

// deploymentContainer returns the named container inside the named Deployment so
// drain-lifecycle rendering can be asserted directly.
func deploymentContainer(t *testing.T, objs []map[string]any, deploy, container string) map[string]any {
	t.Helper()
	for _, o := range objs {
		if o["kind"] != "Deployment" {
			continue
		}
		if o["metadata"].(map[string]any)["name"] != deploy {
			continue
		}
		spec := o["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
		for _, c := range spec["containers"].([]any) {
			cm := c.(map[string]any)
			if cm["name"] == container {
				return cm
			}
		}
		t.Fatalf("container %q not found in deployment %q", container, deploy)
	}
	t.Fatalf("deployment %q not generated", deploy)
	return nil
}

// The Python custom-agent container (python -m ahp_state.container) drains on
// SIGTERM and ships no /ahp-agent binary. A component that declares sigterm must
// get NO preStop exec, while a component that declares the Go agent still gets
// its in-process drain hook -- the renderer cannot require a hook the image does
// not implement.
func TestDrainHookSigtermRendersNoPreStop(t *testing.T) {
	m := mustParse(t, pdfManifest)
	m.Runtime.DrainHooks = map[string]string{"entry": "ahp-agent", "worker": "sigterm"}
	if errs := m.ValidateRuntime(); len(errs) != 0 {
		t.Fatalf("valid drain hooks refused: %v", errs)
	}
	objs := m.Generate()
	if len(objs) == 0 {
		t.Fatal("generate produced nothing")
	}
	if c := deploymentContainer(t, objs, "hr-assistant-worker", "worker"); c["lifecycle"] != nil {
		t.Fatalf("sigterm component rendered a lifecycle hook it cannot run: %v", c["lifecycle"])
	}
	entry := deploymentContainer(t, objs, "hr-assistant-entry", "entry")
	life, ok := entry["lifecycle"].(map[string]any)
	if !ok {
		t.Fatal("ahp-agent component lost its preStop drain hook")
	}
	pre, ok := life["preStop"].(map[string]any)
	if !ok {
		t.Fatal("ahp-agent preStop missing")
	}
	cmd := pre["exec"].(map[string]any)["command"].([]string)
	if len(cmd) != 2 || cmd[0] != "/ahp-agent" || cmd[1] != "--drain" {
		t.Fatalf("ahp-agent preStop command = %v", cmd)
	}
}

// The renderer must not silently default an undeclared drain lifecycle onto the
// Go agent's hook: a Python deployment that forgets to declare would otherwise
// get a preStop its image cannot execute. Undeclared and unknown values are
// refused, and no objects are generated.
func TestUndeclaredOrUnknownDrainHookRefused(t *testing.T) {
	missing := mustParse(t, pdfManifest)
	missing.Runtime.DrainHooks = nil
	if !containsErr(missing.ValidateRuntime(), "drain_hooks") {
		t.Fatal("undeclared drain hook accepted")
	}
	if len(missing.Generate()) != 0 {
		t.Fatal("objects generated for a manifest with an undeclared drain hook")
	}
	unknown := mustParse(t, pdfManifest)
	unknown.Runtime.DrainHooks = map[string]string{"entry": "ahp-agent", "worker": "systemd-notify"}
	if !containsErr(unknown.ValidateRuntime(), "drain_hooks") {
		t.Fatal("unknown drain hook value accepted")
	}
	if len(unknown.Generate()) != 0 {
		t.Fatal("objects generated for an unsupported drain hook")
	}
}

func containsErr(errs []string, substr string) bool {
	for _, e := range errs {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}
