package registry

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// validManifest returns a manifest that passes every rule.
func validManifest() Manifest {
	return Manifest{
		Kind:       KindAgent,
		System:     "acme.briefing",
		Visibility: VisibilityTenant,
		Audience:   AudienceShared,
		Class:      ClassS,
		Components: []Component{
			{Name: "router", A2A: true, MinReplicas: 1, MaxReplicas: 4},
			{Name: "worker", MinReplicas: 1, MaxReplicas: 8},
		},
		State: StateConfig{
			Partition: "subject:acme.briefing",
			Shared:    []string{"space:briefing"},
			Compat:    "briefing-v2",
			Drain:     "7d",
		},
		Capabilities: Capabilities{
			Models:    []string{"model/qwen-max"},
			Tools:     []string{"tool/calendar.read"},
			Memory:    []string{"memory/tenant"},
			Compute:   []string{"compute/ephemeral"},
			Calls:     []string{"agent/acme.other"},
			SelfTasks: 4,
		},
		Limits: Limits{Depth: 4, ActiveTasks: 32, TaskTimeout: "15m"},
	}
}

func TestValidateManifestAcceptsBaseline(t *testing.T) {
	if err := ValidateManifest(validManifest()); err != nil {
		t.Fatalf("baseline manifest rejected: %v", err)
	}
	tool := validManifest()
	tool.Kind = KindTool
	tool.Components = []Component{{Name: "mcp", MinReplicas: 1, MaxReplicas: 2}}
	if err := ValidateManifest(tool); err != nil {
		t.Fatalf("tool manifest rejected: %v", err)
	}
}

func TestValidateManifestRejections(t *testing.T) {
	cases := []struct {
		name  string
		spoil func(m *Manifest)
	}{
		{"empty kind", func(m *Manifest) { m.Kind = "" }},
		{"unknown kind", func(m *Manifest) { m.Kind = "daemon" }},
		{"empty system", func(m *Manifest) { m.System = "" }},
		{"uppercase system", func(m *Manifest) { m.System = "Acme" }},
		{"system path separator", func(m *Manifest) { m.System = "acme/briefing" }},
		{"unknown visibility", func(m *Manifest) { m.Visibility = "public" }},
		{"unknown audience", func(m *Manifest) { m.Audience = "team" }},
		{"unknown class", func(m *Manifest) { m.Class = "B" }},
		{"no components", func(m *Manifest) { m.Components = nil }},
		{"duplicate component", func(m *Manifest) {
			m.Components = append(m.Components, Component{Name: "router", MinReplicas: 1, MaxReplicas: 1})
		}},
		{"component path separator", func(m *Manifest) { m.Components[0].Name = "a/b" }},
		{"zero min replicas", func(m *Manifest) { m.Components[0].MinReplicas = 0 }},
		{"negative min replicas", func(m *Manifest) { m.Components[0].MinReplicas = -1 }},
		{"max below min", func(m *Manifest) { m.Components[1].MaxReplicas = 0 }},
		{"agent without a2a", func(m *Manifest) { m.Components[0].A2A = false }},
		{"agent with two a2a", func(m *Manifest) { m.Components[1].A2A = true }},
		{"tool with a2a", func(m *Manifest) {
			m.Kind = KindTool
			m.Components = []Component{{Name: "mcp", A2A: true, MinReplicas: 1, MaxReplicas: 1}}
		}},
		{"empty partition", func(m *Manifest) { m.State.Partition = "" }},
		{"partition with slash", func(m *Manifest) { m.State.Partition = "subject/briefing" }},
		{"shared repeats partition", func(m *Manifest) { m.State.Shared = []string{m.State.Partition} }},
		{"duplicate shared space", func(m *Manifest) { m.State.Shared = []string{"space:a", "space:a"} }},
		{"shared with wildcard", func(m *Manifest) { m.State.Shared = []string{"space:*"} }},
		{"own tables without delete hook", func(m *Manifest) { m.State.OwnTables = true }},
		{"delete hook escaping", func(m *Manifest) { m.State.DeleteHook = "hooks/../../etc/passwd" }},
		{"delete hook absolute", func(m *Manifest) { m.State.DeleteHook = "/hooks/purge" }},
		{"delete hook traversal segment", func(m *Manifest) { m.State.DeleteHook = "hooks/./purge" }},
		{"delete hook backslash", func(m *Manifest) { m.State.DeleteHook = `hooks\purge` }},
		{"drain escaping", func(m *Manifest) { m.State.Drain = "hooks/../drain" }},
		{"compat with slash", func(m *Manifest) { m.State.Compat = "briefing/v2" }},
		{"negative self tasks", func(m *Manifest) { m.Capabilities.SelfTasks = -1 }},
		{"wildcard models", func(m *Manifest) { m.Capabilities.Models = []string{"*"} }},
		{"wildcard calls", func(m *Manifest) { m.Capabilities.Calls = []string{"agent/*"} }},
		{"wildcard suffix tools", func(m *Manifest) { m.Capabilities.Tools = []string{"tool/all*"} }},
		{"wildcard compute", func(m *Manifest) { m.Capabilities.Compute = []string{"compute/??"} }},
		{"wildcard memory", func(m *Manifest) { m.Capabilities.Memory = []string{"memory/*"} }},
		{"duplicate capability", func(m *Manifest) { m.Capabilities.Models = []string{"model/a", "model/a"} }},
		{"empty capability", func(m *Manifest) { m.Capabilities.Tools = []string{""} }},
		{"depth zero", func(m *Manifest) { m.Limits.Depth = 0 }},
		{"depth above max", func(m *Manifest) { m.Limits.Depth = MaxDepth + 1 }},
		{"active tasks zero", func(m *Manifest) { m.Limits.ActiveTasks = 0 }},
		{"active tasks negative", func(m *Manifest) { m.Limits.ActiveTasks = -5 }},
		{"timeout empty", func(m *Manifest) { m.Limits.TaskTimeout = "" }},
		{"timeout zero", func(m *Manifest) { m.Limits.TaskTimeout = "0s" }},
		{"timeout negative", func(m *Manifest) { m.Limits.TaskTimeout = "-5s" }},
		{"timeout unitless", func(m *Manifest) { m.Limits.TaskTimeout = "900" }},
		{"timeout garbage", func(m *Manifest) { m.Limits.TaskTimeout = "soon" }},
		{"timeout above bound", func(m *Manifest) { m.Limits.TaskTimeout = "25h" }},
		{"class c personal compute", func(m *Manifest) {
			m.Class = ClassC
			m.Capabilities.Compute = []string{"compute/personal"}
		}},
		{"class c personal pool", func(m *Manifest) {
			m.Class = ClassC
			m.Capabilities.Compute = []string{"compute/personal/vm"}
		}},
		{"class c system privilege", func(m *Manifest) {
			m.Class = ClassC
			m.Capabilities.Calls = []string{"system/registry.admin"}
		}},
		{"class c platform memory", func(m *Manifest) {
			m.Class = ClassC
			m.Capabilities.Memory = []string{"platform/memory.write"}
		}},
		{"class c system visibility", func(m *Manifest) { m.Class = ClassC; m.Visibility = VisibilitySystem }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := validManifest()
			tc.spoil(&m)
			err := ValidateManifest(m)
			if err == nil {
				t.Fatalf("manifest was accepted, want rejection (%s)", tc.name)
			}
			if code := contracts.Code(err); code != contracts.ErrInvalid {
				t.Fatalf("code = %q, want %q (err=%v)", code, contracts.ErrInvalid, err)
			}
		})
	}
}

func TestValidateManifestAllowsPersonalComputeForClassS(t *testing.T) {
	m := validManifest()
	m.Capabilities.Compute = []string{"compute/personal"}
	if err := ValidateManifest(m); err != nil {
		t.Fatalf("class S personal compute rejected: %v", err)
	}
}

func TestValidateManifestClassCPersonalizationIsNotPersonal(t *testing.T) {
	m := validManifest()
	m.Class = ClassC
	m.Capabilities.Compute = []string{"compute/personalization"}
	if err := ValidateManifest(m); err != nil {
		t.Fatalf("personalization capability wrongly treated as personal: %v", err)
	}
}

func TestValidateManifestOwnTablesWithSafeHook(t *testing.T) {
	m := validManifest()
	m.State.OwnTables = true
	m.State.DeleteHook = "hooks/v2/purge-tenant-data"
	if err := ValidateManifest(m); err != nil {
		t.Fatalf("own_tables with safe hook rejected: %v", err)
	}
}

func TestValidateManifestDepthBoundary(t *testing.T) {
	m := validManifest()
	m.Limits.Depth = MaxDepth
	if err := ValidateManifest(m); err != nil {
		t.Fatalf("depth %d rejected: %v", MaxDepth, err)
	}
}

func TestParseTaskTimeout(t *testing.T) {
	for _, good := range []string{"1s", "45s", "15m", "2h30m"} {
		d, err := ParseTaskTimeout(good)
		if err != nil {
			t.Fatalf("ParseTaskTimeout(%q) = %v", good, err)
		}
		if d <= 0 {
			t.Fatalf("ParseTaskTimeout(%q) = %v, want positive", good, d)
		}
	}
	for _, bad := range []string{"", "0", "0s", "-1s", "abc", "10"} {
		if _, err := ParseTaskTimeout(bad); err == nil {
			t.Fatalf("ParseTaskTimeout(%q) accepted", bad)
		} else if contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("ParseTaskTimeout(%q) code = %q", bad, contracts.Code(err))
		}
	}
}

func TestValidateImageDigest(t *testing.T) {
	good := "registry.example.com/acme/agent@sha256:" + strings.Repeat("a", 64)
	if err := ValidateImageDigest(good); err != nil {
		t.Fatalf("immutable digest rejected: %v", err)
	}
	bad := []string{
		"registry.example.com/acme/agent:latest",
		"registry.example.com/acme/agent@sha256:" + strings.Repeat("A", 64),
		"registry.example.com/acme/agent@sha256:" + strings.Repeat("a", 63),
		"registry.example.com/acme/agent@sha512:" + strings.Repeat("a", 64),
		"",
	}
	for _, ref := range bad {
		if err := ValidateImageDigest(ref); err == nil {
			t.Fatalf("mutable or malformed reference accepted: %q", ref)
		} else if contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("code = %q for %q", contracts.Code(err), ref)
		}
	}
}

func TestValidateVersionDigest(t *testing.T) {
	if err := ValidateVersionDigest("sha256:" + strings.Repeat("0", 64)); err != nil {
		t.Fatalf("valid digest rejected: %v", err)
	}
	for _, bad := range []string{"", "sha256:", "sha256:zz" + strings.Repeat("0", 62), "md5:" + strings.Repeat("0", 32)} {
		if err := ValidateVersionDigest(bad); err == nil {
			t.Fatalf("invalid digest accepted: %q", bad)
		}
	}
}

func TestDecodeManifestJSON(t *testing.T) {
	doc := `{"kind":"agent","system":"acme.briefing","visibility":"tenant","audience":"shared","class":"S",
		"components":[{"name":"router","a2a":true,"min_replicas":1,"max_replicas":1}],
		"state":{"partition":"subject:acme.briefing"},
		"capabilities":{"models":["model/qwen-max"]},
		"limits":{"depth":2,"active_tasks":4,"task_timeout":"30s"}}`
	m, err := decodeManifestJSON([]byte(doc))
	if err != nil {
		t.Fatalf("bare manifest document rejected: %v", err)
	}
	if err := ValidateManifest(m); err != nil {
		t.Fatalf("decoded manifest invalid: %v", err)
	}

	wrapped, err := json.Marshal(map[string]any{"manifest": m})
	if err != nil {
		t.Fatal(err)
	}
	m2, err := decodeManifestJSON(wrapped)
	if err != nil {
		t.Fatalf("wrapped manifest document rejected: %v", err)
	}
	if m2.System != m.System || m2.Kind != m.Kind {
		t.Fatalf("wrapped document decoded as %+v", m2)
	}

	if _, err := decodeManifestJSON([]byte(`{"kind":"agent","unknown_field":1}`)); err == nil {
		t.Fatal("unknown field accepted")
	}
	if _, err := decodeManifestJSON(nil); err == nil {
		t.Fatal("empty document accepted")
	}
	if _, err := decodeManifestJSON([]byte(`{`)); err == nil {
		t.Fatal("truncated JSON accepted")
	}
}

func TestManifestStringHasNoDocumentContent(t *testing.T) {
	m := validManifest()
	line := m.String()
	for _, secret := range []string{"model/qwen-max", "tool/calendar.read", "subject:acme.briefing", "space:briefing"} {
		if strings.Contains(line, secret) {
			t.Fatalf("manifest summary leaks %q: %s", secret, line)
		}
	}
}

func TestDrainPlanMatrix(t *testing.T) {
	compat := func(c string) Manifest { return compatManifest(c) }
	cases := []struct {
		name    string
		in      DrainInput
		want    string
		recheck bool
	}{
		{"no candidate", DrainInput{CurrentCompat: "v1"}, DrainContinue, false},
		{"same compat", DrainInput{CurrentCompat: "v1", Candidate: compat("v1"), PinnedTasks: 3,
			Statuses: []TaskStatus{TaskRunning}}, DrainContinue, false},
		{"case-insensitive compat", DrainInput{CurrentCompat: "Briefing-V2", Candidate: compat("briefing-v2"),
			PinnedTasks: 1, Statuses: []TaskStatus{TaskRunning}}, DrainContinue, false},
		{"running version has no contract", DrainInput{CurrentCompat: "", Candidate: compat("v2"),
			PinnedTasks: 1, Statuses: []TaskStatus{TaskRunning}}, DrainContinue, false},
		{"candidate has no contract", DrainInput{CurrentCompat: "v1", Candidate: compat(""),
			PinnedTasks: 1, Statuses: []TaskStatus{TaskRunning}}, DrainContinue, false},
		{"incompatible, nothing pinned", DrainInput{CurrentCompat: "v1", Candidate: compat("v2"),
			WithinWindow: true}, DrainContinue, false},
		{"incompatible, all terminal", DrainInput{CurrentCompat: "v1", Candidate: compat("v2"),
			PinnedTasks: 2, Statuses: []TaskStatus{TaskSucceeded, TaskCancelled}, WithinWindow: true},
			DrainContinue, false},
		{"incompatible, running task inside window", DrainInput{CurrentCompat: "v1", Candidate: compat("v2"),
			WithinWindow: true, PinnedTasks: 1, Statuses: []TaskStatus{TaskRunning}}, DrainWait, true},
		{"incompatible, paused task inside window", DrainInput{CurrentCompat: "v1", Candidate: compat("v2"),
			WithinWindow: true, PinnedTasks: 1, Statuses: []TaskStatus{TaskPaused}}, DrainWait, true},
		{"incompatible, unknown status inside window", DrainInput{CurrentCompat: "v1", Candidate: compat("v2"),
			WithinWindow: true, PinnedTasks: 1, Statuses: []TaskStatus{TaskUnknown}}, DrainWait, true},
		{"incompatible, missing status inside window", DrainInput{CurrentCompat: "v1", Candidate: compat("v2"),
			WithinWindow: true, PinnedTasks: 1}, DrainWait, true},
		{"incompatible, empty status inside window", DrainInput{CurrentCompat: "v1", Candidate: compat("v2"),
			WithinWindow: true, PinnedTasks: 1, Statuses: []TaskStatus{""}}, DrainWait, true},
		{"incompatible, running task past window", DrainInput{CurrentCompat: "v1", Candidate: compat("v2"),
			WithinWindow: false, PinnedTasks: 1, Statuses: []TaskStatus{TaskRunning}}, DrainFail, false},
		{"incompatible, unknown status past window", DrainInput{CurrentCompat: "v1", Candidate: compat("v2"),
			WithinWindow: false, PinnedTasks: 1, Statuses: []TaskStatus{TaskUnknown}}, DrainFail, false},
		{"incompatible, mixed statuses past window", DrainInput{CurrentCompat: "v1", Candidate: compat("v2"),
			WithinWindow: false, PinnedTasks: 2,
			Statuses: []TaskStatus{TaskSucceeded, TaskQueued}}, DrainFail, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DrainPlan(tc.in)
			if got.Action != tc.want {
				t.Fatalf("action = %q, want %q (%s)", got.Action, tc.want, tc.name)
			}
			if got.Recheck != tc.recheck {
				t.Fatalf("recheck = %v, want %v (%s)", got.Recheck, tc.recheck, tc.name)
			}
			if got.Reason == "" {
				t.Fatal("decision carries no reason")
			}
			if got.Action == DrainWait && !got.Recheck {
				t.Fatal("wait must ask for a recheck")
			}
		})
	}
}

func TestDrainPlanIsPure(t *testing.T) {
	in := DrainInput{CurrentCompat: "v1", Candidate: compatManifest("v2"), WithinWindow: true,
		PinnedTasks: 1, Statuses: []TaskStatus{TaskRunning}}
	before := len(in.Candidate.Components)
	first := DrainPlan(in)
	for i := 0; i < 5; i++ {
		if got := DrainPlan(in); got != first {
			t.Fatalf("drain plan is not deterministic: %+v vs %+v", got, first)
		}
	}
	if len(in.Candidate.Components) != before {
		t.Fatal("drain plan mutated its input")
	}
}

func compatManifest(c string) Manifest {
	m := validManifest()
	m.State.Compat = c
	return m
}

func TestTaskStatusTerminal(t *testing.T) {
	for _, s := range []TaskStatus{TaskSucceeded, TaskFailed, TaskCancelled} {
		if !s.IsTerminal() {
			t.Fatalf("%s must be terminal", s)
		}
	}
	for _, s := range []TaskStatus{TaskQueued, TaskRunning, TaskPaused, TaskUnknown, ""} {
		if s.IsTerminal() {
			t.Fatalf("%s must not be terminal", s)
		}
	}
}

func TestValidateDrainInput(t *testing.T) {
	if err := ValidateDrainInput(DrainInput{PinnedTasks: -1}); err == nil {
		t.Fatal("negative pinned count accepted")
	}
	if err := ValidateDrainInput(DrainInput{CurrentCompat: "v1", Candidate: compatManifest("v2")}); err != nil {
		t.Fatalf("valid drain input rejected: %v", err)
	}
	broken := compatManifest("v2")
	broken.Limits.Depth = 0
	err := ValidateDrainInput(DrainInput{CurrentCompat: "v1", Candidate: broken})
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("invalid candidate accepted, code=%q", contracts.Code(err))
	}
}

func TestRequireGrantedSubset(t *testing.T) {
	ceiling := validManifest().Capabilities
	if err := requireGrantedSubset(ceiling, Capabilities{
		Models: []string{"model/qwen-max"}, SelfTasks: 2,
	}); err != nil {
		t.Fatalf("narrowed grant rejected: %v", err)
	}
	if err := requireGrantedSubset(ceiling, ceiling); err != nil {
		t.Fatalf("exact grant rejected: %v", err)
	}
	wide := []struct {
		name    string
		granted Capabilities
	}{
		{"undeclared model", Capabilities{Models: []string{"model/gpt"}}},
		{"undeclared tool", Capabilities{Tools: []string{"tool/shell"}}},
		{"undeclared memory", Capabilities{Memory: []string{"memory/global"}}},
		{"undeclared compute", Capabilities{Compute: []string{"compute/project"}}},
		{"undeclared call", Capabilities{Calls: []string{"agent/other"}}},
		{"wider self tasks", Capabilities{SelfTasks: ceiling.SelfTasks + 1}},
	}
	for _, tc := range wide {
		if err := requireGrantedSubset(ceiling, tc.granted); err == nil {
			t.Fatalf("%s accepted", tc.name)
		} else if contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("%s code = %q", tc.name, contracts.Code(err))
		}
	}
}

func TestRequireInstallationAllowed(t *testing.T) {
	m := validManifest()
	if err := requireInstallationAllowed(m, "t1"); err != nil {
		t.Fatalf("tenant artifact refused: %v", err)
	}
	m.Visibility = VisibilityCatalog
	if err := requireInstallationAllowed(m, "t2"); err != nil {
		t.Fatalf("class S catalog artifact refused for another tenant: %v", err)
	}
	m.Class = ClassC
	if err := requireInstallationAllowed(m, "t2"); err == nil {
		t.Fatal("class C shared catalog artifact accepted across tenants")
	} else if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("code = %q, want %q", contracts.Code(err), contracts.ErrForbidden)
	}
	sys := validManifest()
	sys.Class = ClassS
	sys.Visibility = VisibilitySystem
	if err := requireInstallationAllowed(sys, "t1"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("system artifact install code = %q", contracts.Code(err))
	}
}

func TestRequireIDAndEnvironment(t *testing.T) {
	for _, bad := range []string{"", "has space", "has/slash", "has\nnewline", strings.Repeat("x", 129)} {
		if err := requireID("field", bad); contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("requireID(%q) code = %q", bad, contracts.Code(err))
		}
	}
	if err := requireID("field", "ok.id:tag@v1_x"); err != nil {
		t.Fatalf("safe identifier rejected: %v", err)
	}
	if err := requireEnvironment("stage"); err != nil {
		t.Fatalf("stage rejected: %v", err)
	}
	if err := requireEnvironment("Prod"); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("uppercase environment accepted")
	}
}

func TestInstallRequestDigestIsStableAndBodySensitive(t *testing.T) {
	c := Capabilities{Models: []string{"model/qwen-max"}, SelfTasks: 1}
	a, err := installRequestDigest("agent-1", "ver-1", "dev", c)
	if err != nil {
		t.Fatal(err)
	}
	b, err := installRequestDigest("agent-1", "ver-1", "dev", c)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("digest is not stable")
	}
	other, err := installRequestDigest("agent-1", "ver-1", "prod", c)
	if err != nil {
		t.Fatal(err)
	}
	if a == other {
		t.Fatal("environment change did not change the request digest")
	}
	widened, err := installRequestDigest("agent-1", "ver-1", "dev",
		Capabilities{Models: []string{"model/qwen-max", "model/gpt"}, SelfTasks: 1})
	if err != nil {
		t.Fatal(err)
	}
	if a == widened {
		t.Fatal("capability change did not change the request digest")
	}
}

func TestParseTaskTimeoutUnits(t *testing.T) {
	d, err := ParseTaskTimeout("1m")
	if err != nil {
		t.Fatal(err)
	}
	if d != time.Minute {
		t.Fatalf("1m parsed as %v", d)
	}
}
