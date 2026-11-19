package labels

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func codeOf(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		return ""
	}
	var e *contracts.Error
	if !errors.As(err, &e) {
		t.Fatalf("error %v is not a *contracts.Error", err)
	}
	return e.Code
}

func TestMergeSortedUniqueNoMutation(t *testing.T) {
	a := Set{Subjects: []string{"bob", "alice"}, Workspaces: []string{"w2"}, Taints: []string{"external"}}
	b := Set{Subjects: []string{"alice", "carol"}, Workspaces: []string{"w1", "w2"}, Taints: []string{"secret", "external"}}

	got := Merge(a, b)
	want := Set{
		Subjects:   []string{"alice", "bob", "carol"},
		Workspaces: []string{"w1", "w2"},
		Taints:     []string{"external", "secret"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Merge = %+v, want %+v", got, want)
	}

	// Originals must be untouched.
	if !reflect.DeepEqual(a, Set{Subjects: []string{"bob", "alice"}, Workspaces: []string{"w2"}, Taints: []string{"external"}}) {
		t.Fatalf("Merge mutated a: %+v", a)
	}
	if !reflect.DeepEqual(b, Set{Subjects: []string{"alice", "carol"}, Workspaces: []string{"w1", "w2"}, Taints: []string{"secret", "external"}}) {
		t.Fatalf("Merge mutated b: %+v", b)
	}

	// Result must not alias inputs.
	got.Subjects[0] = "zzz"
	if a.Subjects[1] == "zzz" || b.Subjects[0] == "zzz" {
		t.Fatal("Merge result aliases input slices")
	}
}

func TestMergeEmpty(t *testing.T) {
	if got := Merge(); !reflect.DeepEqual(got, Set{}) {
		t.Fatalf("Merge() = %+v, want zero Set", got)
	}
	if got := Merge(Set{}, Set{}); !reflect.DeepEqual(got, Set{}) {
		t.Fatalf("Merge empties = %+v, want zero Set", got)
	}
}

func TestCheckWritePrivateSubject(t *testing.T) {
	s := Set{Subjects: []string{"alice"}}
	if err := CheckWrite(s, Audience{Kind: KindPrivate, Subject: "alice"}); err != nil {
		t.Fatalf("same sole subject denied: %v", err)
	}
	err := CheckWrite(s, Audience{Kind: KindPrivate, Subject: "bob"})
	if codeOf(t, err) != contracts.ErrForbidden {
		t.Fatalf("other subject allowed, err = %v", err)
	}
}

func TestCheckWritePrivateToWorkspaceDenied(t *testing.T) {
	s := Set{Subjects: []string{"alice"}}
	err := CheckWrite(s, Audience{Kind: KindWorkspace, Workspace: "w1"})
	if codeOf(t, err) != contracts.ErrForbidden {
		t.Fatalf("private-restricted data written to workspace sink, err = %v", err)
	}
}

func TestCheckWriteMultipleSubjectsFailAllSinks(t *testing.T) {
	s := Set{Subjects: []string{"alice", "bob"}}
	sinks := []Audience{
		{Kind: KindPrivate, Subject: "alice"},
		{Kind: KindPrivate, Subject: "bob"},
		{Kind: KindWorkspace, Workspace: "w1"},
		{Kind: KindTenant},
		{Kind: KindExternal},
	}
	for _, a := range sinks {
		if codeOf(t, CheckWrite(s, a)) != contracts.ErrForbidden {
			t.Fatalf("multi-subject set allowed sink %+v", a)
		}
	}
}

func TestCheckWriteWorkspaceRestrictions(t *testing.T) {
	s := Set{Workspaces: []string{"w1"}}
	if err := CheckWrite(s, Audience{Kind: KindWorkspace, Workspace: "w1"}); err != nil {
		t.Fatalf("sole matching workspace denied: %v", err)
	}
	if err := CheckWrite(s, Audience{Kind: KindPrivate, Subject: "alice"}); err != nil {
		t.Fatalf("private sink under workspace restriction denied (IAM checked separately): %v", err)
	}
	for _, a := range []Audience{
		{Kind: KindWorkspace, Workspace: "w2"},
		{Kind: KindTenant},
		{Kind: KindExternal},
		{Kind: KindExternal, AllowExternal: true},
	} {
		if codeOf(t, CheckWrite(s, a)) != contracts.ErrForbidden {
			t.Fatalf("workspace-restricted data allowed sink %+v", a)
		}
	}
}

func TestCheckWriteMultipleWorkspacesFailWorkspaceSinks(t *testing.T) {
	s := Set{Workspaces: []string{"w1", "w2"}}
	if err := CheckWrite(s, Audience{Kind: KindPrivate, Subject: "alice"}); err != nil {
		t.Fatalf("private sink under multi-workspace restriction denied: %v", err)
	}
	for _, a := range []Audience{
		{Kind: KindWorkspace, Workspace: "w1"},
		{Kind: KindWorkspace, Workspace: "w2"},
		{Kind: KindTenant},
		{Kind: KindExternal},
	} {
		if codeOf(t, CheckWrite(s, a)) != contracts.ErrForbidden {
			t.Fatalf("multi-workspace set allowed sink %+v", a)
		}
	}
}

func TestCheckWriteUnrestricted(t *testing.T) {
	sinks := []Audience{
		{Kind: KindPrivate, Subject: "alice"},
		{Kind: KindWorkspace, Workspace: "w1"},
		{Kind: KindTenant},
		{Kind: KindExternal},
	}
	for _, a := range sinks {
		if err := CheckWrite(Set{}, a); err != nil {
			t.Fatalf("unrestricted set denied sink %+v: %v", a, err)
		}
	}
}

func TestCheckWriteExternalTaint(t *testing.T) {
	for _, taint := range []string{TaintExternal, TaintUntrustedExternal} {
		s := Set{Taints: []string{taint}}
		// Trusted sinks (all kinds except external) require the trusted
		// AllowExternal flag.
		for _, a := range []Audience{
			{Kind: KindPrivate, Subject: "alice"},
			{Kind: KindWorkspace, Workspace: "w1"},
			{Kind: KindTenant},
		} {
			if codeOf(t, CheckWrite(s, a)) != contracts.ErrForbidden {
				t.Fatalf("%s taint auto-published to trusted sink %+v", taint, a)
			}
			if err := CheckWrite(s, Audience{Kind: a.Kind, Subject: a.Subject, Workspace: a.Workspace, AllowExternal: true}); err != nil {
				t.Fatalf("%s taint with AllowExternal denied trusted sink %+v: %v", taint, a, err)
			}
		}
		// External sink is an explicit external destination.
		if err := CheckWrite(s, Audience{Kind: KindExternal}); err != nil {
			t.Fatalf("%s taint denied external sink: %v", taint, err)
		}
	}
}

func TestCheckWriteAllowExternalNeverOverridesVisibility(t *testing.T) {
	cases := []struct {
		s Set
		a Audience
	}{
		{Set{Subjects: []string{"alice"}}, Audience{Kind: KindWorkspace, Workspace: "w1", AllowExternal: true}},
		{Set{Subjects: []string{"alice"}}, Audience{Kind: KindTenant, AllowExternal: true}},
		{Set{Subjects: []string{"alice"}}, Audience{Kind: KindPrivate, Subject: "bob", AllowExternal: true}},
		{Set{Workspaces: []string{"w1"}}, Audience{Kind: KindTenant, AllowExternal: true}},
		{Set{Workspaces: []string{"w1"}}, Audience{Kind: KindWorkspace, Workspace: "w2", AllowExternal: true}},
	}
	for _, c := range cases {
		if codeOf(t, CheckWrite(c.s, c.a)) != contracts.ErrForbidden {
			t.Fatalf("AllowExternal overrode visibility: set %+v sink %+v", c.s, c.a)
		}
	}
}

func TestCheckWriteSecretTaintDeniesAll(t *testing.T) {
	s := Set{Taints: []string{TaintSecret}}
	sinks := []Audience{
		{Kind: KindPrivate, Subject: "alice", AllowExternal: true},
		{Kind: KindWorkspace, Workspace: "w1", AllowExternal: true},
		{Kind: KindTenant, AllowExternal: true},
		{Kind: KindExternal, AllowExternal: true},
	}
	for _, a := range sinks {
		if codeOf(t, CheckWrite(s, a)) != contracts.ErrForbidden {
			t.Fatalf("secret taint exported to %+v", a)
		}
	}
}

func TestCheckWriteFailsClosedOnMalformedLabels(t *testing.T) {
	bad := []Set{
		{Subjects: []string{""}},
		{Subjects: []string{"  "}},
		{Subjects: []string{"-alice"}},
		{Subjects: []string{"a b"}},
		{Workspaces: []string{""}},
		{Workspaces: []string{"w/1"}},
		{Subjects: []string{"alice"}, Workspaces: []string{""}},
	}
	for _, s := range bad {
		err := CheckWrite(s, Audience{Kind: KindTenant})
		if codeOf(t, err) != contracts.ErrInvalid {
			t.Fatalf("malformed set %+v: err = %v, want INVALID_ARGUMENT", s, err)
		}
	}
}

func TestCheckWriteUnknownTaintFailsClosed(t *testing.T) {
	s := Set{Taints: []string{"definitely_not_a_taint"}}
	err := CheckWrite(s, Audience{Kind: KindTenant})
	if codeOf(t, err) != contracts.ErrInvalid {
		t.Fatalf("unknown taint err = %v, want INVALID_ARGUMENT", err)
	}
	// Even with a matching subject the unknown taint must fail.
	s2 := Set{Subjects: []string{"alice"}, Taints: []string{"maybe"}}
	if codeOf(t, CheckWrite(s2, Audience{Kind: KindPrivate, Subject: "alice"})) != contracts.ErrInvalid {
		t.Fatal("unknown taint did not fail closed for private sink")
	}
}

func TestCheckWriteMalformedAudience(t *testing.T) {
	cases := []struct {
		a Audience
	}{
		{Audience{Kind: ""}},
		{Audience{Kind: "everyone"}},
		{Audience{Kind: KindPrivate}},
		{Audience{Kind: KindPrivate, Subject: " "}},
		{Audience{Kind: KindWorkspace}},
		{Audience{Kind: KindWorkspace, Workspace: ""}},
	}
	for _, c := range cases {
		err := CheckWrite(Set{}, c.a)
		if codeOf(t, err) != contracts.ErrInvalid {
			t.Fatalf("audience %+v: err = %v, want INVALID_ARGUMENT", c.a, err)
		}
	}
}

func TestCheckWriteCombinedRestrictions(t *testing.T) {
	// Subject restriction wins: workspace visibility cannot widen it.
	s := Set{Subjects: []string{"alice"}, Workspaces: []string{"w1"}}
	if err := CheckWrite(s, Audience{Kind: KindPrivate, Subject: "alice"}); err != nil {
		t.Fatalf("sole subject private sink denied: %v", err)
	}
	if codeOf(t, CheckWrite(s, Audience{Kind: KindWorkspace, Workspace: "w1"})) != contracts.ErrForbidden {
		t.Fatal("subject+workspace set leaked to workspace sink")
	}

	// Workspace restriction plus external taint: visibility AND taint are
	// enforced independently. The old implementation let the workspace
	// branch return early and bypass the taint gate; that expectation was
	// incorrect. Trusted publication now requires AllowExternal even when
	// the workspace matches.
	s2 := Set{Workspaces: []string{"w1"}, Taints: []string{TaintExternal}}
	if codeOf(t, CheckWrite(s2, Audience{Kind: KindWorkspace, Workspace: "w1"})) != contracts.ErrForbidden {
		t.Fatal("external-tainted workspace-restricted data auto-published without AllowExternal")
	}
	if err := CheckWrite(s2, Audience{Kind: KindWorkspace, Workspace: "w1", AllowExternal: true}); err != nil {
		t.Fatalf("matching workspace with AllowExternal denied: %v", err)
	}
	// AllowExternal never overrides the workspace visibility rule.
	if codeOf(t, CheckWrite(s2, Audience{Kind: KindWorkspace, Workspace: "w2", AllowExternal: true})) != contracts.ErrForbidden {
		t.Fatal("AllowExternal overrode workspace visibility")
	}
	// External sink is an explicit destination: taint gate does not apply,
	// but the workspace restriction still denies.
	if codeOf(t, CheckWrite(s2, Audience{Kind: KindExternal})) != contracts.ErrForbidden {
		t.Fatal("workspace-restricted data leaked to external sink")
	}

	// Subject restriction plus external taint: private delivery is a
	// trusted memory publication and is gated by the taint as well;
	// callers doing intentional direct display set AllowExternal after
	// trusted policy.
	s3 := Set{Subjects: []string{"alice"}, Taints: []string{TaintUntrustedExternal}}
	if codeOf(t, CheckWrite(s3, Audience{Kind: KindPrivate, Subject: "alice"})) != contracts.ErrForbidden {
		t.Fatal("external-tainted subject-restricted data auto-published to private memory")
	}
	if err := CheckWrite(s3, Audience{Kind: KindPrivate, Subject: "alice", AllowExternal: true}); err != nil {
		t.Fatalf("intentional direct display with AllowExternal denied: %v", err)
	}
	// Taint clearance must not widen subject visibility.
	if codeOf(t, CheckWrite(s3, Audience{Kind: KindPrivate, Subject: "bob", AllowExternal: true})) != contracts.ErrForbidden {
		t.Fatal("AllowExternal overrode subject visibility")
	}
}

func TestCheckWriteRestrictedWorkspaceTaintRegression(t *testing.T) {
	// Exact controller repro.
	s := Set{Workspaces: []string{"w"}, Taints: []string{TaintUntrustedExternal}}
	err := CheckWrite(s, Audience{Kind: KindWorkspace, Workspace: "w", AllowExternal: false})
	if codeOf(t, err) != contracts.ErrForbidden {
		t.Fatalf("taint gate bypassed by workspace branch: err = %v", err)
	}
	// Private sinks are also gated by the taint now; AllowExternal (set
	// only by trusted policy) permits the direct hand-off.
	if codeOf(t, CheckWrite(s, Audience{Kind: KindPrivate, Subject: "alice"})) != contracts.ErrForbidden {
		t.Fatal("untrusted_external data auto-published to private sink")
	}
	if err := CheckWrite(s, Audience{Kind: KindPrivate, Subject: "alice", AllowExternal: true}); err != nil {
		t.Fatalf("private sink with AllowExternal denied: %v", err)
	}
}

func TestCheckWriteDuplicateSubjectsNotDistinct(t *testing.T) {
	// Duplicates are normalized after validation: ["u","u"] behaves like
	// a single subject and must not count as multiple distinct subjects.
	dup := Set{Subjects: []string{"u", "u"}}
	single := Set{Subjects: []string{"u"}}
	sinks := []Audience{
		{Kind: KindPrivate, Subject: "u"},
		{Kind: KindPrivate, Subject: "x"},
		{Kind: KindWorkspace, Workspace: "w"},
		{Kind: KindTenant},
		{Kind: KindExternal},
	}
	for _, a := range sinks {
		if got, want := codeOf(t, CheckWrite(dup, a)), codeOf(t, CheckWrite(single, a)); got != want {
			t.Fatalf("dup set %+v vs single %+v at sink %+v: got %q want %q", dup, single, a, got, want)
		}
	}
	if err := CheckWrite(dup, Audience{Kind: KindPrivate, Subject: "u"}); err != nil {
		t.Fatalf("duplicate-subject set denied its own subject: %v", err)
	}

	// Same normalization for workspaces.
	dupWS := Set{Workspaces: []string{"w", "w", "w"}}
	if err := CheckWrite(dupWS, Audience{Kind: KindWorkspace, Workspace: "w"}); err != nil {
		t.Fatalf("duplicate-workspace set denied its own workspace: %v", err)
	}
	if codeOf(t, CheckWrite(dupWS, Audience{Kind: KindTenant})) != contracts.ErrForbidden {
		t.Fatal("duplicate-workspace set leaked to tenant")
	}
}

func TestErrorMessagesCarryNoRawSecrets(t *testing.T) {
	err := CheckWrite(Set{Taints: []string{TaintSecret}}, Audience{Kind: KindTenant})
	if err == nil || !strings.Contains(err.Error(), contracts.ErrForbidden) {
		t.Fatalf("unexpected error: %v", err)
	}
}
