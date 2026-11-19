package buildinfo

import (
	"runtime/debug"
	"strings"
	"testing"
)

func withVars(t *testing.T, rev, when, mod string, f func()) {
	t.Helper()
	or, oc, om := Revision, CommitTime, Modified
	t.Cleanup(func() { Revision, CommitTime, Modified = or, oc, om })
	Revision, CommitTime, Modified = rev, when, mod
	f()
}

// The injected stamp must win over Go's automatic VCS stamp, because the
// automatic one can describe a DIFFERENT repository when this module is built
// inside someone else's checkout. A test cannot fake ReadBuildInfo, but it can
// prove the injected branch is chosen whenever a revision was linked in — which
// is exactly the branch that protects acceptance measurements.
func TestInjectedRevisionWins(t *testing.T) {
	withVars(t, "1111111111111111111111111111111111111111", "2026-11-01T00:00:00Z", "false", func() {
		got := Resolve()
		if got.Source != SourceInjected {
			t.Fatalf("source = %q, want %q (an automatic stamp must never override an injected one)", got.Source, SourceInjected)
		}
		if got.Revision != "1111111111111111111111111111111111111111" {
			t.Fatalf("revision = %q, want the injected value", got.Revision)
		}
		if got.Modified != "false" || got.CommitTime == "" {
			t.Fatalf("commit fields not carried through: %+v", got)
		}
	})
}

func TestWhitespaceOnlyInjectionIsNotAnIdentity(t *testing.T) {
	withVars(t, "   ", "", "", func() {
		got := Resolve()
		if got.Source == SourceInjected {
			t.Fatal("blank injected revision must not count as an injected identity")
		}
	})
}

// Whatever the environment, the answer must never claim a revision it does not
// have, and must always name its source.
func TestStringNeverClaimsAnEmptyRevision(t *testing.T) {
	withVars(t, "", "", "", func() {
		info := Resolve()
		s := info.String()
		if strings.Contains(s, "revision= ") || strings.HasSuffix(s, "revision=") {
			t.Fatalf("empty revision presented as identity: %q", s)
		}
		if info.Source == SourceNone && !strings.Contains(s, "revision=unknown") {
			t.Fatalf("missing identity must say unknown, got %q", s)
		}
		if info.Source == SourceVCS && !strings.Contains(s, "identity_source=buildinfo") {
			t.Fatalf("automatic stamp must be labelled as such: %q", s)
		}
	})
}

// The attributes land in every startup log line; they must be key/value pairs.
func TestLogAttrsAreKeyValuePaired(t *testing.T) {
	withVars(t, "abc", "when", "true", func() {
		attrs := Resolve().LogAttrs()
		if len(attrs)%2 != 0 {
			t.Fatalf("slog attributes must be pairs, got %d", len(attrs))
		}
		if attrs[1] != "abc" {
			t.Fatalf("revision attribute misplaced: %v", attrs)
		}
	})
}

// Pins the hazard this package exists for: when built inside another
// repository, Go's stamp describes that repository. Nothing here can fix the
// stamp, so the test documents the consequence for anyone who reaches for it.
func TestAutomaticStampIsNotProofOfProductRevision(t *testing.T) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		t.Skip("no build info in this build mode")
	}
	var stamped bool
	for _, s := range bi.Settings {
		if s.Key == "vcs.revision" {
			stamped = true
		}
	}
	if !stamped {
		return // built without VCS stamping (e.g. tests from a bundle): nothing to pin
	}
	t.Log("automatic vcs.revision is present; it proves the repository owning THIS working tree, which is not necessarily this module's repository")
}
