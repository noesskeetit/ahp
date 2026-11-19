// Package buildinfo identifies which source revision a running binary was built
// from, and — just as importantly — says how that identity was obtained.
//
// The reason this exists at all: Go stamps vcs.revision automatically, but it
// resolves the repository that OWNS the working tree. A deployment that builds
// this module inside another checkout (a monorepo, a control repository, a
// vendor tree under a different .git) therefore gets stamped with the OUTER
// repository's commit — silently, and with a plausible-looking hash. Every
// acceptance claim of the form "this binary runs revision X" then rests on a
// number that measures a different repository. That is not hypothetical: it was
// measured, and the automatic stamp named a control-repository commit while the
// product worktree was three commits past it.
//
// So: an explicitly injected stamp wins, the automatic stamp is only a fallback,
// and the SOURCE of the answer is always reported. Unknown is a legitimate
// answer; a wrong answer presented as a measurement is not.
package buildinfo

import (
	"runtime/debug"
	"strings"
)

// Set these at link time; they are the only trustworthy identity when the build
// happens inside a repository that is not this module's own:
//
//	-ldflags "-X github.com/ahp-platform/ahp/internal/buildinfo.Revision=$(git rev-parse HEAD)
//	          -X github.com/ahp-platform/ahp/internal/buildinfo.CommitTime=$(git log -1 --format=%cI)
//	          -X github.com/ahp-platform/ahp/internal/buildinfo.Modified=$(test -n "$(git status --porcelain)" && echo true || echo false)"
var (
	Revision   string
	CommitTime string
	Modified   string
)

// Source tells a reader where the identity came from, so a fallback is never
// mistaken for an injected measurement.
type Source string

const (
	// SourceInjected: values were injected at link time — authoritative.
	SourceInjected Source = "ldflags"
	// SourceVCS: taken from Go's automatic VCS stamp. Trustworthy only when the
	// module was built inside its own repository; otherwise it can name a
	// different repository entirely.
	SourceVCS Source = "buildinfo"
	// SourceNone: no identity available.
	SourceNone Source = "none"
)

// Info is the resolved build identity of this process.
type Info struct {
	Revision   string
	CommitTime string
	Modified   string
	Source     Source
}

// Resolve returns the build identity, preferring the injected stamp over the
// automatic VCS stamp for the reason documented on the package.
func Resolve() Info {
	if strings.TrimSpace(Revision) != "" {
		return Info{
			Revision:   strings.TrimSpace(Revision),
			CommitTime: strings.TrimSpace(CommitTime),
			Modified:   strings.TrimSpace(Modified),
			Source:     SourceInjected,
		}
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return Info{Source: SourceNone}
	}
	var rev, when, mod string
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.time":
			when = s.Value
		case "vcs.modified":
			mod = s.Value
		}
	}
	if rev == "" {
		return Info{Source: SourceNone}
	}
	return Info{Revision: rev, CommitTime: when, Modified: mod, Source: SourceVCS}
}

// LogAttrs renders the identity for structured logging. The source is logged on
// purpose: a reader must be able to tell a measurement from a guess.
func (i Info) LogAttrs() []any {
	return []any{
		"build_revision", i.RevisionOrUnknown(),
		"build_commit_time", i.CommitTime,
		"build_modified", i.Modified,
		"build_identity_source", string(i.Source),
	}
}

// RevisionOrUnknown keeps "unknown" spelled the same everywhere instead of
// letting an empty string mean three different things.
func (i Info) RevisionOrUnknown() string {
	if i.Revision == "" {
		return "unknown"
	}
	return i.Revision
}

// String is the one-line form used by -version.
func (i Info) String() string {
	if i.Source == SourceNone {
		return "revision=unknown identity_source=none"
	}
	return "revision=" + i.Revision + " commit_time=" + i.CommitTime + " modified=" + i.Modified + " identity_source=" + string(i.Source)
}
