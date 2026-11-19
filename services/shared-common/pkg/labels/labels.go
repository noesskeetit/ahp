// Package labels implements visibility labels and sink write policy.
//
// A Set describes the visibility restrictions attached to a source of data:
// which subjects may see it, which workspaces may see it, and which taints
// apply. CheckWrite is the sink-side policy gate that decides whether data
// carrying a Set may be written to an Audience. IAM membership and source
// access are enforced separately; this package only evaluates labels.
package labels

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Audience kinds.
const (
	KindPrivate   = "private"
	KindWorkspace = "workspace"
	KindTenant    = "tenant"
	KindExternal  = "external"
)

// Supported taints. Any other taint fails closed.
const (
	TaintExternal          = "external"
	TaintUntrustedExternal = "untrusted_external"
	TaintSecret            = "secret"
)

// idPattern accepts well-formed subject/workspace identifiers.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}$`)

// Set is a collection of visibility labels on a data source.
type Set struct {
	Subjects   []string `json:"subjects"`
	Workspaces []string `json:"workspaces"`
	Taints     []string `json:"taints"`
}

// Audience identifies a write destination.
type Audience struct {
	Kind          string `json:"kind"`
	Subject       string `json:"subject"`
	Workspace     string `json:"workspace"`
	AllowExternal bool   `json:"allow_external"`
}

// Merge returns the sorted, de-duplicated union of sets. It never mutates
// its inputs. Nil slices merge as empty.
func Merge(sets ...Set) Set {
	var subj, ws, taint []string
	for _, s := range sets {
		subj = append(subj, s.Subjects...)
		ws = append(ws, s.Workspaces...)
		taint = append(taint, s.Taints...)
	}
	return Set{
		Subjects:   sortedUnique(subj),
		Workspaces: sortedUnique(ws),
		Taints:     sortedUnique(taint),
	}
}

func sortedUnique(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// CheckWrite reports whether data restricted by s may be written to a.
// It fails closed on malformed labels, unknown taints and unknown kinds.
// Visibility and taint rules are evaluated independently: both must pass.
// a.AllowExternal must only be set by trusted policy (e.g. intentional
// direct display) and never relaxes subject or workspace visibility.
func CheckWrite(s Set, a Audience) error {
	if err := validateSet(s); err != nil {
		return err
	}
	if err := validateAudience(a); err != nil {
		return err
	}

	// Secret taint always denies export, regardless of audience.
	if contains(s.Taints, TaintSecret) {
		return forbidden("secret-tainted data may not be exported")
	}

	// Taint gate, evaluated independently of visibility: external or
	// untrusted_external taints prohibit automatic trusted publication to
	// every sink kind except an explicit external audience, unless trusted
	// policy passes AllowExternal. AllowExternal never relaxes the
	// visibility checks below.
	if a.Kind != KindExternal && hasExternalTaint(s.Taints) && !a.AllowExternal {
		return forbidden("external-tainted data requires AllowExternal to publish to a trusted sink")
	}

	// Normalize duplicates after validation so ["u","u"] is treated as a
	// single subject, not multiple distinct subjects.
	subjects := distinct(s.Subjects)
	workspaces := distinct(s.Workspaces)

	// Subject restrictions: only a private audience matching the sole
	// subject may receive the data. Multiple distinct subjects fail for
	// every sink.
	if len(subjects) > 0 {
		if len(subjects) > 1 {
			return forbidden("data restricted to multiple subjects may not be written to any sink")
		}
		if a.Kind != KindPrivate || a.Subject != subjects[0] {
			return forbidden("subject restriction does not match audience")
		}
		return nil
	}

	// Workspace restrictions: private sinks are allowed (IAM verifies the
	// caller's source access separately); otherwise the audience must be a
	// workspace sink matching the sole workspace. Tenant and external are
	// never allowed.
	if len(workspaces) > 0 {
		if a.Kind == KindPrivate {
			return nil
		}
		if len(workspaces) > 1 {
			return forbidden("data restricted to multiple workspaces may not be written to a workspace sink")
		}
		if a.Kind != KindWorkspace || a.Workspace != workspaces[0] {
			return forbidden("workspace restriction does not match audience")
		}
		return nil
	}

	// No subject/workspace restrictions: any valid audience may receive
	// the data (subject to the taint gate above).
	return nil
}

// distinct returns v with duplicates removed, preserving order.
func distinct(v []string) []string {
	seen := make(map[string]struct{}, len(v))
	out := make([]string, 0, len(v))
	for _, x := range v {
		if _, dup := seen[x]; dup {
			continue
		}
		seen[x] = struct{}{}
		out = append(out, x)
	}
	return out
}

func hasExternalTaint(taints []string) bool {
	return contains(taints, TaintExternal) || contains(taints, TaintUntrustedExternal)
}

func contains(vals []string, v string) bool {
	for _, x := range vals {
		if x == v {
			return true
		}
	}
	return false
}

func validateSet(s Set) error {
	for _, v := range s.Subjects {
		if !idPattern.MatchString(v) {
			return invalid(fmt.Sprintf("invalid subject label %q", v))
		}
	}
	for _, v := range s.Workspaces {
		if !idPattern.MatchString(v) {
			return invalid(fmt.Sprintf("invalid workspace label %q", v))
		}
	}
	for _, t := range s.Taints {
		switch t {
		case TaintExternal, TaintUntrustedExternal, TaintSecret:
		default:
			return invalid(fmt.Sprintf("unknown taint %q", t))
		}
	}
	return nil
}

func validateAudience(a Audience) error {
	switch a.Kind {
	case KindPrivate:
		if !idPattern.MatchString(a.Subject) {
			return invalid("private audience requires a valid subject")
		}
	case KindWorkspace:
		if !idPattern.MatchString(a.Workspace) {
			return invalid("workspace audience requires a valid workspace")
		}
	case KindTenant, KindExternal:
	default:
		return invalid(fmt.Sprintf("unknown audience kind %q", a.Kind))
	}
	return nil
}

func invalid(msg string) error { return contracts.NewError(contracts.ErrInvalid, msg) }

func forbidden(msg string) error {
	return contracts.NewError(contracts.ErrForbidden, msg)
}
