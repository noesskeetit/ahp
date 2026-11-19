//go:build ahp_data

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// fakeGrants is the Identity seam the ceiling reads through: it reports the
// labels of the live grant and records what was asked.
type fakeGrants struct {
	set    labels.Set
	err    error
	asked  []string
	secret int
}

func (f *fakeGrants) GrantLabelSet(_ context.Context, _ contracts.Scope, task, capability string) (labels.Set, error) {
	f.secret++
	f.asked = append(f.asked, capability)
	if f.err != nil {
		return labels.Set{}, f.err
	}
	return f.set, nil
}

// TestMemoryCeilingFromGrants pins DEF-MEM-1's fix: the ceiling comes from the
// live grant journal, and every way of NOT knowing it refuses the write instead
// of quietly treating the caller as unrestricted.
func TestMemoryCeilingFromGrants(t *testing.T) {
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	ctx := transport.WithPrincipal(context.Background(), transport.Principal{Scope: scope})

	t.Run("no_principal_fails_closed", func(t *testing.T) {
		g := &fakeGrants{set: labels.Set{Subjects: []string{"s1"}}}
		if _, err := memoryCeilingFromGrants(g)(context.Background(), scope); err == nil {
			t.Fatal("a ceiling was answered for an unverified caller")
		}
		if g.secret != 0 {
			t.Fatal("identity consulted without a verified principal")
		}
	})

	t.Run("foreign_scope_fails_closed", func(t *testing.T) {
		other := contracts.Scope{Tenant: "t2", Deployment: "d1", Subject: "s1"}
		g := &fakeGrants{set: labels.Set{Subjects: []string{"s1"}}}
		if _, err := memoryCeilingFromGrants(g)(ctx, other); err == nil {
			t.Fatal("a ceiling was answered for a scope that is not the caller's")
		}
	})

	t.Run("journal_read_failure_fails_closed", func(t *testing.T) {
		g := &fakeGrants{err: errors.New("identity unavailable")}
		_, err := memoryCeilingFromGrants(g)(ctx, scope)
		if err == nil {
			t.Fatal("an unreadable journal was treated as no restriction")
		}
		if len(g.asked) != 1 || g.asked[0] != memoryPlaneCapability {
			t.Fatalf("identity asked about %v, want the plane capability", g.asked)
		}
	})

	t.Run("journal_labels_are_returned_verbatim", func(t *testing.T) {
		want := labels.Set{Subjects: []string{"s1"}, Taints: []string{labels.TaintSecret}}
		g := &fakeGrants{set: want}
		got, err := memoryCeilingFromGrants(g)(ctx, scope)
		if err != nil {
			t.Fatalf("ceiling: %v", err)
		}
		if len(got.Subjects) != 1 || got.Subjects[0] != "s1" || len(got.Taints) != 1 {
			t.Fatalf("ceiling = %+v, want the journal's set verbatim", got)
		}
	})

	t.Run("nil_identity_store_fails_closed", func(t *testing.T) {
		got, err := identityGrantLabels{}.GrantLabelSet(context.Background(), scope, "task-1", memoryPlaneCapability)
		if err == nil {
			t.Fatalf("nil identity store answered a ceiling: %+v", got)
		}
	})
}
