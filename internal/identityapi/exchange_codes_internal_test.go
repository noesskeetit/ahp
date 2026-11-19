package identityapi

import (
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The depth/children_quota/capability/expiry denials are produced by the
// identity Store deep inside the tree (the exchange path only surfaces them
// through exchangeStoreError). They are impractical to force through the HTTP
// surface without building a whole deep tree, so the classification contract is
// locked here: every FORBIDDEN phrase the identity package emits for a tree,
// quota, capability or expiry denial maps to exactly one stable exchange reason
// code. A revoked-subject denial and any unknown error map to NOT-ok, so the
// handler keeps them indistinguishable and fail-closed.

func TestClassifyExchangeStoreError(t *testing.T) {
	forbidden := func(msg string) error { return contracts.NewError(contracts.ErrForbidden, msg) }
	cases := []struct {
		name string
		err  error
		want string
		ok   bool
	}{
		{"depth_ceiling", forbidden("identity: delegation exceeds the parent depth ceiling"), ReasonDepthExceeded, true},
		{"depth_narrow", forbidden("identity: child max_depth may only narrow the parent ceiling"), ReasonDepthExceeded, true},
		{"fanout", forbidden("identity: the parent fan-out limit is exhausted"), ReasonChildrenQuota, true},
		{"active_subtree", forbidden("identity: the root active-subtree limit is exhausted"), ReasonChildrenQuota, true},
		{"ancestor_active", forbidden("identity: an ancestor active-subtree limit is exhausted"), ReasonChildrenQuota, true},
		{"fanout_narrow", forbidden("identity: child max_fanout may only narrow the parent ceiling"), ReasonChildrenQuota, true},
		{"active_narrow", forbidden("identity: child max_active may only narrow the parent ceiling"), ReasonChildrenQuota, true},
		{"cap_widen", forbidden("identity: delegation may only narrow the parent capability set"), ReasonCapabilityWidening, true},
		{"cap_hold", forbidden("identity: grant does not hold capability space:write"), ReasonCapabilityWidening, true},
		{"expired", forbidden("identity: parent grant has expired"), ReasonGrantExpired, true},
		{"root_inactive", forbidden("identity: root grant is not active"), ReasonGrantExpired, true},
		// A revoked subject stays opaque: no distinct wire code.
		{"revoked_is_opaque", forbidden("identity: subject is revoked and may not delegate"), "", false},
		// Non-FORBIDDEN codes are never re-classified.
		{"not_found_not_mapped", contracts.NewError(contracts.ErrNotFound, "identity: grant not found"), "", false},
		{"key_reuse_not_mapped", contracts.NewError(contracts.ErrKeyReuse, "identity: task id reused"), "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := classifyExchangeStoreError(tc.err)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v (err=%v)", ok, tc.ok, tc.err)
			}
			if got != tc.want {
				t.Fatalf("reason = %q, want %q", got, tc.want)
			}
		})
	}
}
