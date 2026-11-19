package artifacts

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestReadScopeGate is the whole rule in one table: the conversation a token is
// bound to decides which artifacts it reads, and the two values that cannot be
// compared (an unbound row, a caller with no context claim) are left alone
// rather than denied - the gate exists to bind new writes, not to strand data
// that predates it.
func TestReadScopeGate(t *testing.T) {
	cases := []struct {
		name       string
		callerCtx  string
		artifact   string
		wantDenied bool
	}{
		{"same context reads", "ctx-1", "ctx-1", false},
		{"other context denied", "ctx-1", "ctx-2", true},
		{"row bound before tokens had no context", "ctx-1", "", false},
		{"caller without a context claim", "", "ctx-1", false},
		{"neither side bound", "", "", false},
	}
	s := &Store{}
	for _, tc := range cases {
		err := s.checkReadScope(WithCallerContext(context.Background(), tc.callerCtx),
			ArtifactRef{ID: "a-1", ContextID: tc.artifact})
		if tc.wantDenied {
			if contracts.Code(err) != contracts.ErrForbidden {
				t.Errorf("%s: err = %v, want FORBIDDEN", tc.name, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: err = %v, want nil", tc.name, err)
		}
	}
}

// TestCallerContextRoundTrip pins the carrier the API layer installs the
// verified claim into.
func TestCallerContextRoundTrip(t *testing.T) {
	ctx := WithCallerContext(context.Background(), "ctx-9")
	if got := CallerContext(ctx); got != "ctx-9" {
		t.Fatalf("CallerContext = %q, want ctx-9", got)
	}
	if got := CallerContext(context.Background()); got != "" {
		t.Fatalf("CallerContext on a bare ctx = %q, want empty", got)
	}
	var wrong context.Context = context.WithValue(context.Background(), callerContextKey{}, 7)
	if got := CallerContext(wrong); got != "" {
		t.Fatalf("CallerContext with a foreign value = %q, want empty", got)
	}
}
