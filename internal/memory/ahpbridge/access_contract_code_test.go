package ahpbridge

// The authority bridge translates AHP's canonical verdicts into this face's error
// envelope, so its mapping is wire-visible, not internal: Service.List calls
// Access.Bind (internal/memory/longterm/list.go:65) and GET /v1/memory/items is
// served by internal/memory/longterm/httpapi (cmd/ahp/mount_hindsight.go,
// ahpbridge/handler.go). DEF-MEM-HTTP-CODE-1 review C1 showed the 404 branch used a
// lowercase literal; this test pins the executed mapping so the family rule
// survives refactors, and pins the still-admitted plane codes (DEF-MEM-HTTP-CODE-2)
// so a silent vocabulary change is noticed rather than discovered by a caller.

import (
	"errors"
	"testing"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestAuthorityErrorKeepsTheCanonicalFourOFour(t *testing.T) {
	got := authorityError(contracts.NewError(contracts.ErrNotFound, "unknown source"))
	// respond() reads the typed plane error's own Code (server.go), so the bar is on
	// that field: it must be the canonical contract code, not a face-local literal.
	var typed *memory.Error
	if !errors.As(got, &typed) {
		t.Fatalf("expected a typed memory error, got %#v", got)
	}
	if typed.Code != contracts.ErrNotFound || typed.Status != 404 {
		t.Fatalf("a missing AHP source must reach the envelope as %q/404, got %q/%d",
			contracts.ErrNotFound, typed.Code, typed.Status)
	}
}

// TestAuthorityErrorAdmittedPlaneCodesShrinkOnly locks the three families that are
// still plane-specific by decision, with their status. Each entry is a member of
// the pinned debt list in httpapi/contract_codes_test.go: promoting one to
// pkg/contracts means deleting it here AND there, in the same change.
func TestAuthorityErrorAdmittedPlaneCodesShrinkOnly(t *testing.T) {
	cases := []struct {
		name     string
		in       error
		wantCode string
		wantHTTP int
	}{
		{"forbidden", contracts.NewError(contracts.ErrForbidden, "no"), "access_denied", 403},
		{"invalid source", contracts.NewError(contracts.ErrInvalid, "bad"), "invalid_memory_source", 400},
		{"authority down", errors.New("dial tcp: refused"), "ahp_authority_unavailable", 503},
	}
	for _, c := range cases {
		got := authorityError(c.in)
		var typed *memory.Error
		if !errors.As(got, &typed) {
			t.Fatalf("%s: expected a typed memory error, got %#v", c.name, got)
		}
		if typed.Code != c.wantCode || typed.Status != c.wantHTTP {
			t.Errorf("%s: mapping moved to code=%q status=%d, want %q/%d - promoting a code to pkg/contracts is a contract change (DEF-MEM-HTTP-CODE-2)",
				c.name, typed.Code, typed.Status, c.wantCode, c.wantHTTP)
		}
	}
}
