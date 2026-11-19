package emil

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestAdapterWithoutClientReturnsError pins the fail-closed seam: an adapter with
// no configured client (a misconfiguration -- nil client / no target URL) must
// return a stable contracts error from both Delegate and Execute, and must NOT
// panic. A single badly configured adapter must never take down the whole composed
// execution plane (it runs inside the Space turn goroutine).
func TestAdapterWithoutClientReturnsError(t *testing.T) {
	args, _ := json.Marshal(map[string]string{"prompt": "do a thing"})

	// A zero-value adapter (nil client) -- Delegate must fail closed, not crash.
	var zero Adapter
	if _, prov, err := zero.Delegate(context.Background(), "do a thing"); err == nil {
		t.Fatal("Delegate on a client-less adapter must return an error")
	} else if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("Delegate err code = %q want %q", contracts.Code(err), contracts.ErrInvalid)
	} else if prov == nil {
		t.Fatal("Delegate must still return an (auditable) non-nil provenance on failure")
	}

	// Execute must return the same fail-closed error rather than SIGSEGV.
	if _, err := zero.Execute(context.Background(), ToolName, args); err == nil {
		t.Fatal("Execute on a client-less adapter must return an error")
	} else if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("Execute err code = %q want %q", contracts.Code(err), contracts.ErrInvalid)
	}

	// A nil adapter must likewise be refused, not dereferenced.
	var nilAdapter *Adapter
	if _, err := nilAdapter.Execute(context.Background(), ToolName, args); err == nil {
		t.Fatal("Execute on a nil adapter must return an error")
	} else if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("nil-adapter Execute err code = %q want %q", contracts.Code(err), contracts.ErrInvalid)
	}
}

// TestNewRequiresBaseURL pins constructor fail-closed: with no target URL New
// returns a contracts error instead of constructing an adapter that later
// dereferences nil.
func TestNewRequiresBaseURL(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New with no BaseURL must fail closed")
	} else if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("New err code = %q want %q", contracts.Code(err), contracts.ErrInvalid)
	}
}
