package treedelegate

import (
	"errors"
	"testing"
)

// The arithmetic of a delegated reserve is the whole product rule in four lines
// (spec: a child reserves its share out of the parent's REMAINDER; an exhausted
// reserve is a refusal), so it is tested as arithmetic before anything touches a
// database. The database is then tested for enforcing exactly this.
func TestGrantTakesTheSmallerOfRequestedAndRemainder(t *testing.T) {
	cases := []struct {
		name                          string
		limit, spent, held, requested int64
		want                          int64
		wantRefusal                   bool
	}{
		{"nothing spent, asks for more than it has", 5, 0, 0, 9, 5, false},
		{"nothing spent, asks for less", 5, 3, 0, 2, 2, false},
		{"remainder after own steps", 5, 3, 0, 5, 2, false},
		{"remainder after another child's hold", 5, 1, 2, 5, 2, false},
		{"own steps and a hold together", 8, 3, 2, 4, 3, false},
		{"unnumbered request gets the remainder", 4, 1, 0, 0, 3, false},
		{"negative request gets the remainder", 4, 1, 0, -7, 3, false},
		{"exhausted by own steps is a refusal", 3, 3, 0, 3, 0, true},
		{"exhausted by holds is a refusal", 3, 1, 2, 1, 0, true},
		{"over-committed tree is a refusal, never a negative grant", 3, 2, 2, 1, 0, true},
		{"a run with no bound cannot delegate", 0, 0, 0, 3, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Grant(tc.limit, tc.spent, tc.held, tc.requested)
			if tc.wantRefusal {
				if !errors.Is(err, ErrNoCapacity) {
					t.Fatalf("Grant(%d,%d,%d,%d) = %d, %v; want a refusal", tc.limit, tc.spent, tc.held, tc.requested, got, err)
				}
				if got != 0 {
					t.Fatalf("refusal must grant nothing, got %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Grant(%d,%d,%d,%d) errored: %v", tc.limit, tc.spent, tc.held, tc.requested, err)
			}
			if got != tc.want {
				t.Fatalf("Grant(%d,%d,%d,%d) = %d, want %d", tc.limit, tc.spent, tc.held, tc.requested, got, tc.want)
			}
		})
	}
}

func TestStepKeyIsStablePerStepAndDistinctAcrossSteps(t *testing.T) {
	if StepKey(0) != StepKey(0) {
		t.Fatal("a step key must replay, or a retried attempt would charge the tree twice")
	}
	if StepKey(0) == StepKey(1) {
		t.Fatal("two different steps must not share a key")
	}
}
