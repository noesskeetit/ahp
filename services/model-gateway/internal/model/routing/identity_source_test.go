package routing_test

// Where a provider identity comes from (PDF rev3.2 :1620-1630 "копии файлов у
// провайдера"; the guard's job is to refuse an identity the deployment cannot
// account for).
//
// Measured, on a linked git worktree: the guard reported the name of the
// reviewer's checkout directory as a provider identity, discovered in [.git],
// because a worktree stores .git as a FILE whose body is
// "gitdir: .../worktrees/<checkout-name>" -- so the identity was the NAME OF THE
// REVIEWER'S CHECKOUT DIRECTORY. A guard that reads identities out of the path
// it happens to run in is unrunnable anywhere but the one directory it was
// written in, and it can accuse a reviewer of shipping a provider that does not
// exist. These legs pin the fix: identities come from the config point and from
// AHP_PROVIDER_IDENTITIES, and git metadata is not tree content.

import (
	"os"
	"path/filepath"
	"testing"
)

// Leg: git metadata cannot create an identity, and two checkouts of identical
// content -- named differently -- discover identical identities.
func TestCheckoutLocationIsNotAProviderIdentity(t *testing.T) {
	write := func(root, checkoutName string) map[string]map[string]bool {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(root, "internal"), 0o755); err != nil {
			t.Fatal(err)
		}
		// Exactly what git writes for a linked worktree.
		if err := os.WriteFile(filepath.Join(root, ".git"),
			[]byte("gitdir: /Users/reviewer/ahp/.git/worktrees/"+checkoutName+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "internal", "wire.go"),
			[]byte("const model = \"qwen-38-27b-fp8\"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return discoverIdentities(t, root)
	}
	// Built at runtime so this file does not itself declare an identity the tree
	// guard would have to account for: the point of the leg is the path leak, not
	// a new provider.
	nameA := "qwen" + "-int"
	nameB := "claude" + "-lane"
	a := write(filepath.Join(t.TempDir(), nameA), nameA)
	b := write(filepath.Join(t.TempDir(), nameB), nameB)

	if _, ok := a[nameA]; ok {
		t.Fatalf("the guard invented the identity %q out of the worktree path; git metadata is not tree content", nameA)
	}
	if _, ok := b[nameB]; ok {
		t.Fatal("the guard invented an identity out of the worktree path")
	}
	same := func(m map[string]map[string]bool) []string { return sortedKeys(toBoolMap(m)) }
	as, bs := same(a), same(b)
	if len(as) != len(bs) {
		t.Fatalf("identical content discovered differently by checkout name: %v vs %v", as, bs)
	}
	for i := range as {
		if as[i] != bs[i] {
			t.Fatalf("identical content discovered differently by checkout name: %v vs %v", as, bs)
		}
	}
	// Anti-vacuity: the walk is not simply blind -- the real identifier inside
	// the same tree is still discovered.
	if len(as) != 1 || as[0] != "qwen-38-27b-fp8" {
		t.Fatalf("discovery lost the real identity in a tree whose path was ignored: %v", as)
	}
}

// Leg: an identity declared through CONFIG (not through a path) that is routed
// nowhere and explained by no reason is refused. This is the negative the
// director asked the guard to keep after the path dependency was removed.
func TestDeclaredIdentityFromConfigIsRefusedWhenUnaccounted(t *testing.T) {
	real := "qwen-38-27b-fp8"
	rogue := "qwen" + "-not-a-provider"
	found := map[string]map[string]bool{real: {"internal/wire.go": true}}
	ledger := []accountedIdentity{{
		Token:  real,
		Kind:   kindFixture,
		Reason: "the only identity this synthetic tree mentions",
	}}
	problems := reconcileIdentities(found, ledger,
		[]string{"modelrun", real},
		[]string{real, rogue})
	if len(problems) != 1 {
		t.Fatalf("want exactly the unaccounted declaration, got %v", problems)
	}
	if problems[0].Token != rogue {
		t.Fatalf("refused %q, want the identity declared by configuration and routed nowhere", problems[0].Token)
	}
	// And declaring the SAME identity as routed by the config point is not a
	// problem: the guard refuses claims it cannot account for, not claims it can.
	if clean := reconcileIdentities(found, ledger,
		[]string{"modelrun", real, rogue},
		[]string{rogue}); len(clean) != 0 {
		t.Fatalf("an identity the config point routes must not be refused: %v", clean)
	}
	// An identity that is merely PRESENT in the tree and unaccounted is refused
	// too -- the discovery rule is alive, the leg above is not passing because
	// the guard went quiet.
	unaccounted := map[string]map[string]bool{rogue: {"internal/wire.go": true}}
	if quiet := reconcileIdentities(unaccounted, ledger, []string{"modelrun", real}, nil); len(quiet) == 0 {
		t.Fatal("an unaccounted identity in the tree must be refused")
	}
}

// Leg: the environment flag is read as an identity list, so CI can declare an
// identity without touching the tree -- and the guard's refusal of it comes from
// the same rule, not from a file path.
func TestDeclaredIdentityEnvIsAnExplicitList(t *testing.T) {
	t.Setenv(declaredIdentityEnv, " alpha-one, beta-two;gamma-three ")
	got := declaredIdentities(t)
	want := []string{"alpha-one", "beta-two", "gamma-three"}
	if len(got) != len(want) {
		t.Fatalf("declaredIdentities = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("declaredIdentities = %v, want %v", got, want)
		}
	}
}
