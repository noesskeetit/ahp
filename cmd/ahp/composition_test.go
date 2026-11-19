package main

import "testing"

// The exchanger allowlist is the only local stand-in for the trusted-service relationship that
// guards /v1/identity/control/exchange. Its parser must therefore be fail-closed: an unset or
// blank variable has to yield no authorisation at all, never a wildcard or an empty-string entry
// that a mis-issued certificate could match.
func TestEnvListOrIsFailClosed(t *testing.T) {
	if got := envListOr("AHP_TEST_UNSET_LIST"); got != nil {
		t.Fatalf("unset variable must yield nil (authorises nobody), got %#v", got)
	}
	t.Setenv("AHP_TEST_BLANK_LIST", "   ")
	if got := envListOr("AHP_TEST_BLANK_LIST"); got != nil {
		t.Fatalf("blank variable must yield nil, got %#v", got)
	}
	t.Setenv("AHP_TEST_LIST", " spacesvc , , other,")
	got := envListOr("AHP_TEST_LIST")
	if len(got) != 2 || got[0] != "spacesvc" || got[1] != "other" {
		t.Fatalf("want [spacesvc other], got %#v", got)
	}
}
