package gateway

// role_test.go -- the ingress role rule and the history-selection rule, as pure
// units. They are the two halves of one defect: a role the library passes
// through verbatim made an existing user message unreachable for the drain.

import (
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestCanonicalInboundRole(t *testing.T) {
	for _, tc := range []struct {
		name string
		role a2a.MessageRole
		want a2a.MessageRole
		ok   bool
	}{
		{"v0 spelling", "user", a2a.MessageRoleUser, true},
		{"v0 spelling padded", " User ", a2a.MessageRoleUser, true},
		{"canonical", a2a.MessageRoleUser, a2a.MessageRoleUser, true},
		{"canonical lowercase", "role_user", a2a.MessageRoleUser, true},
		{"agent v0", "agent", a2a.MessageRoleUnspecified, false},
		{"agent canonical", a2a.MessageRoleAgent, a2a.MessageRoleUnspecified, false},
		{"empty", "", a2a.MessageRoleUnspecified, false},
		{"unspecified enum", "ROLE_UNSPECIFIED", a2a.MessageRoleUnspecified, false},
		{"garbage", "rooter", a2a.MessageRoleUnspecified, false},
		{"user-like garbage is not a match", "usernam", a2a.MessageRoleUnspecified, false},
	} {
		got, err := canonicalInboundRole(tc.role)
		if tc.ok {
			if err != nil || got != tc.want {
				t.Fatalf("%s: canonicalInboundRole(%q) = %q, %v; want %q, nil", tc.name, tc.role, got, err, tc.want)
			}
			continue
		}
		if err == nil {
			t.Fatalf("%s: role %q must be refused, got %q", tc.name, tc.role, got)
		}
		if contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("%s: role %q must fail as ErrInvalid (JSON-RPC -32602), got %v", tc.name, tc.role, err)
		}
	}
}

func TestSelectLastInbound(t *testing.T) {
	text := a2a.NewTextPart("hello")
	user := func(role a2a.MessageRole, id string) *a2a.Message {
		m := a2a.NewMessage(role, text)
		m.ID = id
		return m
	}

	// A v0-spelled row is still the user's turn: selecting only the canonical
	// spelling is what stranded already-journalled tasks.
	if got := selectLastInbound([]*a2a.Message{user("user", "m1")}); got == nil || got.ID != "m1" {
		t.Fatalf("a legacy-spelled user row must be selectable, got %v", got)
	}
	// The most recent user turn wins; agent output never does.
	hist := []*a2a.Message{
		user("user", "m1"),
		user(a2a.MessageRoleAgent, "m2"),
		user(a2a.MessageRoleUser, "m3"),
		user(a2a.MessageRoleAgent, "m4"),
	}
	if got := selectLastInbound(hist); got == nil || got.ID != "m3" {
		t.Fatalf("selection must take the last user turn m3, got %v", got)
	}
	// No user turn at all is a nil input, which the bridge must name honestly.
	if got := selectLastInbound([]*a2a.Message{user(a2a.MessageRoleAgent, "m1"), nil}); got != nil {
		t.Fatalf("an agent-only history has no inbound turn, got %v", got)
	}
	if got := selectLastInbound(nil); got != nil {
		t.Fatalf("an empty history has no inbound turn, got %v", got)
	}
}

func TestIsUserTurnRole(t *testing.T) {
	for _, role := range []a2a.MessageRole{"user", "USER", " ROLE_USER", "role_user", a2a.MessageRoleUser} {
		if !isUserTurnRole(role) {
			t.Fatalf("role %q names the user turn", role)
		}
	}
	for _, role := range []a2a.MessageRole{"", "ROLE_UNSPECIFIED", "agent", a2a.MessageRoleAgent, "rooter"} {
		if isUserTurnRole(role) {
			t.Fatalf("role %q must not name the user turn", role)
		}
	}
}

func TestValidateInboundContent(t *testing.T) {
	usable := []*a2a.Message{
		a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("do it")),
		a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(""), a2a.NewTextPart("second part carries it")),
		a2a.NewMessage(a2a.MessageRoleUser, a2a.NewRawPart([]byte{0x00})),
		a2a.NewMessage(a2a.MessageRoleUser, a2a.NewFileURLPart("https://example.invalid/a.pdf", "application/pdf")),
		a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"k": "v"})),
		a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(false)),
	}
	for i, m := range usable {
		if err := validateInboundContent(m); err != nil {
			t.Fatalf("case %d must be accepted (%v), it carries content the agent can act on", i, err)
		}
	}

	empty := []*a2a.Message{
		nil,
		{Role: a2a.MessageRoleUser},
		{Role: a2a.MessageRoleUser, Parts: a2a.ContentParts{}},
		{Role: a2a.MessageRoleUser, Parts: a2a.ContentParts{nil}},
		a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("")),
		a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("   \n\t ")),
		a2a.NewMessage(a2a.MessageRoleUser, a2a.NewRawPart(nil), a2a.NewTextPart(" ")),
		a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(nil)),
	}
	for i, m := range empty {
		err := validateInboundContent(m)
		if err == nil {
			t.Fatalf("case %d must be refused at the ingress, not journalled into a task that fails later", i)
		}
		if contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("case %d must fail as ErrInvalid (JSON-RPC -32602), got %v", i, err)
		}
	}
}
