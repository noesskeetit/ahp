package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"strings"
	"testing"
	"time"
)

func TestConnectionAssertionUsesLiveGrantAndSeparateAudience(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	sc := uniqueScope(t)
	policy := NewSyntheticResourcePolicy(s.pool)
	s.resources = policy
	for _, r := range []ResourceRule{{sc.Tenant, sc.Subject, Resource{"tenant", sc.Tenant}, "tools", true}, {sc.Tenant, sc.Subject, Resource{"deployment", sc.Deployment}, "tools", true}, {sc.Tenant, sc.Subject, Resource{"connection", "crm"}, "use", true}} {
		if _, err := policy.PutRule(ctx, r, 0); err != nil {
			t.Fatal(err)
		}
	}
	g, err := s.CreateRoot(ctx, sc, rootRequest("exchange-task", "exchange-context", sc.Deployment, time.Hour, "tools"))
	if err != nil {
		t.Fatal(err)
	}
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("ahp", "kid", key)
	c := auth.Claims{Kind: auth.KindTask, TokenUse: auth.TokenUse, Tenant: g.Tenant, Subject: g.Subject, Deployment: g.Deployment, Task: g.Task, Root: g.Root, Context: g.Context, Grant: g.ID, Actors: g.Actors, Depth: g.Depth}
	target := ConnectionTarget{Connection: "crm", TokenURL: "https://iam.example/token", Audience: "mcp-crm", Resource: "urn:mcp:crm", Scope: "read"}
	jwt, err := s.MintConnectionAssertion(ctx, c, target, signer)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(jwt, ".")
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	if !ed25519.Verify(pub, []byte(parts[0]+"."+parts[1]), sig) {
		t.Fatal("invalid delegation signature")
	}
	b, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var a auth.ExchangeAssertion
	if err = json.Unmarshal(b, &a); err != nil {
		t.Fatal(err)
	}
	if a.Audience != target.TokenURL || a.TargetAudience != target.Audience || a.Scope != target.Scope || a.Resource != target.Resource || a.TokenUse != "exchange" || a.GrantEpoch != g.Epoch || a.ExpiresAt-a.IssuedAt != 60 || a.Actor != "ahp:tool-gateway" {
		t.Fatal("incorrect delegation constraints")
	}
	v := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"kid": pub})
	if _, err = v.Verify(jwt, "ahp", auth.KindHuman, ""); err == nil {
		t.Fatal("delegation accepted as public/task credential")
	}
	if _, err = s.MintConnectionAssertion(ctx, c, ConnectionTarget{Connection: "foreign", TokenURL: target.TokenURL, Audience: "foreign", Resource: target.Resource, Scope: target.Scope}, signer); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("foreign connection accepted")
	}
	if _, err = policy.PutRule(ctx, ResourceRule{sc.Tenant, sc.Subject, Resource{"connection", "crm"}, "use", false}, 1); err != nil {
		t.Fatal(err)
	}
	if _, err = s.MintConnectionAssertion(ctx, c, target, signer); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("old task authorized revoked connection")
	}
}
