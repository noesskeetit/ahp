// Command tokengen mints development task tokens that the ahp devAuth verifier
// accepts. It is the out-of-process twin of cmd/ahp devAuth.issue(): the same
// Ed25519 key is derived from AHP_DEV_JWT_SEED (hex, then std, then raw-URL
// base64 of a 32-byte seed) under kid "dev-1", and the same KindTask claim
// shape is signed. It exists so an mTLS test harness can present a Bearer token
// whose tenant/deployment/subject/task are chosen per request.
//
// SECURITY: development-only. It reads the seed from the environment and never
// prints it. Do not use outside local testing.
package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
)

// decodeSeed mirrors cmd/ahp decodeSeed: hex, then std, then raw-URL base64.
func decodeSeed(s string) ([]byte, error) {
	if b, err := hex.DecodeString(s); err == nil {
		return b, nil
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.RawURLEncoding.DecodeString(s)
}

func main() {
	var (
		kid         = flag.String("kid", "dev-1", "JWT key id (must match server dev key)")
		tenant      = flag.String("tenant", "dev", "token tenant")
		deployment  = flag.String("deployment", "dev", "token deployment")
		subject     = flag.String("subject", "dev", "token subject")
		task        = flag.String("task", "task-1", "token task id")
		root        = flag.String("root", "", "token root id (defaults to -task)")
		ctx         = flag.String("ctx", "dev", "token context id")
		grant       = flag.String("grant", "dev", "token grant")
		ttl         = flag.Int("ttl", 600, "token TTL seconds (KindTask max 900)")
		audience    = flag.String("audience", "ahp", "token audience")
		kind        = flag.String("kind", "task", "token kind: task|human")
		issuer      = flag.String("issuer", "ahp", "token issuer")
		seedEnv     = flag.String("seed-env", "AHP_DEV_JWT_SEED", "env var holding the dev seed")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Fprintln(os.Stderr, "tokengen dev")
		return
	}

	raw := strings.TrimSpace(os.Getenv(*seedEnv))
	if raw == "" {
		fmt.Fprintf(os.Stderr, "tokengen: %s is not set\n", *seedEnv)
		os.Exit(2)
	}
	seed, err := decodeSeed(raw)
	if err != nil || len(seed) != ed25519.SeedSize {
		fmt.Fprintf(os.Stderr, "tokengen: seed must decode to 32 bytes\n")
		os.Exit(2)
	}
	priv := ed25519.NewKeyFromSeed(seed)

	signer, err := auth.NewSigner(*issuer, *kid, priv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tokengen: signer: %v\n", err)
		os.Exit(1)
	}

	tk := auth.KindTask
	if *kind == "human" {
		tk = auth.KindHuman
	}
	// A human token carries a tenant and a subject and nothing from the task
	// delegation vocabulary: pkg/auth rejects any task, grant or deployment
	// claim on it. The development defaults therefore have to be dropped for
	// this kind instead of forwarded, otherwise no human token could ever be
	// signed. Deployment selection for a human lives in the create-root request
	// body, never in the token.
	claims := auth.Claims{
		Audience: *audience,
		Kind:     tk,
		Tenant:   *tenant,
		Subject:  *subject,
		Context:  *ctx,
	}
	if tk == auth.KindTask {
		r := *root
		if r == "" {
			r = *task
		}
		claims.Task = *task
		claims.Root = r
		claims.Grant = *grant
		claims.Deployment = *deployment
	}
	claims.Depth = 0
	tok, err := signer.Sign(claims, time.Duration(*ttl)*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tokengen: sign: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(tok)
}
