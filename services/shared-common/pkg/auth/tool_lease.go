package auth

import (
	"crypto/ed25519"
	"encoding/base64"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

const toolLeaseType = "ahp-tool-lease+jwt"

// toolLeaseProof is the authority issued alongside a journal lease. Only the
// State API signer is trusted to issue it. The Gateway also checks the caller's
// workload and access token before verifying this proof.
type toolLeaseProof struct {
	Issuer      string `json:"iss"`
	Tenant      string `json:"tenant"`
	Deployment  string `json:"azp"`
	Subject     string `json:"sub"`
	Task        string `json:"task"`
	Root        string `json:"root"`
	Context     string `json:"ctx"`
	Grant       string `json:"grant"`
	Session     string `json:"session"`
	Epoch       int64  `json:"epoch"`
	ExpiresAtUS int64  `json:"expires_at_us"`
}

func newToolLeaseProof(issuer string, caller Claims, session string, epoch int64, expires time.Time) toolLeaseProof {
	return toolLeaseProof{
		Issuer: issuer, Tenant: caller.Tenant, Deployment: caller.Deployment,
		Subject: caller.Subject, Task: caller.Task, Root: caller.Root,
		Context: caller.Context, Grant: caller.Grant, Session: session,
		Epoch: epoch, ExpiresAtUS: expires.UnixMicro(),
	}
}

// SignToolLease binds the epoch and deadline from a committed session lease
// to the authority which obtained it.
func (s *Signer) SignToolLease(caller Claims, session string, epoch int64, expires time.Time) (string, error) {
	if s == nil || caller.Kind != KindTask || caller.Task == "" || caller.Context == "" ||
		caller.Root == "" || caller.Grant == "" || session == "" || epoch <= 0 || expires.IsZero() {
		return "", contracts.NewError(contracts.ErrInvalid, "auth: incomplete tool lease")
	}
	if err := caller.Scope().Validate(); err != nil {
		return "", err
	}
	h, err := encodeSegment(header{Type: toolLeaseType, Algorithm: "EdDSA", KeyID: s.kid})
	if err != nil {
		return "", err
	}
	p, err := encodeSegment(newToolLeaseProof(s.issuer, caller, session, epoch, expires))
	if err != nil {
		return "", err
	}
	input := string(h) + "." + string(p)
	if len(input)+1+encodedLen(signatureLen) > maxTokenBytes {
		return "", contracts.NewError(contracts.ErrInvalid, "auth: tool lease proof too large")
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(s.key, []byte(input))), nil
}

// VerifyToolLease authenticates a lease proof against already verified task
// claims. It returns the signed deadline so the Gateway can compare it with its
// database clock while holding the epoch lock.
func (v *Verifier) VerifyToolLease(token string, caller Claims, session string, epoch int64) (time.Time, error) {
	denied := contracts.NewError(contracts.ErrForbidden, "auth: invalid tool lease proof")
	if v == nil || caller.Kind != KindTask || session == "" || epoch <= 0 {
		return time.Time{}, denied
	}
	h, p, sig, err := splitToken(token)
	if err != nil {
		return time.Time{}, denied
	}
	var hdr header
	if decodeJSONStrict(h, &hdr) != nil || hdr.Algorithm != "EdDSA" || hdr.Type != toolLeaseType || hdr.Class != "" {
		return time.Time{}, denied
	}
	key, ok := v.keys[hdr.KeyID]
	if !ok || !ed25519.Verify(key, []byte(token[:len(token)-encodedLen(signatureLen)-1]), sig) {
		return time.Time{}, denied
	}
	var proof toolLeaseProof
	if err := decodeJSONStrict(p, &proof); err != nil || proof.ExpiresAtUS <= 0 {
		return time.Time{}, denied
	}
	expires := time.UnixMicro(proof.ExpiresAtUS).UTC()
	if proof != newToolLeaseProof(v.issuer, caller, session, epoch, expires) {
		return time.Time{}, denied
	}
	return expires, nil
}
