package identityapi

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Exchange refusal reason codes: stable, machine-readable identifiers that
// every denial path on /v1/identity/control/exchange MUST carry in the
// response body's "code" field and in the WARN journal entry.
//
// Contract basis: PDF rev3.2 §6 — an operational refusal is part of the
// contract and must be machine-distinguishable. A caller (the Space facade,
// an integration test, or a human operator reading a log) must be able to
// branch on the code without parsing free-text.
//
// The codes do NOT weaken the denial: fail-closed is preserved, and no code
// constitutes a hint that helps an attacker bypass the check.
const (
	// ReasonExchangePermissionMissing: the human subject does not hold the
	// required exchange entitlement (default "space:access").
	ReasonExchangePermissionMissing = "exchange_permission_missing"

	// ReasonCapabilityWidening: the requested capability set (intersection)
	// is empty, meaning the caller asked for capabilities the human does not
	// hold — a widening attempt.
	ReasonCapabilityWidening = "capability_widening"

	// ReasonDepthExceeded: the delegation depth ceiling would be exceeded by
	// minting the child grant.
	ReasonDepthExceeded = "depth_exceeded"

	// ReasonChildrenQuota: the fan-out or active-subtree ceiling is
	// exhausted for the authority.
	ReasonChildrenQuota = "children_quota"

	// ReasonAzpMismatch: the caller's deployment (azp) is not on the
	// authorized exchanger allowlist, or the caller has no verified workload
	// identity at all.
	ReasonAzpMismatch = "azp_mismatch"

	// ReasonTokenUseInvalid: the presented human token fails verification
	// (wrong audience, wrong kind, expired signature, no jti).
	ReasonTokenUseInvalid = "token_use_invalid"

	// ReasonGrantExpired: the human session is no longer live (revoked,
	// rotated, or subject-revoked).
	ReasonGrantExpired = "grant_expired"
)

// reasonTenantMismatchInternal is a WARN-only journal label. The cross-tenant
// exchange boundary (human token tenant != caller certificate tenant) MUST
// stay indistinguishable on the wire: a per-tenant difference in the response
// would be a tenant-existence oracle. The operator still needs to see WHY a
// cross-tenant exchange was refused, so the reason is recorded in the journal
// only. It is deliberately not part of the public reason-code vocabulary.
const reasonTenantMismatchInternal = "tenant_mismatch"

// exchangeRefuse records a machine-readable WARN (never a secret: no token,
// no DSN, only the reason code plus non-identifying request fields and a
// subject hash) and writes the 403 body whose "code" carries the stable reason
// code. The decision to refuse is unchanged from before — only the wire body's
// code and the journal gain the machine-readable reason, so fail-closed is
// preserved 1-в-1.
//
// task/context/azp name the request; subjectHash is a truncated SHA-256 of the
// subject so a log reader can correlate without the log itself carrying the
// subject. tokenHash names the presented token by hash only, never its value.
func (h *api) exchangeRefuse(w http.ResponseWriter, r *http.Request, reason, message string, task, context, azp, subject string) {
	_ = r
	h.logger().Warn("identityapi: exchange refused",
		slog.String("reason_code", reason),
		slog.String("task", task),
		slog.String("context", context),
		slog.String("azp", azp),
		slog.String("subject_hash", subjectHash(subject)),
	)
	h.browserError(w, r, http.StatusForbidden, reason, message)
}

// exchangeRefuseOpaque records an internal WARN carrying a reason the wire
// must NOT reveal, then answers with the SAME indistinguishable 403 every
// opaque exchange denial returns. It is used only where distinguishability is
// forbidden (the cross-tenant boundary), so it cannot become a tenant or
// subject oracle, while the operator journal still records the true cause.
// The deny decision is unchanged — fail-closed and byte-identical to the
// other opaque refusals.
func (h *api) exchangeRefuseOpaque(w http.ResponseWriter, r *http.Request, internalReason, task, context, azp, subject string) {
	_ = r
	h.logger().Warn("identityapi: exchange refused",
		slog.String("reason_code", internalReason),
		slog.String("task", task),
		slog.String("context", context),
		slog.String("azp", azp),
		slog.String("subject_hash", subjectHash(subject)),
		slog.String("wire_code", contracts.ErrForbidden),
	)
	h.browserError(w, r, http.StatusForbidden, contracts.ErrForbidden, "identityapi: not authorized")
}

// classifyExchangeStoreError maps a Store error returned by the exchange
// path's Get/CreateRoot/Delegate/MintTask onto a stable reason code. The
// identity package reports every tree/quota denial as FORBIDDEN with a
// stable phrase; this maps those phrases to the exchange reason vocabulary.
// An unrecognised error keeps its own contract code (it is surfaced verbatim
// by the caller, fail-closed), so this never turns a DENY into an ALLOW.
// A revoked-subject denial is intentionally NOT mapped: it stays the generic
// opaque FORBIDDEN so no distinct code reveals subject revocation state.
func classifyExchangeStoreError(err error) (reason string, ok bool) {
	if contracts.Code(err) != contracts.ErrForbidden {
		return "", false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "depth ceiling"),
		strings.Contains(msg, "max_depth may only narrow"):
		return ReasonDepthExceeded, true
	case strings.Contains(msg, "fan-out limit is exhausted"),
		strings.Contains(msg, "active-subtree limit is exhausted"),
		strings.Contains(msg, "max_fanout may only narrow"),
		strings.Contains(msg, "max_active may only narrow"):
		return ReasonChildrenQuota, true
	case strings.Contains(msg, "narrow the parent capability set"),
		strings.Contains(msg, "does not hold capability"):
		return ReasonCapabilityWidening, true
	case strings.Contains(msg, "has expired"),
		strings.Contains(msg, "root grant is not active"):
		return ReasonGrantExpired, true
	default:
		return "", false
	}
}

// subjectHash returns a short, non-reversible hash of the subject so a WARN
// line can be correlated across records without the journal carrying the
// subject in cleartext.
func subjectHash(subject string) string {
	if subject == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(subject))
	return hex.EncodeToString(sum[:])[:16]
}
