// Package artifacts: DownloadGrant, the named Artifacts operation of PDF rev3.2
// §6 (стр.1582: BeginUpload, Seal, Export, DownloadGrant) and рис. 7 ("скачивание
// через прокси: текущие права, точная версия").
//
// A grant is a short-lived, use-counted capability bound to one EXACT sealed
// version and one recipient subject. It exists so the browser path can be served
// through a proxy without handing a task token to a browser: the component that
// holds machine credentials mints the grant after the CURRENT rights check, and
// the proxy redeems it for bytes. It is not a presigned URL: redemption is
// authenticated and re-checked by this service on every call, and the bytes are
// streamed, never redirected.
//
// Two rules the PDF is explicit about and this file enforces:
//
//   - "текущие права" are evaluated AT ISSUE and again AT CONSUMPTION. Minting
//     runs the same admission every read runs - tenant, the read capability,
//     the conversation gate, the label ceiling of the version - and refuses a
//     tombstone or a legal hold. Redeeming runs it again, so a hold placed or a
//     label narrowed after issuance stops the download instead of honouring a
//     stale decision (рис. 7 подпись, стр.614).
//   - "точная версия" is an input, never a default. "latest" and an empty
//     version are refused, and a grant whose pinned version no longer matches
//     the row stops working.
//
// Only the SHA-256 digest of a grant token is stored, so a database read cannot
// hand out live download capabilities.

package artifacts

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Grant lifetime bounds. The contract says "short-lived": a mint may ask for
// less, never for more, and an unset request gets the default.
const (
	// DefaultGrantTTL is used when the caller does not ask for a lifetime.
	DefaultGrantTTL = 2 * time.Minute
	// MaxGrantTTL is the hard ceiling of any issued grant.
	MaxGrantTTL = 15 * time.Minute
	// MinGrantTTL refuses a request so short it can only be a mistake.
	MinGrantTTL = 5 * time.Second
	// MaxGrantUses bounds the use count of a multi-use grant.
	MaxGrantUses = 8
	// grantTokenBytes is the entropy of one issued token (256 bits).
	grantTokenBytes = 32
	// grantSweepAge is how long an expired grant is kept for forensics before
	// a later mint sweeps it up. There is no background job in this tier.
	grantSweepAge = time.Hour
)

// Stable machine-readable reasons of the grant surface. They ride beside the
// contract code (contracts.NewReasoned + transport.WriteError), so a client can
// branch on "grant_expired" vs "grant_used" without matching message text.
const (
	ReasonGrantVersionRequired   = "grant_version_required"
	ReasonGrantVersionMismatch   = "grant_version_mismatch"
	ReasonGrantRecipientMismatch = "grant_recipient_mismatch"
	ReasonGrantUnknown           = "grant_unknown"
	ReasonGrantExpired           = "grant_expired"
	ReasonGrantUsed              = "grant_used"
	ReasonArtifactDeleted        = "artifact_deleted"
	ReasonArtifactLegalHold      = "artifact_legal_hold"
)

// ddlGrants creates the grant table. It runs in Migrate after ddl and
// ddlLifecycle and is idempotent like them. The artifact id is a real foreign
// key with ON DELETE CASCADE, so Purge (which removes the row) also removes
// whatever grants still named it.
const ddlGrants = `
CREATE TABLE IF NOT EXISTS ahp_artifacts.download_grants (
    digest       text PRIMARY KEY,
    tenant       text NOT NULL,
    deployment   text NOT NULL,
    recipient    text NOT NULL,
    artifact_id  text NOT NULL REFERENCES ahp_artifacts.artifacts(id) ON DELETE CASCADE,
    version      text NOT NULL,
    sha256       text NOT NULL,
    size         bigint NOT NULL,
    one_time     boolean NOT NULL DEFAULT true,
    uses_max     integer NOT NULL DEFAULT 1,
    uses         integer NOT NULL DEFAULT 0,
    issued_at    timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    consumed_at  timestamptz,
    CONSTRAINT download_grants_digest_ck CHECK (digest ~ '^[0-9a-f]{64}$'),
    CONSTRAINT download_grants_sha_ck CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT download_grants_uses_ck CHECK (uses_max BETWEEN 1 AND 8 AND uses BETWEEN 0 AND uses_max),
    CONSTRAINT download_grants_window_ck CHECK (expires_at > issued_at)
);
CREATE INDEX IF NOT EXISTS download_grants_expiry_idx
    ON ahp_artifacts.download_grants (expires_at);
`

// GrantRequest is one mint request. Version is mandatory: the exact sealed
// version the caller was able to read. Recipient may only name the calling
// subject, and an empty value means exactly that subject.
type GrantRequest struct {
	Version   string        `json:"version"`
	Recipient string        `json:"recipient,omitempty"`
	TTL       time.Duration `json:"-"`
	// Uses is the total number of consumptions allowed; 0 means one, and 1 is
	// the default every browser proxy path uses.
	Uses int `json:"uses,omitempty"`
}

// DownloadGrant is the issued grant. Grant is the bearer token and is returned
// exactly once, to the caller that minted it; the table stores only its digest.
type DownloadGrant struct {
	Grant      string    `json:"grant"`
	ArtifactID string    `json:"artifact_id"`
	Version    string    `json:"version"`
	SHA256     string    `json:"sha256"`
	Size       int64     `json:"size"`
	Recipient  string    `json:"recipient"`
	OneTime    bool      `json:"one_time"`
	UsesMax    int       `json:"uses_max"`
	IssuedAt   time.Time `json:"issued_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// grantRecord is one stored grant row (token absent by construction).
type grantRecord struct {
	digest     string
	tenant     string
	recipient  string
	artifactID string
	version    string
	sha256     string
	size       int64
	expiresAt  time.Time
}

// MintDownloadGrant issues a download grant for one exact sealed version.
//
// Every current right is consulted at this moment, and each one is the same
// check the read path makes: the full read gate - tenant, the read capability,
// the conversation gate and the label ceiling - because a caller may only
// issue a download it could perform itself. A distinct "grant" capability
// would let a policy allow reads while refusing issued tickets; enabling it is
// one line in the data-plane action allowlist (cmd/ahp/role_data.go), which
// belongs to the surface owner, so it is recorded as an open item instead of
// being asserted here. Lifecycle is last: a tombstone answers
// NOT_FOUND like any deleted artifact, a legal hold answers FORBIDDEN, and
// neither is papered over by "it is only a grant".
func (s *Store) MintDownloadGrant(ctx context.Context, scope contracts.Scope, id string, req GrantRequest) (DownloadGrant, error) {
	if err := scope.Validate(); err != nil {
		return DownloadGrant{}, err
	}
	if err := validateID(id); err != nil {
		return DownloadGrant{}, err
	}
	version := strings.TrimSpace(req.Version)
	if version == "" || strings.EqualFold(version, "latest") || len(version) > 256 {
		return DownloadGrant{}, contracts.NewReasoned(contracts.ErrInvalid,
			ReasonGrantVersionRequired,
			"a download grant pins one exact sealed version; an empty version or \"latest\" is refused")
	}
	recipient := strings.TrimSpace(req.Recipient)
	if recipient == "" {
		recipient = scope.Subject
	}
	if recipient != scope.Subject {
		// The grant moves bytes to a named subject, so naming someone else is
		// not a convenience: it is a cross-subject read wearing a receipt.
		return DownloadGrant{}, contracts.NewReasoned(contracts.ErrForbidden,
			ReasonGrantRecipientMismatch,
			"a download grant may only be issued to the calling subject")
	}
	ttl, err := clampGrantTTL(req.TTL)
	if err != nil {
		return DownloadGrant{}, err
	}
	uses := req.Uses
	if uses == 0 {
		uses = 1
	}
	if uses < 1 || uses > MaxGrantUses {
		return DownloadGrant{}, contracts.NewError(contracts.ErrInvalid,
			"a download grant allows 1 to 8 uses")
	}

	// The whole read gate applies, including the conversation gate: a token
	// bound to one context must not be able to issue a download of another
	// context's artifact, or the grant would out-run the binding. The refusal
	// code stays real (getForGrant): issuance answers FORBIDDEN, reads answer
	// the B114 parity NOT_FOUND.
	ref, err := s.getForGrant(ctx, scope, id)
	if err != nil {
		return DownloadGrant{}, err
	}
	if ref.Version != version {
		return DownloadGrant{}, contracts.NewReasoned(contracts.ErrForbidden,
			ReasonGrantVersionMismatch,
			"the requested version is not the sealed version of this artifact")
	}
	deleted, hold, err := s.lifecycleFlags(ctx, id)
	if err != nil {
		return DownloadGrant{}, err
	}
	if err := grantLifecycleError(deleted, hold); err != nil {
		return DownloadGrant{}, err
	}

	raw := make([]byte, grantTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return DownloadGrant{}, storageError("mint download grant entropy", err)
	}
	digestSum := sha256.Sum256(raw)
	digest := hex.EncodeToString(digestSum[:])
	token := base64.RawURLEncoding.EncodeToString(raw)
	issued := time.Now().UTC()
	expires := issued.Add(ttl)
	oneTime := uses == 1
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO ahp_artifacts.download_grants
    (digest, tenant, deployment, recipient, artifact_id, version, sha256, size,
     one_time, uses_max, issued_at, expires_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			digest, scope.Tenant, scope.Deployment, recipient, id,
			ref.Version, ref.SHA256, ref.Size, oneTime, uses, issued, expires); err != nil {
			return storageError("store download grant", err)
		}
		// Grants are short-lived by contract and this tier runs no background
		// worker, so issuance also sweeps what expired long ago.
		if _, err := tx.Exec(ctx,
			`DELETE FROM ahp_artifacts.download_grants WHERE expires_at < $1`,
			issued.Add(-grantSweepAge)); err != nil {
			return storageError("sweep expired download grants", err)
		}
		return nil
	})
	if err != nil {
		return DownloadGrant{}, err
	}
	return DownloadGrant{
		Grant: token, ArtifactID: id, Version: ref.Version, SHA256: ref.SHA256,
		Size: ref.Size, Recipient: recipient, OneTime: oneTime, UsesMax: uses,
		IssuedAt: issued, ExpiresAt: expires,
	}, nil
}

// OpenGrantedRange redeems one grant and streams a byte range of the version it
// pins. One call is one use: the counter moves inside the same transaction
// that re-checks the rights, so a replay cannot spend the same use twice and a
// refused call never burns one.
//
// The conversation gate is deliberately NOT re-applied here. The grant is the
// authority: it was issued by a caller that passed that gate, and it names the
// recipient subject whose own label ceiling and lifecycle state are re-checked
// now. That is what lets the human-facing proxy serve a file whose artifact was
// written in an older conversation without handing that proxy a token for it.
func (s *Store) OpenGrantedRange(ctx context.Context, scope contracts.Scope, token string, offset, count int64) (io.ReadCloser, ArtifactRef, error) {
	if err := scope.Validate(); err != nil {
		return nil, ArtifactRef{}, err
	}
	digest, err := grantDigest(token)
	if err != nil {
		return nil, ArtifactRef{}, err
	}
	var (
		ref    ArtifactRef
		object string
	)
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		stored, err := loadGrant(ctx, tx, digest)
		if err != nil {
			return err
		}
		if stored.tenant != scope.Tenant {
			// Nothing is confirmed to another tenant, not even that the
			// capability exists.
			return grantUnknownError()
		}
		if stored.recipient != scope.Subject {
			return contracts.NewReasoned(contracts.ErrForbidden,
				ReasonGrantRecipientMismatch,
				"this grant was issued to another recipient")
		}
		// FOR UPDATE serialises with the tombstone update, so a Delete that
		// wins the race makes this redemption refuse rather than stream bytes
		// of an artifact that is already gone.
		var (
			tenant, version, sha       string
			artifactName, artifactMIME string
			artifactContextID          string
			size                       int64
			deleted, hold              bool
			doc                        []byte
		)
		err = tx.QueryRow(ctx, `
SELECT tenant, object, name, mime, COALESCE(version,''), COALESCE(sha256,''),
       COALESCE(size,0), deleted, legal_hold, labels, context_id
  FROM ahp_artifacts.artifacts WHERE id = $1 FOR UPDATE`, stored.artifactID).
			Scan(&tenant, &object, &artifactName, &artifactMIME, &version, &sha, &size,
				&deleted, &hold, &doc, &artifactContextID)
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "artifact not found")
		}
		if err != nil {
			return storageError("read granted artifact", err)
		}
		if tenant != scope.Tenant || deleted {
			return contracts.NewReasoned(contracts.ErrNotFound,
				ReasonArtifactDeleted, "artifact not found")
		}
		if hold {
			return contracts.NewReasoned(contracts.ErrForbidden,
				ReasonArtifactLegalHold,
				"the artifact is under legal hold and cannot be downloaded")
		}
		// The requested range is validated against the size the grant pins,
		// before the counter moves: a 416 must not spend a use.
		if offset < 0 || count < 0 {
			return contracts.NewError(contracts.ErrInvalid, "offset and count must be non-negative")
		}
		if offset >= stored.size {
			return &RangeNotSatisfiableError{Size: stored.size}
		}
		if version != stored.version || sha != stored.sha256 {
			return contracts.NewReasoned(contracts.ErrConflict,
				ReasonGrantVersionMismatch,
				"the pinned version is no longer the sealed version")
		}
		var storedLabels labels.Set
		if len(doc) > 0 {
			if err := json.Unmarshal(doc, &storedLabels); err != nil {
				return contracts.NewError(contracts.ErrConflict, "stored labels are unreadable")
			}
		}
		if _, err := s.admit(ctx, scope, storedLabels); err != nil {
			return err
		}
		if err := s.authorize(ctx, scope, "read", "artifact:"+stored.artifactID); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
UPDATE ahp_artifacts.download_grants SET uses = uses + 1, consumed_at = now()
 WHERE digest = $1 AND uses < uses_max AND expires_at > now()`, digest)
		if err != nil {
			return storageError("consume download grant", err)
		}
		if tag.RowsAffected() == 0 {
			return consumeRefusedError(ctx, tx, digest)
		}
		ref = ArtifactRef{ID: stored.artifactID, Tenant: tenant, Version: version,
			SHA256: sha, Name: artifactName, MIME: artifactMIME, Size: size,
			Deployment: scope.Deployment, Subject: scope.Subject,
			ContextID: artifactContextID, Labels: storedLabels}
		return nil
	})
	if err != nil {
		return nil, ArtifactRef{}, err
	}
	return s.openVersionRange(ctx, object, ref, offset, count)
}

// lifecycleFlags reads the two lifecycle states a grant refuses on. The row is
// already tenant-verified by the caller's read gate.
func (s *Store) lifecycleFlags(ctx context.Context, id string) (deleted, legalHold bool, err error) {
	err = s.pool.QueryRow(ctx,
		`SELECT deleted, legal_hold FROM ahp_artifacts.artifacts WHERE id = $1`, id).
		Scan(&deleted, &legalHold)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, false, contracts.NewError(contracts.ErrNotFound, "artifact not found")
	case err != nil:
		return false, false, storageError("read artifact lifecycle", err)
	}
	return deleted, legalHold, nil
}

func grantLifecycleError(deleted, legalHold bool) error {
	if deleted {
		return contracts.NewReasoned(contracts.ErrNotFound,
			ReasonArtifactDeleted, "artifact not found")
	}
	if legalHold {
		return contracts.NewReasoned(contracts.ErrForbidden,
			ReasonArtifactLegalHold,
			"the artifact is under legal hold and cannot be downloaded")
	}
	return nil
}

// clampGrantTTL applies the lifetime policy: an unset request gets the default,
// anything over the ceiling is cut to it rather than refused, and a request
// under the floor is a caller bug.
func clampGrantTTL(requested time.Duration) (time.Duration, error) {
	switch {
	case requested == 0:
		return DefaultGrantTTL, nil
	case requested < MinGrantTTL:
		return 0, contracts.NewError(contracts.ErrInvalid,
			"a download grant must live at least 5 seconds")
	case requested > MaxGrantTTL:
		return MaxGrantTTL, nil
	default:
		return requested, nil
	}
}

// grantDigest turns a presented token into its storage digest. A malformed
// token is indistinguishable from an unknown one on purpose: the answer tells
// the caller nothing about why the capability did not work.
func grantDigest(token string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil || len(raw) != grantTokenBytes {
		return "", grantUnknownError()
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func grantUnknownError() error {
	return contracts.NewReasoned(contracts.ErrForbidden, ReasonGrantUnknown,
		"download grant is not valid")
}

// loadGrant reads one grant row by digest.
func loadGrant(ctx context.Context, tx pgx.Tx, digest string) (grantRecord, error) {
	var rec grantRecord
	err := tx.QueryRow(ctx, `
SELECT digest, tenant, recipient, artifact_id, version, sha256, size, expires_at
  FROM ahp_artifacts.download_grants WHERE digest = $1`, digest).
		Scan(&rec.digest, &rec.tenant, &rec.recipient, &rec.artifactID,
			&rec.version, &rec.sha256, &rec.size, &rec.expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return grantRecord{}, grantUnknownError()
	}
	if err != nil {
		return grantRecord{}, storageError("read download grant", err)
	}
	return rec, nil
}

// consumeRefusedError names why a consumption moved no counter: expired or
// already spent. The distinction is stable and machine-readable because the
// acceptance contract asks for it; nothing else about the grant is revealed.
func consumeRefusedError(ctx context.Context, tx pgx.Tx, digest string) error {
	var (
		uses, usesMax int
		expires       time.Time
	)
	err := tx.QueryRow(ctx,
		`SELECT uses, uses_max, expires_at FROM ahp_artifacts.download_grants WHERE digest = $1`,
		digest).Scan(&uses, &usesMax, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return grantUnknownError()
	}
	if err != nil {
		return storageError("read download grant state", err)
	}
	if !expires.After(time.Now().UTC()) {
		return contracts.NewReasoned(contracts.ErrForbidden, ReasonGrantExpired,
			"download grant has expired")
	}
	return contracts.NewReasoned(contracts.ErrForbidden, ReasonGrantUsed,
		"download grant has already been used")
}
