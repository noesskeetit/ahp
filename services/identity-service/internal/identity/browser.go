package identity

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/argon2"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Browser session lifetimes. A browser session is two nested deadlines:
//
//   - the idle deadline, which every successful use (token mint, refresh)
//     pushes forward, and which a period of inactivity lets lapse;
//   - the absolute deadline, which nothing moves. Reaching it ends the
//     session no matter how active the browser has been.
//
// Refreshing therefore proves the session is still live; it never extends the
// authority the session was opened with, which is the same rule the delegation
// tree applies to a grant's expiry.
const (
	// BrowserAccessTTL is the lifetime of a browser access token.
	BrowserAccessTTL = 10 * time.Minute
	// BrowserSessionIdleTTL is how long a session may go unused.
	BrowserSessionIdleTTL = 30 * time.Minute
	// BrowserSessionAbsoluteTTL is the immovable session deadline.
	BrowserSessionAbsoluteTTL = 8 * time.Hour

	// ExchangeAuthorityTTL is the ceiling lifetime of a per-session ingress
	// authority root created by the Human -> Space -> scoped task exchange. It
	// equals the absolute session deadline so a live session is never cut off
	// mid-use, while the task tokens minted under it stay short-lived and are
	// re-attested on every use. It is within the identity grant TTL ceiling.
	ExchangeAuthorityTTL = BrowserSessionAbsoluteTTL

	// minBrowserPassword bounds the credential the local store accepts.
	minBrowserPassword = 10
	// maxBrowserPassword bounds the digest input so a caller cannot make the
	// key derivation loop run on an unbounded string.
	maxBrowserPassword = 1024
)

// argon2id parameters. These are real key-derivation parameters, not a
// placeholder: the password never reaches the database in any form.
const (
	argonTime    = 3
	argonMemory  = 64 * 1024
	argonThreads = 2
	argonKeyLen  = 32
	argonSaltLen = 16
)

// argonAlgo is the only accepted credential algorithm tag.
const argonAlgo = "argon2id"

// BrowserUser is a human account the browser may authenticate as. Tenant and
// Subject come from this row and from nowhere else: a login request cannot
// name them.
type BrowserUser struct {
	UserID      string   `json:"id"`
	Tenant      string   `json:"tenant"`
	Subject     string   `json:"subject"`
	DisplayName string   `json:"display_name"`
	Email       string   `json:"email"`
	Roles       []string `json:"roles"`
	Permissions []string `json:"permissions"`
}

// Scope projects the account onto the platform isolation scope. A human scope
// has no deployment, so the projection is deliberately partial: callers that
// need a full contracts.Scope must add the deployment of the service the human
// is talking to, exactly as the Space facade does.
func (u BrowserUser) Scope() contracts.Scope {
	return contracts.Scope{Tenant: u.Tenant, Subject: u.Subject}
}

// BrowserSession is one server-side browser session.
type BrowserSession struct {
	ID         string    `json:"session_id"`
	UserID     string    `json:"user_id"`
	Tenant     string    `json:"tenant"`
	Subject    string    `json:"subject"`
	Epoch      int64     `json:"epoch"`
	ExpiresAt  time.Time `json:"expires_at"`
	AbsoluteAt time.Time `json:"absolute_at"`
}

// SeedBrowserUser creates or resets a human account together with its
// credential and its role/permission set.
//
// This is the seam a deployment uses to provision the prototype: in a
// production cell the accounts and credentials belong to the corporate IdP and
// this function is never called. Passwords are hashed here and never stored.
func (s *Store) SeedBrowserUser(ctx context.Context, user BrowserUser, username, password string,
	roles, permissions []string) error {
	if s.pool == nil {
		return invalid("identity: store has no pool")
	}
	if err := validateBrowserUser(user); err != nil {
		return err
	}
	username = strings.TrimSpace(username)
	if err := validateBrowserUsername(username); err != nil {
		return err
	}
	if len(password) < minBrowserPassword || len(password) > maxBrowserPassword {
		return invalid(fmt.Sprintf("identity: browser password must be between %d and %d bytes",
			minBrowserPassword, maxBrowserPassword))
	}
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("identity: browser credential salt: %w", err)
	}
	hash := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)

	return s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO ahp_identity.browser_users
				(user_id, tenant, subject, display_name, email)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (user_id) DO UPDATE
				SET tenant = EXCLUDED.tenant,
				    subject = EXCLUDED.subject,
				    display_name = EXCLUDED.display_name,
				    email = EXCLUDED.email`,
			user.UserID, user.Tenant, user.Subject, user.DisplayName, user.Email); err != nil {
			return fmt.Errorf("identity: seed browser user: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO ahp_identity.browser_credentials
				(username, user_id, algo, salt, hash)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (username) DO UPDATE
				SET user_id = EXCLUDED.user_id,
				    algo = EXCLUDED.algo,
				    salt = EXCLUDED.salt,
				    hash = EXCLUDED.hash,
				    updated_at = clock_timestamp()`,
			username, user.UserID, argonAlgo, salt, hash); err != nil {
			return fmt.Errorf("identity: seed browser credential: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM ahp_identity.browser_grants WHERE user_id = $1`, user.UserID); err != nil {
			return fmt.Errorf("identity: seed browser grants: %w", err)
		}
		for kind, values := range map[string][]string{"role": roles, "permission": permissions} {
			seen := make(map[string]bool, len(values))
			for _, v := range values {
				v = strings.TrimSpace(v)
				if v == "" || seen[v] {
					continue
				}
				seen[v] = true
				if _, err := tx.Exec(ctx, `
					INSERT INTO ahp_identity.browser_grants (user_id, kind, value)
					VALUES ($1,$2,$3)`, user.UserID, kind, v); err != nil {
					return fmt.Errorf("identity: seed browser grant: %w", err)
				}
			}
		}
		return nil
	})
}

// dummySalt and dummyHash are derived once so that a login for an unknown
// username still pays the argon2id cost. Without them, an unknown name would
// answer measurably faster than a wrong password and the endpoint would
// enumerate accounts.
var dummySalt, dummyHash = func() (salt, hash []byte) {
	salt = make([]byte, argonSaltLen)
	copy(salt, "ahp-dummy-salt!!")
	return salt, argon2.IDKey([]byte("ahp-dummy-credential"), salt,
		argonTime, argonMemory, argonThreads, argonKeyLen)
}()

// AuthenticateBrowser checks a credential and returns the account it belongs
// to. The second result is false for an unknown username and for a wrong
// password alike, and the error is reserved for storage trouble, so the caller
// cannot tell the two cases apart — not by the response and not by timing,
// because the unknown-username path still runs the key derivation.
func (s *Store) AuthenticateBrowser(ctx context.Context, username, password string) (BrowserUser, bool, error) {
	if s.pool == nil {
		return BrowserUser{}, false, invalid("identity: store has no pool")
	}
	username = strings.TrimSpace(username)
	if username == "" || len(password) > maxBrowserPassword {
		// Still spend the derivation before answering.
		argon2.IDKey([]byte(password), dummySalt, argonTime, argonMemory, argonThreads, argonKeyLen)
		return BrowserUser{}, false, nil
	}
	var (
		user       BrowserUser
		algo       string
		salt, hash []byte
	)
	err := s.pool.QueryRow(ctx, `
		SELECT u.user_id, u.tenant, u.subject, u.display_name, u.email,
		       c.algo, c.salt, c.hash
		  FROM ahp_identity.browser_credentials c
		  JOIN ahp_identity.browser_users u ON u.user_id = c.user_id
		 WHERE c.username = $1`, username).
		Scan(&user.UserID, &user.Tenant, &user.Subject, &user.DisplayName, &user.Email,
			&algo, &salt, &hash)
	if isNoRows(err) {
		argon2.IDKey([]byte(password), dummySalt, argonTime, argonMemory, argonThreads, argonKeyLen)
		return BrowserUser{}, false, nil
	}
	if err != nil {
		return BrowserUser{}, false, fmt.Errorf("identity: browser credential lookup: %w", err)
	}
	if algo != argonAlgo || len(hash) != argonKeyLen || len(salt) < argonSaltLen {
		return BrowserUser{}, false, invalid("identity: stored credential is not argon2id")
	}
	candidate := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	if subtle.ConstantTimeCompare(candidate, hash) != 1 {
		return BrowserUser{}, false, nil
	}
	roles, perms, err := s.browserGrants(ctx, user.UserID)
	if err != nil {
		return BrowserUser{}, false, err
	}
	user.Roles, user.Permissions = roles, perms
	return user, true, nil
}

// BrowserUserBySubject reads the account a verified human principal refers to.
// A foreign scope misses the row and reports NOT_FOUND, so one tenant cannot
// discover whether a subject exists in another.
func (s *Store) BrowserUserBySubject(ctx context.Context, tenant, subject string) (BrowserUser, error) {
	if s.pool == nil {
		return BrowserUser{}, invalid("identity: store has no pool")
	}
	var u BrowserUser
	err := s.pool.QueryRow(ctx, `
		SELECT user_id, tenant, subject, display_name, email
		  FROM ahp_identity.browser_users
		 WHERE tenant = $1 AND subject = $2`, tenant, subject).
		Scan(&u.UserID, &u.Tenant, &u.Subject, &u.DisplayName, &u.Email)
	if isNoRows(err) {
		return BrowserUser{}, contracts.NewError(contracts.ErrNotFound, "identity: browser user not found")
	}
	if err != nil {
		return BrowserUser{}, fmt.Errorf("identity: browser user lookup: %w", err)
	}
	roles, perms, err := s.browserGrants(ctx, u.UserID)
	if err != nil {
		return BrowserUser{}, err
	}
	u.Roles, u.Permissions = roles, perms
	return u, nil
}

// StartBrowserSession opens a session for an authenticated account. Both
// deadlines are fixed here and the absolute one never moves again.
func (s *Store) StartBrowserSession(ctx context.Context, userID string) (BrowserSession, error) {
	if s.pool == nil {
		return BrowserSession{}, invalid("identity: store has no pool")
	}
	id, err := randomToken()
	if err != nil {
		return BrowserSession{}, err
	}
	var out BrowserSession
	err = s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		// A revoked subject cannot open a session: the account may exist and
		// its credential may be valid, and that is still not authority.
		var revoked bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM ahp_identity.subjects t
				 WHERE t.tenant = u.tenant AND t.subject = u.subject) OR EXISTS(SELECT 1 FROM ahp_identity.erased_tenants WHERE tenant=u.tenant)
			  FROM ahp_identity.browser_users u
			 WHERE u.user_id = $1`, userID).Scan(&revoked); err != nil {
			if isNoRows(err) {
				return contracts.NewError(contracts.ErrNotFound, "identity: browser user not found")
			}
			return fmt.Errorf("identity: session subject check: %w", err)
		}
		if revoked {
			return forbidden("identity: subject is revoked")
		}
		var subj, tenant string
		if err := tx.QueryRow(ctx, `
			INSERT INTO ahp_identity.browser_sessions
				(session_id, user_id, tenant, subject, expires_at, absolute_at)
			SELECT $1, u.user_id, u.tenant, u.subject,
			       clock_timestamp() + $2::interval,
			       clock_timestamp() + $3::interval
			  FROM ahp_identity.browser_users u
			 WHERE u.user_id = $4
			RETURNING tenant, subject, expires_at, absolute_at`,
			id, idleInterval(), absoluteInterval(), userID).
			Scan(&tenant, &subj, &out.ExpiresAt, &out.AbsoluteAt); err != nil {
			if isNoRows(err) {
				return contracts.NewError(contracts.ErrNotFound, "identity: browser user not found")
			}
			return fmt.Errorf("identity: session start: %w", err)
		}
		out = BrowserSession{ID: id, UserID: userID, Tenant: tenant, Subject: subj,
			Epoch: 1, ExpiresAt: out.ExpiresAt, AbsoluteAt: out.AbsoluteAt}
		return nil
	})
	if err != nil {
		return BrowserSession{}, err
	}
	return out, nil
}

// BrowserSession reads a live session and pushes its idle deadline forward.
// An unknown, revoked or expired session is NOT_FOUND: the session id is the
// only capability the browser holds, so a dead one is indistinguishable from a
// never-issued one.
func (s *Store) BrowserSession(ctx context.Context, sessionID string) (BrowserSession, error) {
	return s.touchBrowserSession(ctx, sessionID, false)
}

// RefreshBrowserSession rotates a live session: the epoch advances, so every
// access token minted under the previous epoch is retired, and the idle
// deadline moves forward. The absolute deadline is untouched, and a session
// that has reached it cannot be refreshed at all.
func (s *Store) RefreshBrowserSession(ctx context.Context, sessionID string) (BrowserSession, error) {
	return s.touchBrowserSession(ctx, sessionID, true)
}

func (s *Store) touchBrowserSession(ctx context.Context, sessionID string, rotate bool) (BrowserSession, error) {
	if s.pool == nil {
		return BrowserSession{}, invalid("identity: store has no pool")
	}
	if sessionID == "" {
		return BrowserSession{}, invalid("identity: session id must not be empty")
	}
	var out BrowserSession
	err := s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var (
			userID, tenant, subj  string
			epoch                 int64
			expiresAt, absoluteAt time.Time
		)
		err := tx.QueryRow(ctx, `
			UPDATE ahp_identity.browser_sessions
			   SET last_seen_at = clock_timestamp(),
			       expires_at   = LEAST(clock_timestamp() + $2::interval, absolute_at),
			       epoch        = CASE WHEN $3 THEN epoch + 1 ELSE epoch END
			 WHERE session_id = $1
			   AND revoked_at IS NULL
			   AND expires_at > clock_timestamp()
			   AND absolute_at > clock_timestamp()
			RETURNING user_id, tenant, subject, epoch, expires_at, absolute_at`,
			sessionID, idleInterval(), rotate).
			Scan(&userID, &tenant, &subj, &epoch, &expiresAt, &absoluteAt)
		if isNoRows(err) {
			return contracts.NewError(contracts.ErrNotFound, "identity: session not found")
		}
		if err != nil {
			return fmt.Errorf("identity: session touch: %w", err)
		}
		if rotate {
			if _, err := tx.Exec(ctx, `
				UPDATE ahp_identity.browser_token_bindings
				   SET revoked_at = clock_timestamp()
				 WHERE session_id = $1 AND epoch = $2 AND revoked_at IS NULL`,
				sessionID, epoch-1); err != nil {
				return fmt.Errorf("identity: retire previous epoch tokens: %w", err)
			}
		}
		out = BrowserSession{ID: sessionID, UserID: userID, Tenant: tenant, Subject: subj,
			Epoch: epoch, ExpiresAt: expiresAt, AbsoluteAt: absoluteAt}
		return nil
	})
	if err != nil {
		return BrowserSession{}, err
	}
	return out, nil
}

// RevokeBrowserSession ends one session and every access token it minted. The
// revocation is a row write checked against the database clock, so it stops
// operations without any cooperation from the browser holding the cookie.
func (s *Store) RevokeBrowserSession(ctx context.Context, sessionID string) error {
	if s.pool == nil {
		return invalid("identity: store has no pool")
	}
	return s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE ahp_identity.browser_sessions
			   SET revoked_at = clock_timestamp()
			 WHERE session_id = $1 AND revoked_at IS NULL`, sessionID)
		if err != nil {
			return fmt.Errorf("identity: session revoke: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return contracts.NewError(contracts.ErrNotFound, "identity: session not found")
		}
		return revokeBrowserTokensTx(ctx, tx, sessionID)
	})
}

// revokeBrowserSubjectsTx ends every session of a (tenant, subject) pair and
// retires the access tokens they minted. RevokeSubject calls it inside its own
// transaction, so a subject tombstone and the death of that subject's browser
// sessions become one atomic fact.
func revokeBrowserSubjectsTx(ctx context.Context, tx pgx.Tx, tenant, subject string) error {
	rows, err := tx.Query(ctx, `
		UPDATE ahp_identity.browser_sessions
		   SET revoked_at = clock_timestamp()
		 WHERE tenant = $1 AND subject = $2 AND revoked_at IS NULL
		RETURNING session_id`, tenant, subject)
	if err != nil {
		return fmt.Errorf("identity: revoke browser sessions: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("identity: revoke browser sessions scan: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("identity: revoke browser sessions rows: %w", err)
	}
	for _, id := range ids {
		if err := revokeBrowserTokensTx(ctx, tx, id); err != nil {
			return err
		}
	}
	return nil
}

func revokeBrowserTokensTx(ctx context.Context, tx pgx.Tx, sessionID string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE ahp_identity.browser_token_bindings
		   SET revoked_at = clock_timestamp()
		 WHERE session_id = $1 AND revoked_at IS NULL`, sessionID); err != nil {
		return fmt.Errorf("identity: revoke browser tokens: %w", err)
	}
	return nil
}

// BindBrowserToken records the jti of a minted browser access token against the
// session that produced it, so revocation can be observed on a credential that
// is still inside its own validity window. The token itself is never stored.
func (s *Store) BindBrowserToken(ctx context.Context, jti string, session BrowserSession) error {
	if s.pool == nil {
		return invalid("identity: store has no pool")
	}
	if jti == "" || session.ID == "" {
		return invalid("identity: token jti and session must not be empty")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO ahp_identity.browser_token_bindings
			(token_jti, session_id, epoch, expires_at)
		VALUES ($1,$2,$3, clock_timestamp() + $4::interval)`,
		jti, session.ID, session.Epoch, intervalLiteral(BrowserAccessTTL))
	if err != nil {
		if isUniqueViolation(err) {
			// A jti is 256 bits of CSPRNG output; a collision is a broken
			// generator, not something to paper over.
			return forbidden("identity: token identifier collision")
		}
		return fmt.Errorf("identity: bind browser token: %w", err)
	}
	return nil
}

// BrowserTokenActive reports whether a presented browser access token is still
// backed by a live session. The check reads the binding, its session and the
// subject tombstone against the database clock, so a logout, a refresh
// rotation or a subject revocation all answer false immediately.
func (s *Store) BrowserTokenActive(ctx context.Context, jti string) (BrowserSession, error) {
	if s.pool == nil {
		return BrowserSession{}, invalid("identity: store has no pool")
	}
	if jti == "" {
		return BrowserSession{}, invalid("identity: token jti must not be empty")
	}
	var out BrowserSession
	err := s.pool.QueryRow(ctx, `
		SELECT b.session_id, ses.user_id, ses.tenant, ses.subject, b.epoch,
		       b.expires_at, ses.absolute_at
		  FROM ahp_identity.browser_token_bindings b
		  JOIN ahp_identity.browser_sessions ses ON ses.session_id = b.session_id
		 WHERE b.token_jti = $1
		   AND b.revoked_at IS NULL
		   AND ses.revoked_at IS NULL
		   AND b.epoch = ses.epoch
		   AND b.expires_at > clock_timestamp()
		   AND ses.absolute_at > clock_timestamp()
		   AND NOT EXISTS (
		       SELECT 1 FROM ahp_identity.subjects t
		        WHERE t.tenant = ses.tenant AND t.subject = ses.subject)
 AND NOT EXISTS(SELECT 1 FROM ahp_identity.erased_tenants WHERE tenant=ses.tenant)`,
		jti).
		Scan(&out.ID, &out.UserID, &out.Tenant, &out.Subject, &out.Epoch,
			&out.ExpiresAt, &out.AbsoluteAt)
	if isNoRows(err) {
		return BrowserSession{}, contracts.NewError(contracts.ErrNotFound, "identity: token is not active")
	}
	if err != nil {
		return BrowserSession{}, fmt.Errorf("identity: browser token check: %w", err)
	}
	return out, nil
}

func (s *Store) browserGrants(ctx context.Context, userID string) (roles, permissions []string, err error) {
	rows, err := s.pool.Query(ctx, `
		SELECT kind, value FROM ahp_identity.browser_grants
		 WHERE user_id = $1 ORDER BY kind, value`, userID)
	if err != nil {
		return nil, nil, fmt.Errorf("identity: browser grants: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind, value string
		if err := rows.Scan(&kind, &value); err != nil {
			return nil, nil, fmt.Errorf("identity: browser grants scan: %w", err)
		}
		if kind == "role" {
			roles = append(roles, value)
		} else {
			permissions = append(permissions, value)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("identity: browser grants rows: %w", err)
	}
	return roles, permissions, nil
}

func validateBrowserUser(u BrowserUser) error {
	for _, f := range []struct {
		name, value string
	}{
		{"user id", u.UserID}, {"tenant", u.Tenant}, {"subject", u.Subject},
	} {
		if strings.TrimSpace(f.value) == "" {
			return invalid("identity: browser user " + f.name + " must not be empty")
		}
		if len(f.value) > 128 {
			return invalid("identity: browser user " + f.name + " exceeds 128 bytes")
		}
	}
	// The scope characters are the platform rule; reuse it rather than inventing
	// a second alphabet for accounts.
	if err := (contracts.Scope{Tenant: u.Tenant, Deployment: "x", Subject: u.Subject}).Validate(); err != nil {
		return invalid("identity: browser user scope is invalid: " + err.Error())
	}
	return nil
}

func validateBrowserUsername(username string) error {
	if username == "" || len(username) > 254 {
		return invalid("identity: username must be between 1 and 254 bytes")
	}
	return nil
}

func randomToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("identity: random token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// interval literals are built from durations in microseconds so PostgreSQL, not
// the application clock, decides every deadline.
func idleInterval() string     { return intervalLiteral(BrowserSessionIdleTTL) }
func absoluteInterval() string { return intervalLiteral(BrowserSessionAbsoluteTTL) }

func intervalLiteral(d time.Duration) string {
	return fmt.Sprintf("%d microseconds", int64(d/time.Microsecond))
}
