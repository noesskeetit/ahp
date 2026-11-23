package tools

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// FenceSubject closes effect admission and drains OAuth I/O before inventory.
func (s *Service) FenceSubject(ctx context.Context, tenant, subject string) error {
	if err := s.ledger.FenceSubject(ctx, tenant, subject); err != nil {
		return err
	}
	if s.cfg.OAuth != nil {
		return s.cfg.OAuth.FenceSubject(ctx, tenant, subject)
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", s.schema+".oauth_secrets").Scan(&exists); err != nil {
		return err
	}
	if exists {
		return contracts.NewError(contracts.ErrUnavailable, "tools: OAuth owner configuration required for subject cleanup")
	}
	return nil
}

// DeleteSubject retains unsettled receipts. Shared operator policy survives,
// but the erased subject cannot grant it and its actor attribution is removed.
func (s *Service) DeleteSubject(ctx context.Context, tenant, subject string) error {
	if err := s.FenceSubject(ctx, tenant, subject); err != nil {
		return err
	}
	var pending bool
	if err := s.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+s.schema+".operations WHERE tenant=$1 AND subject=$2 AND NOT terminal)", tenant, subject).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return contracts.NewReasoned(contracts.ErrUnavailable, "subject_effects_pending", "tools: effect outcome pending")
	}
	if s.cfg.OAuth != nil {
		if err := s.cfg.OAuth.DeleteSubject(ctx, tenant, subject); err != nil {
			return err
		}
	}
	return s.ledger.DeleteSubjectChecked(ctx, tenant, subject, func(ctx context.Context, tx pgx.Tx) error {
		for _, table := range []string{"approvals", "guardian_log"} {
			if _, err := tx.Exec(ctx, "DELETE FROM "+s.schema+"."+table+" WHERE tenant=$1 AND subject=$2", tenant, subject); err != nil {
				return err
			}
		}
		// A user's signature cannot continue authorizing another user's operation.
		if _, err := tx.Exec(ctx, "UPDATE "+s.schema+".approvals SET approved_by=NULL,approved_at=NULL WHERE tenant=$1 AND approved_by=$2", tenant, subject); err != nil {
			return err
		}
		if s.policy != nil {
			if _, err := tx.Exec(ctx, "UPDATE "+s.schema+".tool_policy SET created_by='[erased]' WHERE tenant=$1 AND created_by=$2", tenant, subject); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "DELETE FROM "+s.schema+".tool_policy_audit WHERE tenant=$1 AND changed_by=$2", tenant, subject); err != nil {
				return err
			}
		}
		return s.deleteToolLeaseRows(ctx, tx, tenant, subject)
	})
}

// FenceSubject takes the same tenant-exclusive gate as deletion, waiting for
// already admitted refresh/callback calls including their Vault writes. The
// permanent subject tombstone denies every later personal lifecycle admission.
func (m *OAuthManager) FenceSubject(ctx context.Context, tenant, subject string) error {
	if err := (contracts.Scope{Tenant: tenant, Subject: subject, Deployment: "control"}).Validate(); err != nil {
		return err
	}
	c, release, err := m.lock(ctx, tenant, "", "", true)
	if err != nil {
		return err
	}
	defer release()
	_, err = c.Exec(ctx, "INSERT INTO "+m.schema+".oauth_subjects VALUES($1,$2) ON CONFLICT DO NOTHING", tenant, subject)
	return err
}

// DeleteSubject revokes known remote grants and removes every Vault version
// before deleting its durable inventory. A failed provider/Vault call is retryable.
func (m *OAuthManager) DeleteSubject(ctx context.Context, tenant, subject string) error {
	if err := m.FenceSubject(ctx, tenant, subject); err != nil {
		return err
	}
	c, release, err := m.lock(ctx, tenant, "", "", true)
	if err != nil {
		return err
	}
	defer release()
	var unknown bool
	if err := c.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+m.schema+".oauth_requests WHERE tenant=$1 AND subject=$2 AND status='exchanging') OR EXISTS(SELECT 1 FROM "+m.schema+".oauth_connections WHERE tenant=$1 AND subject=$2 AND status='refresh_unknown')", tenant, subject).Scan(&unknown); err != nil {
		return err
	}
	if unknown {
		return contracts.NewReasoned(contracts.ErrUnavailable, "oauth_outcome_unknown", "oauth: provider grant outcome requires reconciliation before erasure")
	}
	rows, err := c.Query(ctx, "SELECT connection,path FROM "+m.schema+".oauth_secrets WHERE tenant=$1 AND subject=$2 ORDER BY path", tenant, subject)
	if err != nil {
		return err
	}
	type item struct{ connection, path string }
	var paths []item
	for rows.Next() {
		var v item
		if err = rows.Scan(&v.connection, &v.path); err != nil {
			rows.Close()
			return err
		}
		paths = append(paths, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, v := range paths {
		if strings.HasSuffix(v.path, "-tokens") {
			data, _, readErr := m.cfg.Secrets.ReadVersion(ctx, v.path)
			if readErr == nil {
				p, ok := m.providers[v.connection]
				if !ok {
					return contracts.NewError(contracts.ErrUnavailable, "oauth: recorded provider unavailable for erasure")
				}
				if p.config.RevokeURL != "" {
					secret, e := m.clientSecret(ctx, p)
					if e != nil {
						return e
					}
					for _, kind := range []string{"refresh_token", "access_token"} {
						if token, _ := data[kind].(string); token != "" {
							if _, e = p.request(ctx, p.config.RevokeURL, url.Values{"token": {token}, "token_type_hint": {kind}}, secret, false); e != nil {
								return e
							}
						}
					}
				}
			} else if !errors.Is(readErr, secretstore.ErrSecretNotFound) {
				return readErr
			}
		}
		if err = m.cfg.Secrets.DeleteMetadata(ctx, v.path); err != nil {
			return err
		}
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, table := range []string{"oauth_secrets", "oauth_requests", "oauth_connections"} {
		if _, err = tx.Exec(ctx, "DELETE FROM "+m.schema+"."+table+" WHERE tenant=$1 AND subject=$2", tenant, subject); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
