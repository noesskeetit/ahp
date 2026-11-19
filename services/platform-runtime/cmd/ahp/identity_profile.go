package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func (s *server) identityStore() *identity.Store { return s.identityStoreFor(s.pool) }
func (s *server) identityStoreFor(pool *pgxpool.Pool) *identity.Store {
	evaluator := s.iam
	if _, ok := evaluator.(*identity.SyntheticResourcePolicy); ok {
		evaluator = identity.NewSyntheticResourcePolicy(pool)
	}
	return identity.New(pool, identity.WithResourceEvaluator(evaluator))
}

// Configuration selects the authority explicitly. It never provisions allow
// rules on server startup: a restart must not undo an operator's revocation.
func (s *server) configureIdentityProfile() error {
	switch envOr("AHP_IAM_PROFILE", "local") {
	case "local":
	case "synthetic":
		s.iam = identity.NewSyntheticResourcePolicy(s.pool)
	default:
		return fmt.Errorf("AHP_IAM_PROFILE must be local or synthetic")
	}
	profile := envOr("AHP_AUTH_PROFILE", "local")
	if profile == "local" {
		if s.iam != nil {
			s.authenticator = identity.NewResourceAuthenticator(identity.NewLocalAuthenticator(s.identityStore()), s.iam)
		}
		return nil
	}
	if profile != "oidc" && profile != "synthetic-oidc" {
		return fmt.Errorf("unknown AHP_AUTH_PROFILE")
	}
	if s.iam == nil {
		return fmt.Errorf("OIDC profile requires an explicitly configured resource IAM authority")
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return err
	}
	if path := envOr("AHP_OIDC_CA_FILE", ""); path != "" {
		pem, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("OIDC CA unavailable")
		}
		if !roots.AppendCertsFromPEM(pem) {
			return fmt.Errorf("invalid OIDC CA")
		}
	}
	client := &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
	a, err := identity.NewOIDCAuthenticator(identity.OIDCConfig{Issuer: envOr("AHP_OIDC_ISSUER", ""), DiscoveryURL: envOr("AHP_OIDC_DISCOVERY_URL", ""), ClientID: envOr("AHP_OIDC_AUDIENCE", ""), TenantClaim: envOr("AHP_OIDC_TENANT_CLAIM", "tenant"), HTTPClient: client, Synthetic: profile == "synthetic-oidc"})
	if err != nil {
		return err
	}
	s.authenticator = identity.NewResourceAuthenticator(a, s.iam)
	return nil
}

// provisionSyntheticIAM is an operator CLI; it is not mounted on HTTP.
// Every entry carries an expected revision, so a repeated/stale provisioning
// command cannot silently widen already changed permissions.
func (s *server) provisionSyntheticIAM(ctx context.Context) error {
	if envOr("AHP_IAM_PROFILE", "") != "synthetic" {
		return fmt.Errorf("synthetic IAM profile is required")
	}
	st := s.identityStore()
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	path := envOr("AHP_SYNTHETIC_IAM_RULES_FILE", "")
	if path == "" {
		return fmt.Errorf("AHP_SYNTHETIC_IAM_RULES_FILE is required")
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("IAM policy file unavailable")
	}
	defer f.Close()
	var entries []struct {
		identity.ResourceRule
		ExpectedRevision int64 `json:"expected_revision"`
	}
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return contracts.NewError(contracts.ErrInvalid, "invalid IAM policy document")
	}
	if _, err := contracts.Digest(json.RawMessage(raw)); err != nil {
		return contracts.NewError(contracts.ErrInvalid, "invalid IAM policy document")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&entries); err != nil {
		return contracts.NewError(contracts.ErrInvalid, "invalid IAM policy document")
	}
	p := identity.NewSyntheticResourcePolicy(s.pool)
	for _, entry := range entries {
		if _, err := p.PutRule(ctx, entry.ResourceRule, entry.ExpectedRevision); err != nil {
			return err
		}
	}
	s.logger.Info("synthetic IAM policies provisioned", "count", len(entries))
	return nil
}
