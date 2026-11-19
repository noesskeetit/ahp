//go:build ahp_exec && ahp_data

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Provider names must match the operator's existing Model routing table. Files
// support is opt-in per provider; a chat endpoint never implies this capability.
type modelFilesConfig struct {
	Providers []struct {
		Name         string   `json:"name"`
		BaseURL      string   `json:"base_url"`
		Account      string   `json:"account"`
		SecretPath   string   `json:"secret_path"`
		AllowedHosts []string `json:"allowed_hosts"`
		PrivateHosts []string `json:"private_hosts"`
	} `json:"providers"`
	TTLSeconds     int `json:"ttl_seconds"`
	TimeoutSeconds int `json:"timeout_seconds"`
}

func (s *server) configureModelFiles(ctx context.Context, b *execbridge.Bridge, ids *identity.Store) error {
	raw := strings.TrimSpace(os.Getenv("AHP_MODEL_FILES_CONFIG"))
	if raw == "" {
		return nil
	}
	var cfg modelFilesConfig
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if len(raw) > 64<<10 || dec.Decode(&cfg) != nil || len(cfg.Providers) == 0 || len(cfg.Providers) > 16 {
		return errors.New("model files: invalid bounded operator configuration")
	}
	vault, err := secretstore.NewFromEnv()
	if err != nil {
		return err
	}
	providers := map[string]model.FileProvider{}
	for _, row := range cfg.Providers {
		if row.Name == "" || providers[row.Name] != nil || row.SecretPath == "" {
			return errors.New("model files: unique provider and Vault path required")
		}
		readCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		secret, e := vault.Read(readCtx, row.SecretPath)
		stop()
		if e != nil {
			return errors.New("model files: provider Vault secret unavailable")
		}
		key, _ := secret["api_key"].(string)
		provider, e := model.NewHTTPFileProvider(model.HTTPFileConfig{BaseURL: row.BaseURL, Account: row.Account, APIKey: key, Egress: netpolicy.Policy{AllowedHosts: row.AllowedHosts, PrivateHosts: row.PrivateHosts, Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second}})
		if e != nil {
			return e
		}
		providers[row.Name] = provider
	}
	source, _, _, err := s.newDataPlanes(ctx, artifacts.WithGrantLabels(func(ctx context.Context, scope contracts.Scope) (labels.Set, error) {
		p, ok := transport.FromContext(ctx)
		if !ok || p.Scope != scope {
			return labels.Set{}, contracts.NewError(contracts.ErrForbidden, "model files: verified artifact reader required")
		}
		grant, e := ids.Evaluate(ctx, scope, p.Claims.Task, "model")
		if e != nil {
			return labels.Set{}, e
		}
		if grant.Context != p.Claims.Context {
			return labels.Set{}, contracts.NewError(contracts.ErrForbidden, "model files: context mismatch")
		}
		return grant.Labels, nil
	}))
	if err != nil {
		return err
	}
	if err = source.Migrate(ctx); err != nil {
		return err
	}
	files, err := model.NewFileService(ctx, s.pool, model.FileConfig{Source: source, Providers: providers, TTL: time.Duration(cfg.TTLSeconds) * time.Second, Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second,
		Authorize: func(ctx context.Context, a model.FileActor) error {
			g, e := ids.Evaluate(ctx, a.Scope, a.Task, "model")
			if e != nil {
				return e
			}
			if g.Context != a.Context {
				return contracts.NewError(contracts.ErrForbidden, "model files: context mismatch")
			}
			return nil
		},
		AddLabels: func(ctx context.Context, a model.FileActor, set labels.Set) error {
			_, e := ids.AddLabels(ctx, a.Scope, a.Task, set)
			return e
		}, Route: b.RouteFileProvider,
	})
	if err != nil {
		return err
	}
	b.WithProviderFiles(files)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweepCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
				count, e := files.Sweep(sweepCtx, 100)
				cancel()
				if e != nil {
					s.logger.Warn("Model provider copy cleanup pending", "deleted", count)
				}
			}
		}
	}()
	return nil
}
