package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/triggers"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The trigger names a tenant installation, never a caller-supplied endpoint.
// Each fire pins version/config before routing and delegates to that exact
// runtime workload. Unknown targets fail closed; there is no core fallback.
type registryTriggerDispatcher struct {
	store      *registry.Store
	dispatcher registry.RunDispatcher
}

func (s *server) registryTriggerDispatcher(ctx context.Context, ids *identity.Store, verifier *auth.Verifier, signer *auth.Signer) (triggers.Dispatcher, error) {
	store := registry.New(s.pool)
	if err := store.Migrate(ctx); err != nil {
		return nil, err
	}
	authority := runtimeAuthority{ids: ids, registry: store, verifier: verifier, signer: signer}
	var err error
	authority.budgets, err = newRuntimeBudgetAdmitter(ctx, s.pool, ids)
	if err != nil {
		return nil, err
	}
	cert := envOr("AHP_TRIGGERS_CLIENT_CERT", envOr("AHP_RUNTIME_CLIENT_CERT", ""))
	key := envOr("AHP_TRIGGERS_CLIENT_KEY", envOr("AHP_RUNTIME_CLIENT_KEY", ""))
	ca := envOr("AHP_TRIGGERS_CLIENT_CA", envOr("AHP_RUNTIME_CLIENT_CA", ""))
	dispatcher, err := registry.NewTLSRoutedA2AInvokeDispatcher(store, cert, key, ca)
	if err != nil {
		return nil, err
	}
	dispatcher.SetRuntimeTokenIssuer(authority)
	return &registryTriggerDispatcher{store: store, dispatcher: dispatcher}, nil
}

func (d *registryTriggerDispatcher) Fire(ctx context.Context, scope contracts.Scope, req triggers.FireRequest) (triggers.FireResult, error) {
	refused := func(err error) (triggers.FireResult, error) {
		return triggers.FireResult{}, fmt.Errorf("%w: %w", triggers.ErrDispatchRefused, err)
	}
	if err := scope.Validate(); err != nil {
		return refused(err)
	}
	if req.TriggerID == "" || req.Sequence < 1 || req.Token == "" {
		return refused(contracts.NewError(contracts.ErrInvalid, "triggers: fire identity and credential required"))
	}
	target := strings.TrimPrefix(strings.TrimSpace(req.Target), "installation:")
	installation, err := d.store.GetInstallation(ctx, scope.Tenant, target)
	if err != nil {
		return refused(err)
	}
	taskID := triggers.FireMessageID(req.TriggerID, req.Sequence)
	if _, err = d.store.PinForSubject(ctx, scope, installation.ID, taskID); err != nil {
		return refused(err)
	}
	pinned, err := d.store.ResolveConfig(ctx, scope.Tenant, installation.ID, taskID)
	if err != nil {
		return refused(err)
	}
	id, err := d.dispatcher.Invoke(ctx, registry.InvokeRequest{
		Scope: scope, Tenant: scope.Tenant, Deployment: scope.Deployment, TaskID: taskID,
		AgentID: pinned.AgentID, VersionID: pinned.VersionID, VersionDigest: pinned.Digest,
		InstallationID: installation.ID, Environment: pinned.Environment, Capabilities: pinned.Capabilities,
		Config: pinned.Config, ConfigRevision: pinned.ConfigRevision, Input: req.Payload, Token: req.Token,
	})
	if err != nil {
		// The runtime dispatcher may have sent a request before returning an error.
		// Preserve ambiguity: an error must never authorize automatic redelivery.
		return triggers.FireResult{}, err
	}
	if err = d.store.AliasTaskPin(ctx, scope.Tenant, installation.ID, taskID, id); err != nil {
		return triggers.FireResult{}, err
	}
	detail, _ := json.Marshal(map[string]any{"installation_id": installation.ID, "version_id": pinned.VersionID, "version_digest": pinned.Digest, "config_revision": pinned.ConfigRevision})
	return triggers.FireResult{TaskID: id, Detail: detail}, nil
}
