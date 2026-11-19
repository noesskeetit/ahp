package registry

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// Normal polling is a read of the already pinned snapshot. ResolveConfig also
// applies rollout/drain transitions and locks the installation; doing that on
// every poll serializes all active conversations on one installation row.
// Enter that existing transition path only when the committed version moved.
func (s *Store) agentTurnSnapshot(ctx context.Context, scope contracts.Scope, installation, pin string) (EffectiveConfig, error) {
	var out EffectiveConfig
	var currentVersion string
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err := checkSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		var manifest, capabilities, config []byte
		var revision *int64
		err := tx.QueryRow(ctx, `SELECT v.agent_id,v.id,v.digest,v.manifest,i.environment,i.capabilities,p.config_data,p.config_revision,i.version_id
 FROM ahp_registry.task_pins p JOIN ahp_registry.installations i ON i.id=p.installation_id
 JOIN ahp_registry.versions v ON v.id=p.version_id
 WHERE p.tenant=$1 AND p.subject=$2 AND p.task_id=$3 AND p.installation_id=$4`, scope.Tenant, scope.Subject, pin, installation).Scan(&out.AgentID, &out.VersionID, &out.Digest, &manifest, &out.Environment, &capabilities, &config, &revision, &currentVersion)
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "agent turn snapshot not found")
		}
		if err != nil {
			return storageError("read pinned agent snapshot", err)
		}
		if revision == nil || len(config) == 0 || json.Unmarshal(manifest, &out.Manifest) != nil || json.Unmarshal(capabilities, &out.Capabilities) != nil {
			return contracts.NewError(contracts.ErrConflict, "agent turn has an invalid pinned snapshot")
		}
		if err = requireGrantedSubset(out.Manifest.Capabilities, out.Capabilities); err != nil {
			return contracts.NewError(contracts.ErrConflict, "stored grant exceeds the pinned manifest ceiling")
		}
		out.Tenant, out.Installation, out.Task = scope.Tenant, installation, pin
		out.Config, out.ConfigRevision = config, *revision
		out.SharesState = len(out.Manifest.State.Shared) > 0
		return nil
	})
	if err != nil {
		return EffectiveConfig{}, err
	}
	if currentVersion != out.VersionID {
		return s.ResolveConfig(ctx, scope.Tenant, installation, pin)
	}
	return out, nil
}
