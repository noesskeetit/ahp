package registry

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DeletionTarget names a versioned custom-data owner. Current installations and
// older task-pinned versions both participate; rollback must not hide old data.
type DeletionTarget struct {
	Installation string `json:"installation"`
	Deployment   string `json:"deployment"`
	Environment  string `json:"environment"`
	Agent        string `json:"agent"`
	Version      string `json:"version"`
	Digest       string `json:"digest"`
	Hook         string `json:"hook"`
}

func (s *Store) DeletionTargets(ctx context.Context, tenant string) ([]DeletionTarget, error) {
	rows, err := s.pool.Query(ctx, `WITH selected AS (
 SELECT id,tenant,agent_id,deployment,environment,version_id FROM ahp_registry.installations WHERE tenant=$1
 UNION SELECT i.id,i.tenant,i.agent_id,i.deployment,i.environment,p.version_id FROM ahp_registry.installations i JOIN ahp_registry.task_pins p ON p.installation_id=i.id AND p.tenant=i.tenant WHERE i.tenant=$1
 ) SELECT i.id,i.deployment,i.environment,i.agent_id,v.id,v.digest,v.manifest FROM selected i JOIN ahp_registry.versions v ON v.id=i.version_id ORDER BY i.id,v.id`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeletionTarget
	for rows.Next() {
		var d DeletionTarget
		var raw []byte
		if err = rows.Scan(&d.Installation, &d.Deployment, &d.Environment, &d.Agent, &d.Version, &d.Digest, &raw); err != nil {
			return nil, err
		}
		var manifest Manifest
		if err = json.Unmarshal(raw, &manifest); err != nil {
			return nil, err
		}
		if !manifest.State.OwnTables {
			continue
		}
		if manifest.State.DeleteHook == "" {
			return nil, contracts.NewError(contracts.ErrUnavailable, "registry: custom data owner has no deletion hook")
		}
		d.Hook = manifest.State.DeleteHook
		out = append(out, d)
	}
	return out, rows.Err()
}
