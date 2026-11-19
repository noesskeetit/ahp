package state

import "context"

const DeploymentV6Format = "ahp-state-deployment/6"
const DeploymentV6Native = "ahp-state-common-owner/research-6"
const DeploymentV6Profile = "offline-same-deployment-clean-state-db-shared-artifacts-retention/1"

type DeploymentV6 = DeploymentV4
type DeploymentV6Receipt = DeploymentV4Receipt

var deploymentV6Entities = func() []scopeEntity {
	out := append([]scopeEntity{}, deploymentV5Entities...)
	return append(out,
		scopeEntity{"common_retention_floors", "thread_id,namespace,readable_from", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace", false},
		scopeEntity{"common_retention_roots", "thread_id,namespace,key,version,revision", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,key", false},
		scopeEntity{"common_retention_ops", "thread_id,namespace,key,kind,digest,result", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,key", false})
}()

func (s *Store) ExportDeploymentV6(ctx context.Context, u DeploymentUnit, id, build string) (DeploymentV6, error) {
	return s.exportDeploymentIntervals(ctx, u, id, build, 6)
}
func (s *Store) ImportDeploymentV6(ctx context.Context, u DeploymentUnit, image DeploymentV6, build string) (DeploymentV6Receipt, error) {
	return s.importDeploymentIntervals(ctx, u, image, build, 6)
}
