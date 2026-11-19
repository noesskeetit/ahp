package state

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const DeploymentV5Format = "ahp-state-deployment/5"
const DeploymentV5Native = "ahp-state-common-owner/research-5"
const DeploymentV5Profile = "offline-same-deployment-clean-state-db-shared-artifacts-intervals/1"

// Shape-compatible carrier, explicitly versioned manifest and validation. Cache
// identity is common across formats: a migration ID cannot be re-sealed as V4.
type DeploymentV5 = DeploymentV4
type DeploymentV5Receipt = DeploymentV4Receipt

func deploymentV5Digest(image DeploymentV5) (string, error) { return deploymentV4Digest(image) }

var deploymentV5Entities = func() []scopeEntity {
	out := append([]scopeEntity{}, deploymentV4Entities...)
	return append(out,
		scopeEntity{"common_record_versions", "thread_id,namespace,id,from_revision,to_revision,kind,native_id,sequence,owner_hint,presence,attributes_hash", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,id,from_revision", false},
		scopeEntity{"common_role_versions", "thread_id,namespace,source,role,from_revision,to_revision", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,source,role,from_revision", false},
		scopeEntity{"common_edge_versions", "thread_id,namespace,source,role,ordinal,target,from_revision,to_revision", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,source,role,ordinal,from_revision", false},
		scopeEntity{"common_version_roots", "thread_id,namespace,revision,digest", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,revision", false})
}()

func (s *Store) ExportDeploymentV5(ctx context.Context, u DeploymentUnit, id, build string) (DeploymentV5, error) {
	return s.exportDeploymentIntervals(ctx, u, id, build, 5)
}
func (s *Store) exportDeploymentIntervals(ctx context.Context, u DeploymentUnit, id, build string, version int) (DeploymentV5, error) {
	retained := version >= 6
	specs, format, native, profile, validate := deploymentV5Entities, DeploymentV5Format, DeploymentV5Native, DeploymentV5Profile, ValidateDeploymentV5
	if retained {
		specs, format, native, profile, validate = deploymentV6Entities, DeploymentV6Format, DeploymentV6Native, DeploymentV6Profile, ValidateDeploymentV6
		if e := s.retentionAvailable(); e != nil {
			return DeploymentV5{}, e
		}
	}
	if version == 7 {
		if !s.commonIncrementalAppend {
			return DeploymentV5{}, contracts.NewError(contracts.ErrUnavailable, "incremental common transfer is not configured")
		}
		specs, format, native, profile, validate = deploymentV7Entities, DeploymentV7Format, DeploymentV7Native, DeploymentV7Profile, ValidateDeploymentV7
	}
	cfg, e := s.sharedArtifactMount()
	if e != nil {
		return DeploymentV5{}, e
	}
	if !s.commonRecordVersions {
		return DeploymentV5{}, contracts.NewError(contracts.ErrUnavailable, "interval common storage must be configured")
	}
	if cfg.Exports == nil {
		return DeploymentV5{}, contracts.NewError(contracts.ErrUnavailable, "durable source export cache must be configured")
	}
	if s.deploymentTransfer.Authorize == nil {
		return DeploymentV5{}, contracts.NewError(contracts.ErrForbidden, "deployment transfer requires trusted offline coordinator")
	}
	if build == "" || build != s.deploymentTransfer.NativeBuild {
		return DeploymentV5{}, invalid("owner build not admitted")
	}
	if e = u.Validate(); e != nil {
		return DeploymentV5{}, e
	}
	if e = validIdentifier(id, "migration id"); e != nil {
		return DeploymentV5{}, e
	}
	var here string
	if e = s.pool.QueryRow(ctx, `SELECT id FROM ahp_state.scope_store_v2 WHERE singleton`).Scan(&here); e != nil {
		return DeploymentV5{}, e
	}
	key := DeploymentExportKey{Unit: u, MigrationID: id, SourceStoreID: here}
	cached, e := cfg.Exports.Load(ctx, key)
	if e != nil {
		return DeploymentV5{}, e
	}
	if cached != nil {
		if retained {
			if e = s.deploymentRetentionConfig(cached.State); e != nil {
				return DeploymentV5{}, e
			}
		}
		if cached.State.Manifest.SourceStoreID != here || cached.State.Manifest.MigrationID != id {
			return DeploymentV5{}, contracts.NewError(contracts.ErrKeyReuse, "cached export belongs to a different migration")
		}
		refs, e := validate(*cached, u, build)
		if e != nil {
			return DeploymentV5{}, e
		}
		if e = s.inTx(ctx, func(tx pgx.Tx) error { return deploymentExportControls(ctx, tx, u) }); e != nil {
			return DeploymentV5{}, e
		}
		if e = s.verifySharedArtifacts(ctx, *cached, refs, "after-export", here); e != nil {
			return DeploymentV5{}, e
		}
		if e = s.deploymentCustody(ctx, u, id, here, cached.Digest, "after-export"); e != nil {
			return DeploymentV5{}, e
		}
		return *cached, nil
	}
	data, e := s.exportDeploymentState(ctx, u, id, build, specs)
	if e != nil {
		return DeploymentV5{}, e
	}
	data.Manifest.Format = format
	data.Manifest.Native = native
	data.Manifest.Profile = profile
	data.Digest, e = deploymentDigest(data)
	if e != nil {
		return DeploymentV5{}, e
	}
	image := DeploymentV5{State: data, ArtifactOwnerID: cfg.OwnerID}
	image.Digest, e = deploymentV5Digest(image)
	if e != nil {
		return DeploymentV5{}, e
	}
	refs, e := validate(image, u, build)
	if e != nil {
		return DeploymentV5{}, e
	}
	if retained {
		if e = s.deploymentRetentionConfig(data); e != nil {
			return DeploymentV5{}, e
		}
	}
	if e = s.deploymentPolicyConfig(data); e != nil {
		return DeploymentV5{}, e
	}
	if e = s.verifySharedArtifacts(ctx, image, refs, "after-export", data.Manifest.SourceStoreID); e != nil {
		return DeploymentV5{}, e
	}
	if e = cfg.Exports.Store(ctx, key, image); e != nil {
		return DeploymentV5{}, e
	}
	if e = s.deploymentCustody(ctx, u, id, data.Manifest.SourceStoreID, image.Digest, "after-export"); e != nil {
		return DeploymentV5{}, e
	}
	return image, nil
}
func (s *Store) ImportDeploymentV5(ctx context.Context, u DeploymentUnit, image DeploymentV5, build string) (DeploymentV5Receipt, error) {
	return s.importDeploymentIntervals(ctx, u, image, build, 5)
}
func (s *Store) importDeploymentIntervals(ctx context.Context, u DeploymentUnit, image DeploymentV5, build string, version int) (DeploymentV5Receipt, error) {
	retained := version >= 6
	specs, validate := deploymentV5Entities, ValidateDeploymentV5
	if retained {
		specs, validate = deploymentV6Entities, ValidateDeploymentV6
		if e := s.deploymentRetentionConfig(image.State); e != nil {
			return DeploymentV5Receipt{}, e
		}
	}
	if version == 7 {
		if !s.commonIncrementalAppend {
			return DeploymentV5Receipt{}, contracts.NewError(contracts.ErrUnavailable, "incremental common transfer is not configured")
		}
		specs, validate = deploymentV7Entities, ValidateDeploymentV7
	}
	var out DeploymentV5Receipt
	if !s.commonRecordVersions {
		return out, contracts.NewError(contracts.ErrUnavailable, "interval common storage must be configured")
	}
	if _, e := s.sharedArtifactMount(); e != nil {
		return out, e
	}
	refs, e := validate(image, u, build)
	if e != nil {
		return out, e
	}
	var here string
	if e = s.pool.QueryRow(ctx, `SELECT id FROM ahp_state.scope_store_v2 WHERE singleton`).Scan(&here); e != nil {
		return out, e
	}
	if e = s.verifySharedArtifacts(ctx, image, refs, "before-import", here); e != nil {
		return out, e
	}
	r, e := s.importDeploymentState(ctx, u, image.State, build, specs, image.Digest, func(c context.Context) error { return s.verifySharedArtifacts(c, image, refs, "before-commit", here) })
	if e != nil {
		return out, e
	}
	out.DeploymentV3Receipt = r
	out.ArtifactOwnerID = image.ArtifactOwnerID
	return out, nil
}
