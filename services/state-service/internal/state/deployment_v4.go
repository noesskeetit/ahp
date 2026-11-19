package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"io"
)

const DeploymentV4Format = "ahp-state-deployment/4"
const DeploymentV4Native = "ahp-state-common-owner/research-4"
const DeploymentV4Profile = "offline-same-deployment-clean-state-db-shared-artifacts/1"
const MaxDeploymentV4Artifacts = 256
const MaxDeploymentV4ArtifactBytes int64 = 64 << 20

// This profile migrates State while retaining this SAME independent Artifact
// owner. It does not copy S3 objects or rewrite immutable native references.
type SharedArtifactTransfer struct {
	OwnerID string
	Verify  func(context.Context, SharedArtifactCheck) error
	Exports DeploymentExportCache
}
type SharedArtifactCheck struct {
	DeploymentCustody
	StoreID    string                          `json:"state_owner_store_id"`
	OwnerID    string                          `json:"artifact_owner_id"`
	References []references.CheckpointArtifact `json:"references"`
}
type DeploymentV4 struct {
	State           DeploymentV3 `json:"state"`
	ArtifactOwnerID string       `json:"artifact_owner_id"`
	Digest          string       `json:"digest"`
}
type DeploymentV4Receipt struct {
	DeploymentV3Receipt
	ArtifactOwnerID string `json:"artifact_owner_id"`
}

var deploymentV4Entities = func() []scopeEntity {
	out := append([]scopeEntity{}, deploymentV3Entities...)
	return append(out, scopeEntity{"common_artifact_refs", "thread_id,namespace,revision,record_id,artifact", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,revision,record_id", false})
}()

func deploymentV4Digest(image DeploymentV4) (string, error) {
	image.Digest = ""
	return contracts.Digest(image)
}
func (s *Store) sharedArtifactMount() (*SharedArtifactTransfer, error) {
	cfg := s.deploymentTransfer.SharedArtifacts
	if cfg == nil || cfg.OwnerID == "" || cfg.Verify == nil || !s.commonArtifactReferences || s.checkpointArtifacts == nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "shared Artifact owner and common reference guard must be configured")
	}
	if e := validIdentifier(cfg.OwnerID, "shared Artifact owner"); e != nil {
		return nil, e
	}
	return cfg, nil
}
func (s *Store) verifySharedArtifacts(ctx context.Context, image DeploymentV4, refs []references.CheckpointArtifact, phase, storeID string) error {
	cfg, e := s.sharedArtifactMount()
	if e != nil {
		return e
	}
	if image.ArtifactOwnerID != cfg.OwnerID {
		return contracts.NewError(contracts.ErrForbidden, "archive requires a different Artifact owner")
	}
	m := image.State.Manifest
	return cfg.Verify(ctx, SharedArtifactCheck{DeploymentCustody: DeploymentCustody{Unit: m.Unit, MigrationID: m.MigrationID, SourceStoreID: m.SourceStoreID, Digest: image.Digest, Phase: phase}, StoreID: storeID, OwnerID: cfg.OwnerID, References: refs})
}
func (s *Store) ExportDeploymentV4(ctx context.Context, u DeploymentUnit, id, build string) (DeploymentV4, error) {
	cfg, e := s.sharedArtifactMount()
	if e != nil {
		return DeploymentV4{}, e
	}
	if cfg.Exports == nil {
		return DeploymentV4{}, contracts.NewError(contracts.ErrUnavailable, "durable source export cache must be configured")
	}
	if s.deploymentTransfer.Authorize == nil {
		return DeploymentV4{}, contracts.NewError(contracts.ErrForbidden, "deployment transfer requires trusted offline coordinator")
	}
	if build == "" || build != s.deploymentTransfer.NativeBuild {
		return DeploymentV4{}, invalid("owner build not admitted")
	}
	if e = u.Validate(); e != nil {
		return DeploymentV4{}, e
	}
	if e = validIdentifier(id, "migration id"); e != nil {
		return DeploymentV4{}, e
	}
	var here string
	if e = s.pool.QueryRow(ctx, `SELECT id FROM ahp_state.scope_store_v2 WHERE singleton`).Scan(&here); e != nil {
		return DeploymentV4{}, e
	}
	key := DeploymentExportKey{Unit: u, MigrationID: id, SourceStoreID: here}
	cached, e := cfg.Exports.Load(ctx, key)
	if e != nil {
		return DeploymentV4{}, e
	}
	if cached != nil {
		if cached.State.Manifest.SourceStoreID != here || cached.State.Manifest.MigrationID != id {
			return DeploymentV4{}, contracts.NewError(contracts.ErrKeyReuse, "cached export belongs to a different migration")
		}
		refs, e := ValidateDeploymentV4(*cached, u, build)
		if e != nil {
			return DeploymentV4{}, e
		}
		if e = s.inTx(ctx, func(tx pgx.Tx) error { return deploymentExportControls(ctx, tx, u) }); e != nil {
			return DeploymentV4{}, e
		}
		if e = s.verifySharedArtifacts(ctx, *cached, refs, "after-export", here); e != nil {
			return DeploymentV4{}, e
		}
		if e = s.deploymentCustody(ctx, u, id, here, cached.Digest, "after-export"); e != nil {
			return DeploymentV4{}, e
		}
		return *cached, nil
	}
	data, e := s.exportDeploymentState(ctx, u, id, build, deploymentV4Entities)
	if e != nil {
		return DeploymentV4{}, e
	}
	data.Manifest.Format = DeploymentV4Format
	data.Manifest.Native = DeploymentV4Native
	data.Manifest.Profile = DeploymentV4Profile
	data.Digest, e = deploymentDigest(data)
	if e != nil {
		return DeploymentV4{}, e
	}
	image := DeploymentV4{State: data, ArtifactOwnerID: cfg.OwnerID}
	image.Digest, e = deploymentV4Digest(image)
	if e != nil {
		return DeploymentV4{}, e
	}
	refs, e := ValidateDeploymentV4(image, u, build)
	if e != nil {
		return DeploymentV4{}, e
	}
	if e = s.deploymentPolicyConfig(data); e != nil {
		return DeploymentV4{}, e
	}
	if e = s.verifySharedArtifacts(ctx, image, refs, "after-export", data.Manifest.SourceStoreID); e != nil {
		return DeploymentV4{}, e
	}
	if e = cfg.Exports.Store(ctx, key, image); e != nil {
		return DeploymentV4{}, e
	}
	if e = s.deploymentCustody(ctx, u, id, data.Manifest.SourceStoreID, image.Digest, "after-export"); e != nil {
		return DeploymentV4{}, e
	}
	return image, nil
}
func (s *Store) ImportDeploymentV4(ctx context.Context, u DeploymentUnit, image DeploymentV4, build string) (DeploymentV4Receipt, error) {
	var out DeploymentV4Receipt
	if _, e := s.sharedArtifactMount(); e != nil {
		return out, e
	}
	refs, e := ValidateDeploymentV4(image, u, build)
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
	r, e := s.importDeploymentState(ctx, u, image.State, build, deploymentV4Entities, image.Digest, func(c context.Context) error { return s.verifySharedArtifacts(c, image, refs, "before-commit", here) })
	if e != nil {
		return out, e
	}
	out.DeploymentV3Receipt = r
	out.ArtifactOwnerID = image.ArtifactOwnerID
	return out, nil
}
func (e *DeploymentV4) UnmarshalJSON(raw []byte) error {
	if len(raw) > MaxDeploymentV3Bytes {
		return invalid("deployment archive too large")
	}
	if _, err := contracts.Digest(json.RawMessage(raw)); err != nil {
		return invalid("duplicate or invalid deployment JSON")
	}
	type wire DeploymentV4
	var value wire
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&value) != nil {
		return invalid("invalid dependency-aware deployment archive")
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return invalid("trailing dependency-aware archive")
	}
	*e = DeploymentV4(value)
	return nil
}

// A source cache is an independently owned administrative copy. Hosts must
// durably seal it before acknowledging Export, and erase it on handoff/abort.
// Load must return an exact immutable image or an error, never re-snapshot it.
type DeploymentExportKey struct {
	Unit          DeploymentUnit `json:"unit"`
	MigrationID   string         `json:"migration_id"`
	SourceStoreID string         `json:"source_store_id"`
}
type DeploymentExportCache interface {
	Load(context.Context, DeploymentExportKey) (*DeploymentV4, error)
	Store(context.Context, DeploymentExportKey, DeploymentV4) error
	Forget(context.Context, DeploymentExportKey) error
}
