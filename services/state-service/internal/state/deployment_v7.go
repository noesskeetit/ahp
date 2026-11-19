package state

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/ahp-platform/ahp/internal/references"
)

const DeploymentV7Format = "ahp-state-deployment/7"
const DeploymentV7Native = "ahp-state-common-owner/research-7"
const DeploymentV7Profile = "offline-same-deployment-clean-state-db-shared-artifacts-buckets/1"

type DeploymentV7 = DeploymentV4
type DeploymentV7Receipt = DeploymentV4Receipt

var deploymentV7Entities = func() []scopeEntity {
	out := append([]scopeEntity{}, deploymentV6Entities...)
	return append(out,
		scopeEntity{"common_append_nodes", "thread_id,sha256,data", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,sha256", false},
		scopeEntity{"common_append_roots", "thread_id,namespace,revision,data", "x.thread_id IN (SELECT id FROM scoped_threads)", "thread_id,namespace,revision", false})
}()

func (s *Store) ExportDeploymentV7(ctx context.Context, u DeploymentUnit, id, build string) (DeploymentV7, error) {
	return s.exportDeploymentIntervals(ctx, u, id, build, 7)
}
func (s *Store) ImportDeploymentV7(ctx context.Context, u DeploymentUnit, image DeploymentV7, build string) (DeploymentV7Receipt, error) {
	return s.importDeploymentIntervals(ctx, u, image, build, 7)
}
func ValidateDeploymentV7(image DeploymentV7, u DeploymentUnit, build string) ([]references.CheckpointArtifact, error) {
	m := image.State.Manifest
	if m.Format != DeploymentV7Format || m.Native != DeploymentV7Native || m.Profile != DeploymentV7Profile {
		return nil, invalid("unsupported incremental deployment profile")
	}
	raw, e := json.Marshal(image)
	if e != nil || len(raw) > MaxDeploymentV3Bytes {
		return nil, invalid("incremental archive exceeds wire bound")
	}
	digest, e := deploymentV4Digest(image)
	if e != nil || digest != image.Digest {
		return nil, invalid("incremental archive digest mismatch")
	}
	digest, e = deploymentDigest(image.State)
	if e != nil || digest != image.State.Digest {
		return nil, invalid("incremental State digest mismatch")
	}
	if e = validateDeploymentCatalog(image.State, deploymentV7Entities); e != nil {
		return nil, e
	}
	if e = validateRetentionCatalog(image.State); e != nil {
		return nil, e
	}
	if e = validateAppendCatalog(image.State); e != nil {
		return nil, e
	}
	projected, e := projectDeploymentIntervals(image, true, true)
	if e != nil {
		return nil, e
	}
	return validateDeploymentV4(projected, u, build, true)
}
func validateAppendCatalog(image DeploymentV3) error {
	threads := map[string]bool{}
	for _, r := range deploymentRows(image, "threads") {
		threads[deploymentString(r, "id")] = true
	}
	nodes := map[string]bool{}
	for _, r := range deploymentRows(image, "common_append_nodes") {
		thread, h := deploymentString(r, "thread_id"), deploymentString(r, "sha256")
		raw, e := deploymentBytes(r, "data")
		var entries map[string]json.RawMessage
		if e != nil || !threads[thread] || !deploymentSHA(h) || commonAppendHash(raw) != h || len(raw) > 4<<20 || json.Unmarshal(raw, &entries) != nil || len(entries) == 0 || len(entries) > 8192 {
			return invalid("invalid incremental immutable node")
		}
		var partition *byte
		for key := range entries {
			b := commonAppendBucket(key)
			if partition != nil && *partition != b {
				return invalid("incremental node crosses bucket partition")
			}
			partition = &b
		}
		nodes[deploymentTuple(thread, h)] = true
	}
	cuts := map[string]deploymentRow{}
	for _, r := range deploymentRows(image, "common_cuts") {
		cuts[deploymentV5CutKey(r, "revision")] = r
	}
	wanted := map[string]string{}
	for _, r := range deploymentRows(image, "common_version_roots") {
		d := deploymentString(r, "digest")
		if strings.HasPrefix(d, commonAppendDigestPrefix) {
			wanted[deploymentV5CutKey(r, "revision")] = d
		}
	}
	found := map[string]bool{}
	for _, r := range deploymentRows(image, "common_append_roots") {
		k := deploymentV5CutKey(r, "revision")
		cut := cuts[k]
		raw, e := deploymentBytes(r, "data")
		var root commonAppendRoot
		if e != nil || cut == nil || wanted[k] == "" || commonAppendDigestPrefix+commonAppendHash(raw) != wanted[k] || json.Unmarshal(raw, &root) != nil || root.Format != commonAppendFormat || root.Profile != deploymentString(cut, "profile") || root.Root != deploymentString(cut, "root") || deploymentString(cut, "format") != commonVersionLayout || found[k] {
			return invalid("invalid incremental root binding")
		}
		for _, h := range root.Hashes {
			if h != "" && !nodes[deploymentTuple(deploymentString(r, "thread_id"), h)] {
				return invalid("incremental root has missing node")
			}
		}
		found[k] = true
	}
	for k := range wanted {
		if !found[k] {
			return invalid("incremental cut missing its root")
		}
	}
	return nil
}
