package state

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type prunedCommonMarker struct {
	Format         string `json:"format"`
	OriginalFormat string `json:"original_format"`
	RecordCount    int    `json:"record_count"`
}

func validatePrunedCommon(doc CommonDocument) (prunedCommonMarker, error) {
	var m prunedCommonMarker
	r, ok := doc.Records[doc.Root]
	if !ok || len(doc.Records) != 1 || r.Kind != "metadata" || len(r.NativeID) != 0 || len(r.Sequence) != 0 || len(r.OwnerHint) != 0 || len(r.Relations) != 0 {
		return m, invalid("invalid archived common anchor")
	}
	if _, e := contracts.Digest(r.Attributes); e != nil {
		return m, e
	}
	d := json.NewDecoder(bytes.NewReader(r.Attributes))
	d.DisallowUnknownFields()
	if d.Decode(&m) != nil || m.Format != commonPrunedLayout || (m.OriginalFormat != CommonFormat && m.OriginalFormat != commonVersionLayout) || m.RecordCount < 1 || m.RecordCount > 50000 {
		return m, invalid("invalid archived common marker")
	}
	return m, nil
}

func ValidateDeploymentV6(image DeploymentV6, u DeploymentUnit, build string) ([]references.CheckpointArtifact, error) {
	m := image.State.Manifest
	if m.Format != DeploymentV6Format || m.Native != DeploymentV6Native || m.Profile != DeploymentV6Profile {
		return nil, invalid("unsupported retained deployment profile")
	}
	raw, e := json.Marshal(image)
	if e != nil || len(raw) > MaxDeploymentV3Bytes {
		return nil, invalid("retained archive exceeds wire bound")
	}
	digest, e := deploymentV4Digest(image)
	if e != nil || digest != image.Digest {
		return nil, invalid("retained archive digest mismatch")
	}
	digest, e = deploymentDigest(image.State)
	if e != nil || digest != image.State.Digest {
		return nil, invalid("retained State digest mismatch")
	}
	if e = validateDeploymentCatalog(image.State, deploymentV6Entities); e != nil {
		return nil, e
	}
	if e = validateRetentionCatalog(image.State); e != nil {
		return nil, e
	}
	projected, e := projectDeploymentIntervals(image, true)
	if e != nil {
		return nil, e
	}
	return validateDeploymentV4(projected, u, build, true)
}
func validateDeploymentCatalog(image DeploymentV3, specs []scopeEntity) error {
	if len(image.Entities) != len(specs) || len(image.Manifest.Counts) != len(specs) {
		return invalid("incomplete retained entity catalog")
	}
	total := 0
	for _, sp := range specs {
		rows, ok := image.Entities[sp.name]
		if !ok || len(rows) != image.Manifest.Counts[sp.name] {
			return invalid("retained entity count mismatch")
		}
		total += len(rows)
		if total > MaxDeploymentV3Rows {
			return invalid("retained row bound exceeded")
		}
		fields := strings.Split(sp.columns, ",")
		seen := map[string]bool{}
		for _, raw := range rows {
			if len(raw) > MaxDeploymentV3RowBytes {
				return invalid("retained row too large")
			}
			if _, e := contracts.Digest(raw); e != nil {
				return e
			}
			var r deploymentRow
			if json.Unmarshal(raw, &r) != nil || len(r) != len(fields) {
				return invalid("retained row columns differ")
			}
			for _, c := range fields {
				if r[c] == nil {
					return invalid("missing retained column")
				}
			}
			keys := []json.RawMessage{}
			for _, c := range strings.Split(sp.order, ",") {
				keys = append(keys, r[c])
			}
			id, e := contracts.Digest(keys)
			if e != nil || seen[id] {
				return invalid("duplicate retained entity identity")
			}
			seen[id] = true
		}
	}
	return nil
}
func validateRetentionCatalog(image DeploymentV3) error {
	heads := map[string]int64{}
	floors := map[string]int64{}
	cuts := map[string]deploymentRow{}
	for _, r := range deploymentRows(image, "common_heads") {
		n, e := deploymentInt(r, "revision")
		if e != nil {
			return e
		}
		heads[deploymentV5Group(r)] = n
	}
	for _, r := range deploymentRows(image, "common_cuts") {
		cuts[deploymentV5CutKey(r, "revision")] = r
	}
	for _, r := range deploymentRows(image, "common_retention_floors") {
		g := deploymentV5Group(r)
		n, e := deploymentInt(r, "readable_from")
		if e != nil || n < 1 || n > heads[g] {
			return invalid("invalid common readable floor")
		}
		floors[g] = n
	}
	for _, r := range deploymentRows(image, "common_cuts") {
		n, e := deploymentInt(r, "revision")
		if e != nil {
			return e
		}
		floor := floors[deploymentV5Group(r)]
		if (deploymentString(r, "format") == commonPrunedLayout) != (floor > 0 && n < floor) {
			return invalid("common archived prefix differs from floor")
		}
	}
	type pin struct {
		root     CommonRoot
		versions map[int64]CommonRoot
	}
	pins := map[string]*pin{}
	for _, r := range deploymentRows(image, "common_retention_roots") {
		g := deploymentV5Group(r)
		key := deploymentString(r, "key")
		ver, e := deploymentInt(r, "version")
		if heads[g] == 0 || commonNamespace(deploymentString(r, "namespace")) != nil || validIdentifier(key, "root key") != nil || e != nil || ver < 1 {
			return invalid("invalid retained root identity")
		}
		root := CommonRoot{Namespace: deploymentString(r, "namespace"), Key: key, Version: ver}
		if !deploymentNull(r, "revision") {
			n, e := deploymentInt(r, "revision")
			if e != nil || n < 1 || n < floors[g] || n > heads[g] {
				return invalid("retained root points outside readable history")
			}
			root.Revision = &n
		}
		pins[deploymentTuple(g, key)] = &pin{root: root, versions: map[int64]CommonRoot{}}
	}
	archived := map[string]int64{}
	maxFloor := map[string]int64{}
	for _, r := range deploymentRows(image, "common_retention_ops") {
		g := deploymentV5Group(r)
		key := deploymentString(r, "key")
		digest := deploymentString(r, "digest")
		if heads[g] == 0 || validIdentifier(key, "retention operation") != nil || !strings.HasPrefix(digest, contracts.DigestPrefix) || !deploymentSHA(strings.TrimPrefix(digest, contracts.DigestPrefix)) {
			return invalid("invalid retention receipt identity")
		}
		raw, e := deploymentBytes(r, "result")
		if e != nil {
			return e
		}
		if _, e = contracts.Digest(json.RawMessage(raw)); e != nil {
			return e
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		switch deploymentString(r, "kind") {
		case "root":
			var root CommonRoot
			if dec.Decode(&root) != nil || root.Namespace != deploymentString(r, "namespace") || root.Version < 1 || validIdentifier(root.Key, "root key") != nil {
				return invalid("invalid root receipt")
			}
			p := pins[deploymentTuple(g, root.Key)]
			if p == nil || root.Version > p.root.Version || p.versions[root.Version].Version != 0 {
				return invalid("root operation versions incomplete")
			}
			if root.Revision != nil && (*root.Revision < 1 || *root.Revision > heads[g]) {
				return invalid("invalid original root revision")
			}
			p.versions[root.Version] = root
		case "prune":
			var report CommonPruneReport
			if dec.Decode(&report) != nil || report.ThreadID != deploymentString(r, "thread_id") || report.Namespace != deploymentString(r, "namespace") || report.HeadRevision < 1 || report.HeadRevision > heads[g] || report.ReadableFrom < 1 || report.ReadableFrom > report.HeadRevision || report.ReadableFrom > floors[g] || report.ArchivedCuts < 0 || report.ArchivedCuts > 512 || report.ReclaimedRecords < 0 || report.ReclaimedPayloads < 0 || report.ReleasedArtifactRoots < 0 {
				return invalid("invalid prune receipt")
			}
			archived[g] += int64(report.ArchivedCuts)
			if report.ReadableFrom > maxFloor[g] {
				maxFloor[g] = report.ReadableFrom
			}
		default:
			return invalid("unrecognized retention operation")
		}
	}
	for _, p := range pins {
		if int64(len(p.versions)) != p.root.Version {
			return invalid("missing original root operation")
		}
		for n := int64(1); n <= p.root.Version; n++ {
			if p.versions[n].Version != n {
				return invalid("root operation version gap")
			}
		}
		a, _ := contracts.Digest(p.versions[p.root.Version])
		b, _ := contracts.Digest(p.root)
		if a != b {
			return invalid("current root differs from receipt")
		}
	}
	for g, floor := range floors {
		if archived[g] != floor-1 || maxFloor[g] != floor {
			return invalid("retention prefix receipt accounting differs")
		}
	}
	return nil
}
func (s *Store) deploymentRetentionConfig(image DeploymentV3) error {
	if e := s.retentionAvailable(); e != nil {
		return e
	}
	for _, r := range deploymentRows(image, "common_cuts") {
		if e := s.retentionProfile(deploymentString(r, "profile")); e != nil {
			return e
		}
	}
	return nil
}
