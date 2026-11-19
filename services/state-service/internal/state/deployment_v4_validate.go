package state

import (
	"bytes"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"sort"
)

func ValidateDeploymentV4(image DeploymentV4, u DeploymentUnit, build string) ([]references.CheckpointArtifact, error) {
	return validateDeploymentV4(image, u, build, false)
}
func validateDeploymentV4(image DeploymentV4, u DeploymentUnit, build string, allowPruned bool) ([]references.CheckpointArtifact, error) {
	m := image.State.Manifest
	if m.Format != DeploymentV4Format || m.Native != DeploymentV4Native || m.Profile != DeploymentV4Profile {
		return nil, invalid("unsupported dependency-aware deployment format")
	}
	if e := validIdentifier(image.ArtifactOwnerID, "Artifact owner"); e != nil {
		return nil, e
	}
	raw, e := json.Marshal(image)
	if e != nil || len(raw) > MaxDeploymentV3Bytes {
		return nil, invalid("dependency archive exceeds bounds")
	}
	digest, e := deploymentV4Digest(image)
	if e != nil || digest != image.Digest {
		return nil, invalid("dependency archive digest mismatch")
	}
	digest, e = deploymentDigest(image.State)
	if e != nil || digest != image.State.Digest {
		return nil, invalid("State image digest mismatch")
	}
	if len(image.State.Entities) != len(deploymentV4Entities) || len(m.Counts) != len(deploymentV4Entities) {
		return nil, invalid("incomplete dependency entity catalog")
	}
	rows, ok := image.State.Entities["common_artifact_refs"]
	if !ok || len(rows) != m.Counts["common_artifact_refs"] {
		return nil, invalid("common Artifact root catalog missing")
	}
	total := 0
	for _, r := range image.State.Entities {
		total += len(r)
	}
	if total > MaxDeploymentV3Rows {
		return nil, invalid("dependency row bound exceeded")
	}
	// Reuse the strict V3 semantic validator on a LOCAL projection, never a wire
	// archive claiming to be inline. Public V3 still rejects this format/schema.
	base := image.State
	base.Entities = map[string][]json.RawMessage{}
	for k, v := range image.State.Entities {
		if k != "common_artifact_refs" {
			base.Entities[k] = v
		}
	}
	base.Manifest.Counts = map[string]int{}
	for k, v := range m.Counts {
		if k != "common_artifact_refs" {
			base.Manifest.Counts[k] = v
		}
	}
	base.Manifest.Format = DeploymentV3Format
	base.Manifest.Native = DeploymentV3Native
	base.Manifest.Profile = "offline-same-deployment-clean-db-inline/1"
	base.Digest, e = deploymentDigest(base)
	if e != nil {
		return nil, e
	}
	if e = validateDeploymentV3(base, u, build, allowPruned); e != nil {
		return nil, e
	}
	type cut struct {
		doc           CommonDocument
		scope         contracts.Scope
		context, task string
		refs          []CommonArtifactReference
	}
	key := func(r deploymentRow) string {
		revision, _ := deploymentInt(r, "revision")
		return deploymentTuple(deploymentString(r, "thread_id"), deploymentString(r, "namespace"), revision)
	}
	threads := map[string]deploymentRow{}
	for _, r := range deploymentRows(base, "threads") {
		threads[deploymentString(r, "id")] = r
	}
	runTasks := map[string]string{}
	for _, r := range deploymentRows(base, "runs") {
		runTasks[deploymentString(r, "id")] = deploymentString(r, "task_id")
	}
	cuts := map[string]*cut{}
	for _, r := range deploymentRows(base, "common_cuts") {
		t := threads[deploymentString(r, "thread_id")]
		cuts[key(r)] = &cut{doc: CommonDocument{Format: deploymentString(r, "format"), Profile: deploymentString(r, "profile"), Root: deploymentString(r, "root"), Records: map[string]CommonRecord{}}, scope: contracts.Scope{Tenant: u.Tenant, Deployment: u.Deployment, Subject: deploymentString(t, "subject")}, context: deploymentString(t, "context_id")}
	}
	for _, r := range deploymentRows(base, "common_receipts") {
		b, _ := deploymentBytes(r, "result")
		var receipt CommonReceipt
		if json.Unmarshal(b, &receipt) != nil {
			return nil, invalid("invalid common receipt")
		}
		cuts[key(r)].task = runTasks[receipt.RunID]
	}
	payloads := map[string][]byte{}
	for _, r := range deploymentRows(base, "common_payloads") {
		b, _ := deploymentBytes(r, "attributes")
		payloads[deploymentTuple(deploymentString(r, "thread_id"), deploymentString(r, "sha256"))] = b
	}
	for _, r := range deploymentRows(base, "common_records") {
		attrs, e := deploymentBytes(r, "attributes")
		if e != nil {
			return nil, e
		}
		if !deploymentNull(r, "attributes_hash") {
			attrs = payloads[deploymentTuple(deploymentString(r, "thread_id"), deploymentString(r, "attributes_hash"))]
		}
		cuts[key(r)].doc.Records[deploymentString(r, "id")] = CommonRecord{Kind: deploymentString(r, "kind"), Attributes: attrs, Relations: map[string][]string{}}
	}
	seen := map[string]bool{}
	unique := map[string]references.CheckpointArtifact{}
	var bytesTotal int64
	for _, raw := range rows {
		if len(raw) > MaxDeploymentV3RowBytes {
			return nil, invalid("common ref row too large")
		}
		var row deploymentRow
		if json.Unmarshal(raw, &row) != nil || len(row) != 5 {
			return nil, invalid("invalid common ref columns")
		}
		for _, k := range []string{"thread_id", "namespace", "revision", "record_id", "artifact"} {
			if row[k] == nil {
				return nil, invalid("missing common ref column")
			}
		}
		for _, name := range []string{"thread_id", "namespace", "record_id"} {
			var value string
			if bytes.Equal(bytes.TrimSpace(row[name]), []byte("null")) || json.Unmarshal(row[name], &value) != nil {
				return nil, invalid("invalid common ref string field")
			}
		}
		if revision, e := deploymentInt(row, "revision"); e != nil || revision < 1 {
			return nil, invalid("invalid common ref revision")
		}
		identity := key(row)
		c := cuts[identity]
		if c == nil {
			return nil, invalid("common ref cut missing")
		}
		id := deploymentString(row, "record_id")
		recordKey := deploymentTuple(identity, id)
		if seen[recordKey] {
			return nil, invalid("duplicate common Artifact record")
		}
		seen[recordKey] = true
		var wrapper map[string]json.RawMessage
		if json.Unmarshal(row["artifact"], &wrapper) != nil || len(wrapper) != 1 || wrapper["json"] == nil {
			return nil, invalid("Artifact JSON wrapper required")
		}
		var ref references.CheckpointArtifact
		d := json.NewDecoder(bytes.NewReader(wrapper["json"]))
		d.DisallowUnknownFields()
		if d.Decode(&ref) != nil {
			return nil, invalid("invalid Artifact descriptor fields")
		}
		if ref.ContextID != c.context || ref.TaskID != c.task {
			return nil, contracts.NewError(contracts.ErrForbidden, "Artifact descriptor belongs to another context/task")
		}
		if ref.Size <= 0 || ref.Size > MaxDeploymentV4ArtifactBytes {
			return nil, invalid("Artifact dependency size bound")
		}
		if old, ok := unique[ref.ID]; ok {
			if !old.Equal(ref) {
				return nil, invalid("conflicting immutable Artifact descriptor")
			}
		} else {
			unique[ref.ID] = ref
			bytesTotal += ref.Size
		}
		if len(unique) > MaxDeploymentV4Artifacts || bytesTotal > MaxDeploymentV4ArtifactBytes {
			return nil, invalid("Artifact dependencies exceed bounded profile")
		}
		c.refs = append(c.refs, CommonArtifactReference{RecordID: id, Artifact: ref})
	}
	for _, c := range cuts {
		if e := validateCommonArtifactReferences(c.scope, c.doc, c.refs); e != nil {
			return nil, e
		}
	}
	ids := make([]string, 0, len(unique))
	for id := range unique {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]references.CheckpointArtifact, 0, len(ids))
	for _, id := range ids {
		out = append(out, unique[id])
	}
	return out, nil
}
