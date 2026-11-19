package state

import (
	"bytes"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"sort"
	"strconv"
	"strings"
)

// Explicit bounded offline profile. Compact wire rows stay compact on import;
// only semantic validation expands them. Work is bounded independently of wire
// bytes to prevent a tiny interval archive requesting quadratic validation.
const maxDeploymentV5Visits = 4000000

func ValidateDeploymentV5(image DeploymentV5, u DeploymentUnit, build string) ([]references.CheckpointArtifact, error) {
	m := image.State.Manifest
	if m.Format != DeploymentV5Format || m.Native != DeploymentV5Native || m.Profile != DeploymentV5Profile {
		return nil, invalid("unsupported interval deployment profile")
	}
	raw, e := json.Marshal(image)
	if e != nil || len(raw) > MaxDeploymentV3Bytes {
		return nil, invalid("interval deployment exceeds wire bound")
	}
	digest, e := deploymentV5Digest(image)
	if e != nil || digest != image.Digest {
		return nil, invalid("interval archive digest mismatch")
	}
	digest, e = deploymentDigest(image.State)
	if e != nil || digest != image.State.Digest {
		return nil, invalid("interval State digest mismatch")
	}
	if len(image.State.Entities) != len(deploymentV5Entities) || len(m.Counts) != len(deploymentV5Entities) {
		return nil, invalid("incomplete interval entity catalog")
	}
	total := 0
	for _, sp := range deploymentV5Entities {
		rows, ok := image.State.Entities[sp.name]
		if !ok || len(rows) != m.Counts[sp.name] {
			return nil, invalid("interval entity count mismatch")
		}
		total += len(rows)
		if total > MaxDeploymentV3Rows {
			return nil, invalid("interval row bound exceeded")
		}
		fields := strings.Split(sp.columns, ",")
		seen := map[string]bool{}
		for _, raw := range rows {
			if len(raw) > MaxDeploymentV3RowBytes {
				return nil, invalid("interval row too large")
			}
			var row deploymentRow
			if json.Unmarshal(raw, &row) != nil || len(row) != len(fields) {
				return nil, invalid("interval row columns differ")
			}
			for _, c := range fields {
				if row[c] == nil {
					return nil, invalid("missing interval column")
				}
			}
			keys := []json.RawMessage{}
			for _, c := range strings.Split(sp.order, ",") {
				keys = append(keys, row[c])
			}
			identity, e := contracts.Digest(keys)
			if e != nil || seen[identity] {
				return nil, invalid("duplicate interval entity identity")
			}
			seen[identity] = true
		}
	}
	projected, e := projectDeploymentV5(image)
	if e != nil {
		return nil, e
	}
	// This is a LOCAL semantic projection. V5 wire headers and the physical
	// interval rows are never rewritten or discarded by export/import.
	return ValidateDeploymentV4(projected, u, build)
}

func deploymentV5CutKey(row deploymentRow, revision string) string {
	n, _ := deploymentInt(row, revision)
	return deploymentTuple(deploymentString(row, "thread_id"), deploymentString(row, "namespace"), n)
}
func deploymentV5Group(row deploymentRow) string {
	return deploymentTuple(deploymentString(row, "thread_id"), deploymentString(row, "namespace"))
}
func deploymentV5JSON(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
func deploymentV5String(row deploymentRow, k string) error {
	var s string
	if deploymentNull(row, k) || json.Unmarshal(row[k], &s) != nil {
		return invalid("invalid interval string: " + k)
	}
	return nil
}

type deploymentV5Cut struct {
	row     deploymentRow
	doc     CommonDocument
	anchors map[string]deploymentRow
	wanted  map[string]bool
}
type deploymentV5Interval struct {
	row      deploymentRow
	from, to int64
}

func projectDeploymentV5(image DeploymentV5) (DeploymentV4, error) {
	return projectDeploymentIntervals(image, false)
}
func projectDeploymentIntervals(image DeploymentV5, allowPruned bool, appendAllowed ...bool) (DeploymentV4, error) {
	// Copy row catalogs, not aliased manifest maps: validation must not mutate
	// the caller's sealed export or a concurrently retried immutable cache image.
	out := image
	out.State.Entities = map[string][]json.RawMessage{}
	out.State.Manifest.Counts = map[string]int{}
	for _, sp := range deploymentV4Entities {
		out.State.Entities[sp.name] = append([]json.RawMessage{}, image.State.Entities[sp.name]...)
	}
	cuts := map[string]*deploymentV5Cut{}
	groups := map[string][]*deploymentV5Cut{}
	for _, row := range deploymentRows(image.State, "common_cuts") {
		k := deploymentV5CutKey(row, "revision")
		if cuts[k] != nil {
			return out, invalid("duplicate cut identity")
		}
		c := &deploymentV5Cut{row: row, anchors: map[string]deploymentRow{}, wanted: map[string]bool{}}
		cuts[k] = c
		switch deploymentString(row, "format") {
		case commonVersionLayout:
			c.doc = CommonDocument{Format: CommonFormat, Profile: deploymentString(row, "profile"), Root: deploymentString(row, "root"), Records: map[string]CommonRecord{}}
			c.wanted[c.doc.Root] = true
			groups[deploymentV5Group(row)] = append(groups[deploymentV5Group(row)], c)
		case CommonFormat:
		case commonPrunedLayout:
			if !allowPruned {
				return out, invalid("archived cut requires retention profile")
			}
		default:
			return out, invalid("unrecognized common storage layout")
		}
	}
	boundaries := map[string]bool{}
	for key, c := range cuts {
		if c.doc.Format != "" {
			boundaries[key] = true
		}
	}
	if allowPruned {
		for _, row := range deploymentRows(image.State, "common_records") {
			c := cuts[deploymentV5CutKey(row, "revision")]
			if c == nil || deploymentString(c.row, "format") != commonPrunedLayout || deploymentString(row, "id") != deploymentString(c.row, "root") {
				continue
			}
			raw, e := deploymentBytes(row, "attributes")
			if e != nil {
				return out, e
			}
			var marker prunedCommonMarker
			if json.Unmarshal(raw, &marker) != nil {
				return out, invalid("invalid archived interval boundary")
			}
			if marker.OriginalFormat == commonVersionLayout {
				boundaries[deploymentV5CutKey(row, "revision")] = true
			}
		}
	}
	// Only known legacy cuts retain their ordinary rows. Versioned cuts may have
	// exactly root/Artifact anchors; stale extra legacy relations are corruption.
	for _, name := range []string{"common_records", "common_roles", "common_edges"} {
		out.State.Entities[name] = []json.RawMessage{}
		for i, row := range deploymentRows(image.State, name) {
			c := cuts[deploymentV5CutKey(row, "revision")]
			if c == nil {
				return out, invalid("common row has no cut")
			}
			if c.doc.Format == "" {
				out.State.Entities[name] = append(out.State.Entities[name], image.State.Entities[name][i])
				continue
			}
			if name != "common_records" {
				return out, invalid("interval cut contains legacy relation rows")
			}
			id := deploymentString(row, "id")
			if c.anchors[id] != nil {
				return out, invalid("duplicate interval anchor")
			}
			c.anchors[id] = row
		}
	}
	for _, row := range deploymentRows(image.State, "common_artifact_refs") {
		if c := cuts[deploymentV5CutKey(row, "revision")]; c != nil && c.doc.Format != "" {
			c.wanted[deploymentString(row, "record_id")] = true
		}
	}
	payloads := map[string][]byte{}
	for _, row := range deploymentRows(image.State, "common_payloads") {
		raw, e := deploymentBytes(row, "attributes")
		if e != nil || deploymentPayloadHash(raw) != deploymentString(row, "sha256") {
			return out, invalid("interval payload hash mismatch")
		}
		payloads[deploymentTuple(deploymentString(row, "thread_id"), deploymentString(row, "sha256"))] = raw
	}
	roots := map[string]string{}
	for _, row := range deploymentRows(image.State, "common_version_roots") {
		for _, k := range []string{"thread_id", "namespace", "digest"} {
			if e := deploymentV5String(row, k); e != nil {
				return out, e
			}
		}
		revision, e := deploymentInt(row, "revision")
		if e != nil || revision < 1 {
			return out, invalid("invalid interval root revision")
		}
		k := deploymentV5CutKey(row, "revision")
		c := cuts[k]
		value := deploymentString(row, "digest")
		if c == nil || c.doc.Format == "" || roots[k] != "" || !(deploymentSHA(value) || len(appendAllowed) > 0 && appendAllowed[0] && strings.HasPrefix(value, commonAppendDigestPrefix) && deploymentSHA(strings.TrimPrefix(value, commonAppendDigestPrefix))) {
			return out, invalid("foreign/duplicate interval integrity root")
		}
		roots[k] = value
	}
	catalogs := map[string]map[string][]deploymentV5Interval{}
	for _, name := range []string{"common_record_versions", "common_role_versions", "common_edge_versions"} {
		catalogs[name] = map[string][]deploymentV5Interval{}
		for _, row := range deploymentRows(image.State, name) {
			for _, k := range []string{"thread_id", "namespace"} {
				if e := deploymentV5String(row, k); e != nil {
					return out, e
				}
			}
			from, e := deploymentInt(row, "from_revision")
			if e != nil || from < 1 {
				return out, invalid("invalid interval start")
			}
			to := int64(0)
			if !deploymentNull(row, "to_revision") {
				to, e = deploymentInt(row, "to_revision")
				if e != nil || to <= from {
					return out, invalid("invalid interval end")
				}
			}
			begin := cuts[deploymentV5CutKey(row, "from_revision")]
			if begin == nil || !boundaries[deploymentV5CutKey(begin.row, "revision")] {
				return out, invalid("interval start cut absent")
			}
			if to != 0 {
				end := cuts[deploymentV5CutKey(row, "to_revision")]
				if end == nil || !boundaries[deploymentV5CutKey(end.row, "revision")] {
					return out, invalid("interval end cut absent")
				}
			}
			keys := []string{"source", "role"}
			if name == "common_record_versions" {
				keys = []string{"id", "kind", "attributes_hash"}
			}
			if name == "common_edge_versions" {
				keys = append(keys, "target")
				n, e := deploymentInt(row, "ordinal")
				if e != nil || n < 0 || n > MaxDeploymentV3Rows {
					return out, invalid("invalid interval ordinal")
				}
			}
			for _, k := range keys {
				if e := deploymentV5String(row, k); e != nil {
					return out, e
				}
			}
			if name == "common_record_versions" {
				if _, e := deploymentV5Record(row, payloads); e != nil {
					return out, e
				}
			}
			g := deploymentV5Group(row)
			catalogs[name][g] = append(catalogs[name][g], deploymentV5Interval{row, from, to})
		}
	}
	visits, expandedRows, expandedBytes, expandedNativeBytes := 0, 0, 0, 0
	for _, sp := range deploymentV4Entities {
		expandedRows += len(out.State.Entities[sp.name])
		for _, raw := range out.State.Entities[sp.name] {
			expandedBytes += len(raw)
		}
	}
	appendRow := func(name string, row deploymentRow) error {
		raw := deploymentV5JSON(row)
		expandedRows++
		expandedBytes += len(raw)
		if expandedRows > MaxDeploymentV3Rows || expandedBytes > MaxDeploymentV3Bytes {
			return invalid("interval semantic projection exceeds bounded profile")
		}
		out.State.Entities[name] = append(out.State.Entities[name], raw)
		return nil
	}
	for g, list := range groups {
		for _, c := range list {
			revision, e := deploymentInt(c.row, "revision")
			if e != nil || revision < 1 {
				return out, invalid("invalid interval cut revision")
			}
			alive := func(v deploymentV5Interval) bool { return v.from <= revision && (v.to == 0 || v.to > revision) }
			for _, name := range []string{"common_record_versions", "common_role_versions", "common_edge_versions"} {
				visits += len(catalogs[name][g])
				if visits > maxDeploymentV5Visits {
					return out, invalid("interval validation work bound exceeded")
				}
				if name == "common_record_versions" {
					for _, v := range catalogs[name][g] {
						if !alive(v) {
							continue
						}
						id := deploymentString(v.row, "id")
						if _, ok := c.doc.Records[id]; ok {
							return out, invalid("overlapping interval records")
						}
						record, e := deploymentV5Record(v.row, payloads)
						if e != nil {
							return out, e
						}
						expandedNativeBytes += len(record.NativeID) + len(record.Attributes)
						if expandedNativeBytes > MaxDeploymentV3ExpandedBytes {
							return out, invalid("interval expanded native validation budget exceeded")
						}
						c.doc.Records[id] = record
						row := deploymentRow{}
						for _, k := range []string{"thread_id", "namespace", "id", "kind", "native_id", "sequence", "owner_hint", "presence", "attributes_hash"} {
							row[k] = v.row[k]
						}
						row["revision"] = deploymentV5JSON(revision)
						row["attributes"] = deploymentV5JSON([]byte{})
						if c.wanted[id] {
							anchor := c.anchors[id]
							a, e := contracts.Digest(anchor)
							b, f := contracts.Digest(row)
							if anchor == nil || e != nil || f != nil || a != b {
								return out, invalid("interval anchor differs from reconstructed native record")
							}
						}
						if e := appendRow("common_records", row); e != nil {
							return out, e
						}
					}
				} else if name == "common_role_versions" {
					for _, v := range catalogs[name][g] {
						if !alive(v) {
							continue
						}
						id, role := deploymentString(v.row, "source"), deploymentString(v.row, "role")
						record, ok := c.doc.Records[id]
						if !ok {
							return out, invalid("interval role source absent")
						}
						if _, ok := record.Relations[role]; ok {
							return out, invalid("overlapping interval roles")
						}
						record.Relations[role] = []string{}
						c.doc.Records[id] = record
						row := deploymentRow{}
						for _, k := range []string{"thread_id", "namespace", "source", "role"} {
							row[k] = v.row[k]
						}
						row["revision"] = deploymentV5JSON(revision)
						if e := appendRow("common_roles", row); e != nil {
							return out, e
						}
					}
				} else {
					edges := map[commonRoleKey]map[int64]string{}
					for _, v := range catalogs[name][g] {
						if !alive(v) {
							continue
						}
						id, role := deploymentString(v.row, "source"), deploymentString(v.row, "role")
						record, ok := c.doc.Records[id]
						if !ok {
							return out, invalid("interval edge source absent")
						}
						if _, ok := record.Relations[role]; !ok {
							return out, invalid("interval edge role absent")
						}
						key := commonRoleKey{id, role}
						if edges[key] == nil {
							edges[key] = map[int64]string{}
						}
						n, _ := deploymentInt(v.row, "ordinal")
						if _, ok := edges[key][n]; ok {
							return out, invalid("overlapping interval edge")
						}
						edges[key][n] = deploymentString(v.row, "target")
						row := deploymentRow{}
						for _, k := range []string{"thread_id", "namespace", "source", "role", "ordinal", "target"} {
							row[k] = v.row[k]
						}
						row["revision"] = deploymentV5JSON(revision)
						if e := appendRow("common_edges", row); e != nil {
							return out, e
						}
					}
					for key, values := range edges {
						ordered, e := deploymentSortedOrdinals(values)
						if e != nil {
							return out, e
						}
						r := c.doc.Records[key.source]
						r.Relations[key.role] = ordered
						c.doc.Records[key.source] = r
					}
				}
			}
			if len(c.wanted) != len(c.anchors) {
				return out, invalid("extra/missing interval root/Artifact anchor")
			}
			for id := range c.wanted {
				if c.anchors[id] == nil {
					return out, invalid("required interval anchor absent")
				}
				if _, ok := c.doc.Records[id]; !ok {
					return out, invalid("required interval record absent")
				}
			}
			if e := c.doc.Validate(); e != nil {
				return out, e
			}
			digest, e := commonVersionDigest(c.doc)
			if len(appendAllowed) > 0 && appendAllowed[0] && strings.HasPrefix(roots[deploymentV5CutKey(c.row, "revision")], commonAppendDigestPrefix) {
				root, _, err := commonAppendFromDocument(c.doc)
				if err != nil {
					return out, err
				}
				digest, _ = commonAppendRootDigest(root)
			}
			if e != nil || roots[deploymentV5CutKey(c.row, "revision")] != digest {
				return out, invalid("interval integrity root mismatch")
			}
		}
	}
	// Canonical order is useful for deterministic local projection digests. SQL
	// import still consumes the original physical interval entities and order.
	for _, name := range []string{"common_records", "common_roles", "common_edges"} {
		sort.Slice(out.State.Entities[name], func(i, j int) bool {
			return bytes.Compare(out.State.Entities[name][i], out.State.Entities[name][j]) < 0
		})
	}
	out.State.Entities["common_cuts"] = []json.RawMessage{}
	for _, row := range deploymentRows(image.State, "common_cuts") {
		if deploymentString(row, "format") == commonVersionLayout {
			row["format"] = deploymentV5JSON(CommonFormat)
		}
		out.State.Entities["common_cuts"] = append(out.State.Entities["common_cuts"], deploymentV5JSON(row))
	}
	for _, sp := range deploymentV4Entities {
		out.State.Manifest.Counts[sp.name] = len(out.State.Entities[sp.name])
	}
	out.State.Manifest.Format = DeploymentV4Format
	out.State.Manifest.Native = DeploymentV4Native
	out.State.Manifest.Profile = DeploymentV4Profile
	var e error
	out.State.Digest, e = deploymentDigest(out.State)
	if e != nil {
		return out, e
	}
	out.Digest, e = deploymentV4Digest(out)
	return out, e
}

func deploymentV5Record(row deploymentRow, payloads map[string][]byte) (CommonRecord, error) {
	r := CommonRecord{Kind: deploymentString(row, "kind"), Relations: map[string][]string{}}
	h := deploymentString(row, "attributes_hash")
	r.Attributes = payloads[deploymentTuple(deploymentString(row, "thread_id"), h)]
	if r.Attributes == nil || !deploymentSHA(h) {
		return r, invalid("interval payload absent")
	}
	var e error
	r.NativeID, e = deploymentBytes(row, "native_id")
	if e != nil {
		return r, e
	}
	presence, e := deploymentInt(row, "presence")
	if e != nil || presence < 0 || presence > 3 {
		return r, invalid("invalid interval presence bits")
	}
	if presence&1 != 0 {
		if deploymentNull(row, "sequence") {
			r.Sequence = json.RawMessage(`null`)
		} else {
			n, e := deploymentInt(row, "sequence")
			if e != nil {
				return r, e
			}
			r.Sequence = json.RawMessage(strconv.FormatInt(n, 10))
		}
	} else if !deploymentNull(row, "sequence") {
		return r, invalid("hidden interval sequence")
	}
	if presence&2 != 0 {
		if deploymentNull(row, "owner_hint") {
			r.OwnerHint = json.RawMessage(`null`)
		} else {
			if e := deploymentV5String(row, "owner_hint"); e != nil {
				return r, e
			}
			r.OwnerHint = deploymentV5JSON(deploymentString(row, "owner_hint"))
		}
	} else if !deploymentNull(row, "owner_hint") {
		return r, invalid("hidden interval owner hint")
	}
	return r, nil
}
