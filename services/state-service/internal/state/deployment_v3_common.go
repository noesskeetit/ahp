package state

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func validateDeploymentCommon(e DeploymentV3, threads, runs map[string]string) error {
	return validateDeploymentCommonRetention(e, threads, runs, false)
}
func validateDeploymentCommonRetention(e DeploymentV3, threads, runs map[string]string, allowPruned bool) error {
	type cut struct {
		row     deploymentRow
		doc     CommonDocument
		records map[string]bool
		receipt bool
	}
	runThreads := map[string]string{}
	for _, r := range deploymentRows(e, "runs") {
		runThreads[deploymentString(r, "id")] = deploymentString(r, "thread_id")
	}
	expanded := 0
	cuts := map[string]*cut{}
	ids := map[string]bool{}
	objects := map[string][]byte{}
	maxima := map[string]int64{}
	versions := map[string]int{}
	key := func(r deploymentRow) string {
		return deploymentTuple(deploymentString(r, "thread_id"), deploymentString(r, "namespace"), string(r["revision"]))
	}
	for _, r := range deploymentRows(e, "common_payloads") {
		tid, h := deploymentString(r, "thread_id"), deploymentString(r, "sha256")
		payload, err := deploymentBytes(r, "attributes")
		if err != nil || threads[tid] == "" || !json.Valid(payload) || deploymentPayloadHash(payload) != h {
			return invalid("immutable payload hash/owner mismatch")
		}
		objects[deploymentTuple(tid, h)] = payload
	}
	for _, r := range deploymentRows(e, "common_cuts") {
		tid, ns, id := deploymentString(r, "thread_id"), deploymentString(r, "namespace"), deploymentString(r, "id")
		rev, err := deploymentInt(r, "revision")
		if err != nil || rev < 1 || rev > MaxDeploymentV3Rows || commonNamespace(ns) != nil || validIdentifier(id, "cut id") != nil || ids[id] {
			return invalid("invalid common cut identity")
		}
		ids[id] = true
		k := key(r)
		if cuts[k] != nil {
			return invalid("duplicate common cut")
		}
		cuts[k] = &cut{row: r, doc: CommonDocument{Format: deploymentString(r, "format"), Profile: deploymentString(r, "profile"), Root: deploymentString(r, "root"), Records: map[string]CommonRecord{}}, records: map[string]bool{}}
		nk := deploymentTuple(tid, ns)
		versions[nk]++
		if rev > maxima[nk] {
			maxima[nk] = rev
		}
	}
	for _, r := range deploymentRows(e, "common_records") {
		c := cuts[key(r)]
		if c == nil {
			return invalid("record has no common cut")
		}
		id := deploymentString(r, "id")
		if c.records[id] {
			return invalid("duplicate common record")
		}
		c.records[id] = true
		if allowPruned && c.doc.Format == commonPrunedLayout && !deploymentNull(r, "attributes_hash") {
			return invalid("archived marker must be inline")
		}
		attrs, err := deploymentBytes(r, "attributes")
		if err != nil {
			return err
		}
		if !deploymentNull(r, "attributes_hash") {
			if len(attrs) != 0 {
				return invalid("both inline and object payload present")
			}
			var ok bool
			attrs, ok = objects[deploymentTuple(deploymentString(r, "thread_id"), deploymentString(r, "attributes_hash"))]
			if !ok {
				return invalid("missing immutable payload object")
			}
		}
		nid, err := deploymentBytes(r, "native_id")
		if err != nil {
			return err
		}
		expanded += len(attrs) + len(nid)
		if expanded > MaxDeploymentV3ExpandedBytes {
			return invalid("deployment expanded validation budget exceeded")
		}
		presence, err := deploymentInt(r, "presence")
		if err != nil || presence < 0 || presence > 3 {
			return invalid("invalid common presence bits")
		}
		v := CommonRecord{Kind: deploymentString(r, "kind"), NativeID: nid, Attributes: attrs, Relations: map[string][]string{}}
		if presence&1 != 0 {
			if deploymentNull(r, "sequence") {
				v.Sequence = json.RawMessage(`null`)
			} else {
				seq, err := deploymentInt(r, "sequence")
				if err != nil {
					return err
				}
				v.Sequence = json.RawMessage(strconv.FormatInt(seq, 10))
			}
		} else if !deploymentNull(r, "sequence") {
			return invalid("hidden native sequence")
		}
		if presence&2 != 0 {
			if deploymentNull(r, "owner_hint") {
				v.OwnerHint = json.RawMessage(`null`)
			} else {
				v.OwnerHint, _ = json.Marshal(deploymentString(r, "owner_hint"))
			}
		} else if !deploymentNull(r, "owner_hint") {
			return invalid("hidden native owner hint")
		}
		c.doc.Records[id] = v
	}
	relations := map[string]map[int64]string{}
	for _, r := range deploymentRows(e, "common_roles") {
		c := cuts[key(r)]
		if c == nil {
			return invalid("role has no cut")
		}
		id, role := deploymentString(r, "source"), deploymentString(r, "role")
		v, ok := c.doc.Records[id]
		if !ok {
			return invalid("role source absent")
		}
		if _, ok = v.Relations[role]; ok {
			return invalid("duplicate role")
		}
		v.Relations[role] = []string{}
		c.doc.Records[id] = v
		relations[deploymentTuple(key(r), id, role)] = map[int64]string{}
	}
	for _, r := range deploymentRows(e, "common_edges") {
		c := cuts[key(r)]
		if c == nil {
			return invalid("edge has no cut")
		}
		source, role, target := deploymentString(r, "source"), deploymentString(r, "role"), deploymentString(r, "target")
		rk := deploymentTuple(key(r), source, role)
		ord, err := deploymentInt(r, "ordinal")
		if err != nil || relations[rk] == nil || !c.records[target] {
			return invalid("edge target/source/role absent")
		}
		if _, ok := relations[rk][ord]; ok {
			return invalid("duplicate edge ordinal")
		}
		relations[rk][ord] = target
	}
	for k, c := range cuts {
		for id, v := range c.doc.Records {
			for role := range v.Relations {
				refs, err := deploymentSortedOrdinals(relations[deploymentTuple(k, id, role)])
				if err != nil {
					return err
				}
				v.Relations[role] = refs
			}
			c.doc.Records[id] = v
		}
		if allowPruned && c.doc.Format == commonPrunedLayout {
			if _, err := validatePrunedCommon(c.doc); err != nil {
				return err
			}
			continue
		}
		if err := c.doc.Validate(); err != nil {
			return err
		}
	}
	heads := map[string]bool{}
	for _, r := range deploymentRows(e, "common_heads") {
		tid, ns := deploymentString(r, "thread_id"), deploymentString(r, "namespace")
		nk := deploymentTuple(tid, ns)
		rev, err := deploymentInt(r, "revision")
		if err != nil || cuts[key(r)] == nil || rev != maxima[nk] || heads[nk] || int64(versions[nk]) != rev {
			return invalid("common history/head incomplete")
		}
		heads[nk] = true
	}
	for nk := range maxima {
		if !heads[nk] {
			return invalid("common history missing head")
		}
	}
	for _, r := range deploymentRows(e, "common_receipts") {
		c := cuts[key(r)]
		if c == nil || c.receipt {
			return invalid("common receipt cut absent/duplicate")
		}
		raw, err := deploymentBytes(r, "result")
		if err != nil {
			return err
		}
		if _, err := contracts.Digest(json.RawMessage(raw)); err != nil {
			return invalid("invalid common receipt JSON")
		}
		var receipt CommonReceipt
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if dec.Decode(&receipt) != nil {
			return invalid("unknown common receipt format")
		}
		rev, _ := deploymentInt(r, "revision")
		var created time.Time
		if json.Unmarshal(c.row["created_at"], &created) != nil {
			return invalid("invalid cut timestamp")
		}
		recordCount := len(c.doc.Records)
		if allowPruned && c.doc.Format == commonPrunedLayout {
			marker, err := validatePrunedCommon(c.doc)
			if err != nil {
				return err
			}
			recordCount = marker.RecordCount
		}
		if receipt.CutID != deploymentString(c.row, "id") || receipt.Revision != rev || receipt.Key != deploymentString(r, "key") || receipt.Key != deploymentString(c.row, "operation_key") || receipt.Namespace != deploymentString(r, "namespace") || receipt.RecordCount != recordCount || !receipt.CreatedAt.Equal(created) || runs[receipt.RunID] != threads[deploymentString(r, "thread_id")] || !(strings.HasPrefix(deploymentString(r, "digest"), contracts.DigestPrefix) && deploymentSHA(strings.TrimPrefix(deploymentString(r, "digest"), contracts.DigestPrefix))) {
			return invalid("common receipt identity differs from cut/run")
		}
		// Run must belong to this thread, not merely to the same subject.
		runThread := runThreads[receipt.RunID]
		if runThread != deploymentString(r, "thread_id") {
			return invalid("receipt belongs to another thread")
		}
		if len(receipt.KV) > 8 {
			return invalid("receipt owner update bound")
		}
		for _, item := range receipt.KV {
			if item.Version < 1 || validKVNamespace(item.Namespace) != nil || validKVKey(item.Key) != nil || !json.Valid(item.Value) || item.ThreadID != "" && item.ThreadID != runThread {
				return invalid("invalid common owner receipt")
			}
		}
		c.receipt = true
	}
	for _, c := range cuts {
		if !c.receipt {
			return invalid("common cut missing durable receipt")
		}
	}
	return nil
}
