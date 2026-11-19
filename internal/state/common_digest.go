package state

import (
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Keep the existing digest for every request accepted by earlier candidates.
// Native attributes/IDs are opaque JSON bytes: duplicate native object keys
// are valid carriers even though the generic JSON canonicalizer refuses them.
// The fallback has a distinct domain and represents only opaque fields as
// byte strings, preserving their exact native encoding and optional presence.
func commonMutationDigest(run string, req CommonCommit) (string, error) {
	if req.Delta != nil {
		raw, err := json.Marshal(req)
		if err != nil {
			return "", err
		}
		opaque := map[string]any{}
		for id, row := range req.Delta.Put {
			opaque[id] = struct{ NativeID, Sequence, OwnerHint, Attributes []byte }{row.NativeID, row.Sequence, row.OwnerHint, row.Attributes}
		}
		kv := make([][]byte, len(req.KV))
		for i, v := range req.KV {
			kv[i] = v.Value
		}
		return contracts.Digest(struct {
			Domain string         `json:"domain"`
			Run    string         `json:"run"`
			Body   []byte         `json:"body"`
			Opaque map[string]any `json:"opaque"`
			KV     [][]byte       `json:"kv"`
		}{"ahp-common-delta/1", run, raw, opaque, kv})
	}

	original := struct {
		RunID   string       `json:"run_id"`
		Request CommonCommit `json:"request"`
	}{run, req}
	if value, err := contracts.Digest(original); err == nil {
		return value, nil
	}
	records := make(map[string]any, len(req.Document.Records))
	for id, row := range req.Document.Records {
		records[id] = struct {
			Kind       string              `json:"kind"`
			NativeID   []byte              `json:"native_id"`
			Sequence   []byte              `json:"sequence"`
			OwnerHint  []byte              `json:"owner_hint"`
			Attributes []byte              `json:"attributes"`
			Relations  map[string][]string `json:"relations"`
		}{row.Kind, row.NativeID, row.Sequence, row.OwnerHint, row.Attributes, row.Relations}
	}
	document := struct {
		Format  string         `json:"format"`
		Profile string         `json:"profile"`
		Root    string         `json:"root"`
		Records map[string]any `json:"records"`
	}{req.Document.Format, req.Document.Profile, req.Document.Root, records}
	request := struct {
		*CommonCommit
		Document any `json:"document"`
	}{&req, document}
	return contracts.Digest(struct {
		Domain  string `json:"domain"`
		RunID   string `json:"run_id"`
		Request any    `json:"request"`
	}{"ahp-common-opaque-native-digest/1", run, request})
}
