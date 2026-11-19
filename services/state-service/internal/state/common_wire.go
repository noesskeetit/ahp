package state

import (
	"bytes"
	"encoding/json"
	"io"
)

const CommonFormat = "ahp-common-semantic-records/research-2"
const MaxCommonDocumentBytes = 8 << 20

type CommonRecord struct {
	Kind       string              `json:"kind"`
	NativeID   json.RawMessage     `json:"native_id,omitempty"`
	Sequence   json.RawMessage     `json:"sequence,omitempty"`
	OwnerHint  json.RawMessage     `json:"owner_hint,omitempty"`
	Attributes json.RawMessage     `json:"attributes"`
	Relations  map[string][]string `json:"relations"`
}
type CommonDocument struct {
	Format  string                  `json:"format"`
	Profile string                  `json:"profile"`
	Root    string                  `json:"root"`
	Records map[string]CommonRecord `json:"records"`
}

// commonObject checks duplicate envelope keys without parsing opaque payloads.
// Identifiers are ASCII machine keys; native identifiers remain raw JSON values.
func commonObject(data []byte, allowed []string) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return nil, invalid("common wire requires an object")
	}
	out := map[string]json.RawMessage{}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return nil, invalid("invalid common wire object")
		}
		key, ok := token.(string)
		if !ok {
			return nil, invalid("invalid common wire key")
		}
		if _, exists := out[key]; exists {
			return nil, invalid("duplicate common wire key")
		}
		if allowed != nil {
			found := false
			for _, v := range allowed {
				if key == v {
					found = true
					break
				}
			}
			if !found {
				return nil, invalid("unknown common wire field")
			}
		}
		var value json.RawMessage
		if err = d.Decode(&value); err != nil {
			return nil, invalid("invalid common wire value")
		}
		out[key] = value
	}
	if _, err = d.Token(); err != nil {
		return nil, invalid("invalid common wire ending")
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return nil, invalid("trailing common wire data")
	}
	return out, nil
}
func (v *CommonDocument) UnmarshalJSON(data []byte) error {
	fields, err := commonObject(data, []string{"format", "profile", "root", "records"})
	if err != nil {
		return err
	}
	var doc CommonDocument
	for key, target := range map[string]*string{"format": &doc.Format, "profile": &doc.Profile, "root": &doc.Root} {
		if err = json.Unmarshal(fields[key], target); err != nil {
			return invalid("missing or invalid common document field")
		}
	}
	rows, err := commonObject(fields["records"], nil)
	if err != nil {
		return err
	}
	doc.Records = map[string]CommonRecord{}
	for id, body := range rows {
		if err = validIdentifier(id, "common record id"); err != nil {
			return err
		}
		f, err := commonObject(body, []string{"kind", "native_id", "sequence", "owner_hint", "attributes", "relations"})
		if err != nil {
			return err
		}
		var row CommonRecord
		if err = json.Unmarshal(f["kind"], &row.Kind); err != nil {
			return invalid("missing common record kind")
		}
		row.NativeID = f["native_id"]
		row.Sequence = f["sequence"]
		row.OwnerHint = f["owner_hint"]
		row.Attributes = f["attributes"]
		links, err := commonObject(f["relations"], nil)
		if err != nil {
			return err
		}
		row.Relations = map[string][]string{}
		for role, raw := range links {
			if err = validIdentifier(role, "common relation role"); err != nil {
				return err
			}
			if len(raw) == 0 || raw[0] != '[' {
				return invalid("common relation requires an ordered array")
			}
			var refs []string
			if err = json.Unmarshal(raw, &refs); err != nil {
				return invalid("invalid common relation array")
			}
			if refs == nil {
				refs = []string{}
			}
			row.Relations[role] = refs
		}
		doc.Records[id] = row
	}
	if err = doc.Validate(); err != nil {
		return err
	}
	*v = doc
	return nil
}
func commonSequence(raw json.RawMessage) (*int64, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, invalid("common sequence must be int64 or null")
	}
	return &value, nil
}
func commonOwnerHint(raw json.RawMessage) (*string, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, invalid("common owner hint must be a string or null")
	}
	if value != "" {
		if err := validIdentifier(value, "common owner hint"); err != nil {
			return nil, err
		}
	}
	return &value, nil
}
func (v CommonDocument) Validate() error {
	if v.Format != CommonFormat {
		return invalid("unsupported common wire format")
	}
	if err := validIdentifier(v.Profile, "common runtime profile"); err != nil {
		return err
	}
	if err := validIdentifier(v.Root, "common root"); err != nil {
		return err
	}
	if len(v.Records) == 0 || len(v.Records) > 50000 {
		return invalid("common record count outside limit")
	}
	if _, ok := v.Records[v.Root]; !ok {
		return invalid("common root does not exist")
	}
	edges := 0
	for id, row := range v.Records {
		if err := validIdentifier(id, "common record id"); err != nil {
			return err
		}
		if err := validIdentifier(row.Kind, "common record kind"); err != nil {
			return err
		}
		if len(row.Attributes) == 0 || !json.Valid(row.Attributes) {
			return invalid("common attributes must contain valid JSON")
		}
		if len(row.NativeID) > 0 && !json.Valid(row.NativeID) {
			return invalid("invalid common native id JSON")
		}
		if _, err := commonSequence(row.Sequence); err != nil {
			return err
		}
		if _, err := commonOwnerHint(row.OwnerHint); err != nil {
			return err
		}
		if row.Relations == nil {
			return invalid("common relations must be an object")
		}
		for role, refs := range row.Relations {
			if err := validIdentifier(role, "common relation role"); err != nil {
				return err
			}
			if refs == nil {
				return invalid("common relation cannot be null")
			}
			edges += len(refs)
			if edges > 200000 {
				return invalid("common relation count exceeds limit")
			}
			for _, ref := range refs {
				if _, ok := v.Records[ref]; !ok {
					return invalid("dangling common relation")
				}
			}
		}
	}
	data, err := json.Marshal(v)
	if err != nil {
		return invalid("invalid common document")
	}
	if len(data) > MaxCommonDocumentBytes {
		return invalid("common document exceeds 8MiB")
	}
	return nil
}
