package state

import (
	"encoding/json"
	"strings"
	"testing"
)

const commonWireFixture = `{"format":"ahp-common-semantic-records/research-2","profile":"fixture/1","root":"s","records":{"s":{"kind":"session","attributes":{"nul":"\u0000","surrogate":"\ud800","x":1,"x":2},"relations":{"entries":["v"],"empty":[]}},"v":{"kind":"value","native_id":null,"sequence":null,"owner_hint":"","attributes":[true,123456789012345678901234567890],"relations":{}}}}`

func TestCommonWirePreservesOptionalAndOpaquePayload(t *testing.T) {
	var d CommonDocument
	if err := json.Unmarshal([]byte(commonWireFixture), &d); err != nil {
		t.Fatal(err)
	}
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Records map[string]map[string]json.RawMessage `json:"records"`
	}
	if err = json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw.Records["s"]["native_id"]; ok {
		t.Fatal("absent native_id became present")
	}
	if string(raw.Records["v"]["native_id"]) != "null" || string(raw.Records["v"]["sequence"]) != "null" {
		t.Fatal("explicit null lost")
	}
	for _, literal := range []string{`\u0000`, `\ud800`, `123456789012345678901234567890`, `"x":1,"x":2`} {
		if !strings.Contains(string(data), literal) {
			t.Fatalf("opaque payload changed: %s", literal)
		}
	}
}
func TestCommonWireRejectsAmbiguousEnvelope(t *testing.T) {
	cases := map[string]string{
		"duplicate record":    strings.Replace(commonWireFixture, `"s":{"kind"`, `"s":{},"s":{"kind"`, 1),
		"duplicate field":     strings.Replace(commonWireFixture, `"kind":"session"`, `"kind":"event","kind":"session"`, 1),
		"dangling":            strings.Replace(commonWireFixture, `"entries":["v"]`, `"entries":["missing"]`, 1),
		"boolean sequence":    strings.Replace(commonWireFixture, `"sequence":null`, `"sequence":true`, 1),
		"fractional sequence": strings.Replace(commonWireFixture, `"sequence":null`, `"sequence":1.5`, 1),
		"null links":          strings.Replace(commonWireFixture, `"empty":[]`, `"empty":null`, 1),
		"unknown envelope":    strings.Replace(commonWireFixture, `"kind":"session"`, `"unexpected":true,"kind":"session"`, 1),
		"surrogate record id": strings.Replace(commonWireFixture, `"s":{"kind"`, `"\ud800":{"kind"`, 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			var d CommonDocument
			err := json.Unmarshal([]byte(body), &d)
			if err == nil {
				err = d.Validate()
			}
			if err == nil {
				t.Fatal("invalid common document accepted")
			}
		})
	}
}
