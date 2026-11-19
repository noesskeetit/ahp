// Package contracts: canonicalisation of values encoding/json can marshal.

package contracts

import (
	"encoding/json"
)

// canonicalReflect canonicalises any value encoding/json can marshal (structs,
// pointers, named types, maps with string keys, slices and arrays). Named
// MarshalJSON implementations are honoured; anything json.Marshal refuses is
// rejected here as well.
//
// Raw JSON carried by a value (json.RawMessage fields or a MarshalJSON result)
// is decoded with UseNumber and duplicate-key detection before it is hashed, so
// a document that hides duplicate object keys is rejected instead of being
// silently normalised by encoding/json.
func canonicalReflect(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return canonicalRaw(json.RawMessage(data))
}
