package gateway

import (
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// A part is the unit of A2A content, and a2a-go already guarantees that a part
// decodes into exactly one of text/raw/data/url (Part.UnmarshalJSON rejects
// zero or several discriminators). What nothing guaranteed was the opposite
// extreme: a message whose parts are all present but carry nothing at all --
// {"text":""} is a perfectly well-formed text part. That message was accepted,
// a task was created for it, and the task then died inside the worker with a
// content error, which reads to an operator as a broken executor. An empty
// request is a client error and belongs at the door with the same -32602 as any
// other invalid params, so the caller learns it before a task id exists.
//
// Note what is deliberately NOT enforced here: the wire shape of a part. A2A 1.0
// -- the protocol version this plane's Agent Card declares -- has no "kind"
// discriminator on a part; a2a-go flattens a part to {"text": ...} / {"raw": ...}
// / {"data": ...} / {"url": ...}. A v0.3 client's "kind" is accepted and ignored
// by the library, so a v0 client's text round-trips intact. Echoing "kind" back
// would be a claim about a protocol version this face does not serve.

// messageHasUsableContent reports whether an inbound message carries anything an
// agent can act on: at least one part with non-blank text, non-empty raw bytes,
// a file URL, or a structured data value.
func messageHasUsableContent(m *a2a.Message) bool {
	if m == nil {
		return false
	}
	for _, p := range m.Parts {
		if p == nil {
			continue
		}
		if strings.TrimSpace(p.Text()) != "" || len(p.Raw()) > 0 || p.URL() != "" || p.Data() != nil {
			return true
		}
	}
	return false
}

// validateInboundContent refuses an empty request at the ingress. The reason is
// phrased for the client: it says which field is wrong and what a valid value
// looks like, so the caller does not have to guess from a task that vanished.
func validateInboundContent(m *a2a.Message) error {
	if messageHasUsableContent(m) {
		return nil
	}
	return contracts.NewReasoned(contracts.ErrInvalid, ReasonContentEmpty,
		"gateway: message needs at least one part carrying text, raw, url or data")
}
