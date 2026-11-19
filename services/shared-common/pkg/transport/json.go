package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DefaultMaxBodyBytes bounds ReadJSON when the caller passes maxBytes <= 0.
const DefaultMaxBodyBytes int64 = 1 << 20

// invalidJSON is the single rejection message for malformed API bodies.
func invalidJSON() error {
	return contracts.NewError(contracts.ErrInvalid, "transport: request body is not valid JSON")
}

// decodeFailure names what the decoder actually rejected.
//
// The verdict is not the point of contention: an unknown field stays a 400,
// because a strict decode is the contract (see TestReadJSONRejects) and
// silently accepting a field nobody declared is how a client starts believing
// it sent something the server stored. The message was the lie. A body like
// {"turn_id":"t1","reason":"cleanup"} IS valid JSON - CancelTurn's request is
// {turn_id} and nothing more (docs/space-components-api-contract-2026-10-30.md:43) -
// and telling its author "request body is not valid JSON" sends them to fix
// syntax that was never broken.
func decodeFailure(err error) error {
	var syn *json.SyntaxError
	if errors.As(err, &syn) {
		return invalidJSON()
	}
	var typ *json.UnmarshalTypeError
	if errors.As(err, &typ) {
		if typ.Field != "" {
			return contracts.NewError(contracts.ErrInvalid,
				"transport: field "+strconv.Quote(typ.Field)+" is not of the expected type")
		}
		return contracts.NewError(contracts.ErrInvalid,
			"transport: request body has a value of the wrong type")
	}
	if field, ok := unknownField(err.Error()); ok {
		return contracts.NewError(contracts.ErrInvalid,
			"transport: request body has an unknown field "+strconv.Quote(field))
	}
	return invalidJSON()
}

// unknownField recovers the rejected name from encoding/json's message, which
// is the only place the unknown-field decision survives: the stdlib reports it
// as a plain error, not a typed one.
func unknownField(msg string) (string, bool) {
	rest, ok := strings.CutPrefix(msg, `json: unknown field "`)
	if !ok {
		return "", false
	}
	name, _, _ := strings.Cut(rest, `"`)
	return name, true
}

// ReadJSON decodes at most maxBytes of r into out. Unknown fields, duplicate
// object keys, a second JSON value and trailing data are rejected. All reads
// stay behind http.MaxBytesReader; the body is closed without draining.
func ReadJSON(w http.ResponseWriter, r *http.Request, out any, maxBytes int64) error {
	if out == nil {
		return contracts.NewError(contracts.ErrInvalid, "transport: decode target must not be nil")
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBodyBytes
	}
	if r.Body == nil {
		return contracts.NewError(contracts.ErrInvalid, "transport: request has no body")
	}
	defer func() { _ = r.Body.Close() }()
	limited := http.MaxBytesReader(w, r.Body, maxBytes)
	raw, err := io.ReadAll(limited)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return contracts.NewError(contracts.ErrInvalid, "transport: request body too large")
		}
		return invalidJSON()
	}
	trimmed := bytes.TrimRight(raw, " \t\r\n")
	if len(bytes.TrimLeft(trimmed, " \t\r\n")) == 0 {
		return contracts.NewError(contracts.ErrInvalid, "transport: request body is empty")
	}
	// Duplicate object keys are rejected via the canonicalising digest, which
	// bounds its own work and never touches the original body again.
	if _, err := contracts.Digest(json.RawMessage(trimmed)); err != nil {
		return invalidJSON()
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return decodeFailure(err)
	}
	if err := ensureEOF(dec); err != nil {
		return err
	}
	return nil
}

// ensureEOF rejects anything but whitespace after the first JSON value: it
// decodes exactly one additional value and requires io.EOF.
func ensureEOF(dec *json.Decoder) error {
	var discard json.RawMessage
	switch err := dec.Decode(&discard); {
	case errors.Is(err, io.EOF):
		return nil
	case err == nil:
		return contracts.NewError(contracts.ErrInvalid,
			"transport: request body must contain exactly one JSON value")
	default:
		return contracts.NewError(contracts.ErrInvalid,
			"transport: request body must contain exactly one JSON value")
	}
}

// WriteJSON writes status and a JSON body with nosniff and no-cache.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		body = []byte(`{"code":"INTERNAL","message":"internal error"}`)
		status = http.StatusInternalServerError
	}
	body = append(body, '\n')
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cache-Control", "no-store")
	h.Del("Content-Length")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// domainReason is the error body used when a response has to say more than its
// class allows: contracts.Error stays exactly as every other lane marshals it,
// and a stable machine-readable `reason` rides beside `code`.
type domainReason struct {
	*contracts.Error
	Reason string `json:"reason"`
}

// WriteError maps contract codes to HTTP statuses. Unclassified errors become a
// bare 500 so internal detail never reaches the client - not as a message and
// not as a reason.
//
// `reason` carries a stable machine-readable detail beside `code` in two cases:
// when the HTTP class flattens a domain code (KEY_REUSE_MISMATCH lives on 400
// with the SDK and Space already mapping 400, so the domain code travels as the
// reason instead of being rewritten), and when the domain attached one of its
// own through contracts.NewReasoned.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	code := contracts.Code(err)
	var status int
	var surfaced string
	switch code {
	case contracts.ErrInvalid, contracts.ErrKeyReuse:
		status, surfaced = http.StatusBadRequest, contracts.ErrInvalid
	case contracts.ErrForbidden:
		status, surfaced = http.StatusForbidden, contracts.ErrForbidden
	case contracts.ErrNotFound:
		status, surfaced = http.StatusNotFound, contracts.ErrNotFound
	case contracts.ErrConflict, contracts.ErrStaleEpoch, contracts.ErrLeaseLost:
		status, surfaced = http.StatusConflict, code
	case contracts.ErrUnavailable:
		// A dependency failure is the server's problem, not the caller's, and it
		// must not arrive as CONFLICT: operators triage a 503, clients do not
		// read it as "retry the same write and it will fit".
		status, surfaced = http.StatusServiceUnavailable, code
	default:
		WriteJSON(w, http.StatusInternalServerError,
			&contracts.Error{Code: "INTERNAL", Message: "internal server error"})
		return
	}
	base := &contracts.Error{Code: surfaced, Message: err.Error()}
	reason := contracts.ReasonOf(err)
	if code != surfaced {
		reason = code
	}
	if reason != "" {
		WriteJSON(w, status, &domainReason{Error: base, Reason: reason})
		return
	}
	WriteJSON(w, status, base)
}
