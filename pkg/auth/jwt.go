package auth

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// maxTokenBytes bounds the size of a presented token. Anything larger is
// rejected before decoding, so a hostile token cannot make the verifier
// allocate without limit.
const maxTokenBytes = 16 * 1024

// header is the JOSE header of an AHP token. Unknown members are ignored;
// duplicated members are rejected by decodeJSONStrict.
type header struct {
	Type      string `json:"typ"`
	Class     string `json:"cty"`
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
}

// encodeSegment marshals v and base64url-encodes it without padding.
func encodeSegment(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("auth: cannot encode token segment: %w", err)
	}
	dst := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	base64.RawURLEncoding.Encode(dst, raw)
	return dst, nil
}

// splitToken splits a compact serialisation into decoded header, payload and
// signature segments, applying the transport-level limits first.
func splitToken(token string) (headerRaw, payloadRaw, signature []byte, err error) {
	if token == "" {
		return nil, nil, nil, invalidToken("token is empty")
	}
	if len(token) > maxTokenBytes {
		return nil, nil, nil, invalidToken("token exceeds the 16 KiB limit")
	}
	if strings.ContainsAny(token, " \t\r\n") {
		return nil, nil, nil, invalidToken("token contains whitespace")
	}
	if strings.IndexByte(token, 0) >= 0 {
		return nil, nil, nil, invalidToken("token contains a NUL byte")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, nil, nil, invalidToken("token must have exactly three segments")
	}
	for _, p := range parts {
		if !isBase64URL(p) {
			return nil, nil, nil, invalidToken("token segments must be unpadded base64url")
		}
	}
	if len(parts[2]) != encodedLen(signatureLen) {
		return nil, nil, nil, invalidToken("token signature segment has the wrong length")
	}
	if headerRaw, err = decodeSegment(parts[0]); err != nil {
		return nil, nil, nil, err
	}
	if payloadRaw, err = decodeSegment(parts[1]); err != nil {
		return nil, nil, nil, err
	}
	if signature, err = decodeSegment(parts[2]); err != nil {
		return nil, nil, nil, err
	}
	if len(headerRaw) == 0 || len(payloadRaw) == 0 {
		return nil, nil, nil, invalidToken("token segments must not be empty")
	}
	return headerRaw, payloadRaw, signature, nil
}

// decodeSegment decodes one base64url segment, rejecting padding, characters
// outside the alphabet and non-canonical trailing bits, so that two different
// byte strings can never denote the same segment.
func decodeSegment(seg string) ([]byte, error) {
	raw := make([]byte, base64.RawURLEncoding.DecodedLen(len(seg)))
	n, err := base64.RawURLEncoding.Decode(raw, []byte(seg))
	if err != nil {
		return nil, invalidToken("token segment is not valid base64url")
	}
	raw = raw[:n]
	re := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	base64.RawURLEncoding.Encode(re, raw)
	if string(re) != seg {
		return nil, invalidToken("token segment uses non-canonical base64url")
	}
	return raw, nil
}

// isBase64URL reports whether s is a well-formed unpadded base64url string.
func isBase64URL(s string) bool {
	for i := 0; i < len(s); i++ {
		if base64URLIndex(s[i]) < 0 {
			return false
		}
	}
	switch len(s) % 4 {
	case 2:
		// Two characters carry one byte: the final character must keep its
		// four unused low bits clear.
		return strings.IndexByte("AQgw", s[len(s)-1]) >= 0
	case 3:
		// Three characters carry two bytes: only the two low bits of the
		// final character are unused.
		return strings.IndexByte("AEIMQUYcgkosw048", s[len(s)-1]) >= 0
	case 0:
		// A whole number of quanta carries no leftover data bits.
		return true
	default: // 1: cannot encode a whole number of bytes
		return false
	}
}

// base64URLIndex returns the value of c in the base64url alphabet, or -1.
func base64URLIndex(c byte) int {
	switch {
	case c >= 'A' && c <= 'Z':
		return int(c - 'A')
	case c >= 'a' && c <= 'z':
		return int(c-'a') + 26
	case c >= '0' && c <= '9':
		return int(c-'0') + 52
	case c == '-':
		return 62
	case c == '_':
		return 63
	default:
		return -1
	}
}

// decodeJSONStrict unmarshals a JSON object into target, rejecting trailing
// data and duplicated member names: the classic way to smuggle a second
// meaning past a lenient parser.
func decodeJSONStrict(raw []byte, target any) error {
	if err := rejectDuplicateKeys(raw); err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(target); err != nil {
		return invalidToken("token JSON is malformed: " + err.Error())
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return invalidToken("token JSON has trailing data")
	}
	return nil
}

// rejectDuplicateKeys walks raw JSON and fails on the first member name that
// repeats inside the same object.
func rejectDuplicateKeys(raw []byte) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if _, err := dec.Token(); err != nil {
		return invalidToken("token JSON is malformed: " + err.Error())
	}
	return scanMembers(dec)
}

// scanMembers walks one JSON value; each object tracks its own member names.
func scanMembers(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return invalidToken("token JSON is malformed: " + err.Error())
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil // scalar value
	}
	switch delim {
	case '{':
		names := make(map[string]struct{})
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return invalidToken("token JSON is malformed: " + err.Error())
			}
			key, ok := keyTok.(string)
			if !ok {
				return invalidToken("token JSON has a non-string object key")
			}
			if _, dup := names[key]; dup {
				return invalidToken("token JSON contains a duplicate object key")
			}
			names[key] = struct{}{}
			if err := scanMembers(dec); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := scanMembers(dec); err != nil {
				return err
			}
		}
	default:
		return invalidToken("token JSON has an unexpected delimiter")
	}
	if _, err := dec.Token(); err != nil { // consume the closing delimiter
		return invalidToken("token JSON is malformed: " + err.Error())
	}
	return nil
}
