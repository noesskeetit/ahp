// Package contracts holds the shared, stable vocabulary of the AHP platform:
// identity scope, error codes, identifiers and canonical digests. Service
// packages compile against these types instead of redefining them.
package contracts

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Scope is the three-part isolation boundary of every AHP record. It is always
// derived from verified identity, never from a request body.
type Scope struct {
	Tenant     string `json:"tenant"`
	Deployment string `json:"deployment"`
	Subject    string `json:"subject"`
}

// maxIdentifierLen bounds every scope identifier.
const maxIdentifierLen = 128

// Validate reports whether all three identifiers are present, bounded and made
// only of safe characters: letters, digits, dot, dash, underscore, colon and
// at-sign. Whitespace, slashes, control bytes and NUL are always rejected.
func (s Scope) Validate() error {
	for _, f := range []struct {
		name  string
		value string
	}{
		{"tenant", s.Tenant},
		{"deployment", s.Deployment},
		{"subject", s.Subject},
	} {
		if f.value == "" {
			return NewError(ErrInvalid, "scope "+f.name+" must not be empty")
		}
		if len(f.value) > maxIdentifierLen {
			return NewError(ErrInvalid, "scope "+f.name+" exceeds 128 bytes")
		}
		for i := 0; i < len(f.value); i++ {
			if !safeIdentifierByte(f.value[i]) {
				return NewError(ErrInvalid, "scope "+f.name+" contains an unsafe identifier character")
			}
		}
	}
	return nil
}

// safeIdentifierByte reports whether c belongs to the identifier alphabet.
func safeIdentifierByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '.', c == '-', c == '_', c == ':', c == '@':
		return true
	default:
		return false
	}
}

// idTimeLayout is the sortable UTC prefix of an identifier.
const idTimeLayout = "20060102T150405.000000000Z"

// idEncoding is a NUL-safe, case-insensitive random encoding.
var idEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// NewID returns a globally unique, path-safe identifier built from a UTC
// timestamp and 128 bits of cryptographic randomness. It never returns an
// error: the only failure mode is an exhausted system random source, which
// panics rather than silently producing weak identifiers.
func NewID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("contracts: cryptographic random source unavailable: " + err.Error())
	}
	return time.Now().UTC().Format(idTimeLayout) + "_" + strings.ToLower(idEncoding.EncodeToString(raw[:]))
}

// DigestPrefix marks every canonical digest produced by this package.
const DigestPrefix = "sha256:"

// Digest returns a canonical "sha256:<hex>" digest of v.
//
// Object keys are always sorted, so equivalent documents digest identically no
// matter their original key order or whitespace. Numbers keep their exact
// decimal value: nothing is rounded through float64 or a fixed precision, so
// two different values can never share a digest because of rounding. Raw JSON
// is accepted, but duplicate object keys, non-finite numbers and numbers whose
// decimal exponent leaves the range [-1000, 1000] are rejected, as is any value
// encoding/json cannot marshal.
func Digest(v any) (string, error) {
	canonical, err := canonicalJSON(v)
	if err != nil {
		return "", NewError(ErrInvalid, "cannot canonicalise value for digest: "+err.Error())
	}
	sum := sha256.Sum256(canonical)
	return DigestPrefix + hex.EncodeToString(sum[:]), nil
}

// canonicalJSON renders v as canonical JSON: sorted keys, no insignificant
// whitespace and exact numeric values.
func canonicalJSON(v any) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return []byte("null"), nil
	case json.RawMessage:
		return canonicalRaw(t)
	case *json.RawMessage:
		if t == nil {
			return []byte("null"), nil
		}
		return canonicalRaw(*t)
	case json.Number:
		return canonicalNumber(string(t))
	case string:
		return json.Marshal(t)
	case bool:
		return json.Marshal(t)
	case int:
		return []byte(strconv.FormatInt(int64(t), 10)), nil
	case int8:
		return []byte(strconv.FormatInt(int64(t), 10)), nil
	case int16:
		return []byte(strconv.FormatInt(int64(t), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(t), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(t, 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(t), 10)), nil
	case uint8:
		return []byte(strconv.FormatUint(uint64(t), 10)), nil
	case uint16:
		return []byte(strconv.FormatUint(uint64(t), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(t), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(t, 10)), nil
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return nil, fmt.Errorf("non-finite number %v", t)
		}
		// The shortest representation that round-trips is exact for float64,
		// so canonicalising it loses nothing and matches the same value given
		// as json.Number or raw JSON.
		return canonicalNumber(strconv.FormatFloat(t, 'g', -1, 64))
	case float32:
		return nil, fmt.Errorf("unsupported value type %T (use float64 or json.Number)", v)
	case []byte:
		return json.Marshal(base64.StdEncoding.EncodeToString(t))
	case map[string]any:
		return canonicalObject(t)
	case []any:
		return canonicalArray(t)
	case encoding.TextMarshaler:
		// encoding/json prefers text marshalling over struct reflection, so
		// the canonical form must be the exact JSON it would emit.
		data, mErr := json.Marshal(t)
		if mErr != nil {
			return nil, mErr
		}
		return canonicalRaw(data)
	default:
		return canonicalReflect(t)
	}
}

// canonicalObject canonicalises a decoded JSON object.
func canonicalObject(m map[string]any) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf strings.Builder
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteByte(':')
		val, err := canonicalJSON(m[k])
		if err != nil {
			return nil, err
		}
		buf.Write(val)
	}
	buf.WriteByte('}')
	return []byte(buf.String()), nil
}

// canonicalArray canonicalises a decoded JSON array.
func canonicalArray(a []any) ([]byte, error) {
	var buf strings.Builder
	buf.WriteByte('[')
	for i, e := range a {
		if i > 0 {
			buf.WriteByte(',')
		}
		val, err := canonicalJSON(e)
		if err != nil {
			return nil, err
		}
		buf.Write(val)
	}
	buf.WriteByte(']')
	return []byte(buf.String()), nil
}

// canonicalRaw validates raw JSON and rewrites it in canonical form.
func canonicalRaw(raw json.RawMessage) ([]byte, error) {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	value, err := decodeValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("unexpected data after JSON value")
		}
		return nil, err
	}
	return canonicalJSON(value)
}

// decodeValue decodes one JSON value, rejecting duplicate object keys and
// keeping numbers as json.Number so no literal is rounded.
func decodeValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			return decodeObject(dec)
		case '[':
			return decodeArray(dec)
		default:
			return nil, fmt.Errorf("unexpected delimiter %q", t)
		}
	case string, json.Number, bool:
		return t, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected JSON token %T", tok)
	}
}

// decodeObject decodes an object body, rejecting duplicate keys.
func decodeObject(dec *json.Decoder) (any, error) {
	obj := make(map[string]any)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected object key %v", keyTok)
		}
		if _, dup := obj[key]; dup {
			return nil, fmt.Errorf("duplicate object key %q", key)
		}
		val, err := decodeValue(dec)
		if err != nil {
			return nil, err
		}
		obj[key] = val
	}
	if _, err := dec.Token(); err != nil { // consume '}'
		return nil, err
	}
	return obj, nil
}

// decodeArray decodes an array body.
func decodeArray(dec *json.Decoder) (any, error) {
	arr := []any{}
	for dec.More() {
		val, err := decodeValue(dec)
		if err != nil {
			return nil, err
		}
		arr = append(arr, val)
	}
	if _, err := dec.Token(); err != nil { // consume ']'
		return nil, err
	}
	return arr, nil
}

// canonicalNumber normalises a JSON number literal exactly: the exponent bound
// is checked before any big allocation, and the mantissa is rewritten
// lexically, so no digit is ever rounded away.
func canonicalNumber(lit string) ([]byte, error) {
	sign, digits, exp, err := splitNumberLiteral(lit)
	if err != nil {
		return nil, err
	}
	digits, exp = trimMantissa(digits, exp)
	if exp > maxDecimalExp || exp < minDecimalExp {
		return nil, fmt.Errorf("number %q is out of the representable range", lit)
	}
	return []byte(renderNumber(sign, digits, exp)), nil
}

// Decimal exponent bounds keep canonical numbers finite, comparable and free of
// pathological expansions. Values inside the bound are rendered exactly.
const (
	maxDecimalExp = 1000
	minDecimalExp = -1000
)

// splitNumberLiteral parses a strict JSON number into sign, mantissa digits
// (without the decimal point) and the decimal exponent of the last digit.
// Anything that is not a valid JSON number literal is rejected.
func splitNumberLiteral(lit string) (sign string, digits string, exp int, err error) {
	if lit == "" {
		return "", "", 0, errors.New("empty number literal")
	}
	rest := lit
	if rest[0] == '-' {
		sign = "-"
		rest = rest[1:]
	}
	if rest == "" {
		return "", "", 0, fmt.Errorf("invalid JSON number %q", lit)
	}
	// Integer part: either a single zero or a digit sequence without leading
	// zeros. Anything else (NaN, Infinity, +1, 01, .5) is not a JSON number.
	var intPart string
	if rest[0] == '0' {
		intPart = "0"
		rest = rest[1:]
	} else {
		intPart, rest = takeDigits(rest)
		if intPart == "" {
			return "", "", 0, fmt.Errorf("invalid JSON number %q", lit)
		}
	}
	// Optional fraction.
	var frac string
	if rest != "" && rest[0] == '.' {
		rest = rest[1:]
		frac, rest = takeDigits(rest)
		if frac == "" {
			return "", "", 0, fmt.Errorf("invalid JSON number %q", lit)
		}
	}
	// Optional exponent.
	exp10 := 0
	if rest != "" && (rest[0] == 'e' || rest[0] == 'E') {
		rest = rest[1:]
		neg := false
		if rest != "" && (rest[0] == '+' || rest[0] == '-') {
			neg = rest[0] == '-'
			rest = rest[1:]
		}
		var expDigits string
		expDigits, rest = takeDigits(rest)
		if expDigits == "" {
			return "", "", 0, fmt.Errorf("invalid JSON number %q", lit)
		}
		// The bound is checked against the final canonical exponent in
		// canonicalNumber, so a huge literal exponent only has to not
		// overflow here; anything that large ends up out of range anyway.
		n, convErr := strconv.Atoi(truncateExp(expDigits))
		if convErr != nil {
			return "", "", 0, fmt.Errorf("invalid JSON number %q", lit)
		}
		if neg {
			n = -n
		}
		exp10 = n
	}
	if rest != "" {
		return "", "", 0, fmt.Errorf("trailing characters in number %q", lit)
	}
	digits = intPart + frac
	exp = exp10 - len(frac)
	return sign, digits, exp, nil
}

// truncateExp caps an exponent digit run so strconv.Atoi cannot overflow. Any
// exponent beyond the cap is far outside the canonical range and is rejected
// by the range check in canonicalNumber, so clamping is lossless for the
// accept/reject decision.
func truncateExp(digits string) string {
	const cap = "2147483647"
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return "0"
	}
	if len(digits) > len(cap) || (len(digits) == len(cap) && digits > cap) {
		return cap
	}
	return digits
}

// takeDigits returns the leading ASCII digit run of s and the remainder.
func takeDigits(s string) (digits, rest string) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return s[:i], s[i:]
}

// trimMantissa strips leading mantissa zeros without changing the exponent
// (they scale no digit) and strips trailing mantissa zeros, adding the count
// removed to the exponent so the represented value never changes. A zero value
// collapses to the empty digit string with exponent 0.
func trimMantissa(digits string, exp int) (string, int) {
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return "", 0
	}
	trimmed := strings.TrimRight(digits, "0")
	exp += len(digits) - len(trimmed)
	return trimmed, exp
}

// renderNumber writes the exact positional form of sign * digits * 10^exp,
// where digits are integer mantissa digits, which is the only form that stays
// comparable across JSON implementations.
func renderNumber(sign, digits string, exp int) string {
	if digits == "" {
		return "0" // negative zero canonicalises to zero
	}
	var out strings.Builder
	out.WriteString(sign)
	switch {
	case exp == 0:
		out.WriteString(digits)
	case exp > 0:
		out.WriteString(digits)
		out.WriteString(strings.Repeat("0", exp))
	case -exp >= len(digits):
		out.WriteString("0.")
		out.WriteString(strings.Repeat("0", -exp-len(digits)))
		out.WriteString(digits)
	default:
		split := len(digits) + exp
		out.WriteString(digits[:split])
		out.WriteByte('.')
		out.WriteString(digits[split:])
	}
	return out.String()
}

// OperationKey returns a stable, collision-resistant hex key for one
// idempotent side effect inside a scope. Fields are length-framed before
// hashing, so no boundary between fields can be forged.
func OperationKey(scope Scope, run, node, call string) string {
	h := sha256.New()
	writeFramed(h, scope.Tenant)
	writeFramed(h, scope.Deployment)
	writeFramed(h, scope.Subject)
	writeFramed(h, run)
	writeFramed(h, node)
	writeFramed(h, call)
	return hex.EncodeToString(h.Sum(nil))
}

// writeFramed writes a big-endian length prefix followed by the field bytes.
func writeFramed(h io.Writer, field string) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(field)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(field))
}
