package contracts

import (
	"encoding/json"
	"errors"
	"math"
)

// errTest is returned by the unsupported value used in rejection tests.
var errTest = errors.New("test encoder failure")

// mathNaN returns a non-finite float64 for rejection tests.
func mathNaN() float64 { return math.NaN() }

// mathInf returns an infinite float64 for rejection tests.
func mathInf(sign int) float64 { return math.Inf(sign) }

// unsupported is a value that encoding/json refuses to encode.
type unsupported struct{}

func (unsupported) MarshalJSON() ([]byte, error) { return nil, errTest }

// stringerOnly has only a String method, which encoding/json ignores, so it
// canonicalises as its exported field object.
type stringerOnly struct{ Value string }

func (s stringerOnly) String() string { return "stringer:" + s.Value }

// rawCarrier embeds raw JSON, which must still be validated for duplicate keys
// before it contributes to a digest.
type rawCarrier struct {
	Payload json.RawMessage `json:"payload"`
}
