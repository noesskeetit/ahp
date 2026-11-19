package toolsapi

import (
	"encoding/json"
	"strings"
	"testing"
)

// The face must enforce ONE declared limit: the constant it advertises is the
// byte ceiling actually applied to every task route, and going over it is an
// explicit refusal rather than a truncation or a hang.
func TestRequestBodyLimitIsEnforcedOnEveryTaskRoute(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t)

	oversized := `{"key":"k-limit","tool":"echo","arguments":{"pad":"` + strings.Repeat("p", maxBody) + `"}}`
	bodies := map[string]string{
		"Call":      oversized,
		"Get":       `{"key":"` + strings.Repeat("k", maxBody) + `"}`,
		"Fence":     `{"key":"` + strings.Repeat("k", maxBody) + `"}`,
		"GetResult": `{"key":"` + strings.Repeat("k", maxBody) + `"}`,
	}
	for route, body := range bodies {
		status, out := e.call(t, route, tok, body)
		if status != 400 {
			t.Fatalf("%s oversized body: status = %d, want 400 (body %v)", route, status, out)
		}
		msg, _ := out["message"].(string)
		if !strings.Contains(msg, "too large") {
			t.Fatalf("%s oversized body must name the refusal: %v", route, out)
		}
	}

	// The limit is not tighter than declared: a large but legal call still works.
	pad := strings.Repeat("q", 700<<10)
	args := json.RawMessage(`{"pad":"` + pad + `"}`)
	status, out := e.call(t, "Call", tok, `{"key":"k-limit-ok","tool":"echo","arguments":`+string(args)+`}`)
	mustStatus(t, status, 200, out)
	if out["status"] != "SUCCEEDED" {
		t.Fatalf("legal large call = %v, want SUCCEEDED", out)
	}
}
