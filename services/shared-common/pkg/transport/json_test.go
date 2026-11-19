package transport

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

type payload struct {
	Name string `json:"name"`
	N    int    `json:"n"`
}

func readBody(t *testing.T, body string, maxBytes int64) error {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
	w := httptest.NewRecorder()
	var out payload
	return ReadJSON(w, r, &out, maxBytes)
}

func TestReadJSONAccepts(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"name":"a","n":2}`))
	var out payload
	if err := ReadJSON(httptest.NewRecorder(), r, &out, 0); err != nil {
		t.Fatal(err)
	}
	if out.Name != "a" || out.N != 2 {
		t.Fatalf("decoded = %+v", out)
	}
}

func TestReadJSONRejects(t *testing.T) {
	cases := map[string]string{
		"unknown field":  `{"name":"a","surprise":1}`,
		"trailing data":  `{"name":"a"}garbage`,
		"two values":     `{"name":"a"} {"name":"b"}`,
		"trailing value": `{"name":"a"} {}`,
		"empty":          ``,
		"not json":       `name=a`,
		"truncated":      `{"name":`,
	}
	for name, body := range cases {
		if err := readBody(t, body, 0); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestReadJSONRejectsDuplicateKeys(t *testing.T) {
	if err := readBody(t, `{"name":"a","name":"b"}`, 0); err == nil {
		t.Fatal("duplicate key accepted")
	} else if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("code = %s", contracts.Code(err))
	}
}

func TestReadJSONSizeLimit(t *testing.T) {
	big := `{"name":"` + strings.Repeat("x", 4096) + `"}`
	if err := readBody(t, big, 128); err == nil {
		t.Fatal("oversized body accepted")
	} else if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("code = %s", contracts.Code(err))
	}
	if err := readBody(t, `{"name":"ok"}`, 128); err != nil {
		t.Fatalf("small body rejected: %v", err)
	}
}

func TestWriteJSONHeaders(t *testing.T) {
	w := httptest.NewRecorder()
	WriteJSON(w, http.StatusCreated, payload{Name: "a"})
	if got := w.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("nosniff = %q", got)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content-type = %q", ct)
	}
	if w.Code != http.StatusCreated {
		t.Errorf("status = %d", w.Code)
	}
}

func TestWriteErrorMapping(t *testing.T) {
	cases := []struct {
		code   string
		status int
	}{
		{contracts.ErrInvalid, http.StatusBadRequest},
		{contracts.ErrKeyReuse, http.StatusBadRequest},
		{contracts.ErrForbidden, http.StatusForbidden},
		{contracts.ErrNotFound, http.StatusNotFound},
		{contracts.ErrConflict, http.StatusConflict},
		{contracts.ErrStaleEpoch, http.StatusConflict},
		{contracts.ErrLeaseLost, http.StatusConflict},
		// A database abort is availability: State reports it this way so a caller
		// can tell "replay" from the 409s above, which are decisions.
		{contracts.ErrUnavailable, http.StatusServiceUnavailable},
		{"", http.StatusInternalServerError},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		err := contracts.NewError(c.code, "detail")
		if c.code == "" {
			err = errPlain("internal detail with secret")
		}
		WriteError(w, httptest.NewRequest(http.MethodGet, "/x", nil), err)
		if w.Code != c.status {
			t.Errorf("code %q: status = %d, want %d", c.code, w.Code, c.status)
		}
		var e contracts.Error
		if jErr := json.Unmarshal(w.Body.Bytes(), &e); jErr != nil {
			t.Fatalf("code %q: body %q not error JSON: %v", c.code, w.Body.String(), jErr)
		}
		if c.code == "" {
			if strings.Contains(w.Body.String(), "secret") ||
				strings.Contains(w.Body.String(), "internal detail") {
				t.Fatalf("raw error leaked: %q", w.Body.String())
			}
			if e.Code != "INTERNAL" {
				t.Errorf("fallback code = %q", e.Code)
			}
		}
	}
}

type errPlain string

func (e errPlain) Error() string { return string(e) }

// TestWriteErrorKeepsDomainCodeAsReason pins the ruling of 2026-10-31: a
// rejected idempotency key keeps the 400 class and code INVALID_ARGUMENT -
// Space and SDK already map 400 - while the domain code travels in `reason`,
// so "reused operation_key" and "malformed body" are distinguishable by a
// response field rather than by matching on the message text.
func TestWriteErrorKeepsDomainCodeAsReason(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantReason string
	}{
		{"reused key", contracts.NewError(contracts.ErrKeyReuse, "operation key reused with a different digest"),
			contracts.ErrKeyReuse},
		{"malformed body", invalidJSON(), ""},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		WriteError(w, httptest.NewRequest(http.MethodPost, "/v1/state/PutKV", nil), c.err)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want %d", c.name, w.Code, http.StatusBadRequest)
		}
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s: body %q is not JSON: %v", c.name, w.Body.String(), err)
		}
		if body["code"] != contracts.ErrInvalid {
			t.Errorf("%s: code = %v, want %s", c.name, body["code"], contracts.ErrInvalid)
		}
		reason, present := body["reason"].(string)
		if c.wantReason == "" {
			if present {
				t.Errorf("%s: reason must be absent, got %q in %s", c.name, reason, w.Body.String())
			}
			continue
		}
		if !present || reason != c.wantReason {
			t.Errorf("%s: reason = %q, want %q in %s", c.name, reason, c.wantReason, w.Body.String())
		}
	}
}

// The verdict is the contract; the sentence has to describe the body.
//
// Reported on the live stand: CancelTurn with {"turn_id":...,"reason":...}
// answered 400 "transport: request body is not valid JSON". The body was valid
// JSON - the request is {turn_id} and nothing more
// (docs/space-components-api-contract-2026-10-30.md:43, and web/static/app.js:129
// sends exactly that) - so the caller was sent off to fix syntax that was never
// broken. Rejecting the unknown field is correct and stays; only the message
// changes.
func TestReadJSONNamesWhatItRejected(t *testing.T) {
	cases := []struct {
		body string
		want string
	}{
		{`{"name":"a","surprise":1}`, `request body has an unknown field "surprise"`},
		{`{"name":"a","reason":"cleanup"}`, `request body has an unknown field "reason"`},
		{`{"name":"a","n":"many"}`, "not of the expected type"},
		{`name=a`, "not valid JSON"},
		{`{"name":`, "not valid JSON"},
	}
	for _, c := range cases {
		err := readBody(t, c.body, 0)
		if err == nil {
			t.Fatalf("%s: accepted, want rejection", c.body)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: message = %q, want it to say %q", c.body, err.Error(), c.want)
		}
		if code := contracts.Code(err); code != contracts.ErrInvalid {
			t.Errorf("%s: code = %s, want %s (the verdict must not change)", c.body, code, contracts.ErrInvalid)
		}
	}
}
