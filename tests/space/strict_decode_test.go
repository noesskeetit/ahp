package space_test

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
)

// postSpaceRawAs posts an ARBITRARY raw JSON body (not a marshalled struct) so a
// test can smuggle fields that are NOT in any Space request DTO. The subject is
// still supplied via the verified-principal header, so a 400 here can ONLY come
// from the strict body decoder, never from authentication.
func postSpaceRawAs(t *testing.T, base, op, rawBody, subject string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/v1/space/"+op, bytes.NewReader([]byte(rawBody)))
	if err != nil {
		t.Fatalf("newreq %s: %v", op, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer tok-"+subject)
	req.Header.Set("X-Test-Subject", subject)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", op, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

// TestSpaceStrictDecodeRejectsScopeSmuggling proves the Human->Space boundary:
// a browser cannot choose tenant/subject/deployment via the request BODY. Scope is
// resolved ONLY from the verified principal + server-side task exchange (PDF rev3.2
// sec 3.5). Any attempt to smuggle a scope field is a hard 400 (State strict-decode
// doctrine), never a silent drop or a privilege change.
func TestSpaceStrictDecodeRejectsScopeSmuggling(t *testing.T) {
	spy := &spyExchanger{}
	hs, _ := newExchangeSpace(t, spy, true) // exchange wired => a CLEAN body would succeed (200)

	// A clean body must succeed, so a 400 below is provably the strict decoder.
	if code, body := postSpaceAs(t, hs.URL, "AcceptTurn", map[string]any{
		"key": "sd-clean", "context_id": "ctx-sd-clean", "input": map[string]any{"role": "user", "content": "hi"},
	}, "erin"); code != 200 {
		t.Fatalf("clean AcceptTurn = %d, want 200 (body=%s)", code, body)
	}

	for _, field := range []string{"tenant", "subject", "deployment", "scope"} {
		raw := `{"key":"sd-` + field + `","context_id":"ctx-sd-` + field +
			`","input":{"role":"user","content":"hi"},"` + field + `":"attacker-` + field + `"}`
		code, body := postSpaceRawAs(t, hs.URL, "AcceptTurn", raw, "erin")
		if code != http.StatusBadRequest {
			t.Fatalf("smuggled body field %q = %d, want 400 strict-decode reject (body=%s)", field, code, body)
		}
		// transport.ReadJSON rejects unknown fields as INVALID_ARGUMENT ("not valid JSON").
		if !strings.Contains(string(body), "INVALID_ARGUMENT") {
			t.Fatalf("smuggled %q: want strict-decode INVALID_ARGUMENT reject, got %s", field, body)
		}
	}
}
