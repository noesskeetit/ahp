package artifactsapi

// C2 (reviewer condition): the property "a substituted body is noticed" is
// load-bearing, and on the server it was only an ASSERTION: download.go sets
// ETag = ref.SHA256 and never re-reads the bytes (no sha256 call on the serve
// path), while the SDK client verifies them (sdk/go/artifacts/client.go). This
// leg puts the check at the face: the body that actually crossed the wire is
// hashed here and compared against the durable digest read back through Get -
// not against the ETag, which is the same claim.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestDownloadedBodyHashesToTheDurableDigest(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t, alice)
	body := "digest-at-the-face\n"
	for len(body) < 5000 {
		body += "the sealed promise must be what the reader receives\n"
	}
	id := e.uploadSeal(t, tok, "k/wire-digest", body, labels.Set{})

	code, meta := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": id}))
	if code != 200 {
		t.Fatalf("Get status %d, want 200 (body %v)", code, meta)
	}
	durable, _ := meta["sha256"].(string)
	if len(durable) != 64 {
		t.Fatalf("durable digest missing from the metadata answer: %v", meta)
	}
	if durable != shaHex(body) {
		t.Fatalf("durable digest does not match the bytes that were sealed: %q", durable)
	}

	status, got, hdr := e.downloadRaw(t, tok, mustJSON(t, map[string]any{"id": id}), "")
	if status != 200 {
		t.Fatalf("Download status %d, want 200", status)
	}
	if got != body {
		t.Fatalf("downloaded bytes differ from the sealed bytes (got %d, want %d)", len(got), len(body))
	}
	// The face's own claim and the observed bytes must agree.
	if etag := strings.Trim(hdr.Get("ETag"), `"`); etag != durable {
		t.Fatalf("ETag %q is not the durable digest %q", etag, durable)
	}
	if sum := shaHex(got); sum != durable {
		t.Fatalf("the body the server streamed hashes to %q, not the durable digest %q", sum, durable)
	}

	// And a range: same digest promise, exactly the requested slice.
	status, slice, hdr := e.downloadRaw(t, tok, mustJSON(t, map[string]any{"id": id}), "bytes=64-1023")
	if status != http.StatusPartialContent {
		t.Fatalf("range status %d, want 206", status)
	}
	if slice != body[64:1024] {
		t.Fatal("range bytes differ from the sealed slice")
	}
	if etag := strings.Trim(hdr.Get("ETag"), `"`); etag != durable {
		t.Fatalf("range ETag %q is not the durable digest %q", etag, durable)
	}
	if sum := shaHex(slice); sum != shaHex(body[64:1024]) {
		t.Fatalf("range body hashes to %q, want %q", sum, shaHex(body[64:1024]))
	}
}
