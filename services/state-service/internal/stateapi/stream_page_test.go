package stateapi

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/transport"
)

func TestReadStreamAcceptsExplicitResponseBudget(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/state/ReadStream", strings.NewReader(`{"thread_id":"thread","after":0,"limit":256,"max_response_bytes":8388608}`))
	w := httptest.NewRecorder()
	var in readStreamRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		t.Fatalf("bounded ReadStream contract rejected: %v", err)
	}
	if in.ThreadID != "thread" || in.Limit != 256 {
		t.Fatal("cursor/count changed")
	}
}
