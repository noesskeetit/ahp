package transport

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestWriteErrorUnavailableIsServiceUnavailable pins the HTTP class of a
// dependency failure and that the stable reason survives the flattening, so a
// caller can tell "provision the dependency" from "your write collided".
func TestWriteErrorUnavailableIsServiceUnavailable(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, httptest.NewRequest(http.MethodPost, "/v1/memory/Propose", nil),
		contracts.NewReasoned(contracts.ErrUnavailable, "memory:storage:pgstate_42704_dependency_missing",
			"memory: migrate memory schema: storage failure"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body struct {
		Code   string `json:"code"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	if body.Code != contracts.ErrUnavailable {
		t.Fatalf("code = %s, want %s", body.Code, contracts.ErrUnavailable)
	}
	if body.Reason != "memory:storage:pgstate_42704_dependency_missing" {
		t.Fatalf("reason = %q", body.Reason)
	}
}
