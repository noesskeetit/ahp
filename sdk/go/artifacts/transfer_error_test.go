package artifacts

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestTransferOutcomeReasonSurvivesSDKError(t *testing.T) {
	response := &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(`{"code":"UNAVAILABLE","reason":"transfer_outcome_unknown","message":"sensitive upstream detail"}`))}
	// The same answer is classified for what it is on a mutation: the transfer may
	// have landed. The recovery reason must survive that classification, because
	// reconciling by ID is the only correct next move.
	err := responseError(response, "TransferGrant", true)
	if !IsOutcomeUnknown(err) {
		t.Fatal("a transfer the owner could not confirm was reported as known")
	}
	if contracts.Code(err) != contracts.ErrUnavailable || contracts.ReasonOf(err) != "transfer_outcome_unknown" {
		t.Fatalf("SDK lost recovery reason: code=%s reason=%s", contracts.Code(err), contracts.ReasonOf(err))
	}
	if strings.Contains(err.Error(), "sensitive") {
		t.Fatal("SDK echoed raw upstream error")
	}
}
