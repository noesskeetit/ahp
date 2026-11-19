package artifacts

import (
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// UnknownOutcomeError marks a MUTATING owner call that was handed to the network
// and whose answer was lost: the transport died, the body could not be read or
// decoded, the owner answered 5xx, or the owner itself reported
// transfer_outcome_unknown. None of those prove the recording is absent -- the
// upload may be registered, the artifact sealed, the copy landed.
//
// It exists because the alternative is a lie with a budget attached. When the same
// interruption surfaces as plain INVALID_ARGUMENT, a caller concludes "nothing
// happened" and re-sends under a fresh key, minting a second artifact; or it
// concludes "failed" and refunds a budget line the owner may have spent. The
// Python client has carried this distinction from the start (APIError
// .outcome_unknown, sdk/python/ahp_clients/_transport.py); the Go client carries it
// now, so the two SDKs cannot disagree about what an interrupted mutation means.
//
// The marker WRAPS, it does not replace: Unwrap exposes the underlying error, so a
// code and recovery reason the owner actually sent (for instance
// UNAVAILABLE/transfer_outcome_unknown, which callers reconcile on) survive
// classification, and an interruption with no answer at all carries the code
// UNKNOWN, exactly as the Python client reports it.
//
// The rule for a caller holding one: reconcile by identity, never re-send blindly.
// BeginUpload/Seal/CopyFromTransferGrant keep their operation key and artifact ID,
// so recovery is to read the ID back with Get or re-present the SAME key, which the
// owner deduplicates; Process checks Readiness of the same ID.
type UnknownOutcomeError struct {
	// Op is the owner operation whose outcome cannot be established.
	Op string
	// Cause is what the client observed: the owner's own error when it answered,
	// or the transport/decoding failure when it did not.
	Cause error
}

func (e *UnknownOutcomeError) Error() string {
	// Deliberately no echo of the cause's text: a transport error string carries
	// the request URL, and owner error text is not for the log line. Codes and
	// reasons stay available through contracts.Code / contracts.ReasonOf.
	return "artifacts sdk: " + e.Op + " outcome unknown; reconcile the same ID/key instead of re-sending"
}

// Unwrap keeps the carried error inspectable, which is what lets contracts.Code
// and contracts.ReasonOf see through the marker.
func (e *UnknownOutcomeError) Unwrap() error { return e.Cause }

// IsOutcomeUnknown reports whether err is (or wraps) an interrupted mutation whose
// durable effect cannot be ruled out.
func IsOutcomeUnknown(err error) bool {
	var target *UnknownOutcomeError
	return errors.As(err, &target)
}

// unanswered is the classification used when there was no answer to preserve: the
// caller gets a code to branch on (UNKNOWN, as Python reports it) plus the marker,
// and the raw failure remains reachable through Unwrap.
func unanswered(op string) error {
	return &UnknownOutcomeError{Op: op, Cause: contracts.NewReasoned("UNKNOWN", "outcome_unknown", "artifacts sdk: the owner gave no answer that proves the outcome")}
}

// unansweredBlob classifies a presigned PUT whose answer was lost. The blob
// store is not the owner, but the epistemic rule is the same: no answer that
// proves the write means the outcome is unknown, never INVALID_ARGUMENT - the
// bytes may be durably stored and a blind re-key would mint a second artifact.
func unansweredBlob(op, detail string) error {
	return &UnknownOutcomeError{Op: op, Cause: contracts.NewReasoned("UNKNOWN", "outcome_unknown", "artifacts sdk: "+detail)}
}

// uncertainAnswer marks an error the owner DID send, when that answer leaves the
// effect possible. The original code and reason are kept untouched.
func uncertainAnswer(op string, answered error) error {
	return &UnknownOutcomeError{Op: op, Cause: answered}
}
