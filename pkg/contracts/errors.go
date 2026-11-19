// Package contracts: error vocabulary shared by every AHP service.

package contracts

import "errors"

// Stable machine-readable error codes. Services return these codes and callers
// branch on them; the strings are part of the public API.
const (
	ErrNotFound   = "NOT_FOUND"
	ErrConflict   = "CONFLICT"
	ErrStaleEpoch = "STALE_EPOCH"
	ErrForbidden  = "FORBIDDEN"
	ErrInvalid    = "INVALID_ARGUMENT"
	ErrKeyReuse   = "KEY_REUSE_MISMATCH"
	ErrLeaseLost  = "LEASE_LOST"
	// ErrUnavailable reports that the service is correctly configured for the
	// caller but a DEPENDENCY it needs is not answering or not provisioned
	// (database unreachable, missing schema/relation/extension). It is distinct
	// from CONFLICT, which means the caller's write collided with committed
	// state: a caller may retry CONFLICT unchanged, while UNAVAILABLE tells the
	// operator that something must be provisioned or repaired. Additive: no
	// existing code changed meaning.
	ErrUnavailable = "UNAVAILABLE"
)

// Error is the transport-visible error shape: a stable code plus a
// human-readable message that must never carry secrets.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

// NewError builds an *Error with the given code and message.
func NewError(code, message string) error {
	return &Error{Code: code, Message: message}
}

// Reasoned is an error that reports a stable machine-readable reason beside its
// code. The HTTP class and `code` stay what the class dictates - clients that
// only read `code` keep working - while `reason` names the domain event that
// happened, so two failures sharing a class stay distinguishable by a response
// field instead of by matching on message text.
type Reasoned interface {
	ReasonCode() string
}

// reasoned attaches a reason to an *Error without changing how that error
// marshals for anyone who does not look at the reason.
type reasoned struct {
	err    error
	reason string
}

func (e *reasoned) Error() string      { return e.err.Error() }
func (e *reasoned) Unwrap() error      { return e.err }
func (e *reasoned) ReasonCode() string { return e.reason }

// NewReasoned builds an error with a code and a stable reason. Code(err) keeps
// returning code; ReasonOf(err) returns reason.
func NewReasoned(code, reason, message string) error {
	return &reasoned{err: NewError(code, message), reason: reason}
}

// ReasonOf returns the stable reason carried by err, or "" when the error does
// not carry one. It unwraps wrapped errors.
func ReasonOf(err error) string {
	var target Reasoned
	if errors.As(err, &target) {
		return target.ReasonCode()
	}
	return ""
}

// Code returns the stable code carried by err, or "" when err does not carry
// one. It unwraps wrapped errors.
func Code(err error) string {
	var target *Error
	if errors.As(err, &target) {
		return target.Code
	}
	return ""
}
