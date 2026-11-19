package artifacts

import (
	"fmt"
	"io"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Durable-length attestation on the mounted read path (R6 integrity).
//
// The segmented envelope format identifies itself by a manifest footer. If that footer is destroyed
// by silent object-store damage (or by anyone who can write the bucket), the object cipher's reader
// cannot tell "segmented object with a lost footer" from "one v1 envelope object": the head of such
// an object is a valid envelope of the FIRST segment only, so its documented fallback answers that
// one segment as the whole object - a shorter body delivered with no error at all.
//
// The seam cannot know the intended length; the durable artifact row can. Seal binds PLAINTEXT
// sha256/size by design (see seal.go), so ahp_artifacts.artifacts.size is an attestation about the
// bytes a read must deliver. This file makes that attestation binding on the mounted path: a read
// that would answer with fewer plaintext bytes than the row attests is a refusal, never a 200 with
// a short body. The OFF path is deliberately untouched, because "disabled is inert" is a separate
// accepted property of the mount and the native client path is exactly what it was before.

// AttestedSizeError reports that the stored bytes of one artifact are fewer than the durable record
// attests. It is a server-side integrity condition, so it is reported as UNAVAILABLE (503): the
// caller's request is well-formed, the stored body is not servable.
type AttestedSizeError struct {
	ID   string
	Want int64
	Got  int64
	// wrapped is the same refusal in the shared contract vocabulary, so the HTTP faces answer
	// 503/UNAVAILABLE (contracts.Code resolves it) instead of a bare 500.
	wrapped error
}

// attestedSizeError builds both renderings of the same fact once, at refusal time.
func attestedSizeError(id string, want, got int64) *AttestedSizeError {
	e := &AttestedSizeError{ID: id, Want: want, Got: got}
	e.wrapped = contracts.NewError(contracts.ErrUnavailable, e.Error())
	return e
}

func (e *AttestedSizeError) Error() string {
	return fmt.Sprintf("artifact %s stored bytes are short: %d of %d attested plaintext bytes", e.ID, e.Got, e.Want)
}

// Unwrap exposes the contract-coded form so contracts.Code(err) resolves UNAVAILABLE.
func (e *AttestedSizeError) Unwrap() error { return e.wrapped }

// attestedSizeReader delivers the plaintext stream unchanged until EOF, and turns an early EOF into
// the refusal above. Byte-for-byte behaviour is unchanged while the body is complete.
type attestedSizeReader struct {
	rc   io.ReadCloser
	id   string
	want int64
	got  int64
}

func (r *attestedSizeReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	r.got += int64(n)
	if err == io.EOF && r.got < r.want {
		return n, attestedSizeError(r.id, r.want, r.got)
	}
	return n, err
}

func (r *attestedSizeReader) Close() error { return r.rc.Close() }

// attestRange checks a whole-range answer against the attested size before any of it is served.
// want is the length the durable record implies for this range; a shorter answer is the same defect
// as a short stream, only easier to miss because the range API answers with a buffer.
func attestRange(id string, want int64, got int) error {
	if int64(got) < want {
		return attestedSizeError(id, want, int64(got))
	}
	return nil
}

// attestedRangeLen is the number of plaintext bytes a range request is owed, given the attested size.
// count == 0 means "to the end"; offset is already known to be inside the artifact.
func attestedRangeLen(size, offset, count int64) int64 {
	end := size
	if count > 0 && offset+count < end {
		end = offset + count
	}
	if end <= offset {
		return 0
	}
	return end - offset
}
