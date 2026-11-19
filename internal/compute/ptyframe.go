package compute

import (
	"encoding/binary"
	"errors"
	"io"
)

// PTY duplex framing. A PTY session multiplexes guest terminal bytes and a few
// control messages (window resize, EOF) over a single bidirectional stream: an
// HTTP request body for the client->guest direction and the response body for
// the guest->client direction. Each client->guest frame is:
//
//	[1 byte type][4 byte big-endian payload length][payload]
//
// The guest->client direction is raw terminal bytes (no framing) because it is
// a single untyped stream.
const (
	ptyFrameStdin  byte = 0x00
	ptyFrameResize byte = 0x01
	ptyFrameEOF    byte = 0x02

	ptyFrameHeaderSize   = 5
	ptyFrameMaxPayload   = 1 << 20
	ptyErrFrameTooLarge  = "pty frame exceeds the maximum payload"
	ptyErrFrameTruncated = "pty frame is truncated"
)

var (
	// ErrPTYPayloadTooLarge rejects a frame whose declared length is absurd.
	ErrPTYPayloadTooLarge = errors.New(ptyErrFrameTooLarge)
	// ErrPTYFrameTruncated is returned when the stream ends mid-frame.
	ErrPTYFrameTruncated = errors.New(ptyErrFrameTruncated)
)

// EncodePTYStdin frames terminal input bytes.
func EncodePTYStdin(p []byte) []byte { return framePTY(ptyFrameStdin, p) }

// EncodePTYResize frames a window-size change as an 4-byte rows|cols payload.
func EncodePTYResize(cols, rows uint16) []byte {
	var b [4]byte
	binary.BigEndian.PutUint16(b[0:2], rows)
	binary.BigEndian.PutUint16(b[2:4], cols)
	return framePTY(ptyFrameResize, b[:])
}

// EncodePTYEOF frames end-of-input.
func EncodePTYEOF() []byte { return framePTY(ptyFrameEOF, nil) }

func framePTY(t byte, payload []byte) []byte {
	out := make([]byte, ptyFrameHeaderSize+len(payload))
	out[0] = t
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload)))
	copy(out[5:], payload)
	return out
}

// DecodePTYFrame reads exactly one frame from r. A payload-too-large error is
// returned without consuming the (absurd) payload so the caller can close the
// connection.
func DecodePTYFrame(r io.Reader) (t byte, payload []byte, err error) {
	var hdr [ptyFrameHeaderSize]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, ErrPTYFrameTruncated
		}
		return 0, nil, err
	}
	t = hdr[0]
	n := binary.BigEndian.Uint32(hdr[1:5])
	if n > ptyFrameMaxPayload {
		return t, nil, ErrPTYPayloadTooLarge
	}
	if n == 0 {
		return t, nil, nil
	}
	payload = make([]byte, n)
	if _, err = io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return t, nil, ErrPTYFrameTruncated
		}
		return t, nil, err
	}
	return t, payload, nil
}

// DecodePTYResize unpacks a resize frame payload back into cols and rows.
func DecodePTYResize(payload []byte) (cols, rows uint16, ok bool) {
	if len(payload) != 4 {
		return 0, 0, false
	}
	rows = binary.BigEndian.Uint16(payload[0:2])
	cols = binary.BigEndian.Uint16(payload[2:4])
	return cols, rows, true
}
