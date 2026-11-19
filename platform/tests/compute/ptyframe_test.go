package compute_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ahp-platform/ahp/internal/compute"
)

func TestPTYFrameStdinRoundTrip(t *testing.T) {
	in := []byte("ls -la\r")
	typ, payload, err := compute.DecodePTYFrame(bytes.NewReader(compute.EncodePTYStdin(in)))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if typ != 0x00 {
		t.Errorf("stdin frame type = 0x%02x, want 0x00", typ)
	}
	if !bytes.Equal(payload, in) {
		t.Errorf("payload = %q, want %q", payload, in)
	}
}

func TestPTYFrameResizeRoundTrip(t *testing.T) {
	const cols, rows = uint16(120), uint16(40)
	typ, payload, err := compute.DecodePTYFrame(bytes.NewReader(compute.EncodePTYResize(cols, rows)))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if typ != 0x01 {
		t.Errorf("resize frame type = 0x%02x, want 0x01", typ)
	}
	gotCols, gotRows, ok := compute.DecodePTYResize(payload)
	if !ok {
		t.Fatalf("DecodePTYResize ok=false for payload %x", payload)
	}
	if gotCols != cols || gotRows != rows {
		t.Errorf("resize = (%d,%d), want (%d,%d)", gotCols, gotRows, cols, rows)
	}
	// A non-4-byte payload is not a valid resize.
	if _, _, ok := compute.DecodePTYResize([]byte{1, 2, 3}); ok {
		t.Error("3-byte payload must not decode as a resize")
	}
}

func TestPTYFrameEOF(t *testing.T) {
	typ, payload, err := compute.DecodePTYFrame(bytes.NewReader(compute.EncodePTYEOF()))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if typ != 0x02 {
		t.Errorf("EOF frame type = 0x%02x, want 0x02", typ)
	}
	if len(payload) != 0 {
		t.Errorf("EOF frame must carry no payload, got %d bytes", len(payload))
	}
}

func TestPTYFrameTruncated(t *testing.T) {
	full := compute.EncodePTYStdin([]byte("hello"))
	// Cut the header short.
	if _, _, err := compute.DecodePTYFrame(bytes.NewReader(full[:3])); !errors.Is(err, compute.ErrPTYFrameTruncated) {
		t.Errorf("short header err = %v, want ErrPTYFrameTruncated", err)
	}
	// Header intact, payload cut short.
	if _, _, err := compute.DecodePTYFrame(bytes.NewReader(full[:8])); !errors.Is(err, compute.ErrPTYFrameTruncated) {
		t.Errorf("short payload err = %v, want ErrPTYFrameTruncated", err)
	}
}

func TestPTYFramePayloadTooLarge(t *testing.T) {
	// A frame header declaring a payload past the 1 MiB cap must be rejected
	// without consuming the (absurd) payload.
	var buf bytes.Buffer
	buf.WriteByte(0x00)
	// length = 2 MiB in big-endian.
	buf.Write([]byte{0x00, 0x20, 0x00, 0x00})
	if _, _, err := compute.DecodePTYFrame(&buf); !errors.Is(err, compute.ErrPTYPayloadTooLarge) {
		t.Errorf("err = %v, want ErrPTYPayloadTooLarge", err)
	}
}
