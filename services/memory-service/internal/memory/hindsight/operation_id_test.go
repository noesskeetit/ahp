package hindsight

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"testing"
)

// nativeUUIDShape is what Hindsight 1.1.0 validates an operation id against; a
// reply that is not this shape is a live 422, which no recorded transport shows.
var nativeUUIDShape = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNativeOperationIDIsADeterministicUUID(t *testing.T) {
	const job = "01JZK4Q6R9T2W8FBN5XCYM7PA3"
	first := nativeOperationID(job)
	if second := nativeOperationID(job); first != second {
		t.Fatalf("mapping must be reproducible, the queue recomputes it: %s vs %s", first, second)
	}
	if !nativeUUIDShape.MatchString(first) {
		t.Fatalf("native server rejects this operation id: %q", first)
	}
	if first == nativeOperationID("01JZK4Q6R9T2W8FBN5XCYM7PA4") {
		t.Fatal("distinct AHP jobs collapsed onto one native operation")
	}
}

// TestStatusAndCancelUseTheMappedOperationPath pins the other half of the
// mapping: the worker only ever holds the AHP job id, so polling and cancel
// must recompute the same native id the retain call sent.
func TestStatusAndCancelUseTheMappedOperationPath(t *testing.T) {
	const job = "op-abc"
	want := "/v1/default/banks/bank/operations/" + nativeOperationID(job)
	var paths []string
	c, _ := New("http://engine", "")
	c.HTTP.Transport = transport(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		if r.Method == http.MethodGet {
			return reply(200, `{"status":"processing"}`), nil
		}
		return reply(202, `{}`), nil
	})
	if _, err := c.Status(context.Background(), "bank", job); err != nil {
		t.Fatal(err)
	}
	if err := c.Cancel(context.Background(), "bank", job); err != nil {
		t.Fatal(err)
	}
	// Cancel reads the state before deleting, so the mapped id must survive all
	// three calls: one explicit status poll, then cancel's own read and delete.
	if len(paths) != 3 {
		t.Fatalf("want a status poll then a cancel read and delete, got %v", paths)
	}
	if !strings.HasPrefix(paths[2], http.MethodDelete) {
		t.Fatalf("cancel did not delete the mapped operation: %v", paths)
	}
	for _, p := range paths {
		if !strings.HasSuffix(p, want) {
			t.Fatalf("native operation path not mapped: %s (want suffix %s)", p, want)
		}
	}
}
