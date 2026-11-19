package state

import (
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"strings"
	"testing"
)

func TestRev36ArtifactMarkerWithoutExplicitRootIsRejectedBeforePublication(t *testing.T) {
	x, _, req := rev34Fixture(t)
	req.ArtifactReferences = nil
	receipt, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if contracts.Code(e) != contracts.ErrInvalid {
		t.Fatalf("UNROOTED_ARTIFACT_MARKER_PUBLISHED: revision=%d error=%v; marker requires explicit lifecycle root", receipt.Revision, e)
	}
}
func TestRev36ArtifactAwareCarrierRejectsLargeUnexternalizedField(t *testing.T) {
	x, _, req := rev34Fixture(t)
	req.ArtifactReferences = nil
	row := req.Document.Records["s"]
	raw, e := json.Marshal(map[string]string{"native_value": strings.Repeat("a", (1<<20)+1)})
	if e != nil {
		t.Fatal(e)
	}
	row.Attributes = raw
	req.Document.Records["s"] = row
	receipt, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if contracts.Code(e) != contracts.ErrInvalid {
		t.Fatalf("LARGE_INLINE_ARTIFACT_CARRIER_ACCEPTED: revision=%d error=%v; producer must externalize this field", receipt.Revision, e)
	}
}
