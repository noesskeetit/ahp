package state

import (
	"github.com/ahp-platform/ahp/internal/references"
	"testing"
)

func TestCommonRootsRemainVisibleWhenWriterFeatureIsDisabled(t *testing.T) {
	f, req, _, _ := rev36Fixture(t)
	ref := req.ArtifactReferences[0].Artifact
	result, e := CommonReferenceSourceIfPresent(f.source.pool, "ahp_state", false).InspectReferences(f.ctx, f.unit.Tenant, nil, []references.Artifact{{ID: ref.ID, Digest: ref.SHA256}})
	if e != nil || !result.Referenced[ref.ID] {
		t.Fatal("disabling native writes hid live roots from Artifact GC", e)
	}
}
