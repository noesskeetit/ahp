package state

import (
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestCheckpointReferencesRequireOwnerGuard(t *testing.T) {
	e := cpFixture(t)
	c := cp("unguarded", Part{Channel: "ch", Version: "v1", Encoding: CheckpointArtifactEncoding, Data: []byte(`{"id":"missing","version":"missing"}`)})
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, c, "unguarded"); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("unguarded artifact reference must fail closed: %v", err)
	}
	c.Parts[0].Encoding = ""
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, c, "opaque"); err != nil {
		t.Fatalf("opaque legacy bytes require no artifact owner: %v", err)
	}
}
