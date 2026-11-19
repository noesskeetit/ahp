package labelwire

// Contract legs for the label source. Structural on purpose: a leg that referenced
// a field that must NOT exist would break the build of every importer and read as
// breakage rather than as a named rule.

import (
	"reflect"
	"testing"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// The server source must exist and answer the one question routing asks.
func TestServerSideLabelSourceIsAddressable(t *testing.T) {
	ctor := reflect.TypeOf(model.NewDeploymentLabelStore)
	if ctor.NumIn() != 1 || ctor.NumOut() != 1 {
		t.Fatalf("model.NewDeploymentLabelStore must take one pool and return the store routing reads, got %s", ctor)
	}
	store := ctor.Out(0)
	// Arity counts include the receiver: a method type read off a type carries it.
	for _, want := range []struct {
		method string
		in     int
	}{
		{"LabelsFor", 4}, {"Set", 6}, {"Migrate", 2},
	} {
		m, ok := store.MethodByName(want.method)
		if !ok {
			t.Errorf(`%s has no %s: routing must take labels from the operator's record of the deployment, addressed by the attested tenant+deployment (PDF :1620-1630)`, store, want.method)
			continue
		}
		if m.Type.NumIn() != want.in {
			t.Errorf("%s.%s takes %d args, want %d -- the address is (tenant, deployment) plus what the method carries, and a wider key would let a caller choose which record applies to it",
				store, want.method, m.Type.NumIn(), want.in)
		}
	}
	// The read must be able to say "no record" without failing: an error would
	// turn "the operator classified nobody" into an outage.
	lt, ok := store.MethodByName("LabelsFor")
	if ok {
		if lt.Type.NumOut() != 2 || lt.Type.Out(0).Kind() != reflect.Struct || lt.Type.Out(1).Name() != "error" {
			t.Errorf("LabelsFor must return (labels.Set, error) so an absent record is an empty set rather than a failure, got %s", lt.Type)
		}
	}
}

// mustNotCarryLabels is the anti-regression half of the ruling: nothing a caller
// fills in may carry routing labels, so a struct that starts carrying one is a hole,
// not a feature.
func mustNotCarryLabels(t *testing.T, v any, typeName, why string) {
	t.Helper()
	if f, ok := reflect.TypeOf(v).FieldByName("Labels"); ok {
		t.Errorf(`%s carries a %q field of type %s: routing labels are the server's record of the deployment (model.DeploymentLabelStore), never a caller assertion -- %s`,
			typeName, f.Name, f.Type, why)
	}
}

func TestNoRequestPathStructCarriesAssertedLabels(t *testing.T) {
	mustNotCarryLabels(t, core.ModelRequest{}, "core.ModelRequest",
		"the model request is built from caller data and would let the requester pick the upstream it is routed to")
	mustNotCarryLabels(t, gateway.ExecInput{}, "gateway.ExecInput",
		"the hand-off to the executor arrives from an external caller")
	mustNotCarryLabels(t, transport.Principal{}, "transport.Principal",
		"a principal is authenticated identity, and identity labels the subject rather than what its workload sends")
}
