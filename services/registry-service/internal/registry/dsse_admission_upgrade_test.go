package registry_test

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestAdmissionLegacyDSSERecordRequiresReattestation(t *testing.T) {
	e := newSupplyEnv(t)
	im := e.publishImage(t, "dsse-upgrade-"+randSuffix(t))
	e.admit(t, im)
	// Only this new fixture's own receipt is made to resemble an admission
	// cached by the previous verifier. Migrate must never bless it silently.
	if _, err := e.pool.Exec(context.Background(), `UPDATE ahp_registry.admissions
        SET attestation=attestation-'verification_contract' WHERE oci_digest=$1 AND agent_id=$2`,
		"sha256:"+im.digestHex, im.agent); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := e.store.Install(context.Background(), e.tenant, im.agent, im.version.ID,
		registry.EnvironmentDev, "legacy-"+randSuffix(t), im.version.Manifest.Capabilities)
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("legacy verification receipt authorized installation: %v", err)
	}
	e.admit(t, im) // a current signature and SBOM must be verified again
	e.install(t, im, registry.EnvironmentDev)
}
