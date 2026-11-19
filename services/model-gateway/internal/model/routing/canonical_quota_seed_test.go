package routing

// Drift guard for the canonical stand (PDF rev3.2 :1620-1630 provider quotas; :2495
// quotas per installation/project, tenant-scoped).
//
// Two committed artefacts describe the same policy: infra/model/providers.json
// (what the gateway reads to route) and infra/quotas/modelrun-tenant-limits.sql
// (what the operator provisions into ahp_model.provider_quotas, the table the gate
// actually charges). Anything that can drift between them will eventually: the
// usual failure is a copy that routes traffic and has no limit row, which the gate
// reports as UNLIMITED. This leg fails the build on that state instead of waiting
// for a load test to discover it.

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
)

// acceptanceTenants are the tenants the canonical stand actually runs work under.
// A tenant that only inherits the copy-default row is bounded, but an auditor has
// to derive that; the seed states them so "who is bounded here" is readable as
// rows.
var acceptanceTenants = []string{"djourney", "t-foreign"}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("workdir: %v", err)
	}
	for i := 0; i < 12; i++ {
		if _, err := os.Stat(filepath.Join(dir, "infra", "model", "providers.json")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("cannot locate the repository root from the test working directory; the seed guard reads committed infra files and must not be run outside the repo")
	return ""
}

func TestCanonicalProviderConfigBoundsEveryCopy(t *testing.T) {
	root := repoRoot(t)
	tbl, err := LoadFile(filepath.Join(root, "infra", "model", "providers.json"))
	if err != nil {
		t.Fatalf("the canonical provider config must load: %v", err)
	}
	declared := map[string]DeclaredQuota{}
	for _, q := range tbl.DeclaredQuotas() {
		declared[q.Provider] = q
	}
	for _, p := range tbl.Providers {
		q, ok := declared[p.Name]
		if !ok {
			t.Errorf(`provider %q declares no quota in infra/model/providers.json: routing can send a turn to it and the gate would charge no window (a copy with no limit row is admitted UNLIMITED)`, p.Name)
			continue
		}
		if q.RPM <= 0 || q.TPM <= 0 {
			t.Errorf("provider %q declares a non-positive bound: %+v", p.Name, q)
		}
	}
	if len(declared) != len(tbl.Providers) {
		t.Fatalf("declared quota for %d of %d providers: every copy the gateway can route to must state its own bound",
			len(declared), len(tbl.Providers))
	}
}

func TestCanonicalQuotaSeedPublishesEveryDeclaredCopy(t *testing.T) {
	root := repoRoot(t)
	tbl, err := LoadFile(filepath.Join(root, "infra", "model", "providers.json"))
	if err != nil {
		t.Fatalf("load provider config: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "infra", "quotas", "modelrun-tenant-limits.sql"))
	if err != nil {
		t.Fatalf("read the committed quota seed: %v", err)
	}
	sql := string(raw)
	// VALUES ('<provider>', '<tenant>', <rpm>, <tpm>, <window>, now())
	upsert := regexp.MustCompile(`(?s)VALUES \('([^']+)', '([^']*)',\s*(\d+),\s*(\d+),\s*(\d+), now\(\)\)\s*\n\s*ON CONFLICT \(provider, tenant\) DO UPDATE`)
	rows := map[string][3]int64{}
	for _, m := range upsert.FindAllStringSubmatch(sql, -1) {
		rpm, _ := strconv.ParseInt(m[3], 10, 64)
		tpm, _ := strconv.ParseInt(m[4], 10, 64)
		win, _ := strconv.ParseInt(m[5], 10, 64)
		rows[m[1]+"|"+m[2]] = [3]int64{rpm, tpm, win}
	}
	if len(rows) == 0 {
		t.Fatal("the committed seed contains no parsable (provider, tenant) upserts; the guard would pass vacuously")
	}
	for _, q := range tbl.DeclaredQuotas() {
		got, ok := rows[q.Provider+"|"]
		if !ok {
			t.Errorf("seed publishes no copy-default row for provider %q declared at rpm=%d tpm=%d; the file and the provisioned table have drifted",
				q.Provider, q.RPM, q.TPM)
			continue
		}
		if got[0] != q.RPM || got[1] != q.TPM {
			t.Errorf("provider %q: seed publishes rpm=%d tpm=%d but infra/model/providers.json declares rpm=%d tpm=%d -- the operator and the gateway would disagree",
				q.Provider, got[0], got[1], q.RPM, q.TPM)
		}
	}
	// Acceptance tenants stated explicitly, on every copy the stand can route to.
	for _, q := range tbl.DeclaredQuotas() {
		for _, tenant := range acceptanceTenants {
			if _, ok := rows[q.Provider+"|"+tenant]; !ok {
				t.Errorf("seed publishes no explicit row for provider %q tenant %q: bounded only by inheritance is not a published bound", q.Provider, tenant)
			}
		}
	}
}
