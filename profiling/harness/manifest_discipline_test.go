package harness

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const manifestShape = `{
 "schema":"ahp-acceptance-manifest/v1",
 "generated_utc":"2026-11-09T23:39:46Z",
 "source":{"revision":"684bae9cf0b99c69209c23cc67cb75828ab0cc86","tree":"f2c9e6f9e89d73eee844c949d74e7ef7925f92ed","tracked_and_untracked_dirty":0},
 "binary":{"measured_subject_binary_sha256":"9aae0c9296d9a7a7cf43ed62001bc35a086eca38d92a4ffae2056b944230f9a6","built_from_revision":"b5785c315b32bd715062b975a9a03514f4945432","product_diff_since_measured_revision":["internal/model/routing/config_copies_test.go"]},
 "image":{"live_image_id":"sha256:835eee9ab02f"},
 "quota_attestation":{"cgroup_read_from_inside":"cpu.max=400000 100000 memory.max=4294967296 pids.max=4096"}
}`

func writeManifest(t *testing.T) (string, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ACCEPTANCE-MANIFEST.json")
	if err := os.WriteFile(p, []byte(manifestShape), 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(manifestShape))
	return p, hex.EncodeToString(sum[:])
}

func TestLoadManifestRefParsesThePublishedShape(t *testing.T) {
	path, want := writeManifest(t)
	ref, err := LoadManifestRef(path)
	if err != nil {
		t.Fatalf("the manifest the director actually publishes must load: %v", err)
	}
	if ref.SHA256 != want {
		t.Fatalf("manifest digest %s != %s: a profile must cite the bytes it read", ref.SHA256, want)
	}
	if ref.SourceRevision != "684bae9cf0b99c69209c23cc67cb75828ab0cc86" || ref.SourceTree == "" {
		t.Fatalf("source binding not parsed: %+v", ref)
	}
	if ref.SubjectBinarySHA256 == "" || ref.LiveImageID == "" || ref.Quota == "" {
		t.Fatalf("binary/image/quota binding not parsed: %+v", ref)
	}
	if len(ref.ProductDiffPaths) != 1 {
		t.Fatalf("product diff not parsed: %+v", ref.ProductDiffPaths)
	}
}

func TestLoadManifestRefRefusesUnreadableOrUnbound(t *testing.T) {
	if _, err := LoadManifestRef(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Fatal("a profile may not cite a manifest it cannot read")
	}
	p := filepath.Join(t.TempDir(), "no-rev.json")
	if err := os.WriteFile(p, []byte(`{"schema":"ahp-acceptance-manifest/v1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifestRef(p); err == nil {
		t.Fatal("a manifest without source.revision cannot bind anything")
	}
}

func TestMeasurementRefusingToNameItsCandidateIsRejected(t *testing.T) {
	rep := Report{Kind: "overhead", Version: VersionInfo{ProductRevision: "684bae9c", BinarySHA256: strings.Repeat("a", 64)}}
	v, _ := rep.ManifestDiscipline()
	if len(v) != 1 || !strings.Contains(v[0], "not referenced") {
		t.Fatalf("a measurement phase with no manifest reference was accepted: %v", v)
	}
}

func TestManifestForAnotherRevisionCannotBeCited(t *testing.T) {
	path, _ := writeManifest(t)
	ref, err := LoadManifestRef(path)
	if err != nil {
		t.Fatal(err)
	}
	rep := Report{Kind: "overhead", Version: VersionInfo{
		AcceptanceManifest: ref,
		// a DIFFERENT product revision than the manifest attests
		ProductRevision: "deadbeef00000000000000000000000000000000",
		BinarySHA256:    strings.Repeat("b", 64),
	}}
	v, notes := rep.ManifestDiscipline()
	if len(v) == 0 {
		t.Fatal("numbers of one candidate were allowed to cite another candidate's attestation")
	}
	if !strings.Contains(v[0], "deadbeef") || !strings.Contains(v[0], "684bae9c") {
		t.Fatalf("the violation must name both revisions so the reader sees the mismatch: %v", v)
	}
	if len(notes) != 1 || ref.SubjectBinaryEqualsRun == nil || *ref.SubjectBinaryEqualsRun {
		t.Fatalf("a differing subject binary must be reported as a difference, not assumed equal: notes=%v equals=%v", notes, ref.SubjectBinaryEqualsRun)
	}
}

// TestLoadManifestRefReadsThePublishedManifest runs against the real attestation
// when one is named in the environment (the same variable the -manifest flag
// reads). It skips silently otherwise: a unit test may not depend on a report
// living in the headquarters tree.
func TestLoadManifestRefReadsThePublishedManifest(t *testing.T) {
	path := os.Getenv("AHP_ACCEPTANCE_MANIFEST")
	if path == "" {
		t.Skip("no acceptance manifest named in the environment")
	}
	ref, err := LoadManifestRef(path)
	if err != nil {
		t.Fatalf("the published manifest must load: %v", err)
	}
	if ref.SourceRevision == "" || ref.SourceTree == "" || ref.SubjectBinarySHA256 == "" {
		t.Fatalf("published manifest parsed but bindings are empty: %+v", ref)
	}
	if ref.Schema != "ahp-acceptance-manifest/v1" {
		t.Fatalf("unexpected manifest schema %q: the harness must notice a schema change, not guess fields", ref.Schema)
	}
}
