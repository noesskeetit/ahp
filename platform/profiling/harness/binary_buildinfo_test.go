package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

const goVersionMStamped = `/work/bin/ahp:
	go version go1.26.2 darwin/arm64
	build	-buildmode=exe
	build	-compiler=gc
	build	-tags=ahp_exec ahp_data
	build	-trimpath=true
	build	CGO_ENABLED=0
	build	GOARCH=arm64
	build	GOOS=darwin
	build	vcs=git
	build	vcs.revision=f14585d2df3cd354f443b887c9c3e26d8c9e9371
	build	vcs.modified=false
	mod	github.com/ahp-platform/ahp	(devel)
`

const goVersionMNoStamp = `/work/bin/ahp:
	go version go1.26.2 linux/arm64
	build	-buildmode=exe
	build	-compiler=gc
	build	-tags=ahp_exec ahp_data
	build	vcs=
`

func TestBinaryBuildStampReadsTheSubjectsOwnRevision(t *testing.T) {
	info := &BuildInfo{Source: "fixture"}
	parseGoVersionM(goVersionMStamped, info)
	if info.Revision == nil || *info.Revision != "f14585d2df3cd354f443b887c9c3e26d8c9e9371" {
		t.Fatalf("the binary's own vcs.revision must be read, got %v", info.Revision)
	}
	if info.Modified == nil || *info.Modified {
		t.Fatalf("vcs.modified=false must be read as false, not defaulted: %v", info.Modified)
	}
	if info.BuildTags == nil || *info.BuildTags != "ahp_exec ahp_data" {
		t.Fatalf("build tags decide what the binary can even do; they must be recorded: %v", info.BuildTags)
	}
	if info.GOOS == nil || *info.GOOS != "darwin" || info.GOARCH == nil || *info.GOARCH != "arm64" {
		t.Fatalf("GOOS/GOARCH must be read (a linux binary cannot be measured on darwin): %v/%v", info.GOOS, info.GOARCH)
	}
	if info.Trimpath == nil || !*info.Trimpath {
		t.Fatalf("-trimpath changes what paths mean in a profile; it must be recorded: %v", info.Trimpath)
	}
	if info.Module == nil {
		t.Fatal("module identity must be recorded")
	}
}

// The shape root acceptance keeps asking for: a stamp that is absent is written
// as a null plus a reason, never as an empty string and never as a guess.
func TestMissingVcsStampIsStatedAsNullWithAReason(t *testing.T) {
	info := &BuildInfo{Source: "fixture"}
	parseGoVersionM(goVersionMNoStamp, info)
	if info.Revision != nil {
		t.Fatalf("an unstamped build must not report a revision: %v", *info.Revision)
	}
	if info.GoVersion == nil {
		t.Fatal("the toolchain line is still readable even without a stamp")
	}
	// ReadBinaryBuildInfo fills the reason; the same helper is applied here.
	ensureRevisionReason(info)
	if info.Revision == nil && info.UnavailableReason == "" {
		t.Fatal("a null revision without a reason is the placeholder defect again")
	}
}

// End-to-end against a real Go binary (the toolchain itself): the parser must
// work on bytes, not only on a fixture I wrote to match it.
func TestReadBinaryBuildInfoOnARealBinary(t *testing.T) {
	goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
	if _, err := os.Stat(goBin); err != nil {
		t.Skipf("no go toolchain binary at %s", goBin)
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go not on PATH")
	}
	info := ReadBinaryBuildInfo(goBin)
	if info.GoVersion == nil {
		t.Fatalf("the toolchain line must parse from real output: %+v (%s)", info, info.UnavailableReason)
	}
	if info.GOARCH == nil {
		t.Fatalf("GOARCH must parse from real output: %+v", info)
	}
	if info.Revision == nil && info.UnavailableReason == "" {
		t.Fatal("null revision must carry a reason")
	}
}
