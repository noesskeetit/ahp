package runtimeprofile

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func privateTestRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestProfileEnvironmentIsDisabledByDefaultAndDurationBounded(t *testing.T) {
	t.Setenv("AHP_RUNTIME_PROFILE", "")
	t.Setenv("AHP_RUNTIME_PROFILE_SECONDS", "invalid-private-value")
	stop, err := StartFromEnv("ahp")
	if err != nil {
		t.Fatal("disabled profile parsed diagnostic-only configuration")
	}
	stop()
	t.Setenv("AHP_RUNTIME_PROFILE", "1")
	for _, seconds := range []string{"0", "91", "invalid-private-value"} {
		t.Setenv("AHP_RUNTIME_PROFILE_SECONDS", seconds)
		stop, err = StartFromEnv("ahp")
		stop()
		if err != errProfile {
			t.Fatal("unbounded or invalid profile duration accepted")
		}
	}
}

func TestProfileWriterBoundsOutputAndMarksPartial(t *testing.T) {
	var output bytes.Buffer
	w := boundedWriter{w: &output, remaining: 2}
	n, err := w.Write([]byte("abc"))
	if n != 2 || err != io.ErrShortWrite || !w.failed || output.String() != "ab" {
		t.Fatal("profile sink exceeded byte bound or hid truncation")
	}
}

func TestDisabledProfileIsInert(t *testing.T) {
	root := filepath.Join(t.TempDir(), "must-not-exist")
	stop, err := start("ahp", "", root, time.Millisecond)
	if err != nil {
		t.Fatal("disabled profiling returned an error")
	}
	stop()
	stop()
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("disabled profiling touched the filesystem")
	}
}

func TestProfileRejectsNamesSymlinksAndExistingFiles(t *testing.T) {
	root := privateTestRoot(t)
	if stop, err := start("../outside", "1", root, time.Millisecond); err == nil {
		stop()
		t.Fatal("process name escaped the file whitelist")
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "ahp")); err != nil {
		t.Fatal(err)
	}
	if stop, err := start("ahp", "1", root, time.Millisecond); err == nil {
		stop()
		t.Fatal("symlink output directory accepted")
	}
	entries, _ := os.ReadDir(outside)
	if len(entries) != 0 {
		t.Fatal("profiling wrote through a symlink")
	}
	root = privateTestRoot(t)
	if err := os.Mkdir(filepath.Join(root, "as-runner"), 0700); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(root, "as-runner", "cpu.pprof")
	if err := os.WriteFile(p, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if stop, err := start("as-runner", "1", root, time.Millisecond); err == nil {
		stop()
		t.Fatal("existing output file overwritten")
	}
	raw, _ := os.ReadFile(p)
	if string(raw) != "existing" {
		t.Fatal("existing file changed")
	}
}

func checkProfiles(t *testing.T, root, process, reason string) {
	t.Helper()
	dir := filepath.Join(root, process)
	raw, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta metadata
	if json.Unmarshal(raw, &meta) != nil || meta.Status != "COMPLETE" || meta.Process != process || meta.StopReason != reason || meta.PID != os.Getpid() {
		t.Fatal("missing complete bounded profile metadata")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 3 {
		t.Fatal("unexpected profile files")
	}
	for _, name := range []string{"cpu.pprof", "heap.pprof"} {
		info := meta.Files[name]
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		z, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			t.Fatal("profile not flushed to valid gzip")
		}
		data, readErr := io.ReadAll(z)
		z.Close()
		f.Close()
		if readErr != nil || len(data) == 0 || !info.Complete || info.Bytes <= 0 || len(info.SHA256) != 64 {
			t.Fatal("incomplete profile or metadata digest")
		}
		stat, _ := os.Stat(filepath.Join(dir, name))
		if stat.Mode().Perm() != 0600 || stat.Size() > maxProfileBytes {
			t.Fatal("profile permissions or byte bound violated")
		}
	}
}

func TestProfileFlushOnShutdown(t *testing.T) {
	root := privateTestRoot(t)
	stop, err := start("ahp", "1", root, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	stop()
	stop()
	checkProfiles(t, root, "ahp", "shutdown")
}

func TestProfileAutoStopsBeforeRuntimeDeadline(t *testing.T) {
	root := privateTestRoot(t)
	stop, err := start("as-runner", "1", root, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, _ := os.ReadFile(filepath.Join(root, "as-runner", "metadata.json"))
		var meta metadata
		if json.Unmarshal(raw, &meta) == nil && meta.Status == "COMPLETE" {
			checkProfiles(t, root, "as-runner", "window")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("profile window did not stop and flush")
}
