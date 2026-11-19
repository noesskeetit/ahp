package compute

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestExportSnapshotPinsOpenedFile(t *testing.T) {
	home := t.TempDir()
	p := filepath.Join(home, "out")
	if err := os.WriteFile(p, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	body, size, _, _, cleanup, err := prepareExport(context.Background(), root, "out")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err = os.Rename(p, p+".old"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(p, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(body)
	if err != nil || string(b) != "original" || size != 8 {
		t.Fatalf("read changed path: %q size=%d err=%v", b, size, err)
	}
}
func TestExportArchiveIsBoundedAndRejectsLinks(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "folder")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("payload"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	body, size, mime, _, cleanup, err := prepareExport(context.Background(), root, "folder")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if size <= 0 || mime != "application/gzip" {
		t.Fatalf("bad archive size=%d mime=%s", size, mime)
	}
	gz, err := gzip.NewReader(body)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	h, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(tr)
	if err != nil || h.Name != "a.txt" || string(b) != "payload" {
		t.Fatalf("bad tar name=%s bytes=%s error=%v", h.Name, b, err)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	_ = os.WriteFile(outside, []byte("secret"), 0600)
	if err = os.Symlink(outside, filepath.Join(dir, "escape")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, clean, e := prepareExport(context.Background(), root, "folder"); contracts.Code(e) != contracts.ErrForbidden {
		clean()
		t.Fatalf("directory link accepted: %v", e)
	}
}
func TestExportSpecialFileDoesNotBlock(t *testing.T) {
	home := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(home, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	start := time.Now()
	_, _, _, _, cleanup, err := prepareExport(context.Background(), root, "fifo")
	defer cleanup()
	if contracts.Code(err) != contracts.ErrInvalid || time.Since(start) > time.Second {
		t.Fatalf("special file was not promptly denied: %v", err)
	}
}

func TestExportSnapshotPinsBytesAgainstSameFDOverwrite(t *testing.T) {
	home := t.TempDir()
	p := filepath.Join(home, "out")
	if err := os.WriteFile(p, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	body, _, _, _, cleanup, err := prepareExport(context.Background(), root, "out")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = f.WriteAt([]byte("mutated!"), 0); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(body)
	if err != nil || string(b) != "original" {
		t.Fatalf("snapshot changed by same-inode WriteAt: %q err=%v", b, err)
	}
}

type overwriteDuringSnapshot struct {
	*os.File
	afterRead func()
	used      bool
}

func (s *overwriteDuringSnapshot) Read(p []byte) (int, error) {
	n, e := s.File.Read(p)
	if !s.used {
		s.used = true
		s.afterRead()
	}
	return n, e
}
func TestExportSnapshotRejectsConcurrentSameSizeOverwrite(t *testing.T) {
	p := filepath.Join(t.TempDir(), "out")
	data := bytes.Repeat([]byte("original"), 16000)
	if e := os.WriteFile(p, data, 0600); e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(p, os.O_RDWR, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	before, e := f.Stat()
	if e != nil {
		t.Fatal(e)
	}
	source := &overwriteDuringSnapshot{File: f, afterRead: func() {
		if _, e := f.WriteAt([]byte("mutated!"), 0); e != nil {
			t.Fatal(e)
		}
		if e := os.Chtimes(p, time.Now(), before.ModTime().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
	}}
	var dst bytes.Buffer
	if e = copyExportSnapshot(context.Background(), &dst, source, before); contracts.Code(e) != contracts.ErrConflict {
		t.Fatalf("same-FD overwrite during copy accepted: %v", e)
	}
}
func TestExportSnapshotCancellationStopsCopy(t *testing.T) {
	p := filepath.Join(t.TempDir(), "out")
	if e := os.WriteFile(p, bytes.Repeat([]byte("data"), 16000), 0600); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(p)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	before, e := f.Stat()
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &overwriteDuringSnapshot{File: f, afterRead: cancel}
	var dst bytes.Buffer
	if e = copyExportSnapshot(ctx, &dst, source, before); e != context.Canceled || dst.Len() >= int(before.Size()) {
		t.Fatalf("cancelled copy consumed whole source: n=%d err=%v", dst.Len(), e)
	}
}
