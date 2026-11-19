// Package runtimeprofile provides opt-in bounded diagnostic files, never HTTP.
package runtimeprofile

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"
)

const outputRoot = "/tmp/ahp-runtime-profiles"
const maxProfileBytes int64 = 32 << 20

var errProfile = errors.New("runtime profile initialization refused")

type fileInfo struct {
	SHA256   string `json:"sha256"`
	Bytes    int64  `json:"bytes"`
	Complete bool   `json:"complete"`
}

type metadata struct {
	SchemaVersion int                 `json:"schema_version"`
	Process       string              `json:"process"`
	PID           int                 `json:"pid"`
	Status        string              `json:"status"`
	StopReason    string              `json:"stop_reason"`
	StartedAt     time.Time           `json:"started_at"`
	FinishedAt    time.Time           `json:"finished_at"`
	WindowMS      int64               `json:"window_ms"`
	ElapsedMS     int64               `json:"elapsed_ms"`
	GoVersion     string              `json:"go_version"`
	VCSRevision   string              `json:"vcs_revision"`
	VCSModified   string              `json:"vcs_modified"`
	Files         map[string]fileInfo `json:"files"`
	Runtime       map[string]uint64   `json:"runtime"`
}

// StartFromEnv is inert unless AHP_RUNTIME_PROFILE=1. Output names are fixed per
// process. AHP_RUNTIME_PROFILE_SECONDS may shorten the default90s window (1..90).
// The returned idempotent stop flushes profiles on ordinary process shutdown.
func StartFromEnv(process string) (func(), error) {
	if os.Getenv("AHP_RUNTIME_PROFILE") != "1" {
		return func() {}, nil
	}
	seconds := 90
	if value := os.Getenv("AHP_RUNTIME_PROFILE_SECONDS"); value != "" {
		var err error
		seconds, err = strconv.Atoi(value)
		if err != nil || seconds < 1 || seconds > 90 {
			return func() {}, errProfile
		}
	}
	return start(process, "1", outputRoot, time.Duration(seconds)*time.Second)
}

type boundedWriter struct {
	w         io.Writer
	remaining int64
	failed    bool
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		n, _ := w.w.Write(p[:int(w.remaining)])
		w.remaining -= int64(n)
		w.failed = true
		return n, io.ErrShortWrite
	}
	n, err := w.w.Write(p)
	w.remaining -= int64(n)
	w.failed = w.failed || err != nil
	return n, err
}

func privateDir(path string) error {
	if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
		return errProfile
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return errProfile
	}
	return nil
}

func digest(path string, complete bool) fileInfo {
	f, err := os.Open(path)
	if err != nil {
		return fileInfo{}
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return fileInfo{SHA256: hex.EncodeToString(h.Sum(nil)), Bytes: n, Complete: complete && err == nil}
}

func start(process, enabled, root string, window time.Duration) (func(), error) {
	nop := func() {}
	if enabled != "1" {
		return nop, nil
	}
	if (process != "ahp" && process != "as-runner") || window <= 0 || window > 90*time.Second {
		return nop, errProfile
	}
	dir := filepath.Join(root, process)
	if privateDir(root) != nil || privateDir(dir) != nil {
		return nop, errProfile
	}
	files := make(map[string]*os.File)
	cleanup := func() {
		for name, file := range files {
			file.Close()
			os.Remove(filepath.Join(dir, name)) // only files exclusively created here
		}
	}
	for _, name := range []string{"cpu.pprof", "heap.pprof", "metadata.json"} {
		file, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			cleanup()
			return nop, errProfile
		}
		files[name] = file
	}
	cpu := &boundedWriter{w: files["cpu.pprof"], remaining: maxProfileBytes}
	if err := pprof.StartCPUProfile(cpu); err != nil {
		cleanup()
		return nop, errProfile
	}
	started := time.Now()
	var once sync.Once
	finish := func(reason string) {
		once.Do(func() {
			pprof.StopCPUProfile() // waits for the CPU writer and flushes its stream
			cpuOK := files["cpu.pprof"].Sync() == nil && !cpu.failed
			cpuOK = files["cpu.pprof"].Close() == nil && cpuOK
			heap := &boundedWriter{w: files["heap.pprof"], remaining: maxProfileBytes}
			heapErr := pprof.WriteHeapProfile(heap) // no forced GC or request data dump
			heapOK := heapErr == nil && !heap.failed
			heapOK = files["heap.pprof"].Sync() == nil && heapOK
			heapOK = files["heap.pprof"].Close() == nil && heapOK
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			meta := metadata{SchemaVersion: 1, Process: process, PID: os.Getpid(), Status: "COMPLETE", StopReason: reason,
				StartedAt: started.UTC(), FinishedAt: time.Now().UTC(), WindowMS: window.Milliseconds(), ElapsedMS: time.Since(started).Milliseconds(),
				GoVersion: runtime.Version(), Files: map[string]fileInfo{"cpu.pprof": digest(filepath.Join(dir, "cpu.pprof"), cpuOK), "heap.pprof": digest(filepath.Join(dir, "heap.pprof"), heapOK)},
				Runtime: map[string]uint64{"heap_alloc_bytes": mem.HeapAlloc, "heap_inuse_bytes": mem.HeapInuse, "heap_objects": mem.HeapObjects,
					"total_alloc_bytes": mem.TotalAlloc, "num_gc": uint64(mem.NumGC), "pause_total_ns": mem.PauseTotalNs, "goroutines": uint64(runtime.NumGoroutine())}}
			if !meta.Files["cpu.pprof"].Complete || !meta.Files["heap.pprof"].Complete {
				meta.Status = "PARTIAL"
			}
			if build, ok := debug.ReadBuildInfo(); ok {
				for _, setting := range build.Settings {
					if setting.Key == "vcs.revision" {
						meta.VCSRevision = setting.Value
					}
					if setting.Key == "vcs.modified" {
						meta.VCSModified = setting.Value
					}
				}
			}
			_ = json.NewEncoder(files["metadata.json"]).Encode(meta)
			_ = files["metadata.json"].Sync()
			_ = files["metadata.json"].Close()
		})
	}
	timer := time.AfterFunc(window, func() { finish("window") })
	return func() { timer.Stop(); finish("shutdown") }, nil
}
