package main

import (
	"encoding/json"
	"net/http"
	"runtime"
	"syscall"
	"time"
)

var procStart = time.Now()

type statsResponse struct {
	Status     string  `json:"status"`
	Service    string  `json:"service"`
	GoVersion  string  `json:"go_version"`
	Goroutines int     `json:"goroutines"`
	RSSBytes   uint64  `json:"rss_bytes"`
	RSSMB      float64 `json:"rss_mb"`
	HeapAlloc  uint64  `json:"heap_alloc_bytes"`
	SysBytes   uint64  `json:"sys_bytes"`
	UptimeSec  float64 `json:"uptime_seconds"`
}

// selfRSSBytes returns the process peak resident set size. On darwin ru.Maxrss
// is reported in bytes; on linux in kilobytes. Reporting RSS from inside the
// process keeps memory measurement independent of the `ps` binary, which is not
// always permitted in hardened/sandboxed environments.
func selfRSSBytes() uint64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	maxrss := uint64(ru.Maxrss)
	if runtime.GOOS == "linux" {
		maxrss *= 1024
	}
	return maxrss
}

func (s *server) handleStats(w http.ResponseWriter, _ *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	rss := selfRSSBytes()
	resp := statsResponse{
		Status:     "ok",
		Service:    envOr("AHP_ROLE", "ahp"),
		GoVersion:  runtime.Version(),
		Goroutines: runtime.NumGoroutine(),
		RSSBytes:   rss,
		RSSMB:      float64(rss) / (1024 * 1024),
		HeapAlloc:  ms.HeapAlloc,
		SysBytes:   ms.Sys,
		UptimeSec:  time.Since(procStart).Seconds(),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
