// Package harness is the reproducible AHP baseline profiling / load harness.
//
// It spawns an isolated platform instance (own ports, own lane database, own
// mTLS material), drives deterministic closed-loop workloads against it, and
// samples platform, PostgreSQL, Redis and MinIO metrics. Every report binds
// measurements to the machine, the exact source revision and the measured
// binary hash. Secrets never enter a report.
package harness

import (
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// MachineInfo records the hardware/OS identity a measurement belongs to.
type MachineInfo struct {
	Hostname       string `json:"hostname"`
	OS             string `json:"os"`
	OSVersion      string `json:"os_version"`
	Arch           string `json:"arch"`
	Kernel         string `json:"kernel"`
	CPUModel       string `json:"cpu_model"`
	PhysicalCores  int    `json:"physical_cores"`
	LogicalCores   int    `json:"logical_cores"`
	MemoryBytes    uint64 `json:"memory_bytes"`
	GoVersion      string `json:"go_version"`
	DockerVersion  string `json:"docker_version"`
	LoadAvg1m      string `json:"load_avg_1m"`
	MeasuredAtUTC  string `json:"measured_at_utc"`
	SamplingMethod string `json:"sampling_method"`
	// ResourceLimits declares the quota the measured process runs under,
	// each entry with state measured | unsupported | unavailable (DEF-PROF-3).
	// Silence here is read as "no limits", which is a claim, not a fact.
	ResourceLimits []ResourceLimit `json:"resource_limits"`
}

// CollectMachine gathers hardware/OS facts via OS-native queries. On macOS it
// uses sysctl/sw_vers; on other systems it degrades to uname/runtime facts.
func CollectMachine() MachineInfo {
	m := MachineInfo{
		GoVersion:      runtime.Version(),
		Arch:           runtime.GOARCH,
		OS:             runtime.GOOS,
		MeasuredAtUTC:  time.Now().UTC().Format(time.RFC3339),
		ResourceLimits: CollectResourceLimits(),
	}
	if h, err := os.Hostname(); err == nil {
		m.Hostname = h
	}
	m.SamplingMethod = "sysctl/sw_vers/uname/docker CLI; no privileged access"
	if out, err := exec.Command("uname", "-r").Output(); err == nil {
		m.Kernel = strings.TrimSpace(string(out))
	}
	if runtime.GOOS == "darwin" {
		if out, err := exec.Command("sw_vers", "-productVersion").Output(); err == nil {
			m.OSVersion = "macOS " + strings.TrimSpace(string(out))
		}
		if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			m.CPUModel = strings.TrimSpace(string(out))
		}
		if out, err := exec.Command("sysctl", "-n", "hw.physicalcpu").Output(); err == nil {
			m.PhysicalCores, _ = strconv.Atoi(strings.TrimSpace(string(out)))
		}
		if out, err := exec.Command("sysctl", "-n", "hw.logicalcpu").Output(); err == nil {
			m.LogicalCores, _ = strconv.Atoi(strings.TrimSpace(string(out)))
		}
		if out, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
			m.MemoryBytes, _ = strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
		}
		if out, err := exec.Command("sysctl", "-n", "vm.loadavg").Output(); err == nil {
			// "{  2.10  2.00  1.90 }" -> first number
			fields := strings.Fields(strings.TrimSpace(string(out)))
			if len(fields) > 1 {
				m.LoadAvg1m = fields[1]
			}
		}
	} else {
		if out, err := exec.Command("uname", "-s").Output(); err == nil {
			m.OSVersion = strings.TrimSpace(string(out))
		}
		if out, err := exec.Command("nproc").Output(); err == nil {
			m.LogicalCores, _ = strconv.Atoi(strings.TrimSpace(string(out)))
		}
		if b, err := os.ReadFile("/proc/meminfo"); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, "MemTotal:") {
					f := strings.Fields(line)
					if len(f) > 1 {
						if kb, err := strconv.ParseUint(f[1], 10, 64); err == nil {
							m.MemoryBytes = kb * 1024
						}
					}
				}
			}
		}
	}
	if out, err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Output(); err == nil {
		m.DockerVersion = strings.TrimSpace(string(out))
	}
	return m
}
