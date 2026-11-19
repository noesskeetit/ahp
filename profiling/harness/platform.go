package harness

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// PlatformConfig describes one isolated platform instance. Never reuses live
// stand ports; every instance gets its own listener port, its own TLS
// material, its own tenant/deployment identity, and the lane database named
// by the injected lane environment.
type PlatformConfig struct {
	Binary      string
	Certs       CertMaterial
	GatewayPort int      // HTTP(S) API port for this instance (0 = auto-pick)
	PGProxyPort int      // when > 0 the database URL is rewritten to this local proxy
	ExtraEnv    []string // additional KEY=VALUE entries (must not contain secrets in reports)
	LogPath     string
	Verbose     bool
}

// Platform is a running isolated instance.
type Platform struct {
	cfg   PlatformConfig
	proc  *os.Process
	Port  int
	LogF  *os.File
	start time.Time
}

// FreePort reserves an ephemeral localhost port and releases it immediately.
// The tiny reuse race is acceptable because readiness polling verifies the
// port actually belongs to the spawned instance (correct TLS material).
func FreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// PlatformRole is the role every harness-launched instance runs with. It is a
// constant, not an assumption: with "all" the single process IS gateway +
// runner + data, so one pid's CPU/RSS covers the product being profiled. If
// the harness ever split roles, this constant and the artifact's
// measured_process block are where that becomes visible.
const PlatformRole = "all"

// laneEnv starts from the injected environment (run-tests.py provides the
// lane env file) and applies instance isolation.
func laneEnv(cfg PlatformConfig) []string {
	env := os.Environ()
	set := map[string]string{
		"AHP_ROLE":                    PlatformRole,
		"AHP_LISTEN":                  fmt.Sprintf(":%d", cfg.GatewayPort),
		"AHP_TLS_CERT":                cfg.Certs.ServerCert,
		"AHP_TLS_KEY":                 cfg.Certs.ServerKey,
		"AHP_TLS_CA":                  cfg.Certs.CACert,
		"OTEL_EXPORTER_OTLP_ENDPOINT": "",
	}
	if cfg.Verbose {
		set["AHP_VERBOSE"] = "1"
	}
	if cfg.PGProxyPort > 0 {
		// Route every database DSN the process may use through the harness
		// proxy so dependency-loss can be injected without touching shared
		// infrastructure.
		for _, key := range []string{"AHP_DATABASE_URL", "AHP_TEST_DATABASE_URL", "DATABASE_URL", "AS_DATA_DATABASE_URL"} {
			if v := os.Getenv(key); v != "" {
				set[key] = rewriteDBPort(v, cfg.PGProxyPort)
			}
		}
	}
	out := make([]string, 0, len(env)+len(set))
	seen := map[string]bool{}
	for _, kv := range env {
		key := kv
		if i := indexByte(kv, '='); i >= 0 {
			key = kv[:i]
		}
		if v, ok := set[key]; ok {
			out = append(out, key+"="+v)
		} else {
			out = append(out, kv)
		}
		seen[key] = true
	}
	for k, v := range set {
		if !seen[k] {
			out = append(out, k+"="+v)
		}
	}
	return append(out, cfg.ExtraEnv...)
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func rewriteDBPort(dsn string, port int) string {
	// postgres://user:pass@host:25495/db?...  -> replace the port component.
	prefix := "@127.0.0.1:25495"
	return replace(dsn, prefix, fmt.Sprintf("@127.0.0.1:%d", port))
}

func replace(s, old, new string) string {
	out := ""
	for {
		i := index(s, old)
		if i < 0 {
			return out + s
		}
		out += s[:i] + new
		s = s[i+len(old):]
	}
}

func index(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// Start launches the isolated platform instance and waits for /readyz.
func Start(cfg PlatformConfig) (*Platform, error) {
	if cfg.GatewayPort == 0 {
		p, err := FreePort()
		if err != nil {
			return nil, err
		}
		cfg.GatewayPort = p
	}
	logF, err := os.OpenFile(cfg.LogPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	p := &Platform{cfg: cfg, LogF: logF, Port: cfg.GatewayPort}
	cmd := exec.Command(cfg.Binary)
	cmd.Env = laneEnv(cfg)
	cmd.Stdout = io.MultiWriter(logF)
	cmd.Stderr = io.MultiWriter(logF)
	if err := cmd.Start(); err != nil {
		logF.Close()
		return nil, err
	}
	p.proc = cmd.Process
	go func() { _ = cmd.Wait() }()
	if err := p.waitReady(180 * time.Second); err != nil {
		p.Stop()
		return nil, err
	}
	p.start = time.Now()
	return p, nil
}

// HTTPClient returns an mTLS client trusting this instance's CA and presenting
// the given workload certificate.
func HTTPClient(cm CertMaterial, cc ClientCertFile) (*http.Client, error) {
	caPem, err := os.ReadFile(cm.CACert)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPem) {
		return nil, fmt.Errorf("harness: cannot load CA")
	}
	clientCert, err := tls.LoadX509KeyPair(cc.Cert, cc.Key)
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{clientCert}, ServerName: "localhost", MinVersion: tls.VersionTLS12},
		MaxIdleConns:    512, MaxIdleConnsPerHost: 256, IdleConnTimeout: 90 * time.Second,
	}
	return &http.Client{Transport: tr, Timeout: 120 * time.Second}, nil
}

func (p *Platform) waitReady(timeout time.Duration) error {
	cm := p.cfg.Certs
	client, err := HTTPClient(cm, ClientCertFile{Cert: cm.ServerCert, Key: cm.ServerKey})
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("https://127.0.0.1:%d/readyz", p.Port)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if p.proc != nil {
			var ws syscall.WaitStatus
			done, _ := syscall.Wait4(p.proc.Pid, &ws, syscall.WNOHANG, nil)
			if done == p.proc.Pid {
				return fmt.Errorf("harness: platform exited before ready (see %s)", p.cfg.LogPath)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("harness: platform not ready within %s (see %s)", timeout, p.cfg.LogPath)
}

// Stop terminates the instance gracefully.
func (p *Platform) Stop() {
	if p.proc != nil {
		_ = p.proc.Signal(syscall.SIGTERM)
		for i := 0; i < 40; i++ {
			var ws syscall.WaitStatus
			done, _ := syscall.Wait4(p.proc.Pid, &ws, syscall.WNOHANG, nil)
			if done == p.proc.Pid {
				p.proc = nil
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		if p.proc != nil {
			_ = p.proc.Kill()
			p.proc = nil
		}
	}
	if p.LogF != nil {
		p.LogF.Close()
	}
}

// PID returns the OS pid of the platform process (for external sampling).
func (p *Platform) PID() int {
	if p.proc == nil {
		return -1
	}
	return p.proc.Pid
}

// ProcStat is one external observation of the measured process, bound to the
// pid it was taken for. The pid travels with every observation: an RSS that
// moves by 100+ MiB between ticks is either the runtime releasing memory or
// the sampler having looked at a different process, and without the pid in
// the sample no reader can tell those apart (the 2026-11-09 ladder could
// not, which is why CPU/RSS attribution there is unverifiable).
type ProcStat struct {
	PID      int     `json:"proc_pid"`
	CPUSec   float64 `json:"proc_cpu_sec"`
	RSSBytes uint64  `json:"proc_rss_bytes"`
}

// parsePsProc parses `ps -o pid=,time=,rss=` output and REFUSES a line that is
// not about the pid we asked for. Silently accepting whatever ps returned is
// how a recycled or mistyped pid would leak another process's memory into
// this measurement.
func parsePsProc(out []byte, wantPID int) (ProcStat, error) {
	var pid int
	var cpuStr, rssStr string
	n, err := fmt.Sscanf(strings.TrimSpace(string(out)), "%d %s %s", &pid, &cpuStr, &rssStr)
	if err != nil {
		return ProcStat{}, fmt.Errorf("harness: ps output not parseable (%q): %w", strings.TrimSpace(string(out)), err)
	}
	if n != 3 {
		return ProcStat{}, fmt.Errorf("harness: ps output incomplete: %q", strings.TrimSpace(string(out)))
	}
	if wantPID > 0 && pid != wantPID {
		return ProcStat{}, fmt.Errorf("harness: ps answered for pid %d while measuring %d: identity not established", pid, wantPID)
	}
	rssKB := uint64(0)
	if _, err := fmt.Sscanf(rssStr, "%d", &rssKB); err != nil {
		return ProcStat{}, fmt.Errorf("harness: ps rss %q not a number: %w", rssStr, err)
	}
	return ProcStat{PID: pid, CPUSec: parsePsTime(cpuStr), RSSBytes: rssKB * 1024}, nil
}

// ProcessIdentity observes cpu-time and RSS of one specific pid and proves the
// answer belongs to it.
func (p *Platform) ProcessIdentity(wantPID int) (ProcStat, error) {
	if wantPID <= 0 {
		return ProcStat{}, fmt.Errorf("harness: no pid to sample")
	}
	out, err := exec.Command("ps", "-o", "pid=,time=,rss=", "-p", strconv.Itoa(wantPID)).Output()
	if err != nil {
		return ProcStat{}, err
	}
	return parsePsProc(out, wantPID)
}

// ProcessCPUAndRSS samples cumulative CPU seconds and resident set size of the
// platform process via ps(1). cputime accumulates across the process life, so
// differences over sampling intervals give CPU-seconds consumed. The identity
// is verified against the pid we asked for.
func (p *Platform) ProcessCPUAndRSS() (cpuSeconds float64, rssBytes uint64, err error) {
	st, err := p.ProcessIdentity(p.PID())
	if err != nil {
		return 0, 0, err
	}
	return st.CPUSec, st.RSSBytes, nil
}

// parsePsTime understands ps time formats: "SS", "MM.SS", "MM:SS.CC", "HH:MM:SS", "D-HH:MM:SS".
func parsePsTime(s string) float64 {
	if d := index(s, "-"); d >= 0 {
		days, _ := strconv.ParseFloat(s[:d], 64)
		s = s[d+1:]
		return days*86400 + parsePsTime(s)
	}
	sep := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			sep++
		}
	}
	switch sep {
	case 0:
		v, _ := strconv.ParseFloat(s, 64)
		return v
	case 1:
		var mm, ss float64
		fmt.Sscanf(s, "%f:%f", &mm, &ss)
		return mm*60 + ss
	default:
		var hh, mm, ss float64
		fmt.Sscanf(s, "%f:%f:%f", &hh, &mm, &ss)
		return hh*3600 + mm*60 + ss
	}
}
