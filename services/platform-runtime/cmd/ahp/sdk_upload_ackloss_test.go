//go:build ahp_data

package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

// TestUploadAckLossReconcile is a LIVE leg (real PostgreSQL :25495, real MinIO
// :29000 through AHP_S3_ENDPOINT, real mTLS owner front): a byte-transparent
// proxy sits in front of the real blob store and kills the ACK of presigned
// PUTs on command. It pins three contract properties for BOTH public clients:
//
//  1. a presigned PUT whose answer was lost is classified outcome-unknown
//     (code UNKNOWN, reason outcome_unknown) - never INVALID_ARGUMENT;
//  2. the confirmation is the owner Seal, which verifies the bytes that
//     actually landed end to end (SHA-256 + size + version pin), so a PUT that
//     LANDED but lost its ACK reconciles with zero re-sends;
//  3. a PUT that never landed reconciles with exactly ONE bounded re-send
//     under the same upload ID, and if even that dies the caller still gets a
//     typed outcome-unknown - never a silent swallow, never a second artifact
//     (BeginUpload of the same key must return the same ID).
//
// The proxy rewrites /etc/hosts for host.docker.internal to loopback for the
// duration of this test only and restores it on cleanup; every byte still
// reaches the real MinIO process, which is the only thing that stores them.
func TestUploadAckLossReconcile(t *testing.T) {
	for _, key := range []string{"AHP_TEST_DATABASE_URL", "AHP_S3_ENDPOINT", "AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD"} {
		if os.Getenv(key) == "" {
			t.Fatalf("%s required; no skip", key)
		}
	}
	if runtime.GOOS != "linux" {
		t.Fatal("live proxy lane runs on Linux only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	s3, err := url.Parse(os.Getenv("AHP_S3_ENDPOINT"))
	if err != nil || s3.Hostname() == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT: %v", err)
	}
	dsn, err := url.Parse(os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil || dsn.Hostname() == "" {
		t.Fatalf("bad AHP_TEST_DATABASE_URL: %v", err)
	}
	if s3.Port() == "" || dsn.Port() == "" {
		t.Fatal("proxy lane needs explicit ports in both endpoints")
	}
	ips, err := net.LookupHost(s3.Hostname())
	if err != nil || len(ips) == 0 {
		t.Fatalf("resolve %s before the hosts rewrite: %v", s3.Hostname(), err)
	}
	s3Port, _ := strconv.Atoi(s3.Port())
	dsnPort, _ := strconv.Atoi(dsn.Port())
	proxy, adminURL := startACKProxy(t, net.JoinHostPort(ips[0], s3.Port()), net.JoinHostPort(ips[0], dsn.Port()), s3Port, dsnPort)
	t.Logf("ackproxy real blob upstream=%s (resolved %s -> %s)", net.JoinHostPort(ips[0], s3.Port()), s3.Hostname(), ips[0])
	rerouteDockerHost(t, s3.Hostname())

	pool, store, scope := cascadeEnv(t, ctx, func(_ context.Context, s contracts.Scope, _, _ string) error {
		return s.Validate()
	})
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("ackloss-artifacts", "k", priv)
	verifier := auth.NewVerifier("ackloss-artifacts", map[string]ed25519.PublicKey{"k": pub})
	if _, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: "ack", Context: scope.Tenant + "-ack", Deployment: scope.Deployment, Capabilities: []string{"artifacts"}, TTL: 10 * time.Minute}); err != nil {
		t.Fatal(err)
	}
	token, err := ids.MintTask(ctx, scope, "ack", signer, "ahp", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ah, err := artifactsapi.New(store, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(ah)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	cert := ca.clientCert(t, auth.WorkloadID(scope))
	httpClient := srv.Client()
	tr := httpClient.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{cert}
	httpClient.Transport = tr
	client, err := sdk.New(sdk.Config{BaseURL: srv.URL, HTTPClient: httpClient, TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	// One connection per presigned PUT: keep-alive pooling would let the Go
	// transport transparently re-issue a PUT on a fresh connection after a
	// dead persistent one, hiding exactly the ACK loss this leg injects.
	client, err = sdk.New(sdk.Config{BaseURL: srv.URL, HTTPClient: httpClient, UploadHTTPClient: &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}, TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}

	data := bytes_n(96 << 10)
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])

	t.Run("WarmRealStoreThroughProxy", func(t *testing.T) {
		if _, err := client.Put(ctx, sdk.PutRequest{Key: "warm", Name: "warm.bin", MIME: "application/octet-stream", Data: data}); err != nil {
			t.Fatalf("the real store behind the proxy must answer a plain Put: %v", err)
		}
	})

	t.Run("GoPresignedAckLossIsOutcomeUnknown", func(t *testing.T) {
		up, err := client.BeginUpload(ctx, sdk.UploadRequest{Key: "go-ack-landed", Name: "landed.bin", MIME: "application/octet-stream", SHA256: digest, Size: int64(len(data))})
		if err != nil {
			t.Fatal(err)
		}
		proxy.arm("land", 1, scope.Tenant)
		err = client.Upload(ctx, up, data)
		proxy.disarm()
		if err == nil {
			t.Fatal("the proxy killed the ACK, yet Upload reported success")
		}
		if !sdk.IsOutcomeUnknown(err) {
			t.Errorf("ACK loss must be outcome-unknown, got code=%q reason=%q err=%v", contracts.Code(err), contracts.ReasonOf(err), err)
		}
		if got := contracts.Code(err); got != "UNKNOWN" {
			t.Errorf("code=%q, want UNKNOWN (parity with the Python client)", got)
		}
		if got := contracts.ReasonOf(err); got != "outcome_unknown" {
			t.Errorf("reason=%q, want outcome_unknown", got)
		}
		ref, err := client.Seal(ctx, up.ID)
		if err != nil {
			t.Fatalf("owner Seal must confirm the landed bytes: %v", err)
		}
		if ref.SHA256 != digest || ref.Size != int64(len(data)) {
			t.Fatalf("sealed ref %x/%d differs from %s/%d", ref.SHA256, ref.Size, digest, len(data))
		}
		if got, err := client.Download(ctx, ref.ID); err != nil || string(got) != string(data) {
			t.Fatalf("download after confirmed seal: bytes=%d err=%v", len(got), err)
		}
		if p, k := proxy.counters(); p != 1 || k != 1 {
			t.Errorf("classification leg must not re-send: puts=%d killed=%d, want 1/1", p, k)
		}
	})

	t.Run("GoPutReconcilesLandedAckLossWithoutReSend", func(t *testing.T) {
		proxy.arm("land", 1, scope.Tenant)
		ref, err := client.Put(ctx, sdk.PutRequest{Key: "go-put-landed", Name: "landed.bin", MIME: "application/octet-stream", Data: data})
		proxy.disarm()
		if err != nil {
			t.Fatalf("Put must reconcile a landed ACK loss through Seal: %v", err)
		}
		if ref.SHA256 != digest || ref.Size != int64(len(data)) {
			t.Fatalf("ref mismatch: %s/%d", ref.SHA256, ref.Size)
		}
		if p, k := proxy.counters(); p != 1 || k != 1 {
			t.Errorf("landed bytes confirmed by SHA demand zero re-sends: puts=%d killed=%d, want 1/1", p, k)
		}
	})

	t.Run("GoPutReconcilesLostUploadWithOneBoundedReSend", func(t *testing.T) {
		proxy.arm("lost", 1, scope.Tenant)
		ref, err := client.Put(ctx, sdk.PutRequest{Key: "go-put-lost", Name: "lost.bin", MIME: "application/octet-stream", Data: data})
		proxy.disarm()
		if err != nil {
			t.Fatalf("Put must re-present the SAME upload once: %v", err)
		}
		if ref.SHA256 != digest || ref.Size != int64(len(data)) {
			t.Fatalf("ref mismatch: %s/%d", ref.SHA256, ref.Size)
		}
		if p, k := proxy.counters(); p != 2 || k != 1 {
			t.Errorf("one dead attempt plus one bounded re-send: puts=%d killed=%d, want 2/1", p, k)
		}
		if got, err := client.Download(ctx, ref.ID); err != nil || string(got) != string(data) {
			t.Fatalf("download after reconcile: bytes=%d err=%v", len(got), err)
		}
	})

	var goBothLost string
	t.Run("GoPutReportsTypedUnknownWhenBothAttemptsDie", func(t *testing.T) {
		up, err := client.BeginUpload(ctx, sdk.UploadRequest{Key: "go-put-bothlost", Name: "both.bin", MIME: "application/octet-stream", SHA256: digest, Size: int64(len(data))})
		if err != nil {
			t.Fatal(err)
		}
		proxy.arm("lost", 2, scope.Tenant)
		_, err = client.Put(ctx, sdk.PutRequest{Key: "go-put-bothlost", Name: "both.bin", MIME: "application/octet-stream", Data: data})
		proxy.disarm()
		if err == nil {
			t.Fatal("both PUT attempts died; Put must not report success")
		}
		if !sdk.IsOutcomeUnknown(err) || contracts.Code(err) != "UNKNOWN" {
			t.Errorf("want UNKNOWN/outcome-unknown, got code=%q err=%v", contracts.Code(err), err)
		}
		if p, k := proxy.counters(); p != 2 || k != 2 {
			t.Errorf("exactly two attempts then stop, no third behind Put: puts=%d killed=%d, want 2/2", p, k)
		}
		goBothLost = up.ID
	})
	t.Run("GoSameKeyReconcilesTheSameUploadID", func(t *testing.T) {
		up, err := client.BeginUpload(ctx, sdk.UploadRequest{Key: "go-put-bothlost", Name: "both.bin", MIME: "application/octet-stream", SHA256: digest, Size: int64(len(data))})
		if err != nil {
			t.Fatal(err)
		}
		if up.ID != goBothLost {
			t.Fatalf("same key minted a second upload: %s then %s", goBothLost, up.ID)
		}
		if err := client.Upload(ctx, up, data); err != nil {
			t.Fatal(err)
		}
		ref, err := client.Seal(ctx, up.ID)
		if err != nil {
			t.Fatal(err)
		}
		if ref.ID != goBothLost || ref.SHA256 != digest {
			t.Fatalf("reconciled ref %s/%s differs", ref.ID, ref.SHA256)
		}
	})

	// Python leg: the SAME proxy, the SAME store, the SAME scenario, driven by
	// the public Python client only (ahp_clients).
	dir := t.TempDir()
	write := func(name, data string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	hand := map[string]any{
		"base": srv.URL, "token": token, "admin": adminURL, "tenant": scope.Tenant,
		"output": filepath.Join(dir, "output.json"),
		"ca":     write("ca.pem", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.TLS.Certificates[0].Certificate[0]}))),
	}
	der, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	hand["client_cert"] = write("client.pem", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})))
	hand["client_key"] = write("client.key", string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})))
	raw, _ := json.Marshal(hand)
	config := write("config.json", string(raw))
	t.Run("PythonAckLossParity", func(t *testing.T) {
		command := exec.CommandContext(ctx, "python3", "../../sdk/python/ahp_clients/tests/artifacts_ackloss.py", config)
		command.Env = append(os.Environ(), "PYTHONPATH=../../sdk/python:"+os.Getenv("PYTHONPATH"))
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("Python ACK-loss contract: %v\n%s", err, output)
		} else {
			t.Log(string(output))
		}
		out, err := os.ReadFile(hand["output"].(string))
		if err != nil {
			t.Fatal(err)
		}
		// Python writes its own payload (its bytes differ from the Go legs);
		// verify each reconciled id from the OWNER side against the digest and
		// size Python reported, re-reading the full bytes so a landed-but-wrong
		// object cannot pass.
		var reported struct {
			IDs    map[string]string `json:"ids"`
			SHA256 string            `json:"sha256"`
			Size   int64             `json:"size"`
		}
		if err := json.Unmarshal(out, &reported); err != nil {
			t.Fatal(err)
		}
		if len(reported.IDs) == 0 || len(reported.SHA256) != 64 || reported.Size <= 0 {
			t.Fatalf("python reported no verifiable artifacts: %s", out)
		}
		for name, id := range reported.IDs {
			ref, err := client.Get(ctx, id)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if ref.SHA256 != reported.SHA256 || ref.Size != reported.Size {
				t.Fatalf("%s: owner ref %s/%d differs from python %s/%d", name, ref.SHA256, ref.Size, reported.SHA256, reported.Size)
			}
			got, err := client.Download(ctx, id)
			if err != nil {
				t.Fatalf("%s: owner download: %v", name, err)
			}
			if sum := sha256.Sum256(got); hex.EncodeToString(sum[:]) != reported.SHA256 || int64(len(got)) != reported.Size {
				t.Fatalf("%s: owner-read bytes do not match python payload", name)
			}
		}
	})
}

// --- the ACK-loss proxy -----------------------------------------------------

type ackProxy struct {
	mu     sync.Mutex
	mode   string
	count  int
	tenant string
	puts   int
	killed int
	allPUT int
	dbg    func(string, ...any)
}

func startACKProxy(t *testing.T, realMinIO, realPG string, minioPort, pgPort int) (*ackProxy, string) {
	t.Helper()
	p := &ackProxy{dbg: func(format string, args ...any) { t.Logf("ackproxy "+format, args...) }}
	blob, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", minioPort))
	if err != nil {
		t.Fatalf("bind blob port %d: %v", minioPort, err)
	}
	pg, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", pgPort))
	if err != nil {
		t.Fatalf("bind PG forward port %d: %v", pgPort, err)
	}
	adminLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	admin := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/arm":
			q := r.URL.Query()
			count, _ := strconv.Atoi(q.Get("count"))
			p.arm(q.Get("mode"), count, q.Get("tenant"))
			w.WriteHeader(http.StatusNoContent)
		case "/stats":
			puts, killed := p.counters()
			_ = json.NewEncoder(w).Encode(map[string]int{"puts": puts, "killed": killed})
		default:
			http.NotFound(w, r)
		}
	})}
	go func() { _ = admin.Serve(adminLn); _ = blob.Close(); _ = pg.Close() }()
	t.Cleanup(func() {
		_ = admin.Close()
		_ = blob.Close()
		_ = pg.Close()
	})
	go acceptLoop(blob, func(cn net.Conn) { p.serveBlob(cn, realMinIO) })
	go acceptLoop(pg, func(cn net.Conn) { blindPipe(cn, realPG) })
	return p, fmt.Sprintf("http://%s", adminLn.Addr().String())
}

func (p *ackProxy) arm(mode string, count int, tenant string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode, p.count, p.tenant, p.puts, p.killed = mode, count, tenant, 0, 0
}

func (p *ackProxy) disarm() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode, p.count = "off", 0
}

func (p *ackProxy) counters() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.puts, p.killed
}

func (p *ackProxy) all() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.allPUT
}

func (p *ackProxy) serveBlob(cn net.Conn, real string) {
	defer cn.Close()
	br := bufio.NewReader(cn)
	up, err := net.Dial("tcp", real)
	if err != nil {
		return
	}
	defer up.Close()
	t0 := time.Now()
	if p.dbg != nil {
		p.dbg("conn from %s dialed %s", cn.RemoteAddr(), real)
	}
	ubr := bufio.NewReader(up)
	for {
		head, err := readHTTPHead(br)
		if err != nil {
			return
		}
		fields := strings.Fields(string(head[:min(len(head), 4096)]))
		method, path := "", ""
		if len(fields) >= 2 {
			method, path = fields[0], fields[1]
		}
		length := contentLengthOf(head)
		kill := ""
		p.mu.Lock()
		if method == http.MethodPut {
			p.allPUT++
		}
		if p.dbg != nil {
			p.dbg("blob %s %s (len=%d kill-candidate=%v)", method, path, length, method == http.MethodPut && p.tenant != "" && strings.Contains(path, "/"+p.tenant+"/"))
		}
		if method == http.MethodPut && p.tenant != "" && strings.Contains(path, "/"+p.tenant+"/") {
			p.puts++
			if p.count > 0 {
				p.count--
				p.killed++
				kill = p.mode
			}
		}
		p.mu.Unlock()
		if kill == "lost" {
			// Nothing is forwarded: the bytes never reach the store and the
			// ACK the client waits for never comes.
			return
		}
		if _, err := up.Write(head); err != nil {
			return
		}
		if length > 0 {
			if _, err := io.CopyN(up, br, length); err != nil {
				return
			}
			if p.dbg != nil {
				p.dbg("body %d bytes forwarded in %v (%s)", length, time.Since(t0), path)
			}
		}
		if kill == "land" {
			// Let the store finish the write so the bytes ARE durable,
			// discard its answer, then drop the client connection without an
			// ACK.
			_ = up.SetReadDeadline(time.Now().Add(30 * time.Second))
			drainHTTPResponse(ubr)
			return
		}
		if err := relayResponse(ubr, cn, method); err != nil {
			return
		}
		if p.dbg != nil {
			p.dbg("response relayed for %s after %v", path, time.Since(t0))
		}
	}
}

// relayResponse forwards exactly one framed HTTP response, so the loop can
// keep serving further requests on a persistent connection. A reply to HEAD
// (and 204/304) carries no body: copying one anyway would devour the NEXT
// response on this connection and stall that request until its client gives
// up - 30 seconds per Seal in this lane.
func relayResponse(up *bufio.Reader, cn net.Conn, method string) error {
	head, err := readHTTPHead(up)
	if err != nil {
		return err
	}
	if _, err := cn.Write(head); err != nil {
		return err
	}
	status := 0
	if len(head) > 12 {
		status, _ = strconv.Atoi(string(head[9:12]))
	}
	if method == http.MethodHead || status == http.StatusNoContent || status == http.StatusNotModified {
		return nil
	}
	if strings.Contains(strings.ToLower(string(head)), "transfer-encoding: chunked") {
		_, err := io.Copy(cn, up)
		return err
	}
	if n := contentLengthOf(head); n > 0 {
		_, err := io.CopyN(cn, up, n)
		return err
	}
	return nil
}

func acceptLoop(ln net.Listener, handle func(net.Conn)) {
	for {
		cn, err := ln.Accept()
		if err != nil {
			return
		}
		go handle(cn)
	}
}

func blindPipe(cn net.Conn, real string) {
	defer cn.Close()
	up, err := net.Dial("tcp", real)
	if err != nil {
		return
	}
	defer up.Close()
	go func() {
		_, _ = io.Copy(up, cn)
		if cw, ok := up.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
	}()
	_, _ = io.Copy(cn, up)
}

func readHTTPHead(br *bufio.Reader) ([]byte, error) {
	var head []byte
	for {
		line, err := br.ReadString('\n')
		head = append(head, line...)
		if err != nil {
			return nil, err
		}
		if len(head) > 1<<16 {
			return nil, fmt.Errorf("request head too large")
		}
		if strings.HasSuffix(string(head), "\r\n\r\n") || strings.HasSuffix(string(head), "\n\n") {
			return head, nil
		}
	}
}

func contentLengthOf(head []byte) int64 {
	for _, line := range strings.Split(string(head), "\r\n") {
		if name, value, ok := strings.Cut(line, ":"); ok && strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err != nil {
				return -1
			}
			return n
		}
	}
	return -1
}

func drainHTTPResponse(br *bufio.Reader) {
	length := int64(-1)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if name, value, ok := strings.Cut(line, ":"); ok && strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			length, _ = strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		}
	}
	if length > 0 {
		_, _ = io.CopyN(io.Discard, br, length)
	}
}

func rerouteDockerHost(t *testing.T, hostname string) {
	t.Helper()
	const hostsPath = "/etc/hosts"
	raw, err := os.ReadFile(hostsPath)
	if err != nil {
		t.Fatalf("live ACK-loss lane needs a readable %s: %v", hostsPath, err)
	}
	var keep []string
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.Contains(line, hostname) {
			keep = append(keep, line)
		}
	}
	patched := strings.Join(keep, "\n") + "\n127.0.0.1 " + hostname + "\n"
	if err := os.WriteFile(hostsPath, []byte(patched), 0644); err != nil {
		t.Fatalf("live ACK-loss lane needs a writable %s (run inside the Linux container): %v", hostsPath, err)
	}
	// Go caches the whole parsed /etc/hosts for net/hosts.go cacheMaxAge (5s)
	// and re-stats the file only after that window. The upstream was resolved
	// BEFORE the rewrite on purpose; without outliving the cache this process
	// would keep dialing the real address and the injected ACK loss would miss
	// the Go legs while the fresh Python subprocess (no such cache) would
	// still hit the proxy - the exact asymmetry this lane must not inherit.
	settleHostsCache()
	t.Cleanup(func() {
		if err := os.WriteFile(hostsPath, raw, 0644); err != nil {
			t.Errorf("restore %s failed; a leftover loopback mapping poisons every later test in this package: %v", hostsPath, err)
			return
		}
		if back, err := os.ReadFile(hostsPath); err != nil || string(back) != string(raw) {
			t.Errorf("%s did not read back byte-for-byte after restore: %v", hostsPath, err)
		}
		// Flush the cache AGAIN before returning. Sibling tests in this package
		// run sequentially after this cleanup; any host.docker.internal lookup
		// inside cacheMaxAge of the restore would still resolve to the loopback
		// proxy - whose listener is already gone - the exact leak that made
		// unrelated acceptance legs dial a dead 127.0.0.1:25495.
		settleHostsCache()
	})
}

// settleHostsCache waits out net/hosts.go's cacheMaxAge (5s) so the next
// LookupHost in this process re-stats /etc/hosts and observes the last write.
func settleHostsCache() { time.Sleep(5100 * time.Millisecond) }

func bytes_n(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte('a' + i%26)
	}
	return out
}
