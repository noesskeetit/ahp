package harness

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Call is one measured request.
type Call struct {
	Op        string  `json:"op"`
	Status    int     `json:"status"`
	OK        bool    `json:"ok"`
	TTFBms    float64 `json:"ttfb_ms"`
	Totalms   float64 `json:"total_ms"`
	RespBytes int     `json:"resp_bytes"`
	ErrClass  string  `json:"err_class,omitempty"`
}

// cred is the current bearer plus the expiry read from its own exp claim, so the
// harness rotates against the credential the product actually issued rather than a
// guessed TTL.
type cred struct {
	token string
	exp   time.Time
}

// Caller issues measured HTTPS calls with a bearer obtained through the product
// exchange boundary. The harness never signs a token: a credential comes from the
// live exchange (UseLiveToken, which re-exchanges off the measured path) or the
// run fails closed. See tokens.go.
type Caller struct {
	Base  string
	hc    *http.Client
	fixed string               // static bearer (short runs / tests)
	live  atomic.Pointer[cred] // rotating bearer swapped off the measured path

	rotations atomic.Int64
	rotErr    atomic.Value // string: last rotation failure, for the artifact
	outside   atomic.Bool  // true when credential is refreshed off the sample path
	stopRot   chan struct{}
	rotOnce   sync.Once
	rotDone   sync.WaitGroup
}

// UseFixedToken pins every later request to one bearer. It never expires on its
// own; a run longer than the token TTL must use UseLiveToken instead.
func (c *Caller) UseFixedToken(t string) { c.fixed = t }

// current returns the bearer to attach: the live rotating credential if present,
// else the static one. Reading it is an atomic load with no I/O -- a measured
// request never blocks on, and never measures, a token refresh.
func (c *Caller) current() string {
	if cd := c.live.Load(); cd != nil {
		return cd.token
	}
	return c.fixed
}

// UseLiveToken pins the initial exchanged token and starts a background refresher
// that re-obtains a credential through the product exchange before its exp and
// swaps it atomically. The refresh runs on its own timer using the provider's own
// HTTP path, never inside a measured request, so rotation latency can never enter
// a latency sample; it is the token analogue of an outside-window observer probe.
// The provider must NOT log secrets. margin is how early to refresh before expiry.
func (c *Caller) UseLiveToken(initial string, provider func(context.Context) (string, error), margin time.Duration) error {
	exp, err := TokenExpiry(initial)
	if err != nil {
		return err
	}
	c.live.Store(&cred{token: initial, exp: exp})
	c.fixed = ""
	c.outside.Store(true)
	c.stopRot = make(chan struct{})
	c.rotDone.Add(1)
	go func() {
		defer c.rotDone.Done()
		c.refreshLoop(provider, margin)
	}()
	return nil
}

// refreshLoop re-exchanges only when now+margin has reached the current credential's
// expiry. It never touches a measured request; on repeated provider failure it
// stops (a dead instance must not be hammered) and keeps the last token.
func (c *Caller) refreshLoop(provider func(context.Context) (string, error), margin time.Duration) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	fails := 0
	for {
		if cd := c.live.Load(); cd != nil && !time.Now().Add(margin).Before(cd.exp) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			tok, err := provider(ctx)
			cancel()
			if err != nil {
				fails++
				c.rotErr.Store(err.Error())
				if fails >= 5 {
					c.rotErr.Store("rotation stopped after repeated failures: " + err.Error())
					return
				}
			} else if exp, e := TokenExpiry(tok); e == nil {
				c.live.Store(&cred{token: tok, exp: exp})
				c.rotations.Add(1)
				c.rotErr.Store("")
				fails = 0
			} else {
				fails++
				c.rotErr.Store(e.Error())
			}
		}
		select {
		case <-c.stopRot:
			return
		case <-ticker.C:
		}
	}
}

// Rotations reports how many times the credential was re-exchanged off-window.
func (c *Caller) Rotations() int64 { return c.rotations.Load() }

// RotationsFunc snapshots the rotation counter on demand (the artifact reads it at
// write time so the count reflects the whole run, not the moment of recording).
func (c *Caller) RotationsFunc() func() int64 { return func() int64 { return c.rotations.Load() } }

// RotationError reports the last rotation failure text ("" when healthy).
func (c *Caller) RotationError() string {
	if v, ok := c.rotErr.Load().(string); ok {
		return v
	}
	return ""
}

// RotationOutsideMeasurementWindow is true when the credential is refreshed off the
// measured request path (UseLiveToken); it is what makes a >TTL soak valid without
// the refresh contaminating the latency distribution.
func (c *Caller) RotationOutsideMeasurementWindow() bool { return c.outside.Load() }

// StopRotation stops the background refresher (wired to the session's lifecycle so
// it never outlives the instance it exchanges against).
func (c *Caller) StopRotation() {
	if c.stopRot == nil {
		return
	}
	c.rotOnce.Do(func() { close(c.stopRot) })
	c.rotDone.Wait()
}

// NewCaller builds an mTLS client for one workload identity against port.
func NewCaller(port int, cm CertMaterial, cc ClientCertFile, maxConns int) (*Caller, error) {
	caPem, err := os.ReadFile(cm.CACert)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPem) {
		return nil, fmt.Errorf("harness: bad CA")
	}
	clientCert, err := tls.LoadX509KeyPair(cc.Cert, cc.Key)
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{clientCert}, ServerName: "localhost", MinVersion: tls.VersionTLS12},
		MaxIdleConns:    maxConns, MaxIdleConnsPerHost: maxConns, MaxConnsPerHost: 0, IdleConnTimeout: 90 * time.Second,
		ForceAttemptHTTP2: true,
	}
	return &Caller{
		Base: fmt.Sprintf("https://127.0.0.1:%d", port),
		hc:   &http.Client{Transport: tr},
	}, nil
}

// Do performs one measured request. body nil sends "{}".
func (c *Caller) Do(ctx context.Context, path, op string, body any) Call {
	call, _ := c.DoJSON(ctx, path, op, body)
	return call
}

// DoJSON performs one measured request and also returns the response body.
func (c *Caller) DoJSON(ctx context.Context, path, op string, body any) (Call, []byte) {
	start := time.Now()
	var raw []byte
	if body == nil {
		raw = []byte("{}")
	} else {
		raw, _ = json.Marshal(body)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Base+path, bytes.NewReader(raw))
	if err != nil {
		return Call{Op: op, ErrClass: "request_build", Totalms: ms(time.Since(start))}, nil
	}
	req.Header.Set("content-type", "application/json")
	c.authorize(req)
	return c.exec(req, op, start)
}

// Get performs one measured GET (no bearer needed for /healthz, /readyz, /debug/stats).
func (c *Caller) Get(ctx context.Context, path, op string, withBearer bool) Call {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Base+path, nil)
	if err != nil {
		return Call{Op: op, ErrClass: "request_build", Totalms: ms(time.Since(start))}
	}
	if tok := c.current(); withBearer && tok != "" {
		req.Header.Set("authorization", "Bearer "+tok)
	}
	call, _ := c.exec(req, op, start)
	return call
}

func (c *Caller) exec(req *http.Request, op string, start time.Time) (Call, []byte) {
	var ttfb time.Time
	trace := &httptrace.ClientTrace{GotFirstResponseByte: func() { ttfb = time.Now() }}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := c.hc.Do(req)
	if err != nil {
		return Call{Op: op, ErrClass: classifyError(err), Totalms: ms(time.Since(start))}, nil
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	total := time.Since(start)
	call := Call{Op: op, Status: resp.StatusCode, Totalms: ms(total), RespBytes: len(bodyBytes), OK: resp.StatusCode == http.StatusOK}
	if !ttfb.IsZero() {
		call.TTFBms = ms(ttfb.Sub(start))
	} else {
		call.TTFBms = call.Totalms
	}
	if !call.OK {
		call.ErrClass = fmt.Sprintf("http_%d", resp.StatusCode)
	}
	return call, bodyBytes
}

// StreamCall performs one measured streaming POST and reports TTFB as time to
// the first byte of response body (first streamed delta), not just headers.
func (c *Caller) StreamCall(ctx context.Context, path, op string, body any) (Call, []byte, error) {
	start := time.Now()
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Base+path, bytes.NewReader(raw))
	if err != nil {
		return Call{Op: op, ErrClass: "request_build"}, nil, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("accept", "text/event-stream")
	c.authorize(req)
	resp, err := c.hc.Do(req)
	if err != nil {
		return Call{Op: op, ErrClass: classifyError(err), Totalms: ms(time.Since(start))}, nil, err
	}
	defer resp.Body.Close()
	var firstByte time.Time
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 1024)
	for {
		n, rerr := resp.Body.Read(tmp)
		if n > 0 {
			if firstByte.IsZero() {
				firstByte = time.Now()
			}
			buf = append(buf, tmp[:n]...)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return Call{Op: op, Status: resp.StatusCode, ErrClass: "stream_read", Totalms: ms(time.Since(start))}, buf, rerr
		}
	}
	call := Call{Op: op, Status: resp.StatusCode, Totalms: ms(time.Since(start)), RespBytes: len(buf), OK: resp.StatusCode == http.StatusOK}
	if !firstByte.IsZero() {
		call.TTFBms = ms(firstByte.Sub(start))
	}
	if !call.OK {
		call.ErrClass = fmt.Sprintf("http_%d", resp.StatusCode)
	}
	return call, buf, nil
}

// authorize attaches the current bearer credential (static or live-rotated).
func (c *Caller) authorize(req *http.Request) {
	if tok := c.current(); tok != "" {
		req.Header.Set("authorization", "Bearer "+tok)
	}
}

func classifyError(err error) string {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return "timeout"
	}
	if opErr, ok := err.(*net.OpError); ok {
		if _, isRefused := opErr.Err.(*os.SyscallError); isRefused {
			return "connection"
		}
	}
	return "transport"
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// ---------------------------------------------------------------------------
// State bootstrap (mirrors the accepted acceptance/test composition)
// ---------------------------------------------------------------------------

// Fence mirrors state.Fence wire shape.
type Fence struct {
	RunID  string `json:"run_id"`
	Worker string `json:"worker"`
	Epoch  int64  `json:"epoch"`
}

// Session is a bootstrapped (thread, run, lease) triple plus a heartbeat
// keeper so fences stay valid across a long measurement.
type Session struct {
	Caller   *Caller
	ThreadID string
	Fence    Fence
	worker   string
	stopHB   chan struct{}
	hbOnce   sync.Once
	hbErr    error
	hbMu     sync.Mutex
	hbDone   sync.WaitGroup
}

// Bootstrap creates a thread and a run, claims it, and keeps the lease alive.
func Bootstrap(c *Caller, contextID, worker string) (*Session, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	th, thBody := c.DoJSON(ctx, "/v1/state/CreateThread", "CreateThread", map[string]any{"context_id": contextID, "key": "harness-thread-" + contextID})
	if !th.OK {
		return nil, fmt.Errorf("harness: CreateThread: %+v body=%s", th, string(thBody))
	}
	var thResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(thBody, &thResp); err != nil {
		return nil, fmt.Errorf("harness: CreateThread parse: %w", err)
	}
	runKey := "harness-run-" + contextID
	cr, crBody := c.DoJSON(ctx, "/v1/state/CreateRun", "CreateRun", map[string]any{"thread_id": thResp.ID, "key": runKey, "payload": map[string]any{}})
	if !cr.OK {
		return nil, fmt.Errorf("harness: CreateRun: %+v body=%s", cr, string(crBody))
	}
	// ClaimRun authenticates by workload certificate only.
	claim, claimBody := c.DoJSON(ctx, "/v1/state/ClaimRun", "ClaimRun", map[string]any{"worker": worker, "ttl_ms": 60000})
	if !claim.OK {
		return nil, fmt.Errorf("harness: ClaimRun: %+v body=%s", claim, string(claimBody))
	}
	var claimResp struct {
		Lease struct {
			Run    struct{ ID string } `json:"run"`
			Worker string              `json:"worker"`
			Epoch  int64               `json:"epoch"`
		} `json:"lease"`
	}
	if err := json.Unmarshal(claimBody, &claimResp); err != nil {
		return nil, fmt.Errorf("harness: ClaimRun parse: %w", err)
	}
	s := &Session{
		Caller:   c,
		ThreadID: thResp.ID,
		Fence:    Fence{RunID: claimResp.Lease.Run.ID, Worker: claimResp.Lease.Worker, Epoch: claimResp.Lease.Epoch},
		worker:   worker,
		stopHB:   make(chan struct{}),
	}
	// ClaimRun consumed the bearer on this caller; the fenced operations must
	// present a token for the claimed task. The dev composition binds the
	// task token to the run's task id which equals the token mint task, so the
	// rotating token continues to be valid for fenced ops.
	s.startHeartbeat()
	return s, nil
}

// startHeartbeat keeps the lease fence valid for the whole measurement.
func (s *Session) startHeartbeat() {
	s.hbDone.Add(1)
	go func() {
		defer s.hbDone.Done()
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopHB:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				hb := s.Caller.Do(ctx, "/v1/state/Heartbeat", "Heartbeat", map[string]any{"run_id": s.Fence.RunID, "worker": s.Fence.Worker, "epoch": s.Fence.Epoch, "ttl_ms": 60000})
				cancel()
				if !hb.OK {
					s.hbMu.Lock()
					s.hbErr = fmt.Errorf("heartbeat: %+v", hb)
					s.hbMu.Unlock()
				}
			}
		}
	}()
}

// HeartbeatError reports the last heartbeat failure (fence health).
func (s *Session) HeartbeatError() error {
	s.hbMu.Lock()
	defer s.hbMu.Unlock()
	return s.hbErr
}

// StopHeartbeat ends the lease keeper.
func (s *Session) StopHeartbeat() {
	s.hbOnce.Do(func() { close(s.stopHB) })
	s.hbDone.Wait()
	// The credential refresher exchanges against this same instance, so it must
	// stop with the session -- never left running against a stopped platform.
	s.Caller.StopRotation()
}
