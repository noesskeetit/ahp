package compute

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The tests in this file pin the AUTH behaviour of the compute RESP client
// against a Redis started with requirepass. Two product requirements meet
// here: a client that never authenticates is simply incompatible with an
// authenticated Redis (every command fails NOAUTH), and the credential is a
// runtime-injected secret — it comes from the environment, never from code,
// config files, or an address string with invented query parameters.

// ---------------------------------------------------------------------------
// fake RESP server: observes the exact command sequence on a fresh connection
// ---------------------------------------------------------------------------

type fakeRedis struct {
	t       *testing.T
	addr    string
	require string // requirepass the fake enforces; "" means the server has no password

	// allowAuth mirrors a server started WITHOUT requirepass, which answers
	// AUTH with "ERR Client sent AUTH, but no password is set".
	allowAuth bool

	mu       sync.Mutex
	commands [][]string
}

func newFakeRedis(t *testing.T, require string, allowAuth bool) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeRedis{t: t, addr: ln.Addr().String(), require: require, allowAuth: allowAuth}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serveConn(nc)
		}
	}()
	return f
}

func (f *fakeRedis) observed() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.commands))
	copy(out, f.commands)
	return out
}

func (f *fakeRedis) saw(name string) bool {
	for _, c := range f.observed() {
		if len(c) > 0 && strings.EqualFold(c[0], name) {
			return true
		}
	}
	return false
}

func (f *fakeRedis) serveConn(nc net.Conn) {
	f.t.Cleanup(func() { _ = nc.Close() })
	r := bufio.NewReader(nc)
	authenticated := f.require == ""
	for {
		cmd, err := readFakeCommand(r)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.commands = append(f.commands, cmd)
		f.mu.Unlock()
		_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := nc.Write([]byte(f.reply(cmd, &authenticated))); err != nil {
			return
		}
	}
}

// reply answers one command the way a stock Redis does, so a client that skips
// AUTH gets the same NOAUTH a real requirepass server returns.
func (f *fakeRedis) reply(cmd []string, authenticated *bool) string {
	name := strings.ToUpper(cmd[0])
	if name == "AUTH" {
		if !f.allowAuth {
			return "-ERR Client sent AUTH, but no password is set\r\n"
		}
		got := ""
		if len(cmd) >= 2 {
			got = cmd[len(cmd)-1]
		}
		if got != f.require {
			return "-WRONGPASS invalid username-password pair\r\n"
		}
		*authenticated = true
		return "+OK\r\n"
	}
	if !*authenticated {
		return "-NOAUTH Authentication required.\r\n"
	}
	switch name {
	case "PING":
		return "+PONG\r\n"
	case "SET":
		return "+OK\r\n"
	case "GET":
		return "$-1\r\n"
	case "DEL", "PEXPIRE":
		return ":1\r\n"
	default:
		return "-ERR unknown command\r\n"
	}
}

// readFakeCommand parses one RESP array of bulk strings (the client's only
// request shape).
func readFakeCommand(r *bufio.Reader) ([]string, error) {
	line, err := readFakeLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("fake redis: want array, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := readFakeLine(r)
		if err != nil {
			return nil, err
		}
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, fmt.Errorf("fake redis: want bulk header, got %q", hdr)
		}
		size, err := strconv.Atoi(hdr[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

func readFakeLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}

// ---------------------------------------------------------------------------
// unit evidence: AUTH when a credential is configured, none when it is not
// ---------------------------------------------------------------------------

func clearRedisPasswordEnv(t *testing.T) {
	t.Helper()
	for _, k := range RedisPasswordEnvVars {
		t.Setenv(k, "")
	}
}

func TestRedisAuthenticatesWhenPasswordConfigured(t *testing.T) {
	const pw = "h12-fake-requirepass"
	f := newFakeRedis(t, pw, true)
	clearRedisPasswordEnv(t)

	r := NewRedisAuth(f.addr, pw)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Ping(ctx); err != nil {
		t.Fatalf("Ping against requirepass redis: %v", err)
	}
	// A lease-shaped round trip must work on the authenticated connection too.
	key := RedisKeyPrefix + "h12-auth-lease"
	ok, err := r.SetNX(ctx, key, "holder-a", 5000)
	if err != nil || !ok {
		t.Fatalf("SetNX after auth: ok=%v err=%v", ok, err)
	}

	got := f.observed()
	if len(got) < 2 {
		t.Fatalf("fake redis saw %d commands, want AUTH followed by PING", len(got))
	}
	if !strings.EqualFold(got[0][0], "AUTH") {
		t.Fatalf("first command on a fresh connection = %q, want AUTH (a requirepass server rejects anything else with NOAUTH)", got[0][0])
	}
	if len(got[0]) != 2 || got[0][1] != pw {
		t.Fatalf("AUTH args = %q, want the configured credential", got[0])
	}
	if !strings.EqualFold(got[1][0], "PING") {
		t.Fatalf("second command = %q, want PING (AUTH must precede every other command)", got[1][0])
	}
}

// TestRedisPasswordFromEnvIsRuntimeInjected proves the credential is read from
// the environment (the runtime-injection point), including the REDIS_PASSWORD
// alias the gateway plane already honours — never from a URL/query parameter.
func TestRedisPasswordFromEnvIsRuntimeInjected(t *testing.T) {
	const pw = "h12-env-requirepass"
	for _, tc := range []struct{ name, key string }{
		{"AHP_REDIS_PASSWORD", "AHP_REDIS_PASSWORD"},
		{"REDIS_PASSWORD alias", "REDIS_PASSWORD"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRedis(t, pw, true)
			clearRedisPasswordEnv(t)
			t.Setenv(tc.key, pw)

			r := NewRedis(f.addr)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := r.Ping(ctx); err != nil {
				t.Fatalf("Ping: %v", err)
			}
			if !f.saw("AUTH") {
				t.Fatalf("fake redis observed %v, want an AUTH built from %s", f.observed(), tc.key)
			}
		})
	}
}

// TestRedisUnchangedWithoutPassword pins the no-regression half: with no
// credential injected the client sends no AUTH at all, exactly as before, so a
// password-less Redis keeps working.
func TestRedisUnchangedWithoutPassword(t *testing.T) {
	f := newFakeRedis(t, "", false)
	clearRedisPasswordEnv(t)
	if got := RedisPasswordFromEnv(); got != "" {
		t.Fatalf("RedisPasswordFromEnv() = %q, want empty", got)
	}

	r := NewRedis(f.addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Ping(ctx); err != nil {
		t.Fatalf("Ping without a credential: %v", err)
	}
	if err := r.SetPX(ctx, RedisKeyPrefix+"h12-noauth", "v", 5000); err != nil {
		t.Fatalf("SetPX without a credential: %v", err)
	}
	if f.saw("AUTH") {
		t.Fatalf("client sent AUTH with no credential configured: %v", f.observed())
	}
	for _, c := range f.observed() {
		if len(c) == 0 {
			t.Fatal("fake redis recorded an empty command")
		}
	}
	if !f.saw("PING") || !f.saw("SET") {
		t.Fatalf("fake redis observed %v, want the plain PING/SET traffic", f.observed())
	}
}

// TestRedisAuthFailureIsUnavailableAndDoesNotLeakSecret covers the failure
// surface: a wrong credential is an UNAVAILABLE redis, and the credential never
// appears in the error text (logs are not a secret store).
func TestRedisAuthFailureIsUnavailableAndDoesNotLeakSecret(t *testing.T) {
	const pw = "h12-correct-credential"
	f := newFakeRedis(t, pw, true)
	clearRedisPasswordEnv(t)

	r := NewRedisAuth(f.addr, "h12-wrong-credential")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := r.Ping(ctx)
	if err == nil {
		t.Fatal("Ping with a wrong credential must fail")
	}
	if !IsUnavailable(err) {
		t.Fatalf("want an UNAVAILABLE surface, got %T: %v", err, err)
	}
	if strings.Contains(err.Error(), "h12-wrong-credential") {
		t.Fatalf("error text leaks the credential: %v", err)
	}
	if !strings.Contains(err.Error(), "AUTH") {
		t.Fatalf("error should name the failing step: %v", err)
	}
}

// ---------------------------------------------------------------------------
// live evidence: a real Redis started with --requirepass
// ---------------------------------------------------------------------------

// liveAuthRedis returns the address and credential of a throwaway Redis that
// really runs with requirepass, or skips with the command that reproduces it:
//
//	docker run -d --name go-h12-redis -p 127.0.0.1:16380:6379 redis:7 \
//	  redis-server --requirepass <password>
//
// AHP_TEST_REDIS_AUTH_ADDR / AHP_TEST_REDIS_AUTH_PASSWORD point at it. A green
// unit run above does NOT substitute for this: only a real server proves the
// wire behaviour.
func liveAuthRedis(t *testing.T) (string, string) {
	t.Helper()
	addr := os.Getenv("AHP_TEST_REDIS_AUTH_ADDR")
	pw := os.Getenv("AHP_TEST_REDIS_AUTH_PASSWORD")
	if addr == "" || pw == "" {
		t.Skip("live requirepass redis not configured: set AHP_TEST_REDIS_AUTH_ADDR and AHP_TEST_REDIS_AUTH_PASSWORD")
	}
	return addr, pw
}

func TestRedisLiveAgainstRequirepassServer(t *testing.T) {
	addr, pw := liveAuthRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// (a) The pre-fix behaviour, measured: an client that does not authenticate
	// is rejected by a requirepass server. This is the gap the fix closes.
	bare := NewRedisAuth(addr, "")
	err := bare.Ping(ctx)
	if err == nil {
		t.Fatalf("redis at %s accepted an unauthenticated PING: it does not enforce requirepass, live evidence invalid", addr)
	}
	if !strings.Contains(err.Error(), "NOAUTH") {
		t.Fatalf("want the NOAUTH rejection the pre-fix client hit, got %v", err)
	}

	// (b) The product path: credential from the environment, address unchanged.
	t.Setenv("AHP_REDIS_PASSWORD", pw)
	r := NewRedis(addr)
	if err := r.Ping(ctx); err != nil {
		t.Fatalf("Ping with the injected credential: %v", err)
	}
	key := RedisKeyPrefix + "h12-live-auth-" + leaseID()
	defer func() { _ = r.Del(context.Background(), key) }()
	if ok, err := r.SetNX(ctx, key, "holder-a", 5000); err != nil || !ok {
		t.Fatalf("SetNX on the authenticated connection: ok=%v err=%v", ok, err)
	}
	if ok, err := r.SetNX(ctx, key, "holder-b", 5000); err != nil || ok {
		t.Fatalf("second SetNX must lose the lease: ok=%v err=%v", ok, err)
	}
	if v, err := r.Get(ctx, key); err != nil || v != "holder-a" {
		t.Fatalf("Get = %q (err %v), want holder-a", v, err)
	}
	if err := r.PExpire(ctx, key, 5000); err != nil {
		t.Fatalf("PExpire: %v", err)
	}
	if err := r.SAdd(ctx, key+":holders", "holder-a"); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	members, err := r.SMembers(ctx, key+":holders")
	if err != nil || len(members) != 1 || members[0] != "holder-a" {
		t.Fatalf("SMembers = %v (err %v), want [holder-a]", members, err)
	}
	if err := r.Del(ctx, key, key+":holders"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if v, err := r.Get(ctx, key); err != nil || v != "" {
		t.Fatalf("Get after Del = %q (err %v), want empty", v, err)
	}
}

// TestRedisLiveEnvInjectionMatchesExplicitAuth checks the two ways of supplying
// the credential agree, so no caller can accidentally talk to a different
// server than the lease layer it shares the process with.
func TestRedisLiveEnvInjectionMatchesExplicitAuth(t *testing.T) {
	addr, pw := liveAuthRedis(t)
	t.Setenv("AHP_REDIS_PASSWORD", pw)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	env := NewRedis(addr)
	explicit := NewRedisAuth(addr, pw)
	for name, r := range map[string]*Redis{"env": env, "explicit": explicit} {
		if err := r.Ping(ctx); err != nil {
			t.Fatalf("%s client: Ping: %v", name, err)
		}
	}
}
