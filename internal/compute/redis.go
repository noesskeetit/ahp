package compute

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Redis is a minimal, dependency-free RESP2 client for the Compute plane's
// ephemeral coordination keys (writer leases, activity/sleep leases). It is
// intentionally small: the operations the lease layer needs are SET with PX
// and NX, GET, DEL, PEXPIRE and the SET container operations used to index
// per-Computer lease holders.
//
// All Compute lease keys carry the lane prefix RedisKeyPrefix so this lane's
// keys can never collide with another lane's keys in a shared Redis.
const RedisKeyPrefix = "vm2cmp:"

// RedisDown is the UNAVAILABLE surface for a Redis that cannot be reached.
func RedisDown(cause string) error {
	return Unavailable("redis", cause)
}

type redisConn struct {
	addr   string
	nc     net.Conn
	reader *bufio.Reader
}

func newRedisConn(addr string, timeout time.Duration) (*redisConn, error) {
	nc, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &redisConn{addr: addr, nc: nc, reader: bufio.NewReader(nc)}, nil
}

func (c *redisConn) close() error { return c.nc.Close() }

// do writes one RESP command and reads exactly one reply.
func (c *redisConn) do(ctx context.Context, args ...string) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var sb strings.Builder
	sb.WriteByte('*')
	sb.WriteString(strconv.Itoa(len(args)))
	sb.WriteString("\r\n")
	for _, a := range args {
		sb.WriteByte('$')
		sb.WriteString(strconv.Itoa(len(a)))
		sb.WriteString("\r\n")
		sb.WriteString(a)
		sb.WriteString("\r\n")
	}
	deadline := time.Now().Add(5 * time.Second)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	if err := c.nc.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if _, err := c.nc.Write([]byte(sb.String())); err != nil {
		return nil, err
	}
	return readReply(c.reader)
}

// readReply parses one RESP value (simple string, error, integer, bulk, array).
func readReply(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if line == "" {
		return nil, errors.New("compute: redis: empty reply line")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, errors.New("compute: redis: " + line[1:])
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("compute: redis: bad integer %q", line[1:])
		}
		return n, nil
	case '$':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("compute: redis: bad bulk length %q", line[1:])
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("compute: redis: bad array length %q", line[1:])
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, 0, n)
		for i := int64(0); i < n; i++ {
			v, err := readReply(r)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("compute: redis: unknown reply type %q", line[0])
	}
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// Redis holds a pool of pooled connections to one server.
type Redis struct {
	addr string
	// password is the requirepass credential replayed as AUTH on every fresh
	// connection. It is never part of addr and never reaches an error message.
	password string
	pool     sync.Pool
}

// RedisPasswordEnvVars are the runtime-injected names this plane reads the
// Redis credential from, most specific first. The credential is a runtime
// secret (PDF: secrets are runtime-injected, never baked into code or config
// files), so the address stays a plain host:port and the password arrives
// only through the process environment.
var RedisPasswordEnvVars = []string{"AHP_REDIS_PASSWORD", "REDIS_PASSWORD"}

// RedisPasswordFromEnv returns the Redis credential from the environment, or
// "" when none is injected (a server configured without requirepass).
func RedisPasswordFromEnv() string {
	for _, k := range RedisPasswordEnvVars {
		if pw := strings.TrimSpace(os.Getenv(k)); pw != "" {
			return pw
		}
	}
	return ""
}

// NewRedis dials lazily; the first command validates connectivity. A Redis
// started with requirepass rejects every command with NOAUTH until the client
// authenticates, so the credential is resolved from the environment here and
// replayed by dial. With no credential injected the client behaves exactly as
// before: no AUTH is ever sent.
func NewRedis(addr string) *Redis {
	return NewRedisAuth(addr, RedisPasswordFromEnv())
}

// NewRedisAuth is NewRedis with an explicit credential, for callers that
// resolved the secret themselves and for tests. An empty password means
// "authenticate with nothing", matching an unauthenticated server.
func NewRedisAuth(addr, password string) *Redis {
	if addr == "" {
		addr = "127.0.0.1:16379"
	}
	return &Redis{addr: addr, password: password, pool: sync.Pool{New: func() any { return nil }}}
}

// withConn runs fn with a pooled connection, reconnecting once on transport
// failure. A bounded retry (two attempts) covers the transient race where a
// pooled connection was reaped by the server.
func (r *Redis) withConn(ctx context.Context, fn func(c *redisConn) (any, error)) (any, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		c, closed, err := r.acquire()
		if err != nil {
			return nil, RedisDown(err.Error())
		}
		res, err := fn(c)
		if err != nil && isNetworkError(err) && !closed {
			_ = c.close()
			lastErr = err
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			continue
		}
		if closed {
			_ = c.close()
		} else {
			r.pool.Put(c)
		}
		return res, err
	}
	return nil, lastErr
}

func (r *Redis) acquire() (*redisConn, bool, error) {
	v := r.pool.Get()
	if v != nil {
		c := v.(*redisConn)
		// Probe the pooled connection with PING; a dead socket is replaced.
		if _, err := c.do(context.Background(), "PING"); err != nil {
			_ = c.close()
			return r.dial()
		}
		return c, false, nil
	}
	return r.dial()
}

func (r *Redis) dial() (*redisConn, bool, error) {
	c, err := newRedisConn(r.addr, 3*time.Second)
	if err != nil {
		return nil, false, err
	}
	if r.password != "" {
		// AUTH must precede every other command: a requirepass server fails
		// anything else with NOAUTH. The reply carries no credential and the
		// error text is the server's, so the password cannot leak through it.
		if _, err := c.do(context.Background(), "AUTH", r.password); err != nil {
			_ = c.close()
			return nil, false, fmt.Errorf("AUTH: %w", err)
		}
	}
	if _, err := c.do(context.Background(), "PING"); err != nil {
		_ = c.close()
		return nil, false, err
	}
	return c, true, nil
}

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "connection reset") || strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "EOF") || strings.Contains(s, "i/o timeout")
}

// ---------------------------------------------------------------------------
// typed commands
// ---------------------------------------------------------------------------

// PING validates connectivity.
func (r *Redis) Ping(ctx context.Context) error {
	v, err := r.withConn(ctx, func(c *redisConn) (any, error) { return c.do(ctx, "PING") })
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return RedisDown(err.Error())
	}
	if v != "PONG" {
		return RedisDown("unexpected PING reply")
	}
	return nil
}

// SetNX sets key to value with a millisecond TTL only when the key is absent.
// It reports whether the write happened.
func (r *Redis) SetNX(ctx context.Context, key, value string, ttlMS int64) (bool, error) {
	v, err := r.withConn(ctx, func(c *redisConn) (any, error) {
		return c.do(ctx, "SET", key, value, "PX", strconv.FormatInt(ttlMS, 10), "NX")
	})
	if err != nil {
		if ctxErr(ctx, err) {
			return false, err
		}
		return false, RedisDown(err.Error())
	}
	return v == "OK", nil
}

// SetPX sets key to value unconditionally with a millisecond TTL.
func (r *Redis) SetPX(ctx context.Context, key, value string, ttlMS int64) error {
	_, err := r.withConn(ctx, func(c *redisConn) (any, error) {
		return c.do(ctx, "SET", key, value, "PX", strconv.FormatInt(ttlMS, 10))
	})
	if err != nil {
		if ctxErr(ctx, err) {
			return err
		}
		return RedisDown(err.Error())
	}
	return nil
}

// Get returns the key's value, or "" when absent.
func (r *Redis) Get(ctx context.Context, key string) (string, error) {
	v, err := r.withConn(ctx, func(c *redisConn) (any, error) { return c.do(ctx, "GET", key) })
	if err != nil {
		if ctxErr(ctx, err) {
			return "", err
		}
		return "", RedisDown(err.Error())
	}
	if v == nil {
		return "", nil
	}
	return v.(string), nil
}

// Del removes the keys.
func (r *Redis) Del(ctx context.Context, keys ...string) error {
	args := append([]string{"DEL"}, keys...)
	_, err := r.withConn(ctx, func(c *redisConn) (any, error) { return c.do(ctx, args...) })
	if err != nil {
		if ctxErr(ctx, err) {
			return err
		}
		return RedisDown(err.Error())
	}
	return nil
}

// PExpire refreshes the key's TTL.
func (r *Redis) PExpire(ctx context.Context, key string, ttlMS int64) error {
	_, err := r.withConn(ctx, func(c *redisConn) (any, error) {
		return c.do(ctx, "PEXPIRE", key, strconv.FormatInt(ttlMS, 10))
	})
	if err != nil {
		if ctxErr(ctx, err) {
			return err
		}
		return RedisDown(err.Error())
	}
	return nil
}

// SAdd adds members to a set.
func (r *Redis) SAdd(ctx context.Context, key string, members ...string) error {
	args := append([]string{"SADD", key}, members...)
	_, err := r.withConn(ctx, func(c *redisConn) (any, error) { return c.do(ctx, args...) })
	if err != nil {
		if ctxErr(ctx, err) {
			return err
		}
		return RedisDown(err.Error())
	}
	return nil
}

// SRem removes members from a set.
func (r *Redis) SRem(ctx context.Context, key string, members ...string) error {
	args := append([]string{"SREM", key}, members...)
	_, err := r.withConn(ctx, func(c *redisConn) (any, error) { return c.do(ctx, args...) })
	if err != nil {
		if ctxErr(ctx, err) {
			return err
		}
		return RedisDown(err.Error())
	}
	return nil
}

// SMembers lists a set's members.
func (r *Redis) SMembers(ctx context.Context, key string) ([]string, error) {
	v, err := r.withConn(ctx, func(c *redisConn) (any, error) { return c.do(ctx, "SMEMBERS", key) })
	if err != nil {
		if ctxErr(ctx, err) {
			return nil, err
		}
		return nil, RedisDown(err.Error())
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, nil
	}
	out := make([]string, 0, len(arr))
	for _, m := range arr {
		if s, ok := m.(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

// ctxErr reports whether err is simply the context finishing (cancel or
// deadline); callers pass those through untouched.
func ctxErr(ctx context.Context, err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// leaseID returns a fresh short identifier for a lease record.
func leaseID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
