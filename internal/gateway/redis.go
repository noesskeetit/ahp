package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// RedisNotifier is a best-effort pub/sub Notifier backed by a real Redis
// server. It implements the minimal RESP needed for AUTH, SELECT, PUBLISH and
// SUBSCRIBE with the standard library only, so the execution plane gains no
// third-party dependency and the shared go.mod stays untouched.
//
// Correctness NEVER depends on these signals: the PostgreSQL event journal is
// authoritative, and every Wait has a bounded timeout after which consumers
// fall back to polling. A dropped or missed wake-up only adds latency.
type RedisNotifier struct {
	addr string
	db   int
	user string
	pass string
}

// NewRedisNotifier parses a redis://[user:pass@]host:port[/db] URL, or a bare
// host:port. The rediss:// (TLS) scheme is rejected: the execution plane keeps
// wake-up signals on a private plaintext network and TLS termination belongs
// to the platform boundary, not here.
func NewRedisNotifier(rawURL string) (*RedisNotifier, error) {
	raw := strings.TrimSpace(rawURL)
	if raw == "" {
		return nil, errors.New("gateway: empty redis url")
	}
	n := &RedisNotifier{}
	lower := strings.ToLower(raw)
	switch {
	case strings.HasPrefix(lower, "rediss://"):
		return nil, errors.New("gateway: redis TLS scheme (rediss) is not supported by the notifier")
	case strings.HasPrefix(lower, "redis://"):
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("gateway: parse redis url: %w", err)
		}
		n.addr = u.Host
		if u.User != nil {
			n.user = u.User.Username()
			n.pass, _ = u.User.Password()
		}
		if p := strings.Trim(u.Path, "/"); p != "" {
			db, err := strconv.Atoi(p)
			if err != nil || db < 0 {
				return nil, fmt.Errorf("gateway: invalid redis db %q", p)
			}
			n.db = db
		}
	default:
		n.addr = raw
	}
	if _, port, err := net.SplitHostPort(n.addr); err != nil || port == "" {
		n.addr = net.JoinHostPort(n.addr, "6379")
	}
	return n, nil
}

// RedisNotifierFromEnv builds a notifier from a redis URL taken from the
// runtime environment. It returns (nil, nil) when the variable is unset so the
// caller can fall back to NullNotifier.
func RedisNotifierFromEnv() (*RedisNotifier, error) {
	n, err := NewRedisNotifier(os.Getenv("REDIS_URL"))
	if err != nil {
		return nil, err
	}
	// The URL carries no credentials by convention in this deployment; the
	// password is injected as a scoped runtime secret, matching the common
	// sidecar pattern where the host is known but the password is secret.
	if n != nil && n.pass == "" {
		for _, k := range []string{"REDIS_PASSWORD", "AHP_REDIS_PASSWORD"} {
			if pw := os.Getenv(k); pw != "" {
				n.pass = pw
				break
			}
		}
	}
	return n, nil
}

// maxBulk bounds a single RESP bulk string. Wake-up payloads are tiny; this is
// a defensive cap, not a feature.
const maxBulk = 1 << 16

func (n *RedisNotifier) dial(ctx context.Context) (net.Conn, *bufio.Reader, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", n.addr)
	if err != nil {
		return nil, nil, fmt.Errorf("gateway: redis dial %s: %w", n.addr, err)
	}
	br := bufio.NewReader(conn)
	if err := n.handshake(ctx, conn, br); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, br, nil
}

func (n *RedisNotifier) handshake(ctx context.Context, conn net.Conn, br *bufio.Reader) error {
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	}
	if n.pass != "" {
		if n.user != "" {
			if err := writeCmd(conn, "AUTH", n.user, n.pass); err != nil {
				return err
			}
		} else {
			if err := writeCmd(conn, "AUTH", n.pass); err != nil {
				return err
			}
		}
		if _, err := readReply(br); err != nil {
			return fmt.Errorf("gateway: redis AUTH: %w", err)
		}
	}
	if n.db != 0 {
		if err := writeCmd(conn, "SELECT", strconv.Itoa(n.db)); err != nil {
			return err
		}
		if _, err := readReply(br); err != nil {
			return fmt.Errorf("gateway: redis SELECT: %w", err)
		}
	}
	return nil
}

// Notify publishes a one-shot wake-up. The payload is opaque; consumers only
// care that *something* arrived, so a constant is sufficient and avoids
// echoing channel or task metadata onto the wire.
func (n *RedisNotifier) Notify(ctx context.Context, channel string) error {
	conn, br, err := n.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := writeCmd(conn, "PUBLISH", channel, "1"); err != nil {
		return err
	}
	if _, err := readReply(br); err != nil {
		return fmt.Errorf("gateway: redis PUBLISH: %w", err)
	}
	return nil
}

// Wait subscribes to channel and blocks until the first message arrives, the
// context is cancelled, or timeout elapses. It is best-effort by contract: a
// missed signal is indistinguishable from a timeout and always ends in a
// bounded, non-erroring return.
func (n *RedisNotifier) Wait(ctx context.Context, channel string, timeout time.Duration) {
	if timeout <= 0 {
		timeout = time.Second
	}
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, br, err := n.dial(dctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if dl, ok := dctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if err := writeCmd(conn, "SUBSCRIBE", channel); err != nil {
		return
	}
	// First reply confirms the subscription; later replies carry messages.
	confirmed := false
	for {
		v, err := readReply(br)
		if err != nil {
			return
		}
		arr, ok := v.([]interface{})
		if !ok || len(arr) < 1 {
			continue
		}
		kind, _ := arr[0].(string)
		switch kind {
		case "subscribe":
			confirmed = true
		case "message", "pmessage":
			return
		default:
			if confirmed && kind == "" {
				return
			}
		}
	}
}

// writeCmd encodes a Redis command as a RESP array of bulk strings.
func writeCmd(w io.Writer, args ...string) error {
	var b strings.Builder
	b.WriteString("*")
	b.WriteString(strconv.Itoa(len(args)))
	b.WriteString("\r\n")
	for _, a := range args {
		b.WriteString("$")
		b.WriteString(strconv.Itoa(len(a)))
		b.WriteString("\r\n")
		b.WriteString(a)
		b.WriteString("\r\n")
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// readReply parses one RESP value. It supports the subset the notifier emits
// or receives: simple strings, errors, integers, bulk strings and arrays.
func readReply(r *bufio.Reader) (interface{}, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if line == "" {
		return nil, errors.New("gateway: empty RESP line")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, fmt.Errorf("gateway: redis error: %s", line[1:])
	case ':':
		return strconv.ParseInt(line[1:], 10, 64)
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("gateway: bad bulk length: %w", err)
		}
		if n < 0 {
			return nil, nil // null bulk
		}
		if n > maxBulk {
			return nil, fmt.Errorf("gateway: bulk too large: %d", n)
		}
		buf := make([]byte, n+2) // payload + CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("gateway: bad array length: %w", err)
		}
		if n < 0 {
			return nil, nil
		}
		if n > 16 {
			return nil, fmt.Errorf("gateway: array too large: %d", n)
		}
		out := make([]interface{}, n)
		for i := 0; i < n; i++ {
			out[i], err = readReply(r)
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("gateway: unexpected RESP type %q", line[0])
	}
}

// readLine reads a single CRLF-terminated RESP line, stripping the terminator.
func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}
