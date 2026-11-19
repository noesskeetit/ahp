package harness

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// TCPProxy is a localhost proxy used to inject dependency loss for ONE
// isolated platform instance. Shared infrastructure is never restarted: the
// fault lives entirely in this harness-owned listener.
type TCPProxy struct {
	ListenAddr string
	Upstream   string

	ln        net.Listener
	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	closed    bool
	stop      chan struct{}
	ConnTotal atomic.Int64
	BytesUp   atomic.Int64
	BytesDown atomic.Int64
	Active    atomic.Int64
}

// StartProxy begins listening and forwarding.
func StartProxy(listenPort int, upstream string) (*TCPProxy, error) {
	p := &TCPProxy{
		ListenAddr: fmt.Sprintf("127.0.0.1:%d", listenPort),
		Upstream:   upstream,
		conns:      map[net.Conn]struct{}{},
		stop:       make(chan struct{}),
	}
	ln, err := net.Listen("tcp", p.ListenAddr)
	if err != nil {
		return nil, err
	}
	p.ln = ln
	go p.acceptLoop()
	return p, nil
}

func (p *TCPProxy) acceptLoop() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			select {
			case <-p.stop:
				return
			default:
				return
			}
		}
		go p.handle(conn)
	}
}

func (p *TCPProxy) handle(client net.Conn) {
	upstream, err := net.DialTimeout("tcp", p.Upstream, 3*time.Second)
	if err != nil {
		client.Close()
		return
	}
	p.ConnTotal.Add(1)
	p.track(client, upstream)
	defer p.untrack(client, upstream)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		n, _ := io.Copy(upstream, client)
		p.BytesUp.Add(n)
		closeWrite(upstream)
	}()
	go func() {
		defer wg.Done()
		n, _ := io.Copy(client, upstream)
		p.BytesDown.Add(n)
		closeWrite(client)
	}()
	wg.Wait()
	client.Close()
	upstream.Close()
}

func (p *TCPProxy) track(a, b net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns[a] = struct{}{}
	p.conns[b] = struct{}{}
	p.Active.Add(2)
}

func (p *TCPProxy) untrack(cs ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range cs {
		if _, ok := p.conns[c]; ok {
			delete(p.conns, c)
			p.Active.Add(-1)
		}
	}
}

// Cut simulates dependency loss: the listener stops accepting AND every
// in-flight connection is severed. The upstream itself is untouched.
func (p *TCPProxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	_ = p.ln.Close()
	for c := range p.conns {
		_ = c.Close()
		delete(p.conns, c)
	}
	p.Active.Store(0)
}

// Restore brings the dependency path back on the SAME port without restarting
// the platform: the client pools must reconnect by themselves.
func (p *TCPProxy) Restore() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		return nil
	}
	ln, err := net.Listen("tcp", p.ListenAddr)
	if err != nil {
		return err
	}
	p.ln = ln
	p.closed = false
	go p.acceptLoop()
	return nil
}

// Stop terminates the proxy.
func (p *TCPProxy) Stop() {
	p.Cut()
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
}

func closeWrite(c net.Conn) {
	if tw, ok := c.(interface{ CloseWrite() error }); ok {
		_ = tw.CloseWrite()
	}
}
