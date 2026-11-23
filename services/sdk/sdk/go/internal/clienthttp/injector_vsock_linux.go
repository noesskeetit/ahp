//go:build linux

package clienthttp

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"golang.org/x/sys/unix"
)

// NewVSockInjector connects to the mount's host CID over virtio-vsock.
func NewVSockInjector(port uint32) (*Client, error) {
	if port == 0 || port >= 1<<30 {
		return nil, contracts.NewError(contracts.ErrInvalid, "invalid guest-to-host injector port")
	}
	return NewLocal(func(ctx context.Context) (net.Conn, error) {
		return dialInjectorVSock(ctx, port)
	}), nil
}

func dialInjectorVSock(ctx context.Context, port uint32) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	connected := false
	defer func() {
		if !connected {
			_ = unix.Close(fd)
		}
	}()
	err = unix.Connect(fd, &unix.SockaddrVM{CID: unix.VMADDR_CID_HOST, Port: port})
	if errors.Is(err, unix.EINPROGRESS) || errors.Is(err, unix.EALREADY) || errors.Is(err, unix.EINTR) {
		err = waitVSockConnected(ctx, fd)
	}
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "ahp-vsock")
	if file == nil {
		return nil, errors.New("cannot wrap the injector socket")
	}
	connected = true
	return &vsockConn{File: file, remote: vsockAddr("2:" + strconv.FormatUint(uint64(port), 10))}, nil
}

func waitVSockConnected(ctx context.Context, fd int) error {
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}
	for ctx.Err() == nil {
		n, err := unix.Poll(fds, 50)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if n == 0 {
			continue
		}
		sockErr, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ERROR)
		if err != nil {
			return err
		}
		if sockErr != 0 {
			return unix.Errno(sockErr)
		}
		return nil
	}
	return ctx.Err()
}

type vsockAddr string

func (a vsockAddr) Network() string { return "vsock" }
func (a vsockAddr) String() string  { return string(a) }

// The nonblocking descriptor is owned by os.File so Go can interrupt reads
// and writes using the deadlines required by the HTTP transport.
type vsockConn struct {
	*os.File
	remote vsockAddr
}

func (c *vsockConn) LocalAddr() net.Addr  { return vsockAddr("guest") }
func (c *vsockConn) RemoteAddr() net.Addr { return c.remote }
