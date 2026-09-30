//go:build linux

package sfu

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

const reusePortSupported = true

func setReusePort(_, _ string, c syscall.RawConn) error {
	var opErr error
	if err := c.Control(func(fd uintptr) {
		opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	}); err != nil {
		return err
	}

	return opErr
}

// socketBuffers is the receive and send buffer a socket was actually given,
// in bytes of payload. Linux reports twice what it grants, the second half
// being its own bookkeeping, and grants at most net.core.rmem_max and
// wmem_max whatever was asked for.
func socketBuffers(conn *net.UDPConn) (read, write int, ok bool) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, 0, false
	}

	var readErr, writeErr error
	if err := raw.Control(func(fd uintptr) {
		read, readErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF)
		write, writeErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF)
	}); err != nil || readErr != nil || writeErr != nil {
		return 0, 0, false
	}

	return read / 2, write / 2, true
}
