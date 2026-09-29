//go:build linux

package sfu

import (
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
