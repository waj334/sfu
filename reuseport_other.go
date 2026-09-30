//go:build !linux

package sfu

import (
	"net"
	"syscall"
)

// Elsewhere every address gets the one socket it always had.
const reusePortSupported = false

func setReusePort(_, _ string, _ syscall.RawConn) error {
	return nil
}

// socketBuffers is not read back here; nothing is warned about.
func socketBuffers(_ *net.UDPConn) (read, write int, ok bool) {
	return 0, 0, false
}
