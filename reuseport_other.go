//go:build !linux

package sfu

import "syscall"

// Elsewhere every address gets the one socket it always had.
const reusePortSupported = false

func setReusePort(_, _ string, _ syscall.RawConn) error {
	return nil
}
