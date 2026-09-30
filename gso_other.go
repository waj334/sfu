//go:build !linux

package sfu

import "net"

// Elsewhere every packet is sent on its own; see gso_linux.go.
var gsoOOBSize = 0

func gsoSupported(*net.UDPConn) bool { return false }

func putGSOSize(oob []byte, _ int) []byte { return oob[:0] }

func isGSORefusal(error) bool { return false }
