//go:build linux

package sfu

import (
	"encoding/binary"
	"errors"
	"net"
	"unsafe"

	"golang.org/x/sys/unix"
)

// gsoOOBSize is the control message that carries a send's segment size.
var gsoOOBSize = unix.CmsgSpace(2)

// gsoSupported says whether the kernel takes UDP_SEGMENT on this socket:
// Linux 4.18 and later. Asked of the socket rather than assumed from the
// kernel version, as it answers for whatever it is running under.
func gsoSupported(conn *net.UDPConn) bool {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false
	}

	var optErr error
	if err := raw.Control(func(fd uintptr) {
		_, optErr = unix.GetsockoptInt(int(fd), unix.SOL_UDP, unix.UDP_SEGMENT)
	}); err != nil {
		return false
	}

	return optErr == nil
}

// putGSOSize writes the control message asking the kernel to cut a send into
// segments of size bytes, into oob, which has room for gsoOOBSize.
func putGSOSize(oob []byte, size int) []byte {
	oob = oob[:gsoOOBSize]
	for i := range oob {
		oob[i] = 0
	}

	h := (*unix.Cmsghdr)(unsafe.Pointer(&oob[0]))
	h.Level = unix.SOL_UDP
	h.Type = unix.UDP_SEGMENT
	h.SetLen(unix.CmsgLen(2))
	binary.NativeEndian.PutUint16(oob[unix.CmsgLen(0):], uint16(size))

	return oob
}

// isGSORefusal is an error a send carrying UDP_SEGMENT gets where segmentation
// is not available after all: no checksum offload on the route (EIO), or a
// socket or kernel that does not take it (EINVAL, EOPNOTSUPP, ENOPROTOOPT).
func isGSORefusal(err error) bool {
	return errors.Is(err, unix.EIO) ||
		errors.Is(err, unix.EINVAL) ||
		errors.Is(err, unix.EOPNOTSUPP) ||
		errors.Is(err, unix.ENOPROTOOPT)
}
