package sfu

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sort"
	"strings"

	"github.com/pion/ice/v4"
	"github.com/pion/logging"
)

// udpMuxLog is where the mux says what it could not arrange: buffers the
// kernel capped (warnShortBuffers), segmentation it refused (sendSegmented).
//
// At warning level by default. The default factory's is error, and a warning
// logged through it goes nowhere unless PION_LOG_WARN names the scope, which is
// how these were first written and never once seen. PION_LOG_* still override.
var udpMuxLog = func() logging.LeveledLogger {
	factory := logging.NewDefaultLoggerFactory()
	factory.DefaultLogLevel = logging.LogLevelWarn
	return factory.NewLogger("sfu")
}()

type UDPMux struct {
	Port    int
	mux     *ice.MultiUDPMuxDefault
	context context.Context
	cancel  context.CancelFunc
}

// udpMuxBufferSize is each socket's kernel buffer, both ways.
const udpMuxBufferSize = 67_108_864

// maxUDPMuxShards bounds the sockets per address. Each adds a reading goroutine
// and a kernel buffer of udpMuxBufferSize; past about this many, sendto itself
// rather than the lock in front of it is what is being waited on.
const maxUDPMuxShards = 8

// NewUDPMux is ice.NewMultiUDPMuxFromPort with each address served by several
// sockets rather than one. See shardedPacketConn.
func NewUDPMux(ctx context.Context, port int) *UDPMux {
	localCtx, cancel := context.WithCancel(ctx)

	shards := min(runtime.GOMAXPROCS(0), maxUDPMuxShards)

	addrs, err := udpMuxAddresses()
	if err != nil {
		panic(err)
	}

	muxes := make([]ice.UDPMux, 0, len(addrs))
	for _, addr := range addrs {
		conn, err := listenSharded(&net.UDPAddr{IP: addr, Port: port}, shards, udpMuxBufferSize, udpMuxBufferSize)
		if err != nil {
			for _, m := range muxes {
				_ = m.Close()
			}
			panic(err)
		}

		muxes = append(muxes, ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn: conn}))
	}

	mux := ice.NewMultiUDPMuxDefault(muxes...)

	go func() {
		defer mux.Close()
		<-localCtx.Done()
		cancel()

	}()

	return &UDPMux{
		Port:    port,
		mux:     mux,
		context: localCtx,
		cancel:  cancel,
	}
}

// udpMuxAddresses is every IPv4 address on an interface that is up, loopback
// included, less the VPN and container interfaces — what
// ice.NewMultiUDPMuxFromPort was asked for before, which does not export how it
// finds them.
func udpMuxAddresses() ([]net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	var ips []net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if strings.HasPrefix(iface.Name, "nordlynx") ||
			strings.HasPrefix(iface.Name, "tailscale") ||
			strings.HasPrefix(iface.Name, "docker") ||
			strings.HasPrefix(iface.Name, "br-") {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			if ip4 := ipNet.IP.To4(); ip4 != nil {
				ips = append(ips, ip4)
			}
		}
	}

	if len(ips) == 0 {
		return nil, errors.New("sfu: no IPv4 address to listen on")
	}

	// Loopback last. Behind a 1:1 NAT every address is advertised as the same
	// external one, and ICE keeps whichever socket it meets first and drops the
	// rest as duplicates. Were that loopback, every check would be sent from
	// 127.0.0.1 to a remote address, which the kernel refuses (EINVAL), and no
	// connection could form. Without the NAT the addresses stay distinct and the
	// order changes nothing.
	sort.SliceStable(ips, func(i, j int) bool {
		return !ips[i].IsLoopback() && ips[j].IsLoopback()
	})

	return ips, nil
}

func (u *UDPMux) Mux() *ice.MultiUDPMuxDefault {
	return u.mux
}

func (u *UDPMux) Close() error {
	u.cancel()
	return u.mux.Close()
}
