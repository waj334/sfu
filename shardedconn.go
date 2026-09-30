package sfu

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// shardedPacketConn is one UDP address served by several sockets.
//
// pion's UDPMuxDefault sends every packet for every peer on the address through
// one socket, and Go serialises writes to a socket behind a lock. On a node
// fanning a stream out to a couple of thousand viewers that lock is the node:
// under load the profile showed ~3,700 goroutines queued on it with another
// 1,300 inside sendto, and a STUN reply to a viewer waited behind all of them
// long enough for the viewer's ICE to give up.
//
// The sockets share the address with SO_REUSEPORT, so to the other end there is
// still one address. Writes are spread across them by the remote address, which
// keeps each peer's packets on one socket and so in the order they were sent.
// The kernel spreads arriving packets across them by the same four-tuple, and
// every socket is read, so what the mux reads is everything that arrived at the
// address, as it was before: its demultiplexing by ufrag and remote address is
// untouched.
type shardedPacketConn struct {
	conns []*net.UDPConn

	// One per socket: what WriteTo has queued for it, sent in batches.
	writers []*shardWriter

	packets chan shardedPacket
	done    chan struct{}

	closeOnce sync.Once
	closeErr  error
}

type shardedPacket struct {
	buf  *[]byte
	n    int
	addr net.Addr
}

// shardedBufferSize holds any packet that arrives from a network. Larger ones —
// loopback, where there is no MTU to speak of — are given a buffer of their own.
const shardedBufferSize = 2048

var shardedBuffers = sync.Pool{
	New: func() any {
		b := make([]byte, shardedBufferSize)
		return &b
	},
}

// listenSharded binds shards sockets to addr.
//
// SO_REUSEPORT would equally let a second process — a previous instance of this
// service still shutting down — join the group without complaint, and the
// kernel would quietly hand it a share of this node's traffic. So the address is
// first taken without it, which fails the way binding a taken port always has.
func listenSharded(addr *net.UDPAddr, shards int, readBuffer, writeBuffer int) (*shardedPacketConn, error) {
	if shards < 1 || !reusePortSupported {
		shards = 1
	}

	probe, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	// Port 0 is resolved once, here, or every shard would get a port of its own.
	addr = probe.LocalAddr().(*net.UDPAddr)
	_ = probe.Close()

	lc := net.ListenConfig{}
	if shards > 1 {
		lc.Control = setReusePort
	}

	c := &shardedPacketConn{
		conns:   make([]*net.UDPConn, 0, shards),
		writers: make([]*shardWriter, 0, shards),
		packets: make(chan shardedPacket, 4096),
		done:    make(chan struct{}),
	}

	for i := 0; i < shards; i++ {
		pc, err := lc.ListenPacket(context.Background(), "udp", addr.String())
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("sfu: shard %d of %s: %w", i, addr, err)
		}

		conn := pc.(*net.UDPConn)
		if readBuffer > 0 {
			_ = conn.SetReadBuffer(readBuffer)
		}
		if writeBuffer > 0 {
			_ = conn.SetWriteBuffer(writeBuffer)
		}

		c.conns = append(c.conns, conn)
	}

	warnShortBuffers(addr, c.conns[0], readBuffer, writeBuffer)

	for _, conn := range c.conns {
		go c.readLoop(conn)

		c.writers = append(c.writers, newShardWriter(conn, c.done))
	}

	return c, nil
}

func (c *shardedPacketConn) readLoop(conn *net.UDPConn) {
	scratch := make([]byte, 65535)

	for {
		n, addr, err := conn.ReadFromUDP(scratch)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			select {
			case <-c.done:
				return
			default:
			}

			if os.IsTimeout(err) {
				continue
			}

			// Nothing else a UDP read returns on Linux is about the socket
			// rather than one packet, so the packet is what is given up on. The
			// pause is for the error that would otherwise repeat as fast as it
			// can be read.
			time.Sleep(time.Millisecond)
			continue
		}

		var buf *[]byte
		if n <= shardedBufferSize {
			buf = shardedBuffers.Get().(*[]byte)
		} else {
			b := make([]byte, n)
			buf = &b
		}
		copy(*buf, scratch[:n])

		select {
		case c.packets <- shardedPacket{buf: buf, n: n, addr: addr}:
		case <-c.done:
			return
		}
	}
}

// ReadFrom returns the next packet to arrive on any of the sockets.
//
// Only ever called from the mux's one reading goroutine.
func (c *shardedPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	select {
	case pkt := <-c.packets:
		n := copy(p, (*pkt.buf)[:pkt.n])
		if cap(*pkt.buf) == shardedBufferSize {
			shardedBuffers.Put(pkt.buf)
		}
		return n, pkt.addr, nil
	case <-c.done:
		return 0, nil, net.ErrClosed
	}
}

// WriteTo sends on the socket this remote address belongs to.
//
// Queued rather than written: see shardWriter. The packet is copied, so the
// caller has its buffer back the moment this returns, as it would from a
// write.
func (c *shardedPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	shard := 0
	if len(c.writers) > 1 {
		shard = shardFor(addr, len(c.writers))
	}

	return c.writers[shard].enqueue(p, addr)
}

// shardFor is FNV-1a over the remote IP and port.
func shardFor(addr net.Addr, shards int) int {
	const (
		offset = 2166136261
		prime  = 16777619
	)

	h := uint32(offset)
	if udp, ok := addr.(*net.UDPAddr); ok {
		// The same peer's address can arrive in its 4- and its 16-byte form, and
		// both have to land on the same socket.
		ip := udp.IP
		if ip4 := ip.To4(); ip4 != nil {
			ip = ip4
		}
		for _, b := range ip {
			h = (h ^ uint32(b)) * prime
		}
		h = (h ^ uint32(udp.Port&0xff)) * prime
		h = (h ^ uint32(udp.Port>>8)) * prime
	} else {
		for _, b := range []byte(addr.String()) {
			h = (h ^ uint32(b)) * prime
		}
	}

	return int(h % uint32(shards))
}

func (c *shardedPacketConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		for _, conn := range c.conns {
			if err := conn.Close(); err != nil && c.closeErr == nil {
				c.closeErr = err
			}
		}
		for _, w := range c.writers {
			w.drain()
		}
	})

	return c.closeErr
}

func (c *shardedPacketConn) LocalAddr() net.Addr {
	return c.conns[0].LocalAddr()
}

func (c *shardedPacketConn) SetDeadline(t time.Time) error {
	return c.SetWriteDeadline(t)
}

// SetReadDeadline is not supported: reads are from a queue the sockets fill,
// and the mux reading them never sets one.
func (c *shardedPacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *shardedPacketConn) SetWriteDeadline(t time.Time) error {
	for _, conn := range c.conns {
		if err := conn.SetWriteDeadline(t); err != nil {
			return err
		}
	}

	return nil
}

// warnShortBuffers says so when the kernel gave a socket less buffer than was
// asked for, which it does without an error: SetReadBuffer succeeds and the
// buffer is quietly capped at net.core.rmem_max, 208KiB by default. Media
// arriving faster than a briefly delayed reader drains it is then dropped in
// the kernel, counted only as RcvbufErrors in /proc/net/snmp, and seen as
// publisher packet loss, keyframe requests and every viewer's picture
// stuttering. Every shard is made the same way, so one is enough to check.
func warnShortBuffers(addr *net.UDPAddr, conn *net.UDPConn, readBuffer, writeBuffer int) {
	read, write, ok := socketBuffers(conn)
	if !ok {
		return
	}

	if readBuffer > 0 && read < readBuffer {
		udpMuxLog.Warnf("sfu: %s got a %d byte receive buffer of the %d asked for; raise net.core.rmem_max on the host or media will be dropped under load", addr, read, readBuffer)
	}
	if writeBuffer > 0 && write < writeBuffer {
		udpMuxLog.Warnf("sfu: %s got a %d byte send buffer of the %d asked for; raise net.core.wmem_max on the host", addr, write, writeBuffer)
	}
}
