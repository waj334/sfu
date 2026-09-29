package sfu

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"
)

// Every packet sent to a sharded address is read, whichever of its sockets the
// kernel gave it to, and every reply reaches its peer in the order it was sent.
func TestShardedPacketConnReadsEverythingAndKeepsEachPeerInOrder(t *testing.T) {
	const (
		shards  = 4
		peers   = 32
		perPeer = 200
	)

	conn, err := listenSharded(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, shards, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if got := len(conn.conns); reusePortSupported && got != shards {
		t.Fatalf("listening on %d sockets, want %d", got, shards)
	}

	// Echo everything back to whoever sent it.
	received := make(chan struct{}, peers*perPeer)
	go func() {
		buf := make([]byte, 1500)
		for {
			n, addr, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			if _, err := conn.WriteTo(buf[:n], addr); err != nil {
				t.Errorf("write to %s: %v", addr, err)
			}
			received <- struct{}{}
		}
	}()

	var wg sync.WaitGroup
	for p := 0; p < peers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Error(err)
				return
			}
			defer peer.Close()

			done := make(chan struct{})
			go func() {
				defer close(done)
				buf := make([]byte, 1500)
				next := uint32(0)
				_ = peer.SetReadDeadline(time.Now().Add(10 * time.Second))
				for next < perPeer {
					n, _, err := peer.ReadFrom(buf)
					if err != nil {
						t.Errorf("peer got %d of %d replies: %v", next, perPeer, err)
						return
					}
					if got := binary.BigEndian.Uint32(buf[:n]); got != next {
						t.Errorf("peer got reply %d, want %d", got, next)
						return
					}
					next++
				}
			}()

			msg := make([]byte, 4)
			for i := uint32(0); i < perPeer; i++ {
				binary.BigEndian.PutUint32(msg, i)
				if _, err := peer.WriteTo(msg, conn.LocalAddr()); err != nil {
					t.Error(err)
					return
				}
				// Loopback drops what a full socket buffer cannot take, and this
				// is about ordering and coverage, not throughput.
				if i%20 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
			<-done
		}()
	}
	wg.Wait()

	if got := len(received); got != peers*perPeer {
		t.Fatalf("read %d packets, want %d", got, peers*perPeer)
	}
}

// A port already held is refused, as it was before there was more than one
// socket on it, rather than quietly shared with whoever holds it.
func TestShardedPacketConnRefusesATakenPort(t *testing.T) {
	first, err := listenSharded(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, 4, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	second, err := listenSharded(first.LocalAddr().(*net.UDPAddr), 4, 0, 0)
	if err == nil {
		second.Close()
		t.Fatal("bound a port another listener already holds")
	}
}
