package sfu

import (
	"encoding/binary"
	"fmt"
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

// The kernel caps a buffer request at net.core.rmem_max without an error, so
// asking is not enough to know: socketBuffers has to report what was granted.
func TestSocketBuffersReportsWhatTheKernelGranted(t *testing.T) {
	if !reusePortSupported {
		t.Skip("buffers are only read back on linux")
	}

	const asked = 1 << 30 // more than any host allows
	c, err := listenSharded(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, 1, asked, asked)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	read, write, ok := socketBuffers(c.conns[0])
	if !ok {
		t.Fatal("could not read the buffers back")
	}
	if read <= 0 || read >= asked || write <= 0 || write >= asked {
		t.Fatalf("read %d, write %d: want granted sizes below the %d asked for", read, write, asked)
	}
}

// Packets queued while another writer holds the socket are sent by it in
// batches: with the lock held as if mid-send, 100 writes queue up, and letting
// go sends them in two sendmmsg calls (64 and 36), in the order written.
func TestShardedWritesQueuedBehindASenderGoOutInBatches(t *testing.T) {
	for _, gso := range []bool{true, false} {
		t.Run(fmt.Sprintf("gso=%v", gso), func(t *testing.T) { testWritesQueuedBehindASender(t, gso) })
	}
}

func testWritesQueuedBehindASender(t *testing.T, gso bool) {
	c, err := listenSharded(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, 1, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if !gso {
		for _, w := range c.writers {
			w.gso.Store(false)
		}
	}

	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	w := c.writers[0]
	_, callsBefore, _ := UDPMuxEgressStats()

	const queued = 100
	w.mu.Lock()
	for i := 0; i < queued; i++ {
		p := make([]byte, 8)
		binary.BigEndian.PutUint32(p, uint32(i))
		if _, err := c.WriteTo(p, peer.LocalAddr()); err != nil {
			t.Fatal(err)
		}
	}
	w.mu.Unlock()
	w.flush()

	buf := make([]byte, 64)
	_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
	for i := 0; i < queued; i++ {
		n, _, err := peer.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("after %d of %d: %v", i, queued, err)
		}
		if got := binary.BigEndian.Uint32(buf[:n]); got != uint32(i) {
			t.Fatalf("packet %d arrived as %d: out of order", i, got)
		}
	}

	// Two sendmmsg of 64 and 36, segmented or not: a batch is taken 64
	// packets at a time and segmentation only changes what each call carries.
	if _, callsAfter, _ := UDPMuxEgressStats(); callsAfter-callsBefore != 2 {
		t.Fatalf("%d system calls for %d queued packets; want 2", callsAfter-callsBefore, queued)
	}
}

// Segmented sends: frames for two viewers, interleaved as a node fanning out
// queues them, each a run of full-size packets and a shorter last one. Each
// viewer has to receive exactly the datagrams it was sent, one per packet, at
// their own sizes and in their own order.
func TestSegmentedSendsArriveAsTheDatagramsTheyWere(t *testing.T) {
	c, err := listenSharded(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, 1, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	w := c.writers[0]
	if !w.gso.Load() {
		t.Skip("no UDP segmentation offload here")
	}

	peers := make([]*net.UDPConn, 2)
	for i := range peers {
		peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		_ = peer.SetReadBuffer(4 << 20)
		peers[i] = peer
	}

	// Per frame: five full packets and a short one, for each viewer in turn.
	sizes := []int{1200, 1200, 1200, 1200, 1200, 317}
	const frames = 10
	sent := make([][][]byte, len(peers))

	w.mu.Lock()
	seq := 0
	for f := 0; f < frames; f++ {
		for _, size := range sizes {
			for i, peer := range peers {
				p := make([]byte, size)
				binary.BigEndian.PutUint32(p, uint32(seq))
				seq++
				sent[i] = append(sent[i], p)
				if _, err := c.WriteTo(p, peer.LocalAddr()); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	w.mu.Unlock()
	w.flush()

	if !w.gso.Load() {
		t.Fatal("the kernel refused segmentation offload on loopback")
	}

	buf := make([]byte, 2048)
	for i, peer := range peers {
		_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
		for k, want := range sent[i] {
			n, _, err := peer.ReadFromUDP(buf)
			if err != nil {
				t.Fatalf("peer %d after %d of %d: %v", i, k, len(sent[i]), err)
			}
			if n != len(want) || binary.BigEndian.Uint32(buf[:4]) != binary.BigEndian.Uint32(want[:4]) {
				t.Fatalf("peer %d datagram %d: got %d bytes, seq %d; want %d bytes, seq %d",
					i, k, n, binary.BigEndian.Uint32(buf[:4]), len(want), binary.BigEndian.Uint32(want[:4]))
			}
		}
	}
}

// Many writers at once, as a node fanning out has: every packet arrives, and
// each peer's in the order they were written for it.
func TestShardedConcurrentWritersKeepEachPeerInOrder(t *testing.T) {
	for _, gso := range []bool{true, false} {
		t.Run(fmt.Sprintf("gso=%v", gso), func(t *testing.T) { testConcurrentWritersKeepOrder(t, gso) })
	}
}

func testConcurrentWritersKeepOrder(t *testing.T, gso bool) {
	c, err := listenSharded(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, 4, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if !gso {
		for _, w := range c.writers {
			w.gso.Store(false)
		}
	}

	const writers, each = 8, 500
	peers := make([]*net.UDPConn, writers)
	for i := range peers {
		peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		_ = peer.SetReadBuffer(4 << 20)
		peers[i] = peer
	}

	var wg sync.WaitGroup
	for i := range peers {
		wg.Add(1)
		go func(to net.Addr) {
			defer wg.Done()
			for n := 0; n < each; n++ {
				p := make([]byte, 8)
				binary.BigEndian.PutUint32(p, uint32(n))
				if _, err := c.WriteTo(p, to); err != nil {
					t.Error(err)
					return
				}
			}
		}(peers[i].LocalAddr())
	}
	wg.Wait()

	buf := make([]byte, 64)
	for i, peer := range peers {
		_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
		for n := 0; n < each; n++ {
			got, _, err := peer.ReadFromUDP(buf)
			if err != nil {
				t.Fatalf("peer %d after %d of %d: %v", i, n, each, err)
			}
			if v := binary.BigEndian.Uint32(buf[:got]); v != uint32(n) {
				t.Fatalf("peer %d: packet %d arrived as %d", i, n, v)
			}
		}
	}
}

// A sendmmsg whose first message the kernel refuses fails outright and returns
// -1, which WriteBatch passes through with the error. Taken as a count of what
// was sent it sliced backwards and panicked, taking the node down the moment
// thousands of viewers left at once. The refused packets are given up on; the
// ones after them still go.
func TestSegmentedSendSurvivesARefusedFirstMessage(t *testing.T) {
	c, err := listenSharded(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, 1, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	w := c.writers[0]
	if !w.gso.Load() {
		t.Skip("no UDP segmentation offload here")
	}

	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	// Port 0: every UDP send to it is refused (EINVAL).
	refused := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
	_, _, errorsBefore := UDPMuxEgressStats()

	w.mu.Lock()
	if _, err := c.WriteTo(make([]byte, 100), refused); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		p := make([]byte, 8)
		binary.BigEndian.PutUint32(p, uint32(i))
		if _, err := c.WriteTo(p, peer.LocalAddr()); err != nil {
			t.Fatal(err)
		}
	}
	w.mu.Unlock()
	w.flush()

	buf := make([]byte, 64)
	_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
	for i := 0; i < 3; i++ {
		n, _, err := peer.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("after %d of 3: %v", i, err)
		}
		if got := binary.BigEndian.Uint32(buf[:n]); got != uint32(i) {
			t.Fatalf("packet %d arrived as %d", i, got)
		}
	}
	if _, _, errorsAfter := UDPMuxEgressStats(); errorsAfter-errorsBefore != 1 {
		t.Fatalf("%d write errors counted; want the 1 refused packet", errorsAfter-errorsBefore)
	}
}

// Each socket's send queue is as deep as asked, and 0 is the default.
func TestShardedSendQueueDepth(t *testing.T) {
	for _, tc := range []struct{ asked, want int }{
		{0, DefaultWriteQueueDepth},
		{-1, DefaultWriteQueueDepth},
		{100, 100},
	} {
		c, err := listenShardedWithQueue(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, 2, 0, 0, tc.asked)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range c.writers {
			if got := cap(w.queue); got != tc.want {
				t.Errorf("asked for %d: queue holds %d, want %d", tc.asked, got, tc.want)
			}
		}
		_ = c.Close()
	}
}
