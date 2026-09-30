package sfu

import (
	"net"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/net/ipv4"
)

// shardWriter sends what is written to one socket, as many packets to a
// system call as have queued.
//
// A node fanning a stream out sends one packet per viewer per packet the host
// sends, and a write to a UDP socket is a system call each. Under a couple of
// thousand viewers the profile was a third sendto and the rest the queue in
// front of it: ~1,800 goroutines waiting on the sockets' write locks with ~5
// actually inside sendto, each subscriber's fan-out queue backing up behind
// them until it overflowed and dropped packets, keyframes included, which a
// viewer sees as the picture freezing while the sound plays on.
//
// So a write queues its packet and then tries, without waiting, to become the
// socket's sender: whoever gets the lock sends everything queued, up to
// maxWriteBatch packets to a sendmmsg, and everybody else goes back to work
// at once instead of waiting their turn at the lock. Batches form on their
// own under load, and at idle the writer finds only its own packet and sends
// it straight away.
//
// The sending is done by the writers themselves, on their own share of the
// scheduler, rather than handed to a goroutine per socket: tried first, a
// handful of dedicated senders among thousands of runnable fan-out goroutines
// got too little of it to keep up, and sent less than a third of what the
// writers did for themselves.
type shardWriter struct {
	conn  *net.UDPConn
	batch *ipv4.PacketConn // nil where sendmmsg is not what this socket speaks

	queue chan outboundPacket
	// queued is what is in queue, kept beside it so a sender finishing can
	// see a packet that arrived while it held the lock: see flush.
	queued atomic.Int64

	mu      sync.Mutex // held by whoever is sending, and guards the rest below
	pending []outboundPacket
	msgs    []ipv4.Message

	// Segmentation offload: see sendSegmented. gso is cleared for good the
	// first time the kernel refuses a segmented send.
	gso   atomic.Bool
	order []int    // pending, regrouped by destination
	taken []bool   // which of pending are in order
	oob   [][]byte // each message's segment-size control message
	segs  []int    // how many packets each message carries

	done chan struct{}
}

type outboundPacket struct {
	buf  *[]byte
	n    int
	addr net.Addr
}

const (
	// maxWriteBatch is the most one sendmmsg carries: enough that a system
	// call is shared by dozens of packets under load, few enough that one
	// batch does not hold the socket for long.
	maxWriteBatch = 64

	// writeQueueDepth is how far writers can get ahead of the socket. Past it
	// a writer waits for the lock and makes room itself rather than dropping.
	writeQueueDepth = 4096
)

// Egress counters for the node's metrics: see UDPMuxEgressStats.
var (
	egressPackets     atomic.Uint64
	egressSyscalls    atomic.Uint64
	egressWriteErrors atomic.Uint64

	segmentedSends   atomic.Uint64
	segmentedPackets atomic.Uint64
)

// UDPMuxSegmentationStats is how much of the egress segmentation offload
// carried: the sends that were segmented, and the packets in them. packets over
// sends is how many packets the kernel handled as one.
func UDPMuxSegmentationStats() (sends, packets uint64) {
	return segmentedSends.Load(), segmentedPackets.Load()
}

// UDPMuxEgressStats is what the UDP mux has sent: packets, the system calls
// they took, and the packets the kernel refused. packets/syscalls is the mean
// batch; near 1 means the node is keeping up without batching.
func UDPMuxEgressStats() (packets, syscalls, writeErrors uint64) {
	return egressPackets.Load(), egressSyscalls.Load(), egressWriteErrors.Load()
}

func newShardWriter(conn *net.UDPConn, done chan struct{}) *shardWriter {
	w := &shardWriter{
		conn:    conn,
		queue:   make(chan outboundPacket, writeQueueDepth),
		pending: make([]outboundPacket, 0, maxWriteBatch),
		msgs:    make([]ipv4.Message, maxWriteBatch),
		done:    done,
	}
	for i := range w.msgs {
		w.msgs[i].Buffers = make([][]byte, 1, maxGSOSegments)
	}
	if local, ok := conn.LocalAddr().(*net.UDPAddr); ok && local.IP.To4() != nil {
		w.batch = ipv4.NewPacketConn(conn)
	}
	if w.batch != nil && gsoSupported(conn) {
		w.gso.Store(true)
		w.order = make([]int, 0, maxWriteBatch)
		w.taken = make([]bool, maxWriteBatch)
		w.oob = make([][]byte, maxWriteBatch)
		for i := range w.oob {
			w.oob[i] = make([]byte, gsoOOBSize)
		}
		w.segs = make([]int, maxWriteBatch)
	}
	return w
}

func (w *shardWriter) enqueue(p []byte, addr net.Addr) (int, error) {
	var buf *[]byte
	if len(p) <= shardedBufferSize {
		buf = shardedBuffers.Get().(*[]byte)
	} else {
		b := make([]byte, len(p))
		buf = &b
	}
	n := copy(*buf, p)
	pkt := outboundPacket{buf: buf, n: n, addr: addr}

	for {
		select {
		case <-w.done:
			releaseOutbound(buf)
			return 0, net.ErrClosed
		default:
		}

		select {
		case w.queue <- pkt:
			w.queued.Add(1)
			w.flush()
			return len(p), nil
		default:
			// Full: whoever is sending is not keeping up. Wait to be the
			// sender and make room, as a write once waited on the socket.
			w.mu.Lock()
			w.sendQueued()
			w.mu.Unlock()
		}
	}
}

// flush sends what is queued if nobody else is.
//
// Looping on the count after unlocking is what keeps a packet from being
// stranded: one queued while this writer held the lock found the lock taken
// and left, trusting the holder to send it. The count is added to before that
// writer tries the lock and read here after this one lets it go, and atomics
// are ordered, so the holder sees it.
//
// Before sending, the new sender yields once. Under load that lets the other
// runnable writers — the rest of the frame this packet belongs to among them —
// queue theirs first, so a batch holds whole frames for segmentation offload to
// merge; flushed the moment each packet was written, batches averaged three
// packets and rarely two for one viewer. On an idle node there is nothing else
// to run and the yield returns at once.
func (w *shardWriter) flush() {
	for w.queued.Load() > 0 && w.mu.TryLock() {
		if w.queued.Load() < maxWriteBatch {
			runtime.Gosched()
		}
		w.sendQueued()
		w.mu.Unlock()
	}
}

// sendQueued sends everything queued, in batches. Called holding mu.
func (w *shardWriter) sendQueued() {
	pending := w.pending[:0]
	for {
	fill:
		for len(pending) < maxWriteBatch {
			select {
			case pkt := <-w.queue:
				w.queued.Add(-1)
				pending = append(pending, pkt)
			default:
				break fill
			}
		}
		if len(pending) == 0 {
			return
		}

		w.send(pending)

		for i := range pending {
			releaseOutbound(pending[i].buf)
			pending[i] = outboundPacket{}
		}
		pending = pending[:0]
	}
}

// send writes pending in as few system calls as the kernel will take them in.
// A packet the kernel refuses is dropped, as a failed write drops it: the
// ones after it are still sent. Called holding mu.
func (w *shardWriter) send(pending []outboundPacket) {
	if w.batch == nil || len(pending) == 1 {
		for _, pkt := range pending {
			egressSyscalls.Add(1)
			if _, err := w.conn.WriteTo((*pkt.buf)[:pkt.n], pkt.addr); err != nil {
				egressWriteErrors.Add(1)
			} else {
				egressPackets.Add(1)
			}
		}
		return
	}

	if w.gso.Load() {
		w.sendSegmented(pending)
		return
	}

	msgs := w.msgs[:len(pending)]
	for i, pkt := range pending {
		msgs[i].Buffers = msgs[i].Buffers[:1]
		msgs[i].Buffers[0] = (*pkt.buf)[:pkt.n]
		msgs[i].Addr = pkt.addr
		msgs[i].OOB = nil
	}

	batch := msgs
	for len(batch) > 0 {
		egressSyscalls.Add(1)
		n, err := w.batch.WriteBatch(batch, 0)
		if n > 0 {
			egressPackets.Add(uint64(n))
			batch = batch[n:]
		}
		if err != nil {
			// sendmmsg reports the error of the first message it could not
			// send, having sent the ones before it. That one is given up on.
			egressWriteErrors.Add(1)
			if len(batch) > 0 {
				batch = batch[1:]
			}
			continue
		}
		if n == 0 {
			// Nothing sent and nothing said: do not spin on it.
			egressWriteErrors.Add(uint64(len(batch)))
			break
		}
	}

	for i := range msgs {
		msgs[i].Buffers[0] = nil
		msgs[i].Addr = nil
	}
}

const (
	// maxGSOSegments is the most packets one segmented send carries: the
	// kernel's UDP_MAX_SEGMENTS on the oldest kernels that have it.
	maxGSOSegments = 64

	// maxGSOBytes keeps a segmented send inside one IP datagram's worth of
	// payload, headers aside.
	maxGSOBytes = 65000
)

// sendSegmented sends pending with segmentation offload: each destination's
// packets go to the kernel as one send per run of equal-sized packets, and the
// kernel carries each run through its stack as a single packet — UDP, IP,
// conntrack, the virtual switch — cutting it into datagrams only where it
// leaves. That per-packet work is what batching the system calls could not
// touch: a third of the node's CPU at 2,000 viewers.
//
// A frame of video for one viewer is a run of full-size packets and a shorter
// last one, which is exactly the shape a segmented send takes: every segment
// the same size but the last, which may be smaller.
//
// pending is regrouped by destination first. Each destination's packets keep
// their order; only packets for different destinations, which nothing orders
// against one another, change places. Called holding mu.
func (w *shardWriter) sendSegmented(pending []outboundPacket) {
	order := w.order[:0]
	taken := w.taken[:len(pending)]
	for i := range taken {
		taken[i] = false
	}
	for i := range pending {
		if taken[i] {
			continue
		}
		order = append(order, i)
		for j := i + 1; j < len(pending); j++ {
			if !taken[j] && sameUDPAddr(pending[i].addr, pending[j].addr) {
				taken[j] = true
				order = append(order, j)
			}
		}
	}

	// One message per run: the same destination, the same size, and at most
	// one shorter packet, last.
	count := 0
	for k := 0; k < len(order); {
		first := pending[order[k]]
		size := first.n
		m := &w.msgs[count]
		m.Buffers = append(m.Buffers[:0], (*first.buf)[:first.n])
		m.Addr = first.addr
		total, segs := first.n, 1
		k++
		for k < len(order) && segs < maxGSOSegments {
			p := pending[order[k]]
			if p.n > size || total+p.n > maxGSOBytes || !sameUDPAddr(p.addr, first.addr) {
				break
			}
			m.Buffers = append(m.Buffers, (*p.buf)[:p.n])
			total += p.n
			segs++
			k++
			if p.n < size {
				break
			}
		}
		if segs > 1 {
			m.OOB = putGSOSize(w.oob[count], size)
			segmentedSends.Add(1)
			segmentedPackets.Add(uint64(segs))
		} else {
			m.OOB = nil
		}
		w.segs[count] = segs
		count++
	}

	msgs := w.msgs[:count]
	for sent := 0; sent < len(msgs); {
		egressSyscalls.Add(1)
		n, err := w.batch.WriteBatch(msgs[sent:], 0)
		// A sendmmsg that fails outright returns -1, and WriteBatch passes it
		// through beside the error: nothing was sent. Taken as a count it
		// sliced backwards and brought the node down, the moment thousands of
		// viewers went at once and their addresses stopped taking packets.
		if n < 0 {
			n = 0
		}
		for _, s := range w.segs[sent : sent+n] {
			egressPackets.Add(uint64(s))
		}
		sent += n
		if err == nil {
			if n == 0 {
				// Nothing sent and nothing said: do not spin on it.
				for _, s := range w.segs[sent:count] {
					egressWriteErrors.Add(uint64(s))
				}
				break
			}
			continue
		}
		if sent >= len(msgs) {
			break
		}

		if w.segs[sent] > 1 && isGSORefusal(err) {
			// Segmentation is not available on this path after all. Stop
			// asking, and send what is left one packet at a time.
			w.gso.Store(false)
			udpMuxLog.Warnf("sfu: %s: segmentation offload refused (%v); sending packets one at a time", w.conn.LocalAddr(), err)
			for _, m := range msgs[sent:] {
				for _, b := range m.Buffers {
					egressSyscalls.Add(1)
					if _, err := w.conn.WriteTo(b, m.Addr); err != nil {
						egressWriteErrors.Add(1)
					} else {
						egressPackets.Add(1)
					}
				}
			}
			break
		}

		// sendmmsg reports the error of the first message it could not send,
		// having sent the ones before it. That one is given up on.
		egressWriteErrors.Add(uint64(w.segs[sent]))
		sent++
	}

	for i := range msgs {
		msgs[i].Buffers = msgs[i].Buffers[:1]
		msgs[i].Buffers[0] = nil
		msgs[i].Addr = nil
		msgs[i].OOB = nil
	}
}

// sameUDPAddr is whether two destinations are the same address and port.
func sameUDPAddr(a, b net.Addr) bool {
	ua, ok := a.(*net.UDPAddr)
	if !ok {
		return a.String() == b.String()
	}
	ub, ok := b.(*net.UDPAddr)
	if !ok {
		return false
	}
	return ua.Port == ub.Port && ua.IP.Equal(ub.IP)
}

// drain releases what was queued when the socket closed.
func (w *shardWriter) drain() {
	for {
		select {
		case pkt := <-w.queue:
			w.queued.Add(-1)
			releaseOutbound(pkt.buf)
		default:
			return
		}
	}
}

func releaseOutbound(buf *[]byte) {
	if cap(*buf) == shardedBufferSize {
		*buf = (*buf)[:shardedBufferSize]
		shardedBuffers.Put(buf)
	}
}
