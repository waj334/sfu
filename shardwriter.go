package sfu

import (
	"net"
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

	mu      sync.Mutex // held by whoever is sending, and guards the two below
	pending []outboundPacket
	msgs    []ipv4.Message

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
)

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
		w.msgs[i].Buffers = make([][]byte, 1)
	}
	if local, ok := conn.LocalAddr().(*net.UDPAddr); ok && local.IP.To4() != nil {
		w.batch = ipv4.NewPacketConn(conn)
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
func (w *shardWriter) flush() {
	for w.queued.Load() > 0 && w.mu.TryLock() {
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

	msgs := w.msgs[:len(pending)]
	for i, pkt := range pending {
		msgs[i].Buffers[0] = (*pkt.buf)[:pkt.n]
		msgs[i].Addr = pkt.addr
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
