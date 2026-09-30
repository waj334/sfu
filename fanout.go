package sfu

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pion/rtp"
	"github.com/waj334/sfu/pkg/rtppool"
)

// Simulcast access units arrive as packet bursts, not evenly over their frame
// duration. The bundled 720p fixture has 175 RTP packets in one keyframe and
// all three aligned layers total about 238. A queue smaller than an access-unit
// burst drops the same keyframe for every subscriber and produces synchronized
// stutter. Jobs share one immutable packet copy, so this capacity does not
// multiply payload memory by the subscriber count.
// Simulcast access units arrive as packet bursts, not evenly over their frame
// duration. The bundled 720p fixture has 175 RTP packets in one keyframe and
// all three aligned layers total about 238. A queue smaller than an access-unit
// burst drops the same keyframe for every subscriber and produces synchronized
// stutter. Jobs share one immutable packet copy, so this capacity does not
// multiply payload memory by the subscriber count.
const fanoutQueueDepth = 1024

// fanoutDrops counts packets a subscriber was not given because its queue was
// full: a viewer the node could not keep up with. See FanoutDrops.
var fanoutDrops atomic.Uint64

// FanoutDrops is how many packets the node has dropped for subscribers it had
// fallen behind on. Anything but zero is a viewer seeing loss the network did
// not cause: the node running out of room to send.
func FanoutDrops() uint64 {
	return fanoutDrops.Load()
}

type qualityFilteredTrack interface {
	wantsQuality(QualityLevel) bool
}

type sharedPacket struct {
	packet     rtp.Packet
	buf        []byte
	extensions []rtp.Extension
	csrc       []uint32
	refs       atomic.Int32
	pool       *sync.Pool
}

func (s *sharedPacket) copyHeader(h *rtp.Header) {
	s.packet.Header = *h
	if len(h.Extensions) > 0 {
		if cap(s.extensions) < len(h.Extensions) {
			s.extensions = make([]rtp.Extension, len(h.Extensions))
		}
		s.extensions = s.extensions[:len(h.Extensions)]
		copy(s.extensions, h.Extensions)
		s.packet.Header.Extensions = s.extensions
	} else {
		s.packet.Header.Extensions = nil
	}
	if len(h.CSRC) > 0 {
		if cap(s.csrc) < len(h.CSRC) {
			s.csrc = make([]uint32, len(h.CSRC))
		}
		s.csrc = s.csrc[:len(h.CSRC)]
		copy(s.csrc, h.CSRC)
		s.packet.Header.CSRC = s.csrc
	} else {
		s.packet.Header.CSRC = nil
	}
}

func (s *sharedPacket) release() {
	if s.refs.Add(-1) == 0 {
		s.packet.Header = rtp.Header{}
		s.packet.Payload = nil
		s.pool.Put(s)
	}
}

var sharedPacketPool = &sync.Pool{
	New: func() any {
		return &sharedPacket{
			buf:        make([]byte, 1500),
			extensions: make([]rtp.Extension, 0, 4),
			csrc:       make([]uint32, 0, 4),
		}
	},
}

type fanoutJob struct {
	shared  *sharedPacket
	quality QualityLevel
}

type fanoutWorker struct {
	track  iClientTrack
	queue  chan fanoutJob
	cancel context.CancelFunc
}

type trackFanout struct {
	ctx     context.Context
	pool    *rtppool.RTPPool
	mu      sync.RWMutex
	workers map[iClientTrack]*fanoutWorker
}

func newTrackFanout(ctx context.Context, pool *rtppool.RTPPool, tracks *clientTrackList) *trackFanout {
	f := &trackFanout{ctx: ctx, pool: pool, workers: make(map[iClientTrack]*fanoutWorker)}
	tracks.OnChanged(f.add, f.remove)
	return f
}

func (f *trackFanout) add(track iClientTrack) {
	ctx, cancel := context.WithCancel(f.ctx)
	worker := &fanoutWorker{track: track, queue: make(chan fanoutJob, fanoutQueueDepth), cancel: cancel}

	f.mu.Lock()
	if _, exists := f.workers[track]; exists {
		f.mu.Unlock()
		cancel()
		return
	}
	f.workers[track] = worker
	f.mu.Unlock()
	go f.run(ctx, worker)
}

func (f *trackFanout) remove(track iClientTrack) {
	f.mu.Lock()
	worker := f.workers[track]
	delete(f.workers, track)
	f.mu.Unlock()
	if worker != nil {
		worker.cancel()
	}
}

// Push copies the publisher-owned packet once and offers that immutable copy
// to every subscriber independently. A slow subscriber can fill only its own
// queue; it cannot stall ingress or discard another viewer's frames.
func (f *trackFanout) Push(packet *rtp.Packet, quality QualityLevel) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.workers) == 0 {
		return
	}

	shared := sharedPacketPool.Get().(*sharedPacket)
	shared.pool = sharedPacketPool

	shared.copyHeader(&packet.Header)

	payloadLen := len(packet.Payload)
	if cap(shared.buf) < payloadLen {
		shared.buf = make([]byte, payloadLen)
	}
	shared.buf = shared.buf[:payloadLen]
	copy(shared.buf, packet.Payload)
	shared.packet.Payload = shared.buf

	// 1 initial reference for Push during distribution
	shared.refs.Store(1)

	job := fanoutJob{shared: shared, quality: quality}

	for _, worker := range f.workers {
		if qf, ok := worker.track.(qualityFilteredTrack); ok {
			if !qf.wantsQuality(quality) {
				continue
			}
		}

		shared.refs.Add(1)
		select {
		case worker.queue <- job:
		default:
			// This subscriber is already behind. Keep every other subscriber
			// flowing and, most importantly, keep draining publisher ingress.
			shared.refs.Add(-1)
			fanoutDrops.Add(1)
		}
	}

	// Release Push's initial reference; if no subscriber took the job, it returns to pool.
	shared.release()
}

func (f *trackFanout) run(ctx context.Context, worker *fanoutWorker) {
	defer func() {
		// Drain any remaining jobs in the queue to release their packet references
		for {
			select {
			case job := <-worker.queue:
				job.shared.release()
			default:
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case job := <-worker.queue:
			packet := f.pool.CopyPacket(&job.shared.packet)
			worker.track.push(packet, job.quality)
			f.pool.PutPacket(packet)
			job.shared.release()
		}
	}
}
