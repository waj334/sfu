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
const fanoutQueueDepth = 512

type sharedPacket struct {
	packet rtp.Packet
	buf    []byte
	refs   atomic.Int32
	pool   *sync.Pool
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
			buf: make([]byte, 1500),
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

	shared.packet.Header = packet.Header.Clone()

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
		shared.refs.Add(1)
		select {
		case worker.queue <- job:
		default:
			// This subscriber is already behind. Keep every other subscriber
			// flowing and, most importantly, keep draining publisher ingress.
			shared.refs.Add(-1)
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
