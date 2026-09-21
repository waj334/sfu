package sfu

import (
	"context"
	"sync"

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

type fanoutJob struct {
	packet  *rtp.Packet
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
	shared := &rtp.Packet{Header: packet.Header.Clone(), Payload: append([]byte(nil), packet.Payload...)}
	job := fanoutJob{packet: shared, quality: quality}

	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, worker := range f.workers {
		select {
		case worker.queue <- job:
		default:
			// This subscriber is already behind. Keep every other subscriber
			// flowing and, most importantly, keep draining publisher ingress.
		}
	}
}

func (f *trackFanout) run(ctx context.Context, worker *fanoutWorker) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-worker.queue:
			packet := f.pool.CopyPacket(job.packet)
			worker.track.push(packet, job.quality)
			f.pool.PutPacket(packet)
		}
	}
}
