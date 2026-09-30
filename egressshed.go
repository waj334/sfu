package sfu

import (
	"context"
	"hash/fnv"
	"sync/atomic"
	"time"
)

// Egress shedding: stepping some viewers down a simulcast layer when the node
// cannot send everything it owes on time.
//
// Past what a node can send, packets wait in its sockets' send queues. With a
// shallow queue the excess was dropped, and viewers froze; with a deep one it
// is delayed, and every viewer's picture stutters as late packets arrive in
// bursts and the player discards frames that come too late. Either way every
// viewer suffers. Sending some of them the mid or low layer instead — a third
// of the high layer's packets, or less — brings the load back under what the
// node can send, and everybody plays smoothly, some at lower resolution.
//
// The signal is how long packets wait in the send queues, which is exactly the
// lateness a viewer sees. Every 500ms the controller reads the longest wait
// since the last look. Over the target, it caps another 10% of viewers at the
// mid layer (then, all of them there, at the low layer), and waits 2s — about a
// keyframe interval, which is when a viewer's layer can actually change —
// before shedding more. Under a quarter of the target for 5s, it gives back
// 5%, low caps first. Shedding fast and recovering slowly keeps it from
// swinging back and forth across the limit.
//
// Which viewers are capped is fixed by a hash of each one's client id, so the
// same viewers go down and come back up, rather than whoever happens to be
// picked flickering between layers. Audio is never shed.

// shedStep is how much of the viewers one step caps, in basis points.
const (
	shedStep        = 1000 // 10%
	shedRecoverStep = 500  // 5%
	shedFull        = 10000

	shedTick     = 500 * time.Millisecond
	shedCooldown = 2 * time.Second
	shedCalm     = 5 * time.Second

	// DefaultShedTargetDelay is a suggested UDPMuxOptions.ShedTargetDelay: how
	// long packets may wait in the send queues before viewers are stepped
	// down. Shedding is off unless a target is given.
	DefaultShedTargetDelay = 25 * time.Millisecond
)

var (
	// The share of viewers, in basis points by rank, capped at each layer.
	shedToMid atomic.Int32
	shedToLow atomic.Int32

	// The longest a packet has waited in a send queue since the controller
	// last looked, in nanoseconds; and what it saw then, for metrics.
	egressQueueWaitMax  atomic.Int64
	egressQueueWaitSeen atomic.Int64
)

// EgressShedding is the share of viewers currently capped at the mid and at
// the low layer (0 to 1), and the longest a packet waited in a send queue in
// the last half second.
func EgressShedding() (toMid, toLow float64, queueWait time.Duration) {
	return float64(shedToMid.Load()) / shedFull,
		float64(shedToLow.Load()) / shedFull,
		time.Duration(egressQueueWaitSeen.Load())
}

// shedRank places a viewer in the order shedding takes them in: 0 to 9,999.
func shedRank(clientID string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(clientID))
	return int32(h.Sum32() % shedFull) //nolint:gosec // < 10,000
}

// shedCap is the highest layer a viewer of this rank may be sent now.
func shedCap(rank int32) QualityLevel {
	switch {
	case rank < shedToLow.Load():
		return QualityLow
	case rank < shedToMid.Load():
		return QualityMid
	default:
		return QualityHigh
	}
}

// noteQueueWait records how long the oldest packet of a batch waited.
func noteQueueWait(wait time.Duration) {
	w := int64(wait)
	for {
		seen := egressQueueWaitMax.Load()
		if w <= seen || egressQueueWaitMax.CompareAndSwap(seen, w) {
			return
		}
	}
}

// runEgressMonitor samples how long packets have waited in the send queues,
// for EgressShedding, every shedTick until ctx ends, and with a target over 0
// sheds and recovers against it too.
//
// The sampling runs whether or not shedding is on: the wait is how late
// viewers are being sent their packets, which is worth watching on any node.
// It was first only read by the shedding controller, so with shedding off the
// gauge read zero even while a node was seconds behind.
func runEgressMonitor(ctx context.Context, target time.Duration) {
	ticker := time.NewTicker(shedTick)
	defer ticker.Stop()

	var lastShed, calmSince time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			wait := time.Duration(egressQueueWaitMax.Swap(0))
			egressQueueWaitSeen.Store(int64(wait))
			if target > 0 {
				shedStepFor(now, wait, target, &lastShed, &calmSince)
			}
		}
	}
}

// shedStepFor is one look: shed, recover, or hold.
func shedStepFor(now time.Time, wait, target time.Duration, lastShed, calmSince *time.Time) {
	switch {
	case wait > target:
		*calmSince = time.Time{}
		// Once per cooldown: a viewer's layer changes at its next keyframe, so
		// the last step has not shown in the queues before then.
		if !lastShed.IsZero() && now.Sub(*lastShed) < shedCooldown {
			return
		}
		if mid := shedToMid.Load(); mid < shedFull {
			shedToMid.Store(min(mid+shedStep, shedFull))
		} else if low := shedToLow.Load(); low < shedFull {
			shedToLow.Store(min(low+shedStep, shedFull))
		} else {
			return
		}
		*lastShed = now

	case wait < target/4:
		if calmSince.IsZero() {
			*calmSince = now
			return
		}
		if now.Sub(*calmSince) < shedCalm {
			return
		}
		if low := shedToLow.Load(); low > 0 {
			shedToLow.Store(max(low-shedRecoverStep, 0))
		} else if mid := shedToMid.Load(); mid > 0 {
			shedToMid.Store(max(mid-shedRecoverStep, 0))
		}
		*calmSince = now

	default:
		// Between: neither shed further nor give back.
		*calmSince = time.Time{}
	}
}
