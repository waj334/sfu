package sfu

import (
	"context"
	"io"
	"sync"
	"time"

	"sync/atomic"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/stats"
	"github.com/pion/logging"
	"github.com/pion/rtp"
	"github.com/waj334/sfu/pkg/networkmonitor"
	"github.com/waj334/sfu/pkg/rtppool"
)

const (
	// How long a publisher is left alone between keyframe requests when
	// nothing sets a gap. See SendPLI.
	defaultPLIGap = 250 * time.Millisecond

	// How often a track's statistics are collected. The collection itself
	// refuses to run more than once a second, so there is nothing to gain from
	// looking more often than this.
	statsPollInterval = time.Second
)

type remoteTrack struct {
	context               context.Context
	cancel                context.CancelFunc
	mu                    sync.RWMutex
	track                 IRemoteTrack
	onRead                func(interceptor.Attributes, *rtp.Packet)
	onPLI                 func()
	bitrate               *atomic.Uint32
	previousBytesReceived *atomic.Uint64
	currentBytesReceived  *atomic.Uint64
	latestUpdatedTS       *atomic.Uint64
	lastPLIRequestTime    time.Time
	pliGap                time.Duration
	pliPending            bool
	onEndedCallbacks      []func()
	statsGetter           stats.Getter
	onStatsUpdated        func(*stats.Stats)
	log                   logging.LeveledLogger
	rtppool               *rtppool.RTPPool

	// duplicates is touched only by readRTP's goroutine.
	duplicates duplicateFilter
}

func newRemoteTrack(ctx context.Context, log logging.LeveledLogger, useBuffer bool, track IRemoteTrack, minWait, maxWait, pliInterval, pliGap time.Duration, onPLI func(), statsGetter stats.Getter, onStatsUpdated func(*stats.Stats), onRead func(interceptor.Attributes, *rtp.Packet), pool *rtppool.RTPPool, onNetworkConditionChanged func(networkmonitor.NetworkConditionType)) *remoteTrack {
	localctx, cancel := context.WithCancel(ctx)

	rt := &remoteTrack{
		context:               localctx,
		cancel:                cancel,
		mu:                    sync.RWMutex{},
		track:                 track,
		bitrate:               &atomic.Uint32{},
		previousBytesReceived: &atomic.Uint64{},
		currentBytesReceived:  &atomic.Uint64{},
		latestUpdatedTS:       &atomic.Uint64{},
		onEndedCallbacks:      make([]func(), 0),
		statsGetter:           statsGetter,
		onStatsUpdated:        onStatsUpdated,
		onPLI:                 onPLI,
		onRead:                onRead,
		pliGap:                pliGap,
		log:                   log,
		rtppool:               pool,
	}

	if rt.pliGap <= 0 {
		rt.pliGap = defaultPLIGap
	}

	if pliInterval > 0 {
		rt.enableIntervalPLI(pliInterval)
	}

	go rt.readRTP()
	go rt.pollStats()

	return rt
}

// pollStats reports the track's statistics on a timer.
//
// On a timer rather than per packet: this used to be `go t.updateStats()` from
// inside the read loop, which started a goroutine for every RTP packet that
// arrived only for nearly all of them to find the one-second throttle below
// already satisfied and return. A busy room made a few hundred thousand
// goroutines an hour that way, all of them to do nothing.
func (t *remoteTrack) pollStats() {
	ticker := time.NewTicker(statsPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.context.Done():
			return
		case <-ticker.C:
			// A relay track's statistics belong to the node that took the
			// packets off the wire, and nothing hands this one a getter to
			// read them with.
			if t.IsRelay() || t.statsGetter == nil {
				continue
			}
			t.updateStats()
		}
	}
}

func (t *remoteTrack) Context() context.Context {
	return t.context
}

func (t *remoteTrack) readRTP() {
	readCtx, cancel := context.WithCancel(t.context)

	defer cancel()

	defer t.cancel()

	defer t.onEnded()

	buffer := make([]byte, 1500)
	for {
		select {
		case <-readCtx.Done():
			return
		default:
			if err := t.track.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
				t.log.Errorf("remotetrack: set read deadline error - %s", err.Error())
				return
			}

			n, attrs, readErr := t.track.Read(buffer)
			if readErr != nil {
				if readErr == io.EOF {
					t.log.Infof("remotetrack: track ended %s ", t.track.ID())

					return
				}

				t.log.Tracef("remotetrack: read error: %s", readErr.Error())
				continue
			}

			// could be read deadline reached
			if n == 0 {
				continue
			}

			p := t.rtppool.GetPacket()

			if err := p.Unmarshal(buffer[:n]); err != nil {
				t.log.Errorf("remotetrack: unmarshal error: %s", err.Error())
				t.rtppool.PutPacket(p)
				continue
			}

			// A packet already delivered is not delivered again. The second copy
			// is almost always a retransmission of a packet that also arrived
			// the first time, and forwarding it sends every subscriber the same
			// media twice. See duplicateFilter.
			if t.duplicates.seen(p.SequenceNumber) {
				t.rtppool.PutPacket(p)
				continue
			}

			t.onRead(attrs, p)
			t.rtppool.PutPacket(p)
		}
	}
}

func (t *remoteTrack) updateStats() {
	s := t.statsGetter.Get(uint32(t.track.SSRC()))
	if s == nil {
		t.log.Warnf("remotetrack: stats not found for track: ", t.track.SSRC())
		return
	}

	// update the stats if the last update equal or more than 1 second
	latestUpdated := t.latestUpdatedTS.Load()
	if time.Since(time.Unix(0, int64(latestUpdated))).Seconds() <= 1 {
		return
	}

	if latestUpdated == 0 {
		t.latestUpdatedTS.Store(uint64(s.LastPacketReceivedTimestamp.UnixNano()))
		return
	}

	t.latestUpdatedTS.Store(uint64(s.LastPacketReceivedTimestamp.UnixNano()))

	if t.onStatsUpdated != nil {
		t.onStatsUpdated(s)
	}
}

func (t *remoteTrack) Track() IRemoteTrack {
	return t.track
}

// SendPLI asks the publisher for a keyframe, at most once per pliGap.
//
// A request that arrives inside the gap is held rather than discarded, and one
// PLI is sent when the gap is up however many arrived. Both halves matter in a
// large room. Every subscriber that attaches to a video track asks for a
// keyframe, so a thousand viewers joining at fifty a second is fifty requests a
// second against one publisher; without a gap that is fifty keyframes a second
// asked of a phone. Discarding the ones that arrive inside the gap, which is
// what this did before, left a viewer that joined just after a keyframe waiting
// on some later viewer's request to see a picture at all.
func (t *remoteTrack) SendPLI() {
	t.mu.Lock()

	if since := time.Since(t.lastPLIRequestTime); since < t.pliGap {
		if t.pliPending {
			t.mu.Unlock()

			return
		}

		t.pliPending = true
		wait := t.pliGap - since
		t.mu.Unlock()

		go func() {
			timer := time.NewTimer(wait)
			defer timer.Stop()

			select {
			case <-t.context.Done():
				return
			case <-timer.C:
			}

			t.mu.Lock()
			t.pliPending = false
			t.lastPLIRequestTime = time.Now()
			t.mu.Unlock()

			t.onPLI()
		}()

		return
	}

	t.lastPLIRequestTime = time.Now()
	t.mu.Unlock()

	go t.onPLI()
}

func (t *remoteTrack) enableIntervalPLI(interval time.Duration) {
	go func() {
		ctx, cancel := context.WithCancel(t.context)
		defer cancel()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				t.SendPLI()
			}
		}
	}()
}

func (t *remoteTrack) IsRelay() bool {
	_, ok := t.track.(*RelayTrack)
	return ok
}

func (t *remoteTrack) OnEnded(f func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.onEndedCallbacks = append(t.onEndedCallbacks, f)
}

func (t *remoteTrack) onEnded() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, f := range t.onEndedCallbacks {
		f()
	}
}

// watchRemoteTrack reports a remote track's arrival and its ending to whoever
// asked to be told, so a node can count the remote tracks it holds for one
// publisher and see whether it holds more than one for the same SSRC.
func watchRemoteTrack(client *Client, track *remoteTrack) {
	if client == nil || client.options.OnRemoteTrackChanged == nil || track == nil {
		return
	}

	observe := client.options.OnRemoteTrackChanged
	ssrc := uint32(track.track.SSRC())

	observe(ssrc, true)
	track.OnEnded(func() { observe(ssrc, false) })
}
