package sfu

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/waj334/sfu/pkg/packetmap"
)

type simulcastClientTrack struct {
	id                      string
	streamid                string
	mu                      sync.RWMutex
	client                  *Client
	context                 context.Context
	kind                    webrtc.RTPCodecType
	mimeType                string
	localTrack              *webrtc.TrackLocalStaticRTP
	remoteTrack             *SimulcastTrack
	baseTrack               *baseTrack
	lastBlankSequenceNumber *atomic.Uint32
	sequenceNumber          *atomic.Uint32
	lastQuality             *atomic.Uint32
	paddingTS               *atomic.Uint32
	maxQuality              *atomic.Uint32
	lastTimestamp           *atomic.Uint32
	rewriteMu               sync.Mutex
	clockRate               uint32
	timestampOffset         *atomic.Uint32
	lastSentTimestamp       *atomic.Uint32
	lastSentQuality         *atomic.Uint32
	lastForwardedAt         *atomic.Int64
	forwardedQuality        *atomic.Uint32
	detectsKeyframes        bool
	clock                   func() time.Time
	isScreen                *atomic.Bool
	isEnded                 *atomic.Bool
	packetmapHigh           *packetmap.Map
	packetmapMid            *packetmap.Map
	packetmapLow            *packetmap.Map
	onTrackEndedCallbacks   []func()
}

func newSimulcastClientTrack(c *Client, t *SimulcastTrack) *simulcastClientTrack {
	track, newTrackErr := webrtc.NewTrackLocalStaticRTP(t.base.codec.RTPCodecCapability, t.base.id, t.base.streamid)
	if newTrackErr != nil {
		panic(newTrackErr)
	}

	isScreen := &atomic.Bool{}
	isScreen.Store(t.IsScreen())

	lastQuality := &atomic.Uint32{}

	sequenceNumber := &atomic.Uint32{}

	lastTimestamp := &atomic.Uint32{}

	ctx, cancel := context.WithCancel(t.context)

	ct := &simulcastClientTrack{
		mu:                      sync.RWMutex{},
		id:                      track.ID(),
		streamid:                track.StreamID(),
		context:                 ctx,
		kind:                    track.Kind(),
		mimeType:                track.Codec().MimeType,
		client:                  c,
		localTrack:              track,
		remoteTrack:             t,
		baseTrack:               t.base,
		sequenceNumber:          sequenceNumber,
		lastQuality:             lastQuality,
		paddingTS:               &atomic.Uint32{},
		maxQuality:              &atomic.Uint32{},
		lastBlankSequenceNumber: &atomic.Uint32{},
		lastTimestamp:           lastTimestamp,
		clockRate:               track.Codec().ClockRate,
		timestampOffset:         &atomic.Uint32{},
		lastSentTimestamp:       &atomic.Uint32{},
		lastSentQuality:         &atomic.Uint32{},
		lastForwardedAt:         &atomic.Int64{},
		forwardedQuality:        &atomic.Uint32{},
		detectsKeyframes:        detectsKeyframes(track.Codec().MimeType),
		isScreen:                isScreen,
		isEnded:                 &atomic.Bool{},
		onTrackEndedCallbacks:   make([]func(), 0),
		packetmapHigh:           &packetmap.Map{},
		packetmapMid:            &packetmap.Map{},
		packetmapLow:            &packetmap.Map{},
	}

	ct.SetMaxQuality(QualityHigh)

	ct.remoteTrack.sendPLI()

	// A layer that turns up later is one this subscriber may want to move onto,
	// and it can only move on a keyframe. Registered once here rather than on
	// the first packet forwarded, which is where it used to live and so never
	// ran for a track whose first packet had not arrived yet.
	ct.remoteTrack.onRemoteTrackAdded(func(remote *remoteTrack) {
		ct.remoteTrack.sendPLI()
	})

	t.OnEnded(func() {
		ct.onEnded()
		cancel()
	})

	return ct
}

func (t *simulcastClientTrack) Client() *Client {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.client
}

func (t *simulcastClientTrack) Context() context.Context {
	return t.context
}

func (t *simulcastClientTrack) isFirstKeyframePacket(p *rtp.Packet) bool {
	isKeyframe := IsKeyframe(t.mimeType, p.Payload)

	return isKeyframe && t.lastTimestamp.Load() != p.Timestamp
}

func (t *simulcastClientTrack) send(p *rtp.Packet, quality QualityLevel) {
	t.lastTimestamp.Store(p.Timestamp)

	t.rewritePacket(p, quality)

	// t.client.log.Infof("track: ", t.id, " send packet with quality ", quality, " and sequence number ", p.SequenceNumber)

	t.writeRTP(p)
}

func (t *simulcastClientTrack) writeRTP(p *rtp.Packet) {
	if err := t.localTrack.WriteRTP(p); err != nil {
		t.client.log.Errorf("track: error on write rtp", err)
	}
}

// layerFor names the simulcast layer that carries a rung of the bitrate ladder.
//
// The ladder is finer than simulcast is. Below the low layer it keeps going,
// with rungs that mean "the low layer, and ask the publisher for less" rather
// than a fourth stream to forward, so every rung still has to resolve to one of
// the three layers that exist on the wire.
func layerFor(quality QualityLevel) QualityLevel {
	switch quality {
	case QualityHigh, QualityHighMid, QualityHighLow:
		return QualityHigh
	case QualityMid, QualityMidMid, QualityMidLow:
		return QualityMid
	case QualityLow, QualityLowMid, QualityLowLow:
		return QualityLow
	}

	return QualityNone
}

// detectsKeyframes reports whether [Keyframe] can read this codec.
//
// For anything else it answers "not a keyframe" to every packet it is ever
// given, and a keyframe gate built on that would never open. Every codec this
// library registers for video is here; the check exists so that one which is
// not cannot silently produce a subscriber that receives nothing at all.
func detectsKeyframes(mimeType string) bool {
	for _, known := range []string{
		webrtc.MimeTypeVP8,
		webrtc.MimeTypeVP9,
		webrtc.MimeTypeAV1,
		webrtc.MimeTypeH264,
	} {
		if strings.EqualFold(mimeType, known) {
			return true
		}
	}

	return false
}

// canStartForwarding reports whether a packet is a point at which the layer on
// this subscriber's track may change.
//
// Every condition has to hold at once. The layer being forwarded has to differ
// from the one wanted, or there is nothing to do; the packet has to belong to
// the layer wanted, or it is not the one to switch on; and it has to open a
// keyframe that the sequence map will carry, because that is the only kind of
// frame a decoder can start on without the frames before it.
func canStartForwarding(forwarding, target, quality QualityLevel, isKeyframe, mapped bool) bool {
	return forwarding != target && quality == target && isKeyframe && mapped
}

// push offers a packet from one simulcast layer to this subscriber.
//
// One layer is on the subscriber's track at a time, and which one may only
// change at the first packet of a keyframe. A decoder handed the middle of
// another encoder's stream has none of the reference frames it names and paints
// the difference — the torn, blocky picture that arrived at random and stayed
// until something else happened to force a keyframe.
//
// So the layer being forwarded is kept apart from the layer that is wanted.
// What is wanted moves the moment the bitrate controller says so, or the moment
// the layer being forwarded goes quiet. What is being forwarded follows only
// once the publisher has produced a keyframe to switch on, and the old layer
// keeps playing until it does.
//
// Before this, the two were the same value, and it was read through
// LastQuality, which reports the low layer whenever the layer it was told to
// use has not been read from in 500ms. A publisher pausing its top layer — which
// libwebrtc does routinely under CPU or bandwidth pressure — therefore switched
// the subscriber onto the low layer at whatever packet happened to arrive next.
func (t *simulcastClientTrack) push(p *rtp.Packet, quality QualityLevel) {
	if !t.client.bitrateController.Exist(t.ID()) {
		// do nothing if the bitrate claim is not exist
		return
	}

	forwarding := Uint32ToQualityLevel(t.forwardedQuality.Load())

	// getQuality steps off a layer the publisher has stopped sending on its own,
	// so this is a layer that exists and not merely one that is wanted.
	target := layerFor(t.getQuality())
	if target == QualityNone {
		// TODO: figure out what to do if the target quality is none
		// probably we should send a blank frame
		target = QualityLow
	}

	if forwarding != target && quality == target {
		var mapped bool

		switch quality {
		case QualityHigh:
			mapped, _, _ = t.packetmapHigh.Map(p.SequenceNumber, 0)
		case QualityMid:
			mapped, _, _ = t.packetmapMid.Map(p.SequenceNumber, 0)
		case QualityLow:
			mapped, _, _ = t.packetmapLow.Map(p.SequenceNumber, 0)
		}

		// A codec whose keyframes cannot be read would never open the gate, so
		// it is not gated: it starts on whatever arrives, as every codec did
		// before this.
		opensAFrame := !t.detectsKeyframes || IsKeyframe(t.mimeType, p.Payload)

		if canStartForwarding(forwarding, target, quality, opensAFrame, mapped) {
			t.client.log.Tracef("track: %s forwarding quality %d in place of %d", t.id, target, forwarding)

			forwarding = target
			t.forwardedQuality.Store(uint32(forwarding))
			t.lastQuality.Store(uint32(forwarding))
		} else {
			// Nothing to switch on yet. This is the first packet of all as well,
			// where nothing is being forwarded and the track has not started: it
			// starts on a keyframe or it does not start. remoteTrack throttles
			// these to one every 250ms, so asking on every packet costs nothing.
			t.remoteTrack.sendPLI()
		}
	}

	if forwarding == quality {
		t.send(p, quality)
	}
}

func (t *simulcastClientTrack) GetRemoteTrack() *remoteTrack {
	lastQuality := Uint32ToQualityLevel(t.lastQuality.Load())
	// lastQuality := t.lastQuality
	switch lastQuality {
	case QualityHigh:
		return t.remoteTrack.remoteTrackHigh
	case QualityMid:
		return t.remoteTrack.remoteTrackMid
	case QualityLow:
		return t.remoteTrack.remoteTrackLow
	default:
		if t.remoteTrack.isTrackActive(QualityHigh) {
			return t.remoteTrack.remoteTrackHigh
		}

		if t.remoteTrack.isTrackActive(QualityMid) {
			return t.remoteTrack.remoteTrackMid
		}

		if t.remoteTrack.isTrackActive(QualityLow) {
			return t.remoteTrack.remoteTrackLow
		}
	}

	return nil
}

func (t *simulcastClientTrack) ID() string {
	return t.id
}

func (t *simulcastClientTrack) StreamID() string {
	return t.streamid
}

func (t *simulcastClientTrack) Kind() webrtc.RTPCodecType {
	return t.kind
}

func (t *simulcastClientTrack) LocalTrack() *webrtc.TrackLocalStaticRTP {
	return t.localTrack
}

func (t *simulcastClientTrack) IsScreen() bool {
	return t.isScreen.Load()
}

func (t *simulcastClientTrack) SetSourceType(sourceType TrackType) {
	t.isScreen.Store(sourceType == TrackTypeScreen)
}

func (t *simulcastClientTrack) LastQuality() QualityLevel {
	quality := Uint32ToQualityLevel(t.lastQuality.Load())

	track := t.remoteTrack

	if quality == QualityHigh && track.isTrackActive(QualityHigh) {
		return QualityHigh
	} else if quality == QualityMid && track.isTrackActive(QualityMid) {
		return QualityMid
	}
	return QualityLow
}

func (t *simulcastClientTrack) OnEnded(callback func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.onTrackEndedCallbacks = append(t.onTrackEndedCallbacks, callback)
}

// onEnded runs the ended callbacks exactly once.
//
// Guarded by a compare-and-swap rather than a load followed by a store: the
// store used to happen after the callbacks had run, so concurrent callers all
// read false and all ran them. SimulcastTrack.onEnded is the caller and now
// fires once, but this track is reachable from a track replacement as well, so
// the guard has to hold on its own.
//
// The callbacks are copied out under the lock that OnEnded appends beneath,
// and called outside it — they reach back into the client.
func (t *simulcastClientTrack) onEnded() {
	if !t.isEnded.CompareAndSwap(false, true) {
		return
	}

	t.mu.RLock()
	callbacks := make([]func(), len(t.onTrackEndedCallbacks))
	copy(callbacks, t.onTrackEndedCallbacks)
	t.mu.RUnlock()

	for _, callback := range callbacks {
		callback()
	}
}

func (t *simulcastClientTrack) SetMaxQuality(quality QualityLevel) {
	t.maxQuality.Store(uint32(quality))
	claim := t.Client().bitrateController.GetClaim(t.ID())
	if claim != nil {
		if claim.Quality() > quality && quality != QualityNone {
			claim.SetQuality(quality)
		}
	}

	t.remoteTrack.sendPLI()
}

func (t *simulcastClientTrack) MaxQuality() QualityLevel {
	return Uint32ToQualityLevel(t.maxQuality.Load())
}

func (t *simulcastClientTrack) IsSimulcast() bool {
	return true
}

func (t *simulcastClientTrack) IsScaleable() bool {
	return false
}

// rewritePacket puts a packet on the timeline this subscriber has been watching,
// whichever simulcast layer it arrived on.
//
// Every layer is a separate RTP stream with its own randomly chosen sequence
// number and timestamp base, so neither number can be forwarded as it stands.
// Both are carried by counters of this track's own, and both are stepped on at
// the moment the forwarded layer changes: the sequence number by one packet and
// the timestamp by [simulcastClientTrack.switchGap], placing the first frame of
// the new layer immediately after the last frame of the old one. Off a switch
// they follow whatever the source layer did, so the publisher's own pacing and
// its real gaps come through untouched.
//
// push only changes the forwarded layer at the first packet of a keyframe, which
// is what makes it safe to move the offset here: every packet of a frame is
// rewritten against the same one.
//
// The offset used to be a fixed anchor recorded per layer instead, which made a
// switch move the timeline by the difference between two unrelated random
// numbers — typically minutes, far outside the ten seconds a receiver will hold
// a frame for. libwebrtc threw the frame away ("bad render timing"), reset its
// jitter estimator, and the picture froze until it had rebuilt one. Anchoring
// per layer cannot fix that on its own either: a layer the publisher only
// starts sending once bandwidth allows records its base late, and switching to
// it then rewinds the timeline by however late it was.
func (t *simulcastClientTrack) rewritePacket(p *rtp.Packet, quality QualityLevel) {
	// Packets arrive from one goroutine per layer, and a switch is exactly the
	// moment two of them are live at once. The offset, the last timestamp sent
	// and the layer it belonged to only mean anything together, so they move
	// together. The lock is this track's alone and is uncontended off a switch.
	t.rewriteMu.Lock()
	defer t.rewriteMu.Unlock()

	t.remoteTrack.mu.RLock()
	sequenceDelta := uint16(0)
	switch quality {
	case QualityHigh:
		sequenceDelta = t.remoteTrack.highSequence - t.remoteTrack.lastHighSequence
	case QualityMid:
		sequenceDelta = t.remoteTrack.midSequence - t.remoteTrack.lastMidSequence
	case QualityLow:
		sequenceDelta = t.remoteTrack.lowSequence - t.remoteTrack.lastLowSequence
	}
	t.remoteTrack.mu.RUnlock()

	if previous := Uint32ToQualityLevel(t.lastSentQuality.Load()); previous != QualityNone && previous != quality {
		// A switch lands on a keyframe, so this packet opens the frame after the
		// last one sent and follows the last packet sent. A nominal frame is
		// close enough to place it, because the receiver reads the interval as
		// jitter rather than as a duration.
		//
		// The step of one matters as much as the offset does. The layer's own
		// gap is meaningless here — against a layer being read for the first
		// time it is the whole of that layer's starting sequence number, which
		// reads downstream as tens of thousands of packets lost at once.
		t.timestampOffset.Store(t.lastSentTimestamp.Load() + t.switchGap() - p.Timestamp)
		t.sequenceNumber.Add(1)
	} else {
		// Within a layer the gap is real and is carried through, so a packet
		// lost on the way in stays lost and the subscriber can ask for it back.
		// The first packet of all lands here too: the timeline starts wherever
		// it happens to start, and nothing has been sent for it to disagree with.
		t.sequenceNumber.Add(uint32(sequenceDelta))
	}

	p.Timestamp += t.timestampOffset.Load()
	p.SequenceNumber = uint16(t.sequenceNumber.Load())

	t.lastSentTimestamp.Store(p.Timestamp)
	t.lastSentQuality.Store(uint32(quality))
	t.lastForwardedAt.Store(t.now().UnixNano())
}

// maxSwitchGap bounds how far one switch may move the subscriber's clock.
//
// Not a judgement about timing — it is there so that a track resumed after long
// enough cannot overflow the 32-bit timestamp arithmetic below. Thirty seconds
// is already several times what any receiver will wait for a frame.
const maxSwitchGap = 30 * time.Second

// switchGap is how far the subscriber's clock moves across a layer switch.
//
// The wait for the new layer's keyframe is real time the viewer spent on the old
// layer's last frame, so the timeline has to account for it. Folding it into a
// single nominal frame instead tells the receiver that a frame turned up later
// than its timestamp said it should have, and its jitter estimate grows by the
// difference; enough of those and it holds the picture back by seconds and then
// throws the backlog away.
//
// Floored at one frame, so the clock always moves forward even for a switch that
// lands within a frame of the packet before it.
func (t *simulcastClientTrack) switchGap() uint32 {
	frame := t.frameDuration()

	last := t.lastForwardedAt.Load()
	if last == 0 {
		return frame
	}

	elapsed := t.now().Sub(time.Unix(0, last))
	if elapsed > maxSwitchGap {
		elapsed = maxSwitchGap
	}

	if gap := uint32(elapsed.Seconds() * float64(t.clockRate)); gap > frame {
		return gap
	}

	return frame
}

// now is time.Now unless a test has replaced it.
func (t *simulcastClientTrack) now() time.Time {
	if t.clock != nil {
		return t.clock()
	}

	return time.Now()
}

// frameDuration is one frame at the track's clock, in RTP timestamp units.
//
// Nominal thirty a second. Nothing here knows the publisher's real frame rate,
// and it only has to be close: the number is used once per layer switch to
// leave a gap where the next frame would have been.
func (t *simulcastClientTrack) frameDuration() uint32 {
	if t.clockRate == 0 {
		return 0
	}

	return t.clockRate / 30
}

func (t *simulcastClientTrack) RequestPLI() {
	t.remoteTrack.sendPLI()
}

func (t *simulcastClientTrack) getQuality() QualityLevel {
	track := t.remoteTrack

	claim := t.Client().bitrateController.GetClaim(t.ID())

	if claim == nil {
		return QualityNone
	}

	quality := min(claim.Quality(), t.MaxQuality(), Uint32ToQualityLevel(t.client.quality.Load()))

	if quality != QualityNone && !track.isTrackActive(quality) {
		if quality != QualityLow && track.isTrackActive(QualityLow) {
			return QualityLow
		}

		if quality != QualityMid && track.isTrackActive(QualityMid) {
			return QualityMid
		}

		if quality != QualityHigh && track.isTrackActive(QualityHigh) {
			return QualityHigh
		}
	}

	return quality
}

func (t *simulcastClientTrack) ReceiveBitrate() uint32 {
	total := uint32(0)

	for _, quality := range []QualityLevel{QualityHigh, QualityMid, QualityLow} {
		total += t.ReceiveBitrateAtQuality(quality)
	}

	return total
}

func (t *simulcastClientTrack) SendBitrate() uint32 {
	bitrate, err := t.client.stats.GetSenderBitrate(t.ID())
	if err != nil {
		// Missing stats are the ordinary case for the first second of a
		// subscription: the sender stats are written by a one-second ticker, and
		// addClaims asks for the bitrate before that has ever run. clientTrack
		// already skipped it here; this path did not, and logged it at error on
		// every subscribe — through a format string with no verb for the error,
		// which is where the "%!(EXTRA ...)" in the log came from.
		if !errors.Is(err, ErrCLientStatsNotFound) {
			t.client.log.Errorf("clienttrack: error on get sender bitrate %s", err.Error())
		}

		return 0
	}

	return bitrate
}

func (t *simulcastClientTrack) ReceiveBitrateAtQuality(quality QualityLevel) uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var remoteTrack *remoteTrack

	switch quality {
	case QualityHigh:
		remoteTrack = t.remoteTrack.remoteTrackHigh
	case QualityHighMid:
		remoteTrack = t.remoteTrack.remoteTrackHigh
	case QualityHighLow:
		remoteTrack = t.remoteTrack.remoteTrackHigh
	case QualityMid:
		remoteTrack = t.remoteTrack.remoteTrackMid
	case QualityMidMid:
		remoteTrack = t.remoteTrack.remoteTrackMid
	case QualityMidLow:
		remoteTrack = t.remoteTrack.remoteTrackMid
	case QualityLow:
		remoteTrack = t.remoteTrack.remoteTrackLow
	case QualityLowMid:
		remoteTrack = t.remoteTrack.remoteTrackLow
	case QualityLowLow:
		remoteTrack = t.remoteTrack.remoteTrackLow
	}

	if remoteTrack == nil {
		return 0
	}

	bitrate, err := t.baseTrack.client.stats.GetReceiverBitrate(remoteTrack.track.ID(), remoteTrack.track.RID())
	if err != nil {
		// The receiver's half of what SendBitrate already skips: the stats are
		// written by a ticker, so a layer that has not been read from yet has
		// none, and every subscribe logged one of these per layer. The error
		// also went to a format string with no verb for it, which is where the
		// "%!(EXTRA ...)" came from.
		if !errors.Is(err, ErrCLientStatsNotFound) {
			t.client.log.Errorf("clienttrack: error on get receiver bitrate %s", err.Error())
		}

		return 0
	}

	switch quality {
	case QualityHighMid, QualityMidMid, QualityLowMid:
		return bitrate / 2
	case QualityHighLow, QualityMidLow, QualityLowLow:
		return bitrate / 4
	default:
		return bitrate
	}
}

func (t *simulcastClientTrack) Quality() QualityLevel {
	return t.getQuality()
}

func (t *simulcastClientTrack) MimeType() string {
	return t.mimeType
}
