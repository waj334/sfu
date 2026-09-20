package sfu

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/logging"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// silentTrack is an IRemoteTrack that never delivers a packet, so a remoteTrack
// built on it does nothing but hold the state SendPLI reads.
type silentTrack struct{}

func (silentTrack) ID() string                        { return "silent" }
func (silentTrack) RID() string                       { return "" }
func (silentTrack) PayloadType() webrtc.PayloadType   { return 96 }
func (silentTrack) Kind() webrtc.RTPCodecType         { return webrtc.RTPCodecTypeVideo }
func (silentTrack) StreamID() string                  { return "silent" }
func (silentTrack) SSRC() webrtc.SSRC                 { return 1234 }
func (silentTrack) Msid() string                      { return "silent" }
func (silentTrack) Codec() webrtc.RTPCodecParameters  { return webrtc.RTPCodecParameters{} }
func (silentTrack) SetReadDeadline(_ time.Time) error { return nil }

func (silentTrack) Read(_ []byte) (int, interceptor.Attributes, error) {
	time.Sleep(10 * time.Millisecond)

	return 0, nil, io.EOF
}

func (silentTrack) ReadRTP() (*rtp.Packet, interceptor.Attributes, error) {
	return nil, nil, io.EOF
}

func newTestRemoteTrack(t *testing.T, gap time.Duration, onPLI func()) *remoteTrack {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	return newRemoteTrack(
		ctx, logging.NewDefaultLoggerFactory().NewLogger("test"), false, silentTrack{},
		0, 0, 0, gap, onPLI, nil, nil,
		func(interceptor.Attributes, *rtp.Packet) {}, nil, nil,
	)
}

// TestRemoteTrackTakesThePLIGapItIsGiven is the regression this file exists
// for. newRemoteTrack took a gap and dropped it on the floor, which left every
// track with a zero gap; SendPLI compares elapsed time against it, and nothing
// is ever less than zero, so every request went straight through. A thousand
// viewers joining then asked one phone for hundreds of keyframes a second.
func TestRemoteTrackTakesThePLIGapItIsGiven(t *testing.T) {
	rt := newTestRemoteTrack(t, 700*time.Millisecond, func() {})
	require.Equal(t, 700*time.Millisecond, rt.pliGap)

	// No gap asked for means the default, never an ungated track.
	rt = newTestRemoteTrack(t, 0, func() {})
	require.Equal(t, defaultPLIGap, rt.pliGap)
	require.Positive(t, rt.pliGap)
}

// TestSendPLIHoldsTheGap: however many subscribers ask at once, the publisher
// is asked at most once per gap.
func TestSendPLIHoldsTheGap(t *testing.T) {
	var sent atomic.Int64

	const gap = 150 * time.Millisecond
	rt := newTestRemoteTrack(t, gap, func() { sent.Add(1) })

	// Five hundred subscribers arriving at once, as a room filling up does.
	for range 500 {
		rt.SendPLI()
	}

	// The first is immediate; the rest collapse into one at the end of the gap.
	time.Sleep(gap + 80*time.Millisecond)
	require.LessOrEqual(t, sent.Load(), int64(2),
		"500 simultaneous requests produced %d keyframe requests", sent.Load())
	require.Positive(t, sent.Load(), "no keyframe request was sent at all")

	// Held rather than discarded: a request arriving inside the gap still
	// reaches the publisher, so a viewer joining just after a keyframe does not
	// wait on some later viewer to ask.
	before := sent.Load()
	rt.SendPLI()
	time.Sleep(gap + 80*time.Millisecond)
	require.Greater(t, sent.Load(), before)
}

// TestSendPLIOverALongRunStaysWithinTheGap pins the rate itself.
func TestSendPLIOverALongRunStaysWithinTheGap(t *testing.T) {
	var sent atomic.Int64

	const gap = 100 * time.Millisecond
	rt := newTestRemoteTrack(t, gap, func() { sent.Add(1) })

	// A second of subscribers asking as fast as they can.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rt.SendPLI()
		time.Sleep(time.Millisecond)
	}

	time.Sleep(2 * gap)

	// A second at one per hundred milliseconds is ten, and a little slack for
	// where the boundaries fall.
	require.LessOrEqual(t, sent.Load(), int64(13),
		"a second of continuous requests produced %d keyframe requests", sent.Load())
}
