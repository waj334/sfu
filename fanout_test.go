package sfu

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
	"github.com/waj334/sfu/pkg/rtppool"
)

type dummyClientTrack struct {
	id       string
	received atomic.Int32
	onPush   func(p *rtp.Packet, q QualityLevel)
	onEndedCb func()
}

func (d *dummyClientTrack) push(p *rtp.Packet, q QualityLevel) {
	d.received.Add(1)
	if d.onPush != nil {
		d.onPush(p, q)
	}
}

func (d *dummyClientTrack) ID() string                              { return d.id }
func (d *dummyClientTrack) StreamID() string                        { return "stream-1" }
func (d *dummyClientTrack) Context() context.Context                { return context.Background() }
func (d *dummyClientTrack) Kind() webrtc.RTPCodecType               { return webrtc.RTPCodecTypeVideo }
func (d *dummyClientTrack) MimeType() string                        { return "video/H264" }
func (d *dummyClientTrack) LocalTrack() *webrtc.TrackLocalStaticRTP { return nil }
func (d *dummyClientTrack) IsScreen() bool                          { return false }
func (d *dummyClientTrack) IsSimulcast() bool                       { return false }
func (d *dummyClientTrack) IsScaleable() bool                       { return false }
func (d *dummyClientTrack) SetSourceType(TrackType)                 {}
func (d *dummyClientTrack) Client() *Client                         { return nil }
func (d *dummyClientTrack) RequestPLI()                             {}
func (d *dummyClientTrack) SetMaxQuality(QualityLevel)              {}
func (d *dummyClientTrack) MaxQuality() QualityLevel                { return QualityHigh }
func (d *dummyClientTrack) ReceiveBitrate() uint32                  { return 0 }
func (d *dummyClientTrack) SendBitrate() uint32                     { return 0 }
func (d *dummyClientTrack) Quality() QualityLevel                   { return QualityHigh }
func (d *dummyClientTrack) OnEnded(callback func())                 { d.onEndedCb = callback }
func (d *dummyClientTrack) onEnded()                                { if d.onEndedCb != nil { d.onEndedCb() } }

func TestFanoutPushAndWorkerLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := rtppool.New()
	tracks := newClientTrackList()
	fanout := newTrackFanout(ctx, pool, tracks)

	// 1. Push with no workers (should not panic or leak)
	pkt := &rtp.Packet{
		Header:  rtp.Header{SequenceNumber: 100, Timestamp: 1000},
		Payload: []byte("hello world"),
	}
	fanout.Push(pkt, QualityHigh)

	// 2. Add two subscribers
	var wg sync.WaitGroup
	wg.Add(20) // 10 packets x 2 subscribers

	track1 := &dummyClientTrack{
		id: "track-1",
		onPush: func(p *rtp.Packet, q QualityLevel) {
			require.Equal(t, []byte("hello world"), p.Payload)
			require.Equal(t, QualityLevel(QualityHigh), q)
			wg.Done()
		},
	}
	track2 := &dummyClientTrack{
		id: "track-2",
		onPush: func(p *rtp.Packet, q QualityLevel) {
			require.Equal(t, []byte("hello world"), p.Payload)
			require.Equal(t, QualityLevel(QualityHigh), q)
			wg.Done()
		},
	}

	tracks.Add(track1)
	tracks.Add(track2)

	// Send 10 packets
	for i := 0; i < 10; i++ {
		fanout.Push(&rtp.Packet{
			Header:  rtp.Header{SequenceNumber: uint16(i + 1), Timestamp: uint32(i * 100)},
			Payload: []byte("hello world"),
		}, QualityHigh)
	}

	wg.Wait()
	require.Equal(t, int32(10), track1.received.Load())
	require.Equal(t, int32(10), track2.received.Load())

	// 3. Remove track1 (via tracks.remove or track1.onEnded) and send more packets
	tracks.remove(track1)
	time.Sleep(20 * time.Millisecond)

	var wg2 sync.WaitGroup
	wg2.Add(5)
	track2.onPush = func(p *rtp.Packet, q QualityLevel) {
		wg2.Done()
	}

	for i := 10; i < 15; i++ {
		fanout.Push(&rtp.Packet{
			Header:  rtp.Header{SequenceNumber: uint16(i + 1), Timestamp: uint32(i * 100)},
			Payload: []byte("hello world"),
		}, QualityHigh)
	}

	wg2.Wait()
	require.Equal(t, int32(10), track1.received.Load()) // still 10
	require.Equal(t, int32(15), track2.received.Load()) // received all 15
}

func TestFanoutQueueFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := rtppool.New()
	tracks := newClientTrackList()
	fanout := newTrackFanout(ctx, pool, tracks)

	block := make(chan struct{})
	track := &dummyClientTrack{
		id: "track-slow",
		onPush: func(p *rtp.Packet, q QualityLevel) {
			<-block
		},
	}
	tracks.Add(track)

	// Send fanoutQueueDepth + 10 packets
	for i := 0; i < fanoutQueueDepth+10; i++ {
		fanout.Push(&rtp.Packet{
			Header:  rtp.Header{SequenceNumber: uint16(i + 1)},
			Payload: []byte("burst"),
		}, QualityHigh)
	}

	// Unblock worker and let it drain
	close(block)
	time.Sleep(50 * time.Millisecond)

	// Worker should have received up to fanoutQueueDepth items without deadlock or panic
	require.Greater(t, track.received.Load(), int32(0))
}

type dummyFilteredTrack struct {
	dummyClientTrack
	wanted QualityLevel
}

func (d *dummyFilteredTrack) wantsQuality(q QualityLevel) bool {
	return q == d.wanted
}

func TestFanoutLayerFiltering(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := rtppool.New()
	tracks := newClientTrackList()
	fanout := newTrackFanout(ctx, pool, tracks)

	var wg sync.WaitGroup
	wg.Add(5) // only expect 5 High packets

	highViewer := &dummyFilteredTrack{
		dummyClientTrack: dummyClientTrack{
			id: "high-viewer",
			onPush: func(p *rtp.Packet, q QualityLevel) {
				require.Equal(t, QualityLevel(QualityHigh), q)
				wg.Done()
			},
		},
		wanted: QualityHigh,
	}
	tracks.Add(highViewer)

	// Send 5 High, 5 Mid, 5 Low
	for i := 0; i < 5; i++ {
		fanout.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: uint16(i + 1)}}, QualityHigh)
		fanout.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: uint16(i + 10)}}, QualityMid)
		fanout.Push(&rtp.Packet{Header: rtp.Header{SequenceNumber: uint16(i + 20)}}, QualityLow)
	}

	wg.Wait()
	time.Sleep(30 * time.Millisecond)

	// The high viewer must have received only 5 packets, completely skipping the 10 Mid/Low packets
	require.Equal(t, int32(5), highViewer.received.Load())
}
