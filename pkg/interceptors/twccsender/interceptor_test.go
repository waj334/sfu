package twccsender

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/twcc"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/stretchr/testify/require"
)

const twccExtID = 3

// arrivals is the pattern that puts pion's recorder into the state this package
// exists to get out of: a steady stream, with a packet from `age` sequence
// numbers back re-arriving `every` packets.
func arrivals(count, age, every int) []uint16 {
	out := make([]uint16, 0, count+count/every+1)
	for i := 0; i < count; i++ {
		out = append(out, uint16(i)) //nolint:gosec // G115
		if i > age && every > 0 && i%every == 0 {
			out = append(out, uint16(i-age)) //nolint:gosec // G115
		}
	}

	return out
}

// TestPionsRecorderReportsHistoryForever is the behaviour this package works
// around, pinned so it is clear what changed if it ever stops being true.
func TestPionsRecorderReportsHistoryForever(t *testing.T) {
	recorder := twcc.NewRecorder(1)

	const pps = 80
	step := int64(1_000_000 / pps)

	var us, nextTick int64 = 0, 100_000
	var reported, recorded uint64
	var widest int

	for _, sn := range arrivals(60*pps, 400, 8) {
		recorder.Record(1234, sn, us)
		recorded++
		us += step

		for us >= nextTick {
			n, _, _ := describe(recorder.BuildFeedbackPacket())
			reported += n
			if int(n) > widest {
				widest = int(n)
			}
			nextTick += 100_000
		}
	}

	// Every arrival should be reported about once. It is reported dozens of
	// times, and the report only gets wider.
	require.Greater(t, reported, recorded*10,
		"expected pion's recorder to re-report history; it reported %d arrivals for %d received", reported, recorded)
	require.Greater(t, widest, 200,
		"expected a single report to carry hundreds of arrivals, widest was %d", widest)
}

// fakeReader hands the interceptor one RTP packet per Read, each carrying the
// transport-wide sequence number it is given.
type fakeReader struct {
	mu   sync.Mutex
	seqs []uint16
}

func (r *fakeReader) Read(buf []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.seqs) == 0 {
		return 0, nil, io.EOF
	}

	sn := r.seqs[0]
	r.seqs = r.seqs[1:]

	ext := rtp.TransportCCExtension{TransportSequence: sn}
	payload, err := ext.Marshal()
	if err != nil {
		return 0, nil, err
	}

	pkt := &rtp.Packet{
		Header:  rtp.Header{Version: 2, SSRC: 1234, SequenceNumber: sn},
		Payload: []byte{0x00},
	}
	if err := pkt.Header.SetExtension(twccExtID, payload); err != nil {
		return 0, nil, err
	}

	raw, err := pkt.Marshal()
	if err != nil {
		return 0, nil, err
	}

	return copy(buf, raw), a, nil
}

type capturingWriter struct {
	mu       sync.Mutex
	reported uint64
	widest   uint64
}

func (w *capturingWriter) Write(pkts []rtcp.Packet, _ interceptor.Attributes) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n, _, _ := describe(pkts)
	w.reported += n
	if n > w.widest {
		w.widest = n
	}

	return 0, nil
}

func (w *capturingWriter) totals() (uint64, uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.reported, w.widest
}

// TestSenderStopsReportingHistory drives the interceptor with the same pattern
// and asserts that what reaches the publisher stays proportionate to what
// arrived.
func TestSenderStopsReportingHistory(t *testing.T) {
	var stats *Stats
	factory, err := NewSenderInterceptor(
		SendInterval(20*time.Millisecond),
		OnStats(func(s *Stats) { stats = s }),
	)
	require.NoError(t, err)

	i, err := factory.NewInterceptor("")
	require.NoError(t, err)
	defer func() { require.NoError(t, i.Close()) }()

	writer := &capturingWriter{}
	i.BindRTCPWriter(writer)

	reader := &fakeReader{seqs: arrivals(4000, 400, 8)}
	bound := i.BindRemoteStream(&interceptor.StreamInfo{
		SSRC: 1234,
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: sdp.TransportCCURI, ID: twccExtID},
		},
	}, reader)

	buf := make([]byte, 1500)
	var sent uint64
	for {
		if _, _, err := bound.Read(buf, interceptor.Attributes{}); err != nil {
			break
		}
		sent++

		// Roughly 80 arrivals a second against a 20ms feedback interval, so a
		// report covers a handful of packets, as it would in a live room.
		if sent%2 == 0 {
			time.Sleep(time.Millisecond)
		}
	}

	// Let the last few ticks land.
	time.Sleep(100 * time.Millisecond)

	reported, widest := writer.totals()
	require.NotNil(t, stats)

	// The point of the package: reports stay proportionate to arrivals rather
	// than growing into the whole history.
	require.Less(t, reported, sent*resyncRatio+resyncTolerance*stats.Feedback.Load(),
		"reported %d arrivals for %d received", reported, sent)
	require.Less(t, widest, uint64(200),
		"a single report carried %d arrivals", widest)

	// Deliberately no assertion that the recorder was rebuilt. It is not, any
	// more: the arrivals this pattern sends from four hundred sequence numbers
	// back are now kept out of the recorder by maxLateArrival, so the cursor is
	// never dragged backwards and there is nothing to recover from. The rebuild
	// remains as a backstop for a latch arriving by some other route, and what
	// matters here is the outcome above -- reports that stay proportionate to
	// what arrived -- however it is reached.
	require.Positive(t, stats.Late.Load(),
		"expected the late arrivals to have been kept out of the recorder")
}

// TestSenderLeavesAHealthyStreamAlone: a stream with ordinary reordering must
// not be resynced, because a rebuild costs the estimator a tick of feedback.
func TestSenderLeavesAHealthyStreamAlone(t *testing.T) {
	var stats *Stats
	factory, err := NewSenderInterceptor(
		SendInterval(20*time.Millisecond),
		OnStats(func(s *Stats) { stats = s }),
	)
	require.NoError(t, err)

	i, err := factory.NewInterceptor("")
	require.NoError(t, err)
	defer func() { require.NoError(t, i.Close()) }()

	writer := &capturingWriter{}
	i.BindRTCPWriter(writer)

	// Every twentieth pair swapped: reordering, and nothing more.
	seqs := make([]uint16, 0, 2000)
	for i := 0; i < 2000; i++ {
		seqs = append(seqs, uint16(i)) //nolint:gosec // G115
	}
	for i := 20; i < len(seqs); i += 20 {
		seqs[i], seqs[i-1] = seqs[i-1], seqs[i]
	}

	reader := &fakeReader{seqs: seqs}
	bound := i.BindRemoteStream(&interceptor.StreamInfo{
		SSRC: 1234,
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: sdp.TransportCCURI, ID: twccExtID},
		},
	}, reader)

	buf := make([]byte, 1500)
	var sent uint64
	for {
		if _, _, err := bound.Read(buf, interceptor.Attributes{}); err != nil {
			break
		}
		sent++
		if sent%2 == 0 {
			time.Sleep(time.Millisecond)
		}
	}

	time.Sleep(100 * time.Millisecond)

	require.NotNil(t, stats)
	require.Zero(t, stats.Resyncs.Load(), "a merely reordered stream was resynced")
	require.Positive(t, stats.Reported.Load())
}

func TestSequenceUnwrapperSurvivesTheWrap(t *testing.T) {
	var u sequenceUnwrapper

	require.Equal(t, int64(65530), u.unwrap(65530))
	require.Equal(t, int64(65535), u.unwrap(65535))
	require.Equal(t, int64(65536), u.unwrap(0))
	require.Equal(t, int64(65540), u.unwrap(4))

	// A sequence number from before the wrap still reads as being behind.
	require.Equal(t, int64(65531), u.unwrapTo(65531, 65540))
}

// TestSpanCountsWhatWasSentNotWhatArrivedInOrder is why loss is taken from the
// sequence numbers rather than from the feedback.
//
// Two streams share one transport sequence space and are read by goroutines of
// their own, so arrivals reach the recorder interleaved and often behind the
// high-water mark. Read off the feedback, that reordering looks like loss: a
// packet is described as not received in the report built before it landed, and
// as received in the next one. Read off the sequence numbers it is what it is,
// and a stream that loses nothing measures as losing nothing.
func TestSpanCountsWhatWasSentNotWhatArrivedInOrder(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lose  int // lose one in N, 0 for none
		delay int // deliver one in 3 this many slots late
	}{
		{"clean", 0, 0},
		{"heavily reordered, nothing lost", 0, 40},
		{"one in fifty lost, heavily reordered", 50, 40},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stats Stats
			var unwrapper sequenceUnwrapper
			var newest int64

			// The record path of the loop, which is what Span is built by.
			record := func(sn uint16) {
				if s := unwrapper.unwrap(sn); s > newest {
					if newest > 0 {
						stats.Span.Add(uint64(s - newest))
					} else {
						stats.Span.Add(1)
					}
					newest = s
				}
				stats.Recorded.Add(1)
			}

			const total = 20000
			var sent, lost uint64
			held := []uint16{}

			for i := range total {
				sn := uint16(i) //nolint:gosec // G115
				sent++

				if tc.lose > 0 && i%tc.lose == 0 {
					lost++

					continue
				}

				if tc.delay > 0 && i%3 == 0 {
					held = append(held, sn)

					continue
				}

				record(sn)

				if tc.delay > 0 && len(held) > 0 && i%tc.delay == 0 {
					for _, h := range held {
						record(h)
					}
					held = held[:0]
				}
			}
			for _, h := range held {
				record(h)
			}

			span, recorded := stats.Span.Load(), stats.Recorded.Load()
			var measured float64
			if span > 0 && span > recorded {
				measured = 100 * float64(span-recorded) / float64(span)
			}
			actual := 100 * float64(lost) / float64(sent)

			t.Logf("actual loss %.2f%%, measured %.2f%% (span %d, recorded %d)",
				actual, measured, span, recorded)
			require.InDelta(t, actual, measured, 0.5)
		})
	}
}

// endlessReader hands out arrivals as fast as they are asked for.
type endlessReader struct{ n atomic.Uint32 }

func (r *endlessReader) Read(buf []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
	sn := uint16(r.n.Add(1)) //nolint:gosec // G115

	ext := rtp.TransportCCExtension{TransportSequence: sn}
	payload, err := ext.Marshal()
	if err != nil {
		return 0, nil, err
	}

	pkt := &rtp.Packet{
		Header:  rtp.Header{Version: 2, SSRC: 1234, SequenceNumber: sn},
		Payload: []byte{0x00},
	}
	if err := pkt.Header.SetExtension(twccExtID, payload); err != nil {
		return 0, nil, err
	}

	raw, err := pkt.Marshal()
	if err != nil {
		return 0, nil, err
	}

	return copy(buf, raw), a, nil
}

// blockingWriter takes a long time over every feedback packet, as a transport
// with somewhere to be does.
type blockingWriter struct {
	writes atomic.Uint32
	hold   time.Duration
}

func (w *blockingWriter) Write(_ []rtcp.Packet, _ interceptor.Attributes) (int, error) {
	w.writes.Add(1)
	time.Sleep(w.hold)

	return 0, nil
}

// TestSlowFeedbackWriteDoesNotStallTheReadPath is the property the channel
// could not give.
//
// An arrival is recorded on the goroutine that read it, and the RTCP write
// happens with the recorder's lock let go, so a transport that takes half a
// second over a feedback packet costs the media path nothing. Handed over a
// channel instead -- pion's unbuffered, or buffered and full -- the reader
// waits on the goroutine doing that write. What waits behind the reader is the
// track's own buffer, which drops what it cannot hold, and those drops reach
// the publisher as loss it is told it caused.
func TestSlowFeedbackWriteDoesNotStallTheReadPath(t *testing.T) {
	factory, err := NewSenderInterceptor(SendInterval(10 * time.Millisecond))
	require.NoError(t, err)

	i, err := factory.NewInterceptor("")
	require.NoError(t, err)
	defer func() { require.NoError(t, i.Close()) }()

	writer := &blockingWriter{hold: 500 * time.Millisecond}
	i.BindRTCPWriter(writer)

	bound := i.BindRemoteStream(&interceptor.StreamInfo{
		SSRC: 1234,
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: sdp.TransportCCURI, ID: twccExtID},
		},
	}, &endlessReader{})

	buf := make([]byte, 1500)
	var worst time.Duration
	var reads int

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		start := time.Now()
		if _, _, err := bound.Read(buf, interceptor.Attributes{}); err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if took := time.Since(start); took > worst {
			worst = took
		}
		reads++

		time.Sleep(time.Millisecond)
	}

	require.Positive(t, writer.writes.Load(), "the writer was never exercised, so nothing was under test")
	require.Greater(t, reads, 500, "too few reads to say anything")

	// Generous, because this runs on a loaded CI box as well as a quiet one.
	// The failure it guards against is half a second, not a handful of
	// milliseconds.
	require.Less(t, worst, 100*time.Millisecond,
		"a read waited %s while the feedback writer was busy", worst)
}

// TestAWideMostlyEmptyReportIsCaught is the case the guard used to miss.
//
// The recorder falls a long way behind and then describes a very wide range of
// which almost nothing arrived: thousands of sequence numbers, a handful of
// receive deltas. Judged by the deltas that is a quiet tick and the guard never
// fires, which is how an emulator publishing to a thousand viewers came to take
// a hundred and eight copies of every packet it sent while the node reported
// everything healthy.
func TestAWideMostlyEmptyReportIsCaught(t *testing.T) {
	// One arrival every hundredth sequence number, which puts the arrivals per
	// tick and the width of the report in roughly the proportion the emulator
	// produced: a couple of thousand sequence numbers described, a few dozen
	// of them arrivals.
	recorder := twcc.NewRecorder(1)

	var us int64
	for i := range 4000 {
		if i%100 == 0 {
			recorder.Record(1234, uint16(i), us) //nolint:gosec // G115
		}
		us += 1000
	}

	pkts := recorder.BuildFeedbackPacket()
	require.NotEmpty(t, pkts)

	reported, span, _ := describe(pkts)
	t.Logf("span %d, arrivals reported %d", span, reported)

	require.Greater(t, span, reported*10,
		"the test is not producing a wide, mostly empty report")

	// What the guard is given each tick: a handful of arrivals.
	const recordedSince = 12

	require.False(t, reported > recordedSince*resyncRatio+resyncTolerance,
		"judged by arrivals this report looks quiet, which is the bug -- reported %d", reported)
	require.True(t, span > recordedSince*resyncRatio+resyncTolerance,
		"judged by span it must trip the guard -- span %d", span)
}

// TestFeedbackKeepsFlowingWhileTheGuardKeepsFiring is the failure the cooldown
// exists to prevent, and it is the one that matters most of the two.
//
// A publisher receiving feedback it half understands still has something to go
// on. A publisher receiving none at all has no acknowledged rate and no
// round-trip time and falls to its floor, which is what happened when the guard
// could fire on every tick: ten rebuilds a second and not one feedback packet
// sent.
func TestFeedbackKeepsFlowingWhileTheGuardKeepsFiring(t *testing.T) {
	var stats *Stats
	factory, err := NewSenderInterceptor(
		SendInterval(10*time.Millisecond),
		OnStats(func(s *Stats) { stats = s }),
	)
	require.NoError(t, err)

	i, err := factory.NewInterceptor("")
	require.NoError(t, err)
	defer func() { require.NoError(t, i.Close()) }()

	writer := &capturingWriter{}
	i.BindRTCPWriter(writer)

	// Arrivals scattered across a wide sequence range, so every report is wide
	// and mostly empty and the guard wants to fire continuously.
	seqs := make([]uint16, 0, 4000)
	for n := range 4000 {
		if n%60 == 0 {
			seqs = append(seqs, uint16(n)) //nolint:gosec // G115
		}
	}

	reader := &fakeReader{seqs: seqs}
	bound := i.BindRemoteStream(&interceptor.StreamInfo{
		SSRC: 1234,
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: sdp.TransportCCURI, ID: twccExtID},
		},
	}, reader)

	buf := make([]byte, 1500)
	for {
		if _, _, err := bound.Read(buf, interceptor.Attributes{}); err != nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	time.Sleep(50 * time.Millisecond)

	require.NotNil(t, stats)
	reported, _ := writer.totals()

	// The guard must have wanted to fire, or this proves nothing.
	require.Positive(t, stats.Resyncs.Load(), "the guard never fired, so the cooldown is not under test")

	// And the publisher must still have been told something.
	require.Positive(t, stats.Feedback.Load(),
		"no feedback was sent at all while the guard was firing")
	require.Positive(t, reported, "feedback was sent but described nothing")

	// At a 10ms interval over roughly a second and a half of arrivals, a one
	// second cooldown allows very few rebuilds. The failure was ten a second.
	require.Less(t, stats.Resyncs.Load(), uint64(6),
		"rebuilt %d times; the cooldown is not holding", stats.Resyncs.Load())
}

// TestDelayedStreamDoesNotDropLatePacketsOrStarveFeedback tests the scenario where
// two tracks (e.g. Video High and Audio) share the transport sequence space, but
// one track arrives delayed by 250 packets. With the monotonic recorder, all
// arrivals are recorded, feedback is sent every tick, and the publisher is not
// starved by rebuilds or 100x span explosion.
func TestDelayedStreamDoesNotDropLatePacketsOrStarveFeedback(t *testing.T) {
	var stats *Stats
	factory, err := NewSenderInterceptor(
		SendInterval(20*time.Millisecond),
		OnStats(func(s *Stats) { stats = s }),
	)
	require.NoError(t, err)

	i, err := factory.NewInterceptor("")
	require.NoError(t, err)
	defer func() { require.NoError(t, i.Close()) }()

	writer := &capturingWriter{}
	i.BindRTCPWriter(writer)

	// Stream 1 (fast, e.g. Video) runs sequence numbers 1..1000
	// Stream 2 (delayed, e.g. Audio) delivers sequence numbers late once stream is underway
	seqs := make([]uint16, 0, 1200)
	for sn := 1; sn < 1000; sn++ {
		seqs = append(seqs, uint16(sn)) //nolint:gosec // G115
		// Once stream is running past 300, interleave delayed packets from the lagging track (>200 behind)
		if sn >= 300 && sn%4 == 0 && sn-250 >= 50 {
			seqs = append(seqs, uint16(sn-250)) //nolint:gosec // G115
		}
	}

	reader := &fakeReader{seqs: seqs}
	bound := i.BindRemoteStream(&interceptor.StreamInfo{
		SSRC: 1234,
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{
			{URI: sdp.TransportCCURI, ID: twccExtID},
		},
	}, reader)

	buf := make([]byte, 1500)
	var sent uint64
	for {
		if _, _, err := bound.Read(buf, interceptor.Attributes{}); err != nil {
			break
		}
		sent++
		if sent%4 == 0 {
			time.Sleep(time.Millisecond)
		}
	}

	time.Sleep(100 * time.Millisecond)

	require.NotNil(t, stats)
	require.Positive(t, stats.Feedback.Load(), "feedback packets must have been sent")
	require.Equal(t, uint64(0), stats.Resyncs.Load(), "recorder should never have resynced/rebuilt")
	require.Positive(t, stats.Recorded.Load())
	require.Equal(t, sent, stats.Recorded.Load(), "all sent packets must have been recorded (never dropped)")

	reported, widest := writer.totals()
	require.Positive(t, reported, "feedback must have reported arrivals")
	// The reports must stay compact and not explode in width
	require.Less(t, widest, uint64(200), "feedback packet span exploded")
}
