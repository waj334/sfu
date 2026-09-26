package senderreport

import (
	"sync"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/logging"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockTime struct {
	mu sync.Mutex
	t  time.Time
}

func (m *mockTime) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.t
}

func (m *mockTime) Set(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.t = t
}

func (m *mockTime) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.t = m.t.Add(d)
}

type mockTicker struct {
	ch chan time.Time
}

func newMockTicker() *mockTicker {
	return &mockTicker{ch: make(chan time.Time, 10)}
}

func (m *mockTicker) Ch() <-chan time.Time {
	return m.ch
}

func (m *mockTicker) Stop() {}

func (m *mockTicker) Tick(t time.Time) {
	m.ch <- t
}

type mockRTCPWriter struct {
	ch chan []rtcp.Packet
}

func newMockRTCPWriter() *mockRTCPWriter {
	return &mockRTCPWriter{ch: make(chan []rtcp.Packet, 100)}
}

func (m *mockRTCPWriter) Write(pkts []rtcp.Packet, _ interceptor.Attributes) (int, error) {
	m.ch <- pkts
	return len(pkts), nil
}

type mockRTPWriter struct{}

func (m *mockRTPWriter) Write(header *rtp.Header, payload []byte, a interceptor.Attributes) (int, error) {
	return len(payload), nil
}

func TestToNTPAndToTime(t *testing.T) {
	refTime := time.Date(2026, 9, 25, 20, 0, 0, 500_000_000, time.UTC)
	ntp := toNTP(refTime)
	require.NotZero(t, ntp)

	recovered := toTime(ntp)
	// Round trip precision within 1 microsecond
	assert.WithinDuration(t, refTime, recovered, time.Microsecond)
}

func TestSenderInterceptor_NoReportBeforePackets(t *testing.T) {
	mt := &mockTime{t: time.Date(2026, 9, 25, 20, 0, 0, 0, time.UTC)}
	ticker := newMockTicker()
	started := make(chan struct{})

	f, err := NewSenderInterceptor(
		SenderInterval(10*time.Millisecond),
		SenderNow(mt.Now),
		SenderTicker(func(d time.Duration) Ticker { return ticker }),
		SenderStarted(started),
		SenderLog(logging.NewDefaultLoggerFactory().NewLogger("test")),
	)
	require.NoError(t, err)

	i, err := f.NewInterceptor("")
	require.NoError(t, err)

	rtcpWriter := newMockRTCPWriter()
	i.BindRTCPWriter(rtcpWriter)

	streamInfo := &interceptor.StreamInfo{SSRC: 12345, ClockRate: 90000}
	_ = i.BindLocalStream(streamInfo, &mockRTPWriter{})

	<-started

	// Trigger ticker without any RTP packets sent
	ticker.Tick(mt.Now())

	select {
	case pkts := <-rtcpWriter.ch:
		t.Fatalf("expected no RTCP packets before any RTP data, got: %+v", pkts)
	case <-time.After(50 * time.Millisecond):
		// Success: no report generated when packetCount == 0
	}

	require.NoError(t, i.Close())
}

func TestSenderInterceptor_MonotonicReports(t *testing.T) {
	startTime := time.Date(2026, 9, 25, 20, 0, 0, 0, time.UTC)
	mt := &mockTime{t: startTime}
	ticker := newMockTicker()
	started := make(chan struct{})

	f, err := NewSenderInterceptor(
		SenderInterval(10*time.Millisecond),
		SenderNow(mt.Now),
		SenderTicker(func(d time.Duration) Ticker { return ticker }),
		SenderStarted(started),
	)
	require.NoError(t, err)

	i, err := f.NewInterceptor("")
	require.NoError(t, err)

	rtcpWriter := newMockRTCPWriter()
	i.BindRTCPWriter(rtcpWriter)

	streamInfo := &interceptor.StreamInfo{SSRC: 99999, ClockRate: 90000}
	rtpWriter := i.BindLocalStream(streamInfo, &mockRTPWriter{})

	<-started

	// 1. Send first batch of RTP packets at t0
	packet1Time := mt.Now()
	for seq := uint16(0); seq < 5; seq++ {
		_, err := rtpWriter.Write(&rtp.Header{
			SequenceNumber: seq,
			Timestamp:      1000,
			SSRC:           99999,
		}, []byte{0x01, 0x02}, interceptor.Attributes{})
		require.NoError(t, err)
	}

	// Advance time and tick
	mt.Advance(1 * time.Second)
	ticker.Tick(mt.Now())

	var sr1 *rtcp.SenderReport
	select {
	case pkts := <-rtcpWriter.ch:
		require.Len(t, pkts, 1)
		var ok bool
		sr1, ok = pkts[0].(*rtcp.SenderReport)
		require.True(t, ok)
		assert.Equal(t, uint32(99999), sr1.SSRC)
		assert.Equal(t, uint32(1000), sr1.RTPTime)
		assert.Equal(t, toNTP(packet1Time), sr1.NTPTime)
		assert.Equal(t, uint32(5), sr1.PacketCount)
		assert.Equal(t, uint32(10), sr1.OctetCount)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for first sender report")
	}

	// 2. Idle tick: no new packets have been written
	mt.Advance(1 * time.Second)
	ticker.Tick(mt.Now())

	select {
	case pkts := <-rtcpWriter.ch:
		t.Fatalf("expected no redundant report during idle period, got: %+v", pkts)
	case <-time.After(50 * time.Millisecond):
		// Success: idle suppressed
	}

	// 3. Send second batch of RTP packets at newer timestamp
	packet2Time := mt.Now()
	for seq := uint16(5); seq < 10; seq++ {
		_, err := rtpWriter.Write(&rtp.Header{
			SequenceNumber: seq,
			Timestamp:      91000,
			SSRC:           99999,
		}, []byte{0x03, 0x04}, interceptor.Attributes{})
		require.NoError(t, err)
	}

	mt.Advance(1 * time.Second)
	ticker.Tick(mt.Now())

	var sr2 *rtcp.SenderReport
	select {
	case pkts := <-rtcpWriter.ch:
		require.Len(t, pkts, 1)
		var ok bool
		sr2, ok = pkts[0].(*rtcp.SenderReport)
		require.True(t, ok)
		assert.Equal(t, uint32(99999), sr2.SSRC)
		assert.Equal(t, uint32(91000), sr2.RTPTime)
		assert.Equal(t, toNTP(packet2Time), sr2.NTPTime)
		assert.Equal(t, uint32(10), sr2.PacketCount)
		assert.Equal(t, uint32(20), sr2.OctetCount)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for second sender report")
	}

	// Verify strict monotonicity across reports
	assert.Greater(t, sr2.NTPTime, sr1.NTPTime, "NTP time must be strictly monotonic")
	assert.Positive(t, int32(sr2.RTPTime-sr1.RTPTime), "RTP timestamp must be strictly monotonic")

	require.NoError(t, i.Close())
}

func TestSenderInterceptor_RTPTimestampRollover(t *testing.T) {
	mt := &mockTime{t: time.Date(2026, 9, 25, 20, 0, 0, 0, time.UTC)}
	ticker := newMockTicker()
	started := make(chan struct{})

	f, err := NewSenderInterceptor(
		SenderInterval(10*time.Millisecond),
		SenderNow(mt.Now),
		SenderTicker(func(d time.Duration) Ticker { return ticker }),
		SenderStarted(started),
	)
	require.NoError(t, err)

	i, err := f.NewInterceptor("")
	require.NoError(t, err)

	rtcpWriter := newMockRTCPWriter()
	i.BindRTCPWriter(rtcpWriter)

	streamInfo := &interceptor.StreamInfo{SSRC: 55555, ClockRate: 90000}
	rtpWriter := i.BindLocalStream(streamInfo, &mockRTPWriter{})

	<-started

	// 1. Packet near uint32 max
	_, err = rtpWriter.Write(&rtp.Header{
		SequenceNumber: 100,
		Timestamp:      0xFFFFFF00,
		SSRC:           55555,
	}, []byte{0x00}, interceptor.Attributes{})
	require.NoError(t, err)

	mt.Advance(1 * time.Second)
	ticker.Tick(mt.Now())

	var sr1 *rtcp.SenderReport
	select {
	case pkts := <-rtcpWriter.ch:
		require.Len(t, pkts, 1)
		sr1 = pkts[0].(*rtcp.SenderReport)
		assert.Equal(t, uint32(0xFFFFFF00), sr1.RTPTime)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for first sender report")
	}

	// 2. Packet that rolled over 0
	mt.Advance(100 * time.Millisecond)
	_, err = rtpWriter.Write(&rtp.Header{
		SequenceNumber: 101,
		Timestamp:      0x00000100, // rolled over
		SSRC:           55555,
	}, []byte{0x00}, interceptor.Attributes{})
	require.NoError(t, err)

	mt.Advance(1 * time.Second)
	ticker.Tick(mt.Now())

	select {
	case pkts := <-rtcpWriter.ch:
		require.Len(t, pkts, 1)
		sr2 := pkts[0].(*rtcp.SenderReport)
		assert.Equal(t, uint32(0x00000100), sr2.RTPTime)
		assert.Greater(t, sr2.NTPTime, sr1.NTPTime)
		// Rollover diff is positive
		assert.Positive(t, int32(sr2.RTPTime-sr1.RTPTime))
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for rolled-over sender report")
	}

	require.NoError(t, i.Close())
}

func TestSenderInterceptor_OutOfOrderSuppression(t *testing.T) {
	mt := &mockTime{t: time.Date(2026, 9, 25, 20, 0, 0, 0, time.UTC)}
	ticker := newMockTicker()
	started := make(chan struct{})

	f, err := NewSenderInterceptor(
		SenderInterval(10*time.Millisecond),
		SenderNow(mt.Now),
		SenderTicker(func(d time.Duration) Ticker { return ticker }),
		SenderStarted(started),
	)
	require.NoError(t, err)

	i, err := f.NewInterceptor("")
	require.NoError(t, err)

	rtcpWriter := newMockRTCPWriter()
	i.BindRTCPWriter(rtcpWriter)

	streamInfo := &interceptor.StreamInfo{SSRC: 77777, ClockRate: 90000}
	rtpWriter := i.BindLocalStream(streamInfo, &mockRTPWriter{})

	<-started

	// Write packet with seq 10, ts 50000
	_, err = rtpWriter.Write(&rtp.Header{
		SequenceNumber: 10,
		Timestamp:      50000,
		SSRC:           77777,
	}, []byte{0x00}, interceptor.Attributes{})
	require.NoError(t, err)

	mt.Advance(1 * time.Second)
	ticker.Tick(mt.Now())

	var sr1 *rtcp.SenderReport
	select {
	case pkts := <-rtcpWriter.ch:
		sr1 = pkts[0].(*rtcp.SenderReport)
		assert.Equal(t, uint32(50000), sr1.RTPTime)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for SR")
	}

	// Now deliver an out-of-order packet with seq 9, ts 45000
	mt.Advance(100 * time.Millisecond)
	_, err = rtpWriter.Write(&rtp.Header{
		SequenceNumber: 9,
		Timestamp:      45000,
		SSRC:           77777,
	}, []byte{0x00}, interceptor.Attributes{})
	require.NoError(t, err)

	// Tick: packetCount increased (from 1 to 2), but lastRTPTimeRTP was NOT updated to 45000
	// because seq 9 < seq 10. Thus candidate RTP timestamp remains 50000.
	// Because 50000 - 50000 == 0 <= 0, monotonicity guard suppresses emission!
	mt.Advance(1 * time.Second)
	ticker.Tick(mt.Now())

	select {
	case pkts := <-rtcpWriter.ch:
		t.Fatalf("expected no report for out-of-order packet without timestamp advancement, got: %+v", pkts)
	case <-time.After(50 * time.Millisecond):
		// Success: older timestamp was not emitted
	}

	require.NoError(t, i.Close())
}
