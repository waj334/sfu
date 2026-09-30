package sfu

import (
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

// Counting outgoing RTP instead of recording it has to report what the
// recorder would have: the same packets and bytes sent, and the same view of
// a receiver report, which needs the first packet's sequence number.
func TestCountingRecorderReportsWhatTheRecorderWould(t *testing.T) {
	const ssrc = 1234

	reference := defaultStatsRecorderFactory(ssrc, 90000)
	counting := newCountingRecorder(ssrc, 90000)
	reference.Start()
	counting.Start()
	defer reference.Stop()
	defer counting.Stop()

	now := time.Now()
	for i := 0; i < 500; i++ {
		header := &rtp.Header{
			Version:        2,
			SSRC:           ssrc,
			SequenceNumber: uint16(60000 + i), // wraps
			Timestamp:      uint32(i * 3000),
		}
		if i%3 == 0 {
			_ = header.SetExtension(1, []byte{byte(i), 2, 3})
		}
		payload := make([]byte, 100+i%900)
		reference.QueueOutgoingRTP(now, header, payload, interceptor.Attributes{})
		counting.QueueOutgoingRTP(now, header, payload, interceptor.Attributes{})
	}

	// Another stream's packet is not this one's to count.
	other := &rtp.Header{Version: 2, SSRC: ssrc + 1}
	reference.QueueOutgoingRTP(now, other, make([]byte, 50), interceptor.Attributes{})
	counting.QueueOutgoingRTP(now, other, make([]byte, 50), interceptor.Attributes{})

	report := []rtcp.Packet{&rtcp.ReceiverReport{Reports: []rtcp.ReceptionReport{{
		SSRC:               ssrc,
		FractionLost:       12,
		TotalLost:          7,
		LastSequenceNumber: 1<<16 + uint32(uint16(60000+499)),
		Jitter:             90,
	}}}}
	reference.QueueIncomingRTCP(now, marshalRTCP(t, report), interceptor.Attributes{})
	counting.QueueIncomingRTCP(now, marshalRTCP(t, report), interceptor.Attributes{})

	want, got := reference.GetStats(), counting.GetStats()
	require.Equal(t, want.OutboundRTPStreamStats.PacketsSent, got.OutboundRTPStreamStats.PacketsSent)
	require.Equal(t, want.OutboundRTPStreamStats.BytesSent, got.OutboundRTPStreamStats.BytesSent)
	require.Equal(t, want.OutboundRTPStreamStats.HeaderBytesSent, got.OutboundRTPStreamStats.HeaderBytesSent)
	require.Equal(t, want.RemoteInboundRTPStreamStats.PacketsReceived, got.RemoteInboundRTPStreamStats.PacketsReceived)
	require.Equal(t, want.RemoteInboundRTPStreamStats.PacketsLost, got.RemoteInboundRTPStreamStats.PacketsLost)
	require.Equal(t, want.RemoteInboundRTPStreamStats.FractionLost, got.RemoteInboundRTPStreamStats.FractionLost)
	require.EqualValues(t, 500, got.OutboundRTPStreamStats.PacketsSent)
}

// The point of it: a packet sent costs no allocation once the first is in.
func TestCountingRecorderDoesNotAllocatePerPacket(t *testing.T) {
	r := newCountingRecorder(1, 90000)
	r.Start()
	defer r.Stop()

	header := &rtp.Header{Version: 2, SSRC: 1}
	_ = header.SetExtension(1, []byte{1, 2, 3})
	payload := make([]byte, 1100)
	attr := interceptor.Attributes{}
	r.QueueOutgoingRTP(time.Now(), header, payload, attr)

	now := time.Now()
	allocs := testing.AllocsPerRun(1000, func() {
		r.QueueOutgoingRTP(now, header, payload, attr)
	})
	require.Zero(t, allocs)
}

func marshalRTCP(t *testing.T, pkts []rtcp.Packet) []byte {
	t.Helper()
	b, err := rtcp.Marshal(pkts)
	require.NoError(t, err)
	return b
}
