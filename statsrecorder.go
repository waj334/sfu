package sfu

import (
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/stats"
	"github.com/pion/rtp"
)

// countingRecorder is pion's stats recorder with its outgoing RTP counted
// rather than recorded.
//
// The default recorder clones the header of every packet a node sends, to
// take three numbers from it under a lock. A node sends one packet per viewer
// per packet a host sends, so that clone was the largest single source of the
// node's garbage: 8GB of the 60GB allocated in a few minutes at ~3,800
// viewers, and the garbage collector it fed took over a quarter of the CPU
// once the node was loaded. What those packets contribute is PacketsSent,
// BytesSent and HeaderBytesSent, which counters give exactly, and the first
// packet's sequence number, which the recorder needs to turn a receiver
// report's highest sequence number into packets received; that one packet is
// still handed over. Everything else — RTCP both ways, incoming RTP — goes to
// the recorder as before.
type countingRecorder struct {
	stats.Recorder

	ssrc uint32

	handedOver  atomic.Bool
	packets     atomic.Uint64
	bytes       atomic.Uint64
	headerBytes atomic.Uint64
}

// defaultStatsRecorderFactory is the one pion's stats interceptor uses by
// default. Not exported by pion, so it is read from an interceptor made with
// no options, once.
var defaultStatsRecorderFactory = func() stats.RecorderFactory {
	factory, err := stats.NewInterceptor()
	if err != nil {
		panic(err)
	}
	built, err := factory.NewInterceptor("")
	if err != nil {
		panic(err)
	}
	return built.(*stats.Interceptor).RecorderFactory
}()

// newCountingRecorder is a stats.RecorderFactory: see countingRecorder.
func newCountingRecorder(ssrc uint32, clockRate float64) stats.Recorder {
	return &countingRecorder{
		Recorder: defaultStatsRecorderFactory(ssrc, clockRate),
		ssrc:     ssrc,
	}
}

func (r *countingRecorder) QueueOutgoingRTP(ts time.Time, header *rtp.Header, payload []byte, attr interceptor.Attributes) {
	if header.SSRC != r.ssrc {
		return
	}

	// The first one to the recorder, for its sequence number: see above.
	if !r.handedOver.Load() && r.handedOver.CompareAndSwap(false, true) {
		r.Recorder.QueueOutgoingRTP(ts, header, payload, attr)
		return
	}

	headerSize := header.MarshalSize()
	r.packets.Add(1)
	r.bytes.Add(uint64(headerSize + len(payload)))
	r.headerBytes.Add(uint64(headerSize))
}

func (r *countingRecorder) GetStats() stats.Stats {
	s := r.Recorder.GetStats()
	s.OutboundRTPStreamStats.PacketsSent += r.packets.Load()
	s.OutboundRTPStreamStats.BytesSent += r.bytes.Load()
	s.OutboundRTPStreamStats.HeaderBytesSent += r.headerBytes.Load()
	return s
}
