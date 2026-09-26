// Package senderreport provides an RTCP Sender Report interceptor that eliminates
// extrapolation drift and guarantees strict monotonicity across reports.
//
// Unlike upstream pion/interceptor/pkg/report, this implementation anchors NTPTime
// and RTPTime directly to observed packet arrivals (per RFC 3550 §6.4.1) rather than
// extrapolating RTP timestamps forward using system wall-clock time. Under high SFU
// loads (e.g. 4,000+ viewers) with worker scheduling latency, wall-clock extrapolation
// overshoots true packet timestamps, causing libwebrtc's RtpToNtpEstimator to drop
// subsequent reports with "Newer RTCP SR report with older RTP timestamp, dropping".
//
// In addition, this implementation enforces strict monotonicity:
// 1. No reports are generated before media packets have been processed.
// 2. No redundant reports are generated if no new packets arrived between intervals.
// 3. Reports are suppressed if NTPTime <= lastSentNTP or RTPTime <= lastSentRTP (modulo 2^32).
package senderreport

import (
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/report"
	"github.com/pion/logging"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

const ntpEpochOffset = 2208988800

// toNTP converts a time.Time to a 64-bit NTP timestamp.
// The higher 32 bits are seconds since 1 Jan 1900 UTC.
// The lower 32 bits are fractional seconds.
func toNTP(t time.Time) uint64 {
	secs := uint64(t.Unix() + ntpEpochOffset)
	nanos := uint64(t.Nanosecond())
	// fractional seconds: (nanos * (1 << 32)) / 1,000,000,000
	frac := (nanos << 32) / 1_000_000_000
	return (secs << 32) | frac
}

// toTime converts a 64-bit NTP timestamp to time.Time.
func toTime(ntp uint64) time.Time {
	secs := int64((ntp >> 32) - ntpEpochOffset)
	frac := ntp & 0xFFFFFFFF
	nanos := (frac * 1_000_000_000) >> 32
	return time.Unix(secs, int64(nanos)).UTC()
}

// Ticker is an interface for *time.Ticker for use with the SenderTicker option.
type Ticker interface {
	Ch() <-chan time.Time
	Stop()
}

type timeTicker struct {
	*time.Ticker
}

func (t *timeTicker) Ch() <-chan time.Time {
	return t.C
}

// TickerFactory is a factory to create new tickers.
type TickerFactory func(d time.Duration) Ticker

// SenderOption can be used to configure SenderInterceptor.
type SenderOption func(r *SenderInterceptor) error

// SenderLog sets a logger for the interceptor.
func SenderLog(log logging.LeveledLogger) SenderOption {
	return func(r *SenderInterceptor) error {
		r.log = log
		return nil
	}
}

// SenderInterval sets send interval for the interceptor.
func SenderInterval(interval time.Duration) SenderOption {
	return func(r *SenderInterceptor) error {
		r.interval = interval
		return nil
	}
}

// SenderNow sets an alternative for the time.Now function.
func SenderNow(f func() time.Time) SenderOption {
	return func(r *SenderInterceptor) error {
		r.now = f
		return nil
	}
}

// SenderTicker sets an alternative for the time.NewTicker function.
func SenderTicker(f TickerFactory) SenderOption {
	return func(r *SenderInterceptor) error {
		r.newTicker = f
		return nil
	}
}

// SenderUseLatestPacket sets the interceptor to always use the latest packet, even
// if it appears to be out-of-order.
func SenderUseLatestPacket() SenderOption {
	return func(r *SenderInterceptor) error {
		r.useLatestPacket = true
		return nil
	}
}

// SenderStarted signals when the background reporting loop has started.
func SenderStarted(startedCh chan struct{}) SenderOption {
	return func(r *SenderInterceptor) error {
		r.started = startedCh
		return nil
	}
}

// SenderInterceptorFactory is an interceptor.Factory for a SenderInterceptor.
type SenderInterceptorFactory struct {
	opts []SenderOption
}

// NewInterceptor constructs a new SenderInterceptor.
func (s *SenderInterceptorFactory) NewInterceptor(_ string) (interceptor.Interceptor, error) {
	senderInterceptor := &SenderInterceptor{
		interval: 1 * time.Second,
		now:      time.Now,
		newTicker: func(d time.Duration) Ticker {
			return &timeTicker{time.NewTicker(d)}
		},
		log:   logging.NewDefaultLoggerFactory().NewLogger("sender_interceptor"),
		close: make(chan struct{}),
	}

	for _, opt := range s.opts {
		if err := opt(senderInterceptor); err != nil {
			return nil, err
		}
	}

	return senderInterceptor, nil
}

// NewSenderInterceptor returns a new SenderInterceptorFactory.
func NewSenderInterceptor(opts ...SenderOption) (*SenderInterceptorFactory, error) {
	return &SenderInterceptorFactory{opts: opts}, nil
}

// ConfigureRTCPReports configures receiver reports (from pion) and monotonic sender reports on the registry.
func ConfigureRTCPReports(i *interceptor.Registry, opts ...SenderOption) error {
	receiver, err := report.NewReceiverInterceptor()
	if err != nil {
		return err
	}

	sender, err := NewSenderInterceptor(opts...)
	if err != nil {
		return err
	}

	i.Add(receiver)
	i.Add(sender)

	return nil
}

// SenderInterceptor generates sender reports without wall-clock extrapolation.
type SenderInterceptor struct {
	interceptor.NoOp
	interval  time.Duration
	now       func() time.Time
	newTicker TickerFactory
	streams   sync.Map
	log       logging.LeveledLogger
	m         sync.Mutex
	wg        sync.WaitGroup
	close     chan struct{}
	started   chan struct{}

	useLatestPacket bool
}

func (s *SenderInterceptor) isClosed() bool {
	select {
	case <-s.close:
		return true
	default:
		return false
	}
}

// Close closes the interceptor.
func (s *SenderInterceptor) Close() error {
	defer s.wg.Wait()
	s.m.Lock()
	defer s.m.Unlock()

	if !s.isClosed() {
		close(s.close)
	}

	return nil
}

// BindRTCPWriter lets you modify any outgoing RTCP packets. It is called once per PeerConnection.
func (s *SenderInterceptor) BindRTCPWriter(writer interceptor.RTCPWriter) interceptor.RTCPWriter {
	s.m.Lock()
	defer s.m.Unlock()

	if s.isClosed() {
		return writer
	}

	s.wg.Add(1)
	go s.loop(writer)

	return writer
}

func (s *SenderInterceptor) loop(rtcpWriter interceptor.RTCPWriter) {
	defer s.wg.Done()

	ticker := s.newTicker(s.interval)
	defer ticker.Stop()
	if s.started != nil {
		close(s.started)
	}

	for {
		select {
		case <-ticker.Ch():
			now := s.now()
			s.streams.Range(func(_, value interface{}) bool {
				if stream, ok := value.(*senderStream); !ok {
					s.log.Warnf("failed to cast SenderInterceptor stream")
				} else if rep := stream.generateReport(now); rep != nil {
					if _, err := rtcpWriter.Write(
						[]rtcp.Packet{rep}, interceptor.Attributes{},
					); err != nil {
						s.log.Warnf("failed sending: %+v", err)
					}
				}

				return true
			})

		case <-s.close:
			return
		}
	}
}

// BindLocalStream intercepts outgoing RTP packets to maintain stream statistics.
func (s *SenderInterceptor) BindLocalStream(
	info *interceptor.StreamInfo, writer interceptor.RTPWriter,
) interceptor.RTPWriter {
	stream := newSenderStream(info.SSRC, info.ClockRate, s.useLatestPacket)
	s.streams.Store(info.SSRC, stream)

	return interceptor.RTPWriterFunc(func(header *rtp.Header, payload []byte, a interceptor.Attributes) (int, error) {
		stream.processRTP(s.now(), header, payload)
		return writer.Write(header, payload, a)
	})
}

// UnbindLocalStream cleans up stream statistics when a track is removed.
func (s *SenderInterceptor) UnbindLocalStream(info *interceptor.StreamInfo) {
	s.streams.Delete(info.SSRC)
}

type senderStream struct {
	ssrc      uint32
	clockRate float64
	m         sync.Mutex

	useLatestPacket bool

	// data from rtp packets
	lastRTPTimeRTP  uint32
	lastRTPTimeTime time.Time
	lastRTPSN       uint16
	packetCount     uint32
	octetCount      uint32

	// state from generated reports for monotonicity enforcement
	hasSentReport       bool
	lastSentNTP         uint64
	lastSentRTP         uint32
	lastSentPacketCount uint32
}

func newSenderStream(ssrc uint32, clockRate uint32, useLatestPacket bool) *senderStream {
	return &senderStream{
		ssrc:            ssrc,
		clockRate:       float64(clockRate),
		useLatestPacket: useLatestPacket,
	}
}

func (stream *senderStream) processRTP(now time.Time, header *rtp.Header, payload []byte) {
	stream.m.Lock()
	defer stream.m.Unlock()

	diff := header.SequenceNumber - stream.lastRTPSN
	if stream.useLatestPacket || stream.packetCount == 0 || (diff > 0 && diff < (1<<15)) {
		// Told to consider every packet, or this was the first packet, or it's in-order
		stream.lastRTPSN = header.SequenceNumber
		// Anchor timestamp only on the first packet of a frame to prevent multi-packet
		// transmission delay within a frame from skewing the RTP-to-NTP clock estimator.
		if header.Timestamp != stream.lastRTPTimeRTP || stream.packetCount == 0 {
			stream.lastRTPTimeRTP = header.Timestamp
			stream.lastRTPTimeTime = now
		}
	}

	stream.packetCount++
	stream.octetCount += uint32(len(payload)) //nolint:gosec // G115
}

func (stream *senderStream) generateReport(_ time.Time) *rtcp.SenderReport {
	stream.m.Lock()
	defer stream.m.Unlock()

	// If no packets have been processed at all, or no new packets arrived since the last report,
	// do not generate a report.
	if stream.packetCount == 0 || stream.packetCount == stream.lastSentPacketCount {
		return nil
	}

	ntpTime := toNTP(stream.lastRTPTimeTime)
	rtpTime := stream.lastRTPTimeRTP

	if stream.hasSentReport {
		// Strict monotonicity checks:
		// 1. NTP time must advance.
		if ntpTime <= stream.lastSentNTP {
			return nil
		}
		// 2. RTP timestamp must advance (accounting for 32-bit rollover).
		diff := int32(rtpTime - stream.lastSentRTP)
		if diff <= 0 {
			return nil
		}
	}

	stream.hasSentReport = true
	stream.lastSentNTP = ntpTime
	stream.lastSentRTP = rtpTime
	stream.lastSentPacketCount = stream.packetCount

	return &rtcp.SenderReport{
		SSRC:        stream.ssrc,
		NTPTime:     ntpTime,
		RTPTime:     rtpTime,
		PacketCount: stream.packetCount,
		OctetCount:  stream.octetCount,
	}
}
