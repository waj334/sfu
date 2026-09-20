// Package twccsender generates transport-wide congestion control feedback for
// a publisher, and keeps generating feedback the publisher can actually use.
//
// It is pion's twcc sender interceptor with two things changed: a watch on how
// much the recorder is reporting, and a rebuild of the recorder when it starts
// reporting history instead of new arrivals; and arrivals recorded on the
// goroutine that read them rather than handed to the feedback goroutine over a
// channel.
//
// The second is there because the channel could not be made safe. pion's is
// unbuffered, so every arrival parks the goroutine reading the track until the
// feedback goroutine is scheduled. Buffering it only moves the problem: when
// the buffer fills the reader waits anyway, and a blocked reader does not stop
// packets arriving, it stops them being taken out of the track's buffer, which
// overflows and drops them where nothing counts them. Those drops are then
// reported to the publisher as loss it caused, and it slows down. Dropping
// arrivals here instead is no better, for the same reason by a shorter route.
// Recording under a lock costs a mutex on a path that already does far more
// per packet, and the media read path never waits on anything.
//
// Why that is needed. pion's Recorder only discards packets it has already
// reported while its cursor has caught up with the newest arrival
// (maybeCullOldPackets requires startSequenceNumber >= EndSequenceNumber).
// Should one feedback packet fail to carry the cursor to the live edge, the
// cursor stays behind, culling never runs again, the arrival-time map grows
// without bound, and every tick from then on re-reports the whole of it. There
// is no path in the Recorder that recovers from that.
//
// What it costs is the publisher's uplink, which is the surprising part. A
// receiver discards a report for a packet it has already had acknowledged, so
// once the reports are history the send-side estimator is left with no
// acknowledged rate and no round-trip time at all, and falls to its configured
// floor. Measured on a 1,000-viewer load test: 4,458 packet reports a second
// for a stream sending 79 packets a second, a 56x amplification that grew for
// as long as the room was up, the publisher's estimate pinned at its 177 kbps
// minimum, and video at 28 kbps and 14 fps on a link with megabits to spare.
package twccsender

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/logging"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
)

const (
	// How often feedback is sent. pion's default, and what a send-side
	// estimator expects.
	defaultInterval = 100 * time.Millisecond

	// How much more than the tick's own arrivals a report may cover before the
	// recorder is considered to be reporting history.
	//
	// A report covers the packets that arrived since the last one, plus
	// whatever reordering dragged back in. Genuine reordering costs a few
	// packets and settles within a tick or two; the failure this guards
	// against costs hundreds and never settles. Both terms are needed: the
	// multiplier keeps a busy stream from tripping it, and the constant keeps
	// a nearly idle one -- where two arrivals and eighteen reports is a ratio
	// and not a problem -- from tripping it either.
	resyncRatio     = 3
	resyncTolerance = 48

	// The least time between two rebuilds.
	//
	// A rebuild costs the publisher that tick's feedback, which is affordable
	// once in a while and ruinous every time. Without this the guard can fire
	// on every tick -- it does, when whatever drags the recorder backwards is
	// still dragging it the moment the new one starts -- and then the
	// publisher receives no feedback at all, which is a worse thing to do to
	// it than sending feedback it half understands. Measured on an emulator
	// publishing to a thousand viewers: ten rebuilds a second, not one
	// feedback packet sent, the estimate pinned at its floor.
	resyncCooldown = time.Second
)

// Stats is what the interceptor did over the life of one peer connection. It is
// read while the interceptor is running, so every field is loaded atomically.
type Stats struct {
	// Recorded is arrivals handed to the recorder.
	Recorded atomic.Uint64

	// Reported is packet arrivals described by the feedback that was sent. In
	// health it tracks Recorded; it is the excess that matters.
	Reported atomic.Uint64

	// Span is how many transport-wide sequence numbers the publisher has used,
	// which is how many packets it has sent. Against Recorded -- how many
	// turned up -- the difference is its uplink loss, the one property of that
	// link this node cannot otherwise see: loss on the way in happens before
	// anything here counts it, so the node's own receive counters are silent
	// about it.
	//
	// Taken from the sequence numbers rather than from the feedback, because
	// the feedback cannot answer it. A report describes a range as it stands
	// when it is built, so a packet that arrives a moment late is described as
	// not received and then, in a later report, as received. Counting the
	// first mention makes reordering look like loss; counting every mention
	// makes it look like far more. Sequence numbers have neither problem:
	// the publisher issues them in order, once each, whatever order they turn
	// up in.
	Span atomic.Uint64

	// Feedback is feedback packets written.
	Feedback atomic.Uint64

	// Resyncs is how many times the recorder was rebuilt because it had
	// started reporting history. Anything other than zero is worth looking at;
	// a number that climbs steadily means something is dragging the recorder
	// backwards and the rebuild is all that is holding the uplink up.
	Resyncs atomic.Uint64

	// Late is arrivals that came so far behind the newest that they were kept
	// out of the recorder; see maxLateArrival.
	//
	// They are still arrivals and still counted as such. A number that climbs
	// says something upstream is delivering a publisher's packets badly out of
	// order -- separate read paths for the tracks sharing one sequence space
	// is the usual reason -- and that is worth knowing even though the
	// recorder is no longer harmed by it.
	Late atomic.Uint64

	// Lag is how far behind the newest arrival the most recent report began,
	// in packets. The live measure of the condition Resyncs counts.
	Lag atomic.Int64

	// Closed is set when the peer connection these belong to has gone. The
	// counters stay readable afterwards, so a caller collecting them can fold
	// in the last of a departed publisher's numbers before letting go of it.
	Closed atomic.Bool
}

// Option configures a SenderInterceptor.
type Option func(*SenderInterceptor) error

// SendInterval sets how often feedback is sent.
func SendInterval(interval time.Duration) Option {
	return func(s *SenderInterceptor) error {
		s.interval = interval

		return nil
	}
}

// WithLogger sets the logger. A nil logger leaves the default in place.
func WithLogger(log logging.LeveledLogger) Option {
	return func(s *SenderInterceptor) error {
		if log != nil {
			s.log = log
		}

		return nil
	}
}

// OnStats hands the caller the stats of each peer connection's interceptor as
// it is created, so they can be read for as long as it runs.
func OnStats(fn func(*Stats)) Option {
	return func(s *SenderInterceptor) error {
		s.onStats = fn

		return nil
	}
}

// NewSenderInterceptor returns a factory for SenderInterceptor.
func NewSenderInterceptor(opts ...Option) (*SenderInterceptorFactory, error) {
	return &SenderInterceptorFactory{opts: opts}, nil
}

// SenderInterceptorFactory builds one SenderInterceptor per peer connection.
type SenderInterceptorFactory struct {
	opts []Option
}

var defaultLoggerFactory = logging.NewDefaultLoggerFactory()

// NewInterceptor constructs a SenderInterceptor.
func (f *SenderInterceptorFactory) NewInterceptor(_ string) (interceptor.Interceptor, error) {
	s := &SenderInterceptor{
		log:       defaultLoggerFactory.NewLogger("twccsender"),
		close:     make(chan struct{}),
		interval:  defaultInterval,
		startTime: time.Now(),
		stats:     &Stats{},
	}

	for _, opt := range f.opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}

	if s.onStats != nil {
		s.onStats(s.stats)
	}

	return s, nil
}

// SenderInterceptor sends transport-wide congestion control feedback for the
// streams it is bound to.
type SenderInterceptor struct {
	interceptor.NoOp

	log logging.LeveledLogger

	m     sync.Mutex
	wg    sync.WaitGroup
	close chan struct{}

	interval  time.Duration
	startTime time.Time

	// rec guards the recorder and the counters kept alongside it. It is taken
	// by a media read path for the length of one Record and by the feedback
	// goroutine for the length of one build, and by nothing else; the RTCP
	// write happens after it is let go.
	rec            sync.Mutex
	recorder       *Recorder
	newestRecorded int64
	unwrapper      sequenceUnwrapper
	recordedSince  uint64
	lastResync     time.Time

	stats   *Stats
	onStats func(*Stats)
}

// Stats is what this interceptor has done so far.
func (s *SenderInterceptor) Stats() *Stats {
	return s.stats
}

// BindRTCPWriter starts the feedback goroutine. Called once per peer
// connection.
func (s *SenderInterceptor) BindRTCPWriter(writer interceptor.RTCPWriter) interceptor.RTCPWriter {
	s.m.Lock()
	defer s.m.Unlock()

	if s.isClosed() {
		return writer
	}

	s.rec.Lock()
	s.recorder = newRecorder(rand.Uint32()) // #nosec G404 -- an SSRC, not a secret
	s.rec.Unlock()

	s.wg.Add(1)
	go s.loop(writer)

	return writer
}

// BindRemoteStream records the transport-wide sequence number of every arrival
// on a stream that carries one.
func (s *SenderInterceptor) BindRemoteStream(
	info *interceptor.StreamInfo, reader interceptor.RTPReader,
) interceptor.RTPReader {
	var hdrExtID uint8
	for _, e := range info.RTPHeaderExtensions {
		if e.URI == sdp.TransportCCURI {
			hdrExtID = uint8(e.ID) //nolint:gosec // G115 -- extension ids are 1..14

			break
		}
	}

	// Zero is not a valid extension id, so it stands for "this stream does not
	// carry one" and there is nothing for us to read.
	if hdrExtID == 0 {
		return reader
	}

	return interceptor.RTPReaderFunc(
		func(buf []byte, attributes interceptor.Attributes) (int, interceptor.Attributes, error) {
			i, attr, err := reader.Read(buf, attributes)
			if err != nil {
				return 0, nil, err
			}

			if attr == nil {
				attr = make(interceptor.Attributes)
			}

			header, err := attr.GetRTPHeader(buf[:i])
			if err != nil {
				return 0, nil, err
			}

			ext := header.GetExtension(hdrExtID)
			if ext == nil {
				return i, attr, nil
			}

			var tccExt rtp.TransportCCExtension
			if err := tccExt.Unmarshal(ext); err != nil {
				return 0, nil, err
			}

			// Recorded here, on this goroutine, rather than handed to the
			// feedback goroutine. See the note on the package: the read path
			// must not wait, because what waits behind it is the track's own
			// buffer, and what that does when it fills is drop packets the
			// publisher is then told it lost.
			s.record(info.SSRC, tccExt.TransportSequence)

			return i, attr, nil
		},
	)
}

// Close closes the interceptor.
func (s *SenderInterceptor) Close() error {
	defer s.wg.Wait()

	s.m.Lock()
	defer s.m.Unlock()

	if !s.isClosed() {
		close(s.close)
	}

	s.rec.Lock()
	s.recorder = nil
	s.rec.Unlock()

	s.stats.Closed.Store(true)

	return nil
}

func (s *SenderInterceptor) isClosed() bool {
	select {
	case <-s.close:
		return true
	default:
		return false
	}
}

// record takes one arrival, on the goroutine that read it.
func (s *SenderInterceptor) record(ssrc uint32, sequenceNumber uint16) {
	arrival := time.Since(s.startTime).Microseconds()

	s.rec.Lock()
	defer s.rec.Unlock()

	// Nil once the interceptor is closed, so a read still in flight finishes
	// without recording into a recorder nobody will ever build from.
	if s.recorder == nil {
		return
	}

	// The sequence numbers between the last high-water mark and this one are
	// packets the publisher sent. They are counted as it passes them, so an
	// arrival that comes in behind the mark adds nothing here and reordering
	// cannot be mistaken for the publisher sending more.
	sn := s.unwrapper.unwrap(sequenceNumber)
	if sn > s.newestRecorded {
		if s.newestRecorded > 0 {
			s.stats.Span.Add(uint64(sn - s.newestRecorded))
		} else {
			s.stats.Span.Add(1)
		}

		s.newestRecorded = sn
	}

	s.recordedSince++
	s.stats.Recorded.Add(1)

	if s.recorder.IsLate(sequenceNumber) {
		s.stats.Late.Add(1)
	}

	s.recorder.Record(ssrc, sequenceNumber, arrival)
}

// buildFeedback is one tick's worth of feedback, or nothing.
//
// Nothing when there is nothing to say, and nothing when what the recorder
// wants to say is its own history -- in which case it is rebuilt here and the
// next tick starts again at the live edge.
//
// The packets are returned rather than written, so that the write happens with
// the lock let go: writing waits on a transport, and waiting on a transport
// while holding the lock would stop the media read path for as long as it took.
func (s *SenderInterceptor) buildFeedback() []rtcp.Packet {
	s.rec.Lock()
	defer s.rec.Unlock()

	if s.recorder == nil {
		return nil
	}

	pkts := s.recorder.BuildFeedbackPacket()
	if len(pkts) == 0 {
		s.recordedSince = 0

		return nil
	}

	reported, span, base := describe(pkts)
	s.stats.Lag.Store(s.newestRecorded - s.unwrapper.unwrapTo(base, s.newestRecorded))

	// Judged on the span rather than on the arrivals reported. The two agree
	// on a healthy stream, where a report covers what arrived and little else.
	// They part company exactly when it matters: a recorder two thousand
	// packets behind reports two thousand statuses of which forty arrived, and
	// read by the arrivals alone that is a quiet tick and the guard never
	// fires. Measured on an emulator publishing to a thousand viewers, that
	// was the difference between forty reports a tick and two thousand, and
	// the publisher took a hundred and eight copies of every packet it sent.
	if span > s.recordedSince*resyncRatio+resyncTolerance &&
		time.Since(s.lastResync) >= resyncCooldown {
		s.log.Warnf(
			"twccsender: recorder was reporting history (%d sequence numbers described, %d arrivals reported, %d received, %d packets behind); rebuilding it",
			span, reported, s.recordedSince, s.stats.Lag.Load(),
		)

		s.recorder = newRecorder(rand.Uint32()) // #nosec G404
		s.lastResync = time.Now()
		s.stats.Resyncs.Add(1)
		s.recordedSince = 0

		// This tick's feedback is the history itself. Sending it would be the
		// very thing being stopped -- and the cooldown above is what keeps
		// that from becoming every tick.
		return nil
	}

	s.recordedSince = 0
	s.stats.Reported.Add(reported)
	s.stats.Feedback.Add(uint64(len(pkts)))

	return pkts
}

// loop sends feedback on the interval, until the interceptor is closed.
func (s *SenderInterceptor) loop(writer interceptor.RTCPWriter) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.close:
			return

		case <-ticker.C:
			pkts := s.buildFeedback()
			if len(pkts) == 0 {
				continue
			}

			if _, err := writer.Write(pkts, nil); err != nil {
				s.log.Errorf("twccsender: %s", err.Error())
			}
		}
	}
}

// describe is what a set of feedback packets says: how many arrivals it
// reports, how many sequence numbers it speaks about at all, and where the
// first of them starts.
//
// Both counts, because they answer different questions and only one of them
// can see the failure. A feedback packet writes a status for every sequence
// number in the range it covers and a receive delta only for one that turned
// up, so a recorder that has fallen a long way behind describes a very wide
// range of which almost nothing arrived: thousands of statuses, a handful of
// deltas. Measured by the deltas that looks like a quiet tick. Measured by the
// span it is unmistakable.
//
// Every mention is counted, not each sequence number once, because a report
// saying far more than arrived is the whole signal.
func describe(pkts []rtcp.Packet) (reported, span uint64, base uint16) {
	for i, p := range pkts {
		cc, ok := p.(*rtcp.TransportLayerCC)
		if !ok {
			continue
		}

		if i == 0 {
			base = cc.BaseSequenceNumber
		}

		reported += uint64(len(cc.RecvDeltas))
		span += uint64(cc.PacketStatusCount)
	}

	return reported, span, base
}

// sequenceUnwrapper turns 16-bit transport-wide sequence numbers back into the
// monotonic counter the sender kept, so "how far behind" is a number that
// survives the wrap.
type sequenceUnwrapper struct {
	init bool
	last int64
}

func (u *sequenceUnwrapper) unwrap(sn uint16) int64 {
	if !u.init {
		u.init = true
		u.last = int64(sn)

		return u.last
	}

	u.last = u.unwrapTo(sn, u.last)

	return u.last
}

// unwrapTo expands sn into the wrap window nearest to reference, without
// disturbing the unwrapper's own position.
func (u *sequenceUnwrapper) unwrapTo(sn uint16, reference int64) int64 {
	const half = 1 << 15

	unwrapped := reference - reference%(1<<16) + int64(sn)
	switch {
	case unwrapped-reference > half:
		unwrapped -= 1 << 16
	case reference-unwrapped > half:
		unwrapped += 1 << 16
	}

	return unwrapped
}

// ConfigureSender registers this interceptor and the feedback and header
// extensions it needs. It is webrtc.ConfigureTWCCSender with this package's
// sender in place of pion's.
func ConfigureSender(m *webrtc.MediaEngine, i *interceptor.Registry, opts ...Option) error {
	for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeAudio} {
		m.RegisterFeedback(webrtc.RTCPFeedback{Type: webrtc.TypeRTCPFBTransportCC}, kind)
		if err := m.RegisterHeaderExtension(
			webrtc.RTPHeaderExtensionCapability{URI: sdp.TransportCCURI}, kind,
		); err != nil {
			return err
		}
	}

	generator, err := NewSenderInterceptor(opts...)
	if err != nil {
		return err
	}

	i.Add(generator)

	return nil
}
