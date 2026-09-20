package twccsender

import (
	"math"

	"github.com/pion/rtcp"
)

const (
	packetWindowMicroseconds  = 500_000
	maxMissingSequenceNumbers = 0x7FFE
	minCapacity               = 128
	maxNumberOfPackets        = 1 << 15

	maxRunLengthCap = 0x1fff // 13 bits
	maxOneBitCap    = 14     // bits
	maxTwoBitCap    = 7      // bits
)

// Recorder records incoming RTP packets and their delays and creates
// transport wide congestion control feedback reports.
//
// Unlike Pion's stock Recorder, this implementation guarantees that
// startSequenceNumber moves monotonically forward across feedback intervals:
// an arrival whose sequence number falls on or before lastReportedSN has
// already had a feedback report sent covering its window, and does not drag
// startSequenceNumber backwards. This prevents the recorder from ever
// re-reporting history or causing feedback packet span explosion.
type Recorder struct {
	arrivalTimeMap      packetArrivalTimeMap
	sequenceUnwrapper   sequenceUnwrapper
	startSequenceNumber *int64
	lastReportedSN      int64
	hasReported         bool

	senderSSRC  uint32
	mediaSSRC   uint32
	fbPktCnt    uint8
	packetsHeld int
}

func newRecorder(senderSSRC uint32) *Recorder {
	return &Recorder{
		senderSSRC: senderSSRC,
	}
}

// IsLate returns whether sequenceNumber belongs to a window that has already
// been reported in feedback.
func (r *Recorder) IsLate(sequenceNumber uint16) bool {
	if !r.hasReported {
		return false
	}
	sn := r.sequenceUnwrapper.unwrapTo(sequenceNumber, r.lastReportedSN)
	return sn <= r.lastReportedSN
}

// Record marks a packet with mediaSSRC and a transport wide sequence number
// sequenceNumber as received at arrivalTime.
func (r *Recorder) Record(mediaSSRC uint32, sequenceNumber uint16, arrivalTime int64) {
	r.mediaSSRC = mediaSSRC

	unwrappedSN := r.sequenceUnwrapper.unwrap(sequenceNumber)
	r.maybeCullOldPackets(unwrappedSN, arrivalTime)

	if !r.hasReported {
		if r.startSequenceNumber == nil || unwrappedSN < *r.startSequenceNumber {
			r.startSequenceNumber = &unwrappedSN
		}
	} else {
		// Only un-reported arrivals may update startSequenceNumber.
		if unwrappedSN > r.lastReportedSN {
			if r.startSequenceNumber == nil || unwrappedSN < *r.startSequenceNumber {
				r.startSequenceNumber = &unwrappedSN
			}
		}
	}

	if r.arrivalTimeMap.HasReceived(unwrappedSN) {
		return
	}

	r.arrivalTimeMap.AddPacket(unwrappedSN, arrivalTime)
	r.packetsHeld++

	if r.startSequenceNumber != nil && *r.startSequenceNumber < r.arrivalTimeMap.BeginSequenceNumber() {
		sn := r.arrivalTimeMap.BeginSequenceNumber()
		r.startSequenceNumber = &sn
	}
}

func (r *Recorder) maybeCullOldPackets(sequenceNumber int64, arrivalTime int64) {
	if arrivalTime >= packetWindowMicroseconds {
		r.arrivalTimeMap.RemoveOldPackets(sequenceNumber, arrivalTime-packetWindowMicroseconds)
	}
}

// PacketsHeld returns the number of received packets currently held by the recorder.
func (r *Recorder) PacketsHeld() int {
	return r.packetsHeld
}

// BuildFeedbackPacket creates RTCP packets containing TWCC feedback reports.
func (r *Recorder) BuildFeedbackPacket() []rtcp.Packet {
	if r.startSequenceNumber == nil {
		return nil
	}

	endSN := r.arrivalTimeMap.EndSequenceNumber()
	if *r.startSequenceNumber >= endSN {
		return nil
	}

	var feedbacks []rtcp.Packet
	for *r.startSequenceNumber < endSN {
		feedback := r.maybeBuildFeedbackPacket(*r.startSequenceNumber, endSN)
		if feedback == nil {
			// Advance startSequenceNumber to endSN so recorder does not get stuck.
			sn := endSN
			r.startSequenceNumber = &sn
			break
		}
		feedbacks = append(feedbacks, feedback.getRTCP())
	}
	r.packetsHeld = 0

	if len(feedbacks) > 0 {
		r.hasReported = true
		r.lastReportedSN = endSN - 1
		r.startSequenceNumber = &endSN
	}

	return feedbacks
}

func (r *Recorder) maybeBuildFeedbackPacket(beginSeqNumInclusive, endSeqNumExclusive int64) *feedback {
	startSNInclusive, endSNExclusive := r.arrivalTimeMap.Clamp(beginSeqNumInclusive), r.arrivalTimeMap.Clamp(endSeqNumExclusive)

	var fb *feedback
	nextSequenceNumber := beginSeqNumInclusive

	for seq := startSNInclusive; seq < endSNExclusive; seq++ {
		foundSeq, arrivalTime, ok := r.arrivalTimeMap.FindNextAtOrAfter(seq)
		seq = foundSeq
		if !ok || seq >= endSNExclusive {
			break
		}

		if fb == nil {
			fb = newFeedback(r.senderSSRC, r.mediaSSRC, r.fbPktCnt)
			r.fbPktCnt++

			baseSequenceNumber := max(beginSeqNumInclusive, seq-maxMissingSequenceNumbers)
			fb.setBase(uint16(baseSequenceNumber), arrivalTime) //nolint:gosec // G115

			if !fb.addReceived(uint16(seq), arrivalTime) { //nolint:gosec // G115
				r.startSequenceNumber = &seq
				return nil
			}
		} else if !fb.addReceived(uint16(seq), arrivalTime) { //nolint:gosec // G115
			break
		}

		nextSequenceNumber = seq + 1
	}

	if fb != nil {
		r.startSequenceNumber = &nextSequenceNumber
	}

	return fb
}

type feedback struct {
	rtcp                *rtcp.TransportLayerCC
	baseSequenceNumber  uint16
	refTimestamp64MS    int64
	lastTimestampUS     int64
	nextSequenceNumber  uint16
	sequenceNumberCount uint16
	len                 int
	lastChunk           chunk
	chunks              []rtcp.PacketStatusChunk
	deltas              []*rtcp.RecvDelta
}

func newFeedback(senderSSRC, mediaSSRC uint32, count uint8) *feedback {
	return &feedback{
		rtcp: &rtcp.TransportLayerCC{
			SenderSSRC: senderSSRC,
			MediaSSRC:  mediaSSRC,
			FbPktCount: count,
		},
	}
}

func (f *feedback) setBase(sequenceNumber uint16, timeUS int64) {
	f.baseSequenceNumber = sequenceNumber
	f.nextSequenceNumber = f.baseSequenceNumber
	f.refTimestamp64MS = timeUS / 64e3
	f.lastTimestampUS = f.refTimestamp64MS * 64e3
}

func (f *feedback) getRTCP() *rtcp.TransportLayerCC {
	f.rtcp.PacketStatusCount = f.sequenceNumberCount
	f.rtcp.ReferenceTime = uint32(f.refTimestamp64MS) //nolint:gosec // G115
	f.rtcp.BaseSequenceNumber = f.baseSequenceNumber
	for len(f.lastChunk.deltas) > 0 {
		f.chunks = append(f.chunks, f.lastChunk.encode())
	}
	f.rtcp.PacketChunks = append(f.rtcp.PacketChunks, f.chunks...)
	f.rtcp.RecvDeltas = f.deltas

	padLen := 20 + len(f.rtcp.PacketChunks)*2 + f.len
	padding := padLen%4 != 0
	for padLen%4 != 0 {
		padLen++
	}
	f.rtcp.Header = rtcp.Header{
		Count:   rtcp.FormatTCC,
		Type:    rtcp.TypeTransportSpecificFeedback,
		Padding: padding,
		Length:  uint16((padLen / 4) - 1), //nolint:gosec // G115
	}

	return f.rtcp
}

func (f *feedback) addReceived(sequenceNumber uint16, timestampUS int64) bool {
	deltaUS := timestampUS - f.lastTimestampUS
	var delta250US int64
	if deltaUS >= 0 {
		delta250US = (deltaUS + rtcp.TypeTCCDeltaScaleFactor/2) / rtcp.TypeTCCDeltaScaleFactor
	} else {
		delta250US = (deltaUS - rtcp.TypeTCCDeltaScaleFactor/2) / rtcp.TypeTCCDeltaScaleFactor
	}
	if delta250US < math.MinInt16 || delta250US > math.MaxInt16 {
		return false
	}
	deltaUSRounded := delta250US * rtcp.TypeTCCDeltaScaleFactor

	for ; f.nextSequenceNumber != sequenceNumber; f.nextSequenceNumber++ {
		if !f.lastChunk.canAdd(rtcp.TypeTCCPacketNotReceived) {
			f.chunks = append(f.chunks, f.lastChunk.encode())
		}
		f.lastChunk.add(rtcp.TypeTCCPacketNotReceived)
		f.sequenceNumberCount++
	}

	var recvDelta uint16
	switch {
	case delta250US >= 0 && delta250US <= 0xff:
		f.len++
		recvDelta = rtcp.TypeTCCPacketReceivedSmallDelta
	default:
		f.len += 2
		recvDelta = rtcp.TypeTCCPacketReceivedLargeDelta
	}

	if !f.lastChunk.canAdd(recvDelta) {
		f.chunks = append(f.chunks, f.lastChunk.encode())
	}
	f.lastChunk.add(recvDelta)
	f.deltas = append(f.deltas, &rtcp.RecvDelta{
		Type:  recvDelta,
		Delta: deltaUSRounded,
	})
	f.lastTimestampUS += deltaUSRounded
	f.sequenceNumberCount++
	f.nextSequenceNumber++

	return true
}

type chunk struct {
	hasLargeDelta     bool
	hasDifferentTypes bool
	deltas            []uint16
}

func (c *chunk) canAdd(delta uint16) bool {
	if len(c.deltas) < maxTwoBitCap {
		return true
	}
	if len(c.deltas) < maxOneBitCap && !c.hasLargeDelta && delta != rtcp.TypeTCCPacketReceivedLargeDelta {
		return true
	}
	if len(c.deltas) < maxRunLengthCap && !c.hasDifferentTypes && delta == c.deltas[0] {
		return true
	}

	return false
}

func (c *chunk) add(delta uint16) {
	c.deltas = append(c.deltas, delta)
	c.hasLargeDelta = c.hasLargeDelta || delta == rtcp.TypeTCCPacketReceivedLargeDelta
	c.hasDifferentTypes = c.hasDifferentTypes || delta != c.deltas[0]
}

func (c *chunk) encode() rtcp.PacketStatusChunk {
	if !c.hasDifferentTypes {
		defer c.reset()

		return &rtcp.RunLengthChunk{
			PacketStatusSymbol: c.deltas[0],
			RunLength:          uint16(len(c.deltas)), //nolint:gosec // G115
		}
	}
	if len(c.deltas) == maxOneBitCap {
		defer c.reset()

		return &rtcp.StatusVectorChunk{
			SymbolSize: rtcp.TypeTCCSymbolSizeOneBit,
			SymbolList: c.deltas,
		}
	}

	minCap := min(maxTwoBitCap, len(c.deltas))
	svc := &rtcp.StatusVectorChunk{
		SymbolSize: rtcp.TypeTCCSymbolSizeTwoBit,
		SymbolList: c.deltas[:minCap],
	}
	c.deltas = c.deltas[minCap:]
	c.hasDifferentTypes = false
	c.hasLargeDelta = false

	if len(c.deltas) > 0 {
		tmp := c.deltas[0]
		for _, d := range c.deltas {
			if tmp != d {
				c.hasDifferentTypes = true
			}
			if d == rtcp.TypeTCCPacketReceivedLargeDelta {
				c.hasLargeDelta = true
			}
		}
	}

	return svc
}

func (c *chunk) reset() {
	c.deltas = []uint16{}
	c.hasLargeDelta = false
	c.hasDifferentTypes = false
}

type packetArrivalTimeMap struct {
	arrivalTimes                           []int64
	beginSequenceNumber, endSequenceNumber int64
}

func (m *packetArrivalTimeMap) AddPacket(sequenceNumber int64, arrivalTime int64) {
	if m.arrivalTimes == nil {
		m.reallocate(minCapacity)
		m.beginSequenceNumber = sequenceNumber
		m.endSequenceNumber = sequenceNumber + 1
		m.arrivalTimes[m.index(sequenceNumber)] = arrivalTime

		return
	}

	if sequenceNumber >= m.beginSequenceNumber && sequenceNumber < m.endSequenceNumber {
		m.arrivalTimes[m.index(sequenceNumber)] = arrivalTime

		return
	}

	if sequenceNumber < m.beginSequenceNumber {
		newSize := int(m.endSequenceNumber - sequenceNumber)
		if newSize > maxNumberOfPackets {
			return
		}
		m.adjustToSize(newSize)
		m.arrivalTimes[m.index(sequenceNumber)] = arrivalTime
		m.setNotReceived(sequenceNumber+1, m.beginSequenceNumber)
		m.beginSequenceNumber = sequenceNumber

		return
	}

	newEndSequenceNumber := sequenceNumber + 1

	if newEndSequenceNumber >= m.endSequenceNumber+maxNumberOfPackets {
		m.beginSequenceNumber = sequenceNumber
		m.endSequenceNumber = newEndSequenceNumber
		m.arrivalTimes[m.index(sequenceNumber)] = arrivalTime

		return
	}

	if m.beginSequenceNumber < newEndSequenceNumber-maxNumberOfPackets {
		m.beginSequenceNumber = newEndSequenceNumber - maxNumberOfPackets
	}

	m.adjustToSize(int(newEndSequenceNumber - m.beginSequenceNumber))
	m.setNotReceived(m.endSequenceNumber, sequenceNumber)
	m.endSequenceNumber = newEndSequenceNumber
	m.arrivalTimes[m.index(sequenceNumber)] = arrivalTime
}

func (m *packetArrivalTimeMap) setNotReceived(startInclusive, endExclusive int64) {
	for sn := startInclusive; sn < endExclusive; sn++ {
		m.arrivalTimes[m.index(sn)] = -1
	}
}

func (m *packetArrivalTimeMap) BeginSequenceNumber() int64 {
	return m.beginSequenceNumber
}

func (m *packetArrivalTimeMap) EndSequenceNumber() int64 {
	return m.endSequenceNumber
}

func (m *packetArrivalTimeMap) FindNextAtOrAfter(sequenceNumber int64) (int64, int64, bool) {
	for seq := m.Clamp(sequenceNumber); seq < m.endSequenceNumber; seq++ {
		if arrivalTime := m.get(seq); arrivalTime >= 0 {
			return seq, arrivalTime, true
		}
	}

	return -1, -1, false
}

func (m *packetArrivalTimeMap) EraseTo(sequenceNumber int64) {
	if sequenceNumber < m.beginSequenceNumber {
		return
	}
	if sequenceNumber >= m.endSequenceNumber {
		m.beginSequenceNumber = m.endSequenceNumber

		return
	}
	m.beginSequenceNumber = sequenceNumber
	m.adjustToSize(int(m.endSequenceNumber - m.beginSequenceNumber))
}

func (m *packetArrivalTimeMap) RemoveOldPackets(sequenceNumber int64, arrivalTimeLimit int64) {
	checkTo := min(sequenceNumber, m.endSequenceNumber)
	for m.beginSequenceNumber < checkTo && m.get(m.beginSequenceNumber) <= arrivalTimeLimit {
		m.beginSequenceNumber++
	}
	m.adjustToSize(int(m.endSequenceNumber - m.beginSequenceNumber))
}

func (m *packetArrivalTimeMap) HasReceived(sequenceNumber int64) bool {
	return m.get(sequenceNumber) >= 0
}

func (m *packetArrivalTimeMap) Clamp(sequenceNumber int64) int64 {
	if sequenceNumber < m.beginSequenceNumber {
		return m.beginSequenceNumber
	}
	if m.endSequenceNumber < sequenceNumber {
		return m.endSequenceNumber
	}

	return sequenceNumber
}

func (m *packetArrivalTimeMap) get(sequenceNumber int64) int64 {
	if sequenceNumber < m.beginSequenceNumber || sequenceNumber >= m.endSequenceNumber {
		return -1
	}

	return m.arrivalTimes[m.index(sequenceNumber)]
}

func (m *packetArrivalTimeMap) index(sequenceNumber int64) int {
	return int(sequenceNumber & int64(m.capacity()-1))
}

func (m *packetArrivalTimeMap) adjustToSize(newSize int) {
	if newSize > m.capacity() {
		newCapacity := m.capacity()
		for newCapacity < newSize {
			newCapacity *= 2
		}
		m.reallocate(newCapacity)
	}
	if m.capacity() > max(minCapacity, newSize*4) {
		newCapacity := m.capacity()
		for newCapacity >= 2*max(newSize, minCapacity) {
			newCapacity /= 2
		}
		m.reallocate(newCapacity)
	}
}

func (m *packetArrivalTimeMap) capacity() int {
	return len(m.arrivalTimes)
}

func (m *packetArrivalTimeMap) reallocate(newCapacity int) {
	newBuffer := make([]int64, newCapacity)
	for sn := m.beginSequenceNumber; sn < m.endSequenceNumber; sn++ {
		newBuffer[int(sn&(int64(newCapacity-1)))] = m.get(sn)
	}
	m.arrivalTimes = newBuffer
}
