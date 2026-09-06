package sfu

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

// newRewriteTestTrack builds the least of a subscriber's simulcast track that
// rewritePacket touches: the layer bookkeeping it reads off the published
// track, and its own counters.
func newRewriteTestTrack(published *SimulcastTrack) *simulcastClientTrack {
	return &simulcastClientTrack{
		remoteTrack:       published,
		clockRate:         90000,
		sequenceNumber:    &atomic.Uint32{},
		timestampOffset:   &atomic.Uint32{},
		lastSentTimestamp: &atomic.Uint32{},
		lastSentQuality:   &atomic.Uint32{},
	}
}

// forward runs one packet through the path a forwarded packet takes: the
// published track records the layer's sequence numbers as its read loop does,
// then the subscriber's track rewrites the packet.
func forward(
	published *SimulcastTrack,
	subscriber *simulcastClientTrack,
	quality QualityLevel,
	timestamp uint32,
	sequence uint16,
) *rtp.Packet {
	switch quality {
	case QualityHigh:
		published.lastHighSequence, published.highSequence = published.highSequence, sequence
	case QualityMid:
		published.lastMidSequence, published.midSequence = published.midSequence, sequence
	case QualityLow:
		published.lastLowSequence, published.lowSequence = published.lowSequence, sequence
	}

	packet := &rtp.Packet{Header: rtp.Header{Timestamp: timestamp, SequenceNumber: sequence}}
	subscriber.rewritePacket(packet, quality)

	return packet
}

// Each simulcast layer is its own RTP stream and picks its own timestamp base,
// so the two bases in a switch are unrelated random numbers. Forwarding the new
// layer's timestamps against an anchor taken from the old one moved the
// subscriber's timeline by that difference — minutes, usually, where a receiver
// will only hold a frame for ten seconds. libwebrtc dropped the frame, reset its
// jitter estimator and froze the picture until it had rebuilt one.
//
// The bases here are as far apart as two 32-bit numbers get, and the high layer
// is behind the low one, so the timeline would run backwards under the old
// anchoring.
func TestSimulcastRewriteKeepsTimestampsContinuousAcrossASwitch(t *testing.T) {
	const frame = uint32(90000 / 30)

	published := &SimulcastTrack{mu: sync.RWMutex{}}
	subscriber := newRewriteTestTrack(published)

	lowTimestamp, highTimestamp := uint32(4_000_000_000), uint32(12_345)
	sequence := uint16(65500) // wraps partway through, as it does on the wire

	var sent []uint32

	for range 30 {
		sent = append(sent, forward(published, subscriber, QualityLow, lowTimestamp, sequence).Timestamp)
		lowTimestamp += frame
		highTimestamp += frame
		sequence++
	}

	// The bitrate controller allows the high layer and the keyframe arrives.
	for range 30 {
		sent = append(sent, forward(published, subscriber, QualityHigh, highTimestamp, sequence).Timestamp)
		lowTimestamp += frame
		highTimestamp += frame
		sequence++
	}

	for i := 1; i < len(sent); i++ {
		require.Equal(t, frame, sent[i]-sent[i-1],
			"frame %d moved the timeline by %d instead of one frame", i, sent[i]-sent[i-1])
	}
}

// Only the first packet of a frame is a keyframe packet, so a switch lands
// partway through a burst that all shares one timestamp. Recomputing the offset
// per packet rather than per switch would spread one frame across several.
func TestSimulcastRewriteHoldsOneTimestampAcrossAFrame(t *testing.T) {
	published := &SimulcastTrack{mu: sync.RWMutex{}}
	subscriber := newRewriteTestTrack(published)

	forward(published, subscriber, QualityLow, 4_000_000_000, 100)

	first := forward(published, subscriber, QualityHigh, 12_345, 101).Timestamp
	for i := range uint16(9) {
		require.Equal(t, first, forward(published, subscriber, QualityHigh, 12_345, 102+i).Timestamp,
			"packet %d of the keyframe left the frame", i+2)
	}
}

// The sequence number is the subscriber's own count and has to keep stepping by
// one whichever layer the packet came from, whatever that layer's own numbering
// is doing. The high layer here is nowhere near the low one and is about to
// wrap.
func TestSimulcastRewriteKeepsSequenceNumbersContinuousAcrossASwitch(t *testing.T) {
	published := &SimulcastTrack{mu: sync.RWMutex{}}
	subscriber := newRewriteTestTrack(published)

	var sent []uint16

	lowSequence := uint16(200)
	for range 10 {
		sent = append(sent, forward(published, subscriber, QualityLow, 4_000_000_000, lowSequence).SequenceNumber)
		lowSequence++
	}

	highSequence := uint16(65530)
	for range 10 {
		sent = append(sent, forward(published, subscriber, QualityHigh, 12_345, highSequence).SequenceNumber)
		highSequence++
	}

	for i := 1; i < len(sent); i++ {
		require.Equal(t, uint16(1), sent[i]-sent[i-1],
			"packet %d moved the sequence by %d", i, sent[i]-sent[i-1])
	}
}

// A layer that goes quiet takes LastQuality down with it, and the subscriber
// falls back and then climbs again. Each of those is a switch, and the timeline
// has to survive all of them rather than only the first.
func TestSimulcastRewriteSurvivesRepeatedSwitches(t *testing.T) {
	const frame = uint32(90000 / 30)

	published := &SimulcastTrack{mu: sync.RWMutex{}}
	subscriber := newRewriteTestTrack(published)

	timestamps := map[QualityLevel]uint32{
		QualityLow:  4_000_000_000,
		QualityMid:  1,
		QualityHigh: 2_147_483_647,
	}

	sequence := uint16(0)
	previous := uint32(0)
	first := true

	for _, quality := range []QualityLevel{
		QualityLow, QualityHigh, QualityLow, QualityMid, QualityHigh, QualityMid, QualityLow,
	} {
		for range 5 {
			packet := forward(published, subscriber, quality, timestamps[quality], sequence)

			if !first {
				require.Equal(t, frame, packet.Timestamp-previous,
					"switching to quality %d moved the timeline by %d", quality, packet.Timestamp-previous)
			}

			first = false
			previous = packet.Timestamp
			sequence++

			for level := range timestamps {
				timestamps[level] += frame
			}
		}
	}
}
