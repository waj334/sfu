package sfu

import (
	"context"
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// bitrateOnlyClientTrack answers the two bitrate questions and nothing else.
// maxReceiveBitrate asks no more than that, and a panic is a better report than
// a zero if it ever starts to.
type bitrateOnlyClientTrack struct {
	receive uint32
	send    uint32
}

func (t *bitrateOnlyClientTrack) ReceiveBitrate() uint32 { return t.receive }
func (t *bitrateOnlyClientTrack) SendBitrate() uint32    { return t.send }

func (t *bitrateOnlyClientTrack) push(*rtp.Packet, QualityLevel) { panic("not used") }
func (t *bitrateOnlyClientTrack) ID() string                     { panic("not used") }
func (t *bitrateOnlyClientTrack) StreamID() string               { panic("not used") }
func (t *bitrateOnlyClientTrack) Context() context.Context       { panic("not used") }
func (t *bitrateOnlyClientTrack) Kind() webrtc.RTPCodecType      { panic("not used") }
func (t *bitrateOnlyClientTrack) MimeType() string               { panic("not used") }
func (t *bitrateOnlyClientTrack) IsScreen() bool                 { panic("not used") }
func (t *bitrateOnlyClientTrack) IsSimulcast() bool              { panic("not used") }
func (t *bitrateOnlyClientTrack) IsScaleable() bool              { panic("not used") }
func (t *bitrateOnlyClientTrack) SetSourceType(TrackType)        { panic("not used") }
func (t *bitrateOnlyClientTrack) Client() *Client                { panic("not used") }
func (t *bitrateOnlyClientTrack) RequestPLI()                    { panic("not used") }
func (t *bitrateOnlyClientTrack) SetMaxQuality(QualityLevel)     { panic("not used") }
func (t *bitrateOnlyClientTrack) MaxQuality() QualityLevel       { panic("not used") }
func (t *bitrateOnlyClientTrack) Quality() QualityLevel          { panic("not used") }
func (t *bitrateOnlyClientTrack) OnEnded(func())                 { panic("not used") }
func (t *bitrateOnlyClientTrack) LocalTrack() *webrtc.TrackLocalStaticRTP {
	panic("not used")
}

// The tracks reaching qualityLevelPerTrack have only just been subscribed, so
// nothing has been sent on them and SendBitrate is 0 for every one. Reading
// that as the maximum left it at zero, which made the quality ladder in
// qualityLevelPerTrack unreachable — QualityHigh whenever any bandwidth
// remained, QualityLowLow when none did, and nothing in between.
func TestMaxReceiveBitrateIgnoresUnsentTracks(t *testing.T) {
	tracks := []iClientTrack{
		&bitrateOnlyClientTrack{receive: 300_000, send: 0},
		&bitrateOnlyClientTrack{receive: 900_000, send: 0},
		&bitrateOnlyClientTrack{receive: 150_000, send: 0},
	}

	require.Equal(t, uint32(900_000), maxReceiveBitrate(tracks))
}

// Comparing one metric while assigning another meant the running value was not
// a maximum of anything. The second track here wins the comparison it is
// judged by and loses the one it contributes, so it used to overwrite a larger
// figure with a smaller one and the widest track stopped being accounted for.
func TestMaxReceiveBitrateIsMonotonic(t *testing.T) {
	tracks := []iClientTrack{
		&bitrateOnlyClientTrack{receive: 900_000, send: 10},
		&bitrateOnlyClientTrack{receive: 150_000, send: 1_000_000},
	}

	require.Equal(t, uint32(900_000), maxReceiveBitrate(tracks))
}

func TestMaxReceiveBitrateOfNothingIsZero(t *testing.T) {
	require.Equal(t, uint32(0), maxReceiveBitrate(nil))
}
