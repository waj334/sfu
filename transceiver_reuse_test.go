package sfu

import (
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// subscriberAndSFU is the moment a subscriber joins: it says what it wants to
// receive, this side answers, and the tracks it is subscribed to are added
// afterwards. Offer and answer only — none of this needs a connection.
func subscriberAndSFU(t *testing.T) (subscriber, sfu *webrtc.PeerConnection) {
	t.Helper()

	api := webrtc.NewAPI()

	subscriber, err := api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = subscriber.Close() })

	sfu, err = api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sfu.Close() })

	for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeAudio} {
		_, err = subscriber.AddTransceiverFromKind(kind, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionRecvonly,
		})
		require.NoError(t, err)
	}

	exchange(t, subscriber, sfu)

	return subscriber, sfu
}

// exchange runs one offer and answer between two peer connections.
func exchange(t *testing.T, offerer, answerer *webrtc.PeerConnection) {
	t.Helper()

	offer, err := offerer.CreateOffer(nil)
	require.NoError(t, err)
	require.NoError(t, offerer.SetLocalDescription(offer))
	require.NoError(t, answerer.SetRemoteDescription(offer))

	answer, err := answerer.CreateAnswer(nil)
	require.NoError(t, err)
	require.NoError(t, answerer.SetLocalDescription(answer))
	require.NoError(t, offerer.SetRemoteDescription(answer))
}

func newVideoTrack(t *testing.T, id string) *webrtc.TrackLocalStaticRTP {
	t.Helper()

	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		id, "stream",
	)
	require.NoError(t, err)

	return track
}

func countMLines(sdp, kind string) int {
	count := 0
	for _, line := range strings.Split(sdp, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "m="+kind) {
			count++
		}
	}

	return count
}

// danglingSenders counts transceivers of a kind that will send and have nothing
// to send on. pion never treats one of these as negotiated, so each one keeps
// the connection renegotiating for as long as it exists.
func danglingSenders(pc *webrtc.PeerConnection, kind webrtc.RTPCodecType) int {
	count := 0
	for _, transceiver := range pc.GetTransceivers() {
		if transceiver.Kind() != kind {
			continue
		}

		if sender := transceiver.Sender(); sender == nil || sender.Track() == nil {
			count++
		}
	}

	return count
}

// A subscriber's own m-lines are the ones its tracks belong on. Adding a track
// beside them leaves them behind with nothing to send, which is the state that
// renegotiates forever.
func TestSubscribedTrackReusesTheSubscribersOwnTransceiver(t *testing.T) {
	subscriber, sfu := subscriberAndSFU(t)

	require.Equal(t, 1, danglingSenders(sfu, webrtc.RTPCodecTypeVideo),
		"the subscriber asked for one video line, so there is one waiting")

	sender, err := addSendingTrack(sfu, newVideoTrack(t, "video"))
	require.NoError(t, err)
	require.NotNil(t, sender)

	offer, err := sfu.CreateOffer(nil)
	require.NoError(t, err)

	require.Equal(t, 1, countMLines(offer.SDP, "video"),
		"the track went onto a second video line instead of the one already there")
	require.Equal(t, 0, danglingSenders(sfu, webrtc.RTPCodecTypeVideo),
		"a video line is left saying it will send with nothing to send")

	// And it settles: the renegotiation completes and leaves nothing outstanding.
	exchange(t, sfu, subscriber)
	require.Equal(t, 0, danglingSenders(sfu, webrtc.RTPCodecTypeVideo))
}

// What it replaces, kept as the thing being asserted against: adding every
// track on a transceiver of its own is what left the subscriber's behind.
func TestAddingBesideTheSubscribersTransceiverIsWhatDangled(t *testing.T) {
	_, sfu := subscriberAndSFU(t)

	_, err := sfu.AddTransceiverFromTrack(newVideoTrack(t, "video"), webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	require.NoError(t, err)

	offer, err := sfu.CreateOffer(nil)
	require.NoError(t, err)

	require.Equal(t, 2, countMLines(offer.SDP, "video"))
	require.Equal(t, 1, danglingSenders(sfu, webrtc.RTPCodecTypeVideo))
}

// Each kind is answered by a transceiver of its own kind. An audio track must
// not be put on the video line the subscriber is waiting on.
func TestReuseIsPerKind(t *testing.T) {
	_, sfu := subscriberAndSFU(t)

	audio, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "stream",
	)
	require.NoError(t, err)

	_, err = addSendingTrack(sfu, audio)
	require.NoError(t, err)

	offer, err := sfu.CreateOffer(nil)
	require.NoError(t, err)

	require.Equal(t, 1, countMLines(offer.SDP, "audio"))
	require.Equal(t, 1, countMLines(offer.SDP, "video"), "the video line was taken by audio")
	require.Equal(t, 1, danglingSenders(sfu, webrtc.RTPCodecTypeVideo), "video is still waiting")
	require.Equal(t, 0, danglingSenders(sfu, webrtc.RTPCodecTypeAudio))
}

// A second track of a kind the subscriber only asked for once has nowhere to go
// but a new line, and that line exists to send: sendrecv would invite the
// subscriber to publish back onto it.
func TestASecondTrackGetsItsOwnSendOnlyLine(t *testing.T) {
	_, sfu := subscriberAndSFU(t)

	_, err := addSendingTrack(sfu, newVideoTrack(t, "first"))
	require.NoError(t, err)

	_, err = addSendingTrack(sfu, newVideoTrack(t, "second"))
	require.NoError(t, err)

	offer, err := sfu.CreateOffer(nil)
	require.NoError(t, err)

	require.Equal(t, 2, countMLines(offer.SDP, "video"))
	require.Equal(t, 0, danglingSenders(sfu, webrtc.RTPCodecTypeVideo))

	sendonly := strings.Count(offer.SDP, "a=sendonly")
	require.GreaterOrEqual(t, sendonly, 1, "the line added for the second track should be send-only")
	require.NotContains(t, offer.SDP, "a=sendrecv",
		"a line that exists to send should not offer to receive")
}
