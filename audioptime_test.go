package sfu

import (
	"strings"
	"testing"
	"time"

	"github.com/pion/sdp/v3"
	"github.com/stretchr/testify/require"
)

const answerSDP = "v=0\r\no=- 1 2 IN IP4 0.0.0.0\r\ns=-\r\nt=0 0\r\n" +
	"m=audio 9 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 0.0.0.0\r\na=mid:0\r\na=ptime:20\r\na=rtpmap:111 opus/48000/2\r\n" +
	"m=video 9 UDP/TLS/RTP/SAVPF 96\r\nc=IN IP4 0.0.0.0\r\na=mid:1\r\na=rtpmap:96 H264/90000\r\n" +
	"m=audio 9 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 0.0.0.0\r\na=mid:2\r\na=rtpmap:111 opus/48000/2\r\n"

// Every audio section asks for the ptime given, once; video is untouched; the
// result still parses.
func TestWithAudioPTime(t *testing.T) {
	got := withAudioPTime(answerSDP, 40*time.Millisecond)

	var parsed sdp.SessionDescription
	require.NoError(t, parsed.UnmarshalString(got))
	for _, m := range parsed.MediaDescriptions {
		ptime, ok := m.Attribute("ptime")
		if m.MediaName.Media == "audio" {
			require.True(t, ok)
			require.Equal(t, "40", ptime)
		} else {
			require.False(t, ok)
		}
	}
	require.Equal(t, 2, strings.Count(got, "a=ptime:"))
}

func TestWithAudioPTimeOff(t *testing.T) {
	require.Equal(t, answerSDP, withAudioPTime(answerSDP, 0))
}
