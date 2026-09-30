package sfu

import (
	"fmt"
	"strings"
	"time"
)

// withAudioPTime asks the other side for audio packets ptime long, by an
// a=ptime line in every audio section of sdp; 0 or less leaves sdp as it is.
//
// A node sends every audio packet a host sends to every viewer, one send each:
// audio does not shrink with a lower video layer, and one packet at a time is
// nothing segmentation offload can merge. At 50 a second per viewer that was
// most of a loaded node's sends. a=ptime in an answer is the packetization
// its sender would like to receive (RFC 4566), and libwebrtc takes it into the
// Opus encoder's frame length, so a host answered with 40 sends half as many
// audio packets, and every viewer is sent half as many. Opus carries 10 to 120
// ms a packet; 40 or 60 trade a little latency, and a little more lost to a
// dropped packet, for a third or half of the sends.
//
// pion's answer echoes the offer's codec parameters, not the node's, so this
// is set on the answer itself rather than in the codec's registration.
func withAudioPTime(sdp string, ptime time.Duration) string {
	if ptime <= 0 {
		return sdp
	}

	line := fmt.Sprintf("a=ptime:%d", ptime.Milliseconds())
	lines := strings.Split(sdp, "\r\n")
	out := make([]string, 0, len(lines)+4)
	inAudio := false
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l, "m="):
			inAudio = strings.HasPrefix(l, "m=audio ")
			out = append(out, l)
			if inAudio {
				out = append(out, line)
			}
			continue
		case inAudio && strings.HasPrefix(l, "a=ptime:"):
			// Replaced by the one given, just after the m= line.
			continue
		}
		out = append(out, l)
	}

	return strings.Join(out, "\r\n")
}
