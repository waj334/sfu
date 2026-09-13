package sfu

import (
	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/nack"
	"github.com/pion/webrtc/v4"
)

// maxNacksPerPacket is how many times a client's peer connection asks for one
// lost packet.
//
// pion's default is no limit, which is only safe when the generator can see the
// packet arrive. It cannot see a packet recovered by RTX: pion restores that
// packet to the media stream on a path that bypasses the generator's receive
// log, so the packet is never marked received and is asked for again every
// 100ms until it leaves the 512-packet window. On a low-rate stream that is half
// a minute of retransmissions for each packet lost -- ninety-five for one packet
// in TestViewerNeverReceivesARepeatedSequenceNumberOverALossyUplink -- all of
// them uplink the publisher spends for nothing.
//
// Three covers a retransmission that is itself lost, twice.
const maxNacksPerPacket = 3

// registerInterceptors is webrtc.RegisterDefaultInterceptors with the NACK
// generator limited to maxNacksPerPacket. Everything else is pion's defaults as
// of the pion/webrtc this module requires (v4.1.3), registered in pion's order.
//
// Later pion adds a stats interceptor to its defaults. It is not added here:
// NewClient registers its own, and a module building this against a newer pion
// would otherwise end up with two.
func registerInterceptors(m *webrtc.MediaEngine, i *interceptor.Registry) error {
	generator, err := nack.NewGeneratorInterceptor(nack.GeneratorMaxNacksPerPacket(maxNacksPerPacket))
	if err != nil {
		return err
	}

	responder, err := nack.NewResponderInterceptor()
	if err != nil {
		return err
	}

	m.RegisterFeedback(webrtc.RTCPFeedback{Type: "nack"}, webrtc.RTPCodecTypeVideo)
	m.RegisterFeedback(webrtc.RTCPFeedback{Type: "nack", Parameter: "pli"}, webrtc.RTPCodecTypeVideo)
	i.Add(responder)
	i.Add(generator)

	if err := webrtc.ConfigureRTCPReports(i); err != nil {
		return err
	}

	if err := webrtc.ConfigureSimulcastExtensionHeaders(m); err != nil {
		return err
	}

	return webrtc.ConfigureTWCCSender(m, i)
}
