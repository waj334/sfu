package sfu

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
	"github.com/waj334/sfu/pkg/interceptors/simulcast"
)

// seqRecorder counts, per SSRC, the packets a viewer received under a sequence
// number it had already received.
//
// That is the packet libwebrtc refuses as an SRTP replay
// (srtp_err_status_replay_fail, logged as "Failed to unprotect SRTP packet,
// err=9"). A real receiver drops it before anything above the transport sees it,
// so the viewer this records for has replay protection turned off and counts
// them instead.
type seqRecorder struct {
	mu      sync.Mutex
	streams map[uint32]*seqStream
}

type seqStream struct {
	kind    string
	packets int
	repeats int
	started bool
	last    uint16
	lastExt int64
	highest int64
	seen    map[int64]struct{}

	// copies counts packets whose media had already arrived: the same payload
	// at the same RTP timestamp, whatever sequence number it came under. A
	// simulcast subscriber's sequence numbers are the SFU's own, so a packet
	// forwarded twice usually arrives under a fresh number and only this sees it.
	copies    int
	media     map[uint64]int
	mediaSeen []uint64

	// rtx counts packets this viewer's own pion restored from an RTX
	// retransmission. They are not counted as repeats: libwebrtc's replay check
	// is per SSRC, and a retransmission arrives on the RTX SSRC, so one that
	// duplicates a packet already received is discarded above the transport and
	// never logged as an SRTP replay.
	rtx int
}

func mediaKey(timestamp uint32, payload []byte) uint64 {
	h := fnv.New64a()
	var ts [4]byte
	ts[0], ts[1], ts[2], ts[3] = byte(timestamp>>24), byte(timestamp>>16), byte(timestamp>>8), byte(timestamp)
	_, _ = h.Write(ts[:])
	_, _ = h.Write(payload)

	return h.Sum64()
}

func (r *seqRecorder) record(kind string, p *rtp.Packet, restoredFromRTX bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ssrc, seq := p.SSRC, p.SequenceNumber

	s := r.streams[ssrc]
	if s == nil {
		s = &seqStream{kind: kind, seen: map[int64]struct{}{}, media: map[uint64]int{}}
		r.streams[ssrc] = s
	}

	if restoredFromRTX {
		s.rtx++
		return
	}

	// Padding carries nothing to compare.
	if len(p.Payload) > 0 {
		key := mediaKey(p.Timestamp, p.Payload)
		if s.media[key] > 0 {
			s.copies++
		}
		s.media[key]++
		s.mediaSeen = append(s.mediaSeen, key)
		if len(s.mediaSeen) > 4096 {
			old := s.mediaSeen[0]
			s.mediaSeen = s.mediaSeen[1:]
			if s.media[old]--; s.media[old] <= 0 {
				delete(s.media, old)
			}
		}
	}

	// Extended across wraps, relative to the packet before, so a number that
	// comes round again after 65536 packets is not mistaken for a repeat.
	var ext int64
	if !s.started {
		ext = int64(seq) + 1<<20
		s.started = true
	} else {
		ext = s.lastExt + int64(int16(seq-s.last))
	}

	s.packets++
	if _, ok := s.seen[ext]; ok {
		s.repeats++
	} else {
		s.seen[ext] = struct{}{}
	}

	s.last = seq
	s.lastExt = ext
	if ext > s.highest {
		s.highest = ext
	}

	// Only the recent past can be repeated into a receiver's replay window.
	if s.packets%2048 == 0 {
		for k := range s.seen {
			if k < s.highest-4096 {
				delete(s.seen, k)
			}
		}
	}
}

// totals is what a viewer received, logged per stream.
func (r *seqRecorder) totals(t *testing.T, name string) (received, repeated, copies int) {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	for ssrc, s := range r.streams {
		t.Logf("%s %s ssrc=%d packets=%d repeated-seq=%d repeated-media=%d restored-from-rtx=%d",
			name, s.kind, ssrc, s.packets, s.repeats, s.copies, s.rtx)
		received += s.packets
		repeated += s.repeats
		copies += s.copies
	}

	return received, repeated, copies
}

// addRecordingViewer joins a room the way the app's viewer does: receive-only
// lines for video and audio offered up front, subscribed to every track the
// room has, with the client options the service runs every client with.
func addRecordingViewer(t *testing.T, room *Room, name string) (*webrtc.PeerConnection, *Client, *seqRecorder) {
	t.Helper()

	m := GetMediaEngine()
	i := &interceptor.Registry{}
	require.NoError(t, webrtc.RegisterDefaultInterceptors(m, i))

	se := webrtc.SettingEngine{}
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	se.SetIncludeLoopbackCandidate(true)
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	// Let a repeated packet through to be counted, rather than dropped where
	// nothing can see it.
	se.DisableSRTPReplayProtection(true)

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(i), webrtc.WithSettingEngine(se))
	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: DefaultTestIceServers()})
	require.NoError(t, err)

	rec := &seqRecorder{streams: map[uint32]*seqStream{}}

	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go func() {
			for {
				p, attributes, err := track.ReadRTP()
				if err != nil {
					return
				}
				_, restoredFromRTX := attributes[webrtc.AttributeRtxSequenceNumber]
				rec.record(track.Kind().String(), p, restoredFromRTX)
			}
		}()
	})

	for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeAudio} {
		_, err = pc.AddTransceiverFromKind(kind, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionRecvonly,
		})
		require.NoError(t, err)
	}

	id := room.CreateClientID()

	client, err := room.AddClient(id, id, swrvClientOptions())
	require.NoError(t, err, "adding %s", name)

	client.OnTracksAvailable(func(tracks []ITrack) {
		reqs := make([]SubscribeTrackRequest, 0, len(tracks))
		for _, tr := range tracks {
			reqs = append(reqs, SubscribeTrackRequest{ClientID: tr.ClientID(), TrackID: tr.ID()})
		}
		_ = client.SubscribeTracks(reqs)
	})

	answerRenegotiation(pc, client)
	trickle(pc, client)

	negotiate(pc, client, TestLogger, true)

	return pc, client, rec
}

// swrvClientOptions is what swrv's realtime service passes, where it differs
// from the defaults.
func swrvClientOptions() ClientOptions {
	opts := DefaultClientOptions()
	opts.EnableVoiceDetection = false
	opts.EnableOpusDTX = false
	opts.MinPlayoutDelay = 100
	opts.MaxPlayoutDelay = 200
	opts.JitterBufferMaxWait = 200 * time.Millisecond
	opts.ReorderPackets = true
	opts.Log = TestLogger

	return opts
}

func answerRenegotiation(pc *webrtc.PeerConnection, client *Client) {
	client.OnRenegotiation(func(_ context.Context, offer webrtc.SessionDescription) (webrtc.SessionDescription, error) {
		if err := pc.SetRemoteDescription(offer); err != nil {
			return webrtc.SessionDescription{}, err
		}
		answer, err := pc.CreateAnswer(nil)
		if err != nil {
			return webrtc.SessionDescription{}, err
		}
		if err := pc.SetLocalDescription(answer); err != nil {
			return webrtc.SessionDescription{}, err
		}
		return *pc.LocalDescription(), nil
	})

	client.OnAllowedRemoteRenegotiation(func() {
		negotiate(pc, client, TestLogger, true)
	})
}

func trickle(pc *webrtc.PeerConnection, client *Client) {
	client.OnIceCandidate(func(_ context.Context, c *webrtc.ICECandidate) {
		if c != nil {
			_ = pc.AddICECandidate(c.ToJSON())
		}
	})

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			_ = client.PeerConnection().PC().AddICECandidate(c.ToJSON())
		}
	})
}

// dropper loses one video packet in every `every` on its way to the wire, the
// first time it is sent.
//
// Registered first, so it sits nearest the wire and below the NACK responder:
// the responder has already buffered what this drops, and its retransmissions,
// which go out on the RTX SSRC, pass through untouched. That is a lossy uplink
// as the SFU sees one -- a gap, a NACK, and the packet arriving again as RTX.
type dropper struct {
	every   uint32
	dropped atomic.Uint32

	// Retransmissions seen going out on the RTX SSRC, by the original sequence
	// number each one carries.
	mu          sync.Mutex
	retransmits map[uint16]int
}

func (d *dropper) retransmitted(osn uint16) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.retransmits == nil {
		d.retransmits = map[uint16]int{}
	}
	d.retransmits[osn]++
}

// retransmissionSummary is how many RTX packets went out, for how many distinct
// packets, and the most any one packet was sent again.
func (d *dropper) retransmissionSummary() (total, distinct, most int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, n := range d.retransmits {
		total += n
		distinct++
		if n > most {
			most = n
		}
	}

	return total, distinct, most
}

func (d *dropper) NewInterceptor(_ string) (interceptor.Interceptor, error) {
	return &dropInterceptor{d: d}, nil
}

type dropInterceptor struct {
	interceptor.NoOp
	d *dropper
}

func (i *dropInterceptor) BindLocalStream(info *interceptor.StreamInfo, writer interceptor.RTPWriter) interceptor.RTPWriter {
	if info.SSRCRetransmission == 0 || len(info.MimeType) < 6 || info.MimeType[:6] != "video/" {
		return writer
	}

	var count uint32

	return interceptor.RTPWriterFunc(func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
		if header.SSRC == info.SSRC {
			count++
			if count%i.d.every == 0 {
				i.d.dropped.Add(1)
				return header.MarshalSize() + len(payload), nil
			}
		} else if header.SSRC == info.SSRCRetransmission && len(payload) >= 2 {
			i.d.retransmitted(uint16(payload[0])<<8 | uint16(payload[1]))
		}

		return writer.Write(header, payload, attributes)
	})
}

// addLossyPublisher publishes video (and, when not simulcast, audio) into a
// room over an uplink that loses packets.
func addLossyPublisher(t *testing.T, ctx context.Context, room *Room, name string, isSimulcast bool, every uint32) (*webrtc.PeerConnection, *Client, *dropper) {
	t.Helper()

	m := GetMediaEngine()
	i := &interceptor.Registry{}

	drops := &dropper{every: every}
	i.Add(drops)

	var simulcastI *simulcast.Interceptor
	if isSimulcast {
		factory := simulcast.NewInterceptor()
		i.Add(factory)
		factory.OnNew(func(s *simulcast.Interceptor) {
			simulcastI = s
		})
	}

	require.NoError(t, webrtc.RegisterDefaultInterceptors(m, i))

	if isSimulcast {
		RegisterSimulcastHeaderExtensions(m, webrtc.RTPCodecTypeVideo)
	}

	se := webrtc.SettingEngine{}
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	se.SetIncludeLoopbackCandidate(true)
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(i), webrtc.WithSettingEngine(se))
	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: DefaultTestIceServers()})
	require.NoError(t, err)

	iceConnected, iceConnectedCancel := context.WithCancel(ctx)
	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		if state == webrtc.ICEConnectionStateConnected {
			iceConnectedCancel()
		}
	})

	id := room.CreateClientID()
	client, err := room.AddClient(id, id, swrvClientOptions())
	require.NoError(t, err, "adding %s", name)

	pc.OnNegotiationNeeded(func() {
		if client.state.Load() == ClientStateEnded {
			return
		}
		negotiate(pc, client, TestLogger, true)
	})

	client.OnTracksAdded(func(added []ITrack) {
		setTracks := make(map[string]TrackType, len(added))
		for _, track := range added {
			setTracks[track.ID()] = TrackTypeMedia
		}
		client.SetTracksSourceType(setTracks)
	})

	answerRenegotiation(pc, client)
	trickle(pc, client)

	if isSimulcast {
		require.NoError(t, AddSimulcastVideoTracks(ctx, iceConnected, pc, GenerateSecureToken(), name))
		for _, sender := range pc.GetSenders() {
			parameters := sender.GetParameters()
			if parameters.Encodings[0].RID != "" {
				simulcastI.SetSenderParameters(parameters)
			}
		}
	} else {
		tracks, _ := GetStaticTracks(ctx, iceConnected, name, true)
		SetPeerConnectionTracks(ctx, pc, tracks)
	}

	return pc, client, drops
}

// runViewers puts viewers on a room for a while and fails for any that received
// nothing, or received a packet under a sequence number it already had.
func runViewers(t *testing.T, room *Room, count int, duration time.Duration) {
	t.Helper()

	recorders := make([]*seqRecorder, 0, count)
	for i := 0; i < count; i++ {
		pc, client, rec := addRecordingViewer(t, room, fmt.Sprintf("viewer-%d", i))
		t.Cleanup(func() {
			_ = room.StopClient(client.ID())
			_ = pc.Close()
		})
		recorders = append(recorders, rec)
	}

	time.Sleep(duration)

	for i, rec := range recorders {
		received, repeated, copies := rec.totals(t, fmt.Sprintf("viewer-%d", i))
		if received == 0 {
			t.Errorf("viewer-%d received nothing, so the test proves nothing", i)
		}
		if repeated > 0 {
			t.Errorf("viewer-%d received %d packets under a sequence number it already had", i, repeated)
		}
		if copies > 0 {
			t.Errorf("viewer-%d received %d packets carrying media it already had", i, copies)
		}
	}
}

func newRepeatsRoom(t *testing.T) (context.Context, *Room) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	manager := NewManager(ctx, "test", sfuOpts)

	room, err := manager.NewRoom(manager.CreateRoomID(), "repeats", RoomTypeLocal, DefaultRoomOptions())
	require.NoError(t, err)

	t.Cleanup(func() {
		room.Close()
		manager.Close()
		cancel()
	})

	return ctx, room
}

// A viewer must never be sent two packets under one sequence number on one SSRC.
//
// Every device watching a battle was logging thousands of SRTP replay failures a
// minute, on video and never on audio. This puts a host and several viewers
// through a room over a clean network.
func TestViewerNeverReceivesARepeatedSequenceNumber(t *testing.T) {
	if testing.Short() {
		t.Skip("runs media through a room for several seconds")
	}

	for _, isSimulcast := range []bool{true, false} {
		name := "single"
		if isSimulcast {
			name = "simulcast"
		}

		t.Run(name, func(t *testing.T) {
			ctx, room := newRepeatsRoom(t)

			host, hostClient, _, _ := CreatePeerPair(ctx, TestLogger, room, DefaultTestIceServers(), "host", true, isSimulcast, true)
			t.Cleanup(func() {
				_ = room.StopClient(hostClient.ID())
				_ = host.PeerConnection.Close()
			})

			runViewers(t, room, 3, 20*time.Second)
		})
	}
}

// The same, over an uplink that loses packets -- which is what produced the
// replays.
//
// A lost packet is asked for with a NACK and comes back as RTX. pion restores
// it to an ordinary packet on a path that bypasses the NACK generator's receive
// log, so the generator goes on asking for a packet it already has, and every
// retransmission that answers arrives as another copy of it. Each copy was being
// forwarded to every subscriber.
func TestViewerNeverReceivesARepeatedSequenceNumberOverALossyUplink(t *testing.T) {
	if testing.Short() {
		t.Skip("runs media through a room for several seconds")
	}

	for _, isSimulcast := range []bool{true, false} {
		name := "single"
		if isSimulcast {
			name = "simulcast"
		}

		t.Run(name, func(t *testing.T) {
			ctx, room := newRepeatsRoom(t)

			host, hostClient, drops := addLossyPublisher(t, ctx, room, "host", isSimulcast, 25)
			t.Cleanup(func() {
				_ = room.StopClient(hostClient.ID())
				_ = host.Close()
			})

			runViewers(t, room, 3, 20*time.Second)

			total, distinct, most := drops.retransmissionSummary()
			t.Logf("publisher dropped %d video packets; sent %d RTX retransmissions for %d distinct packets, at most %d for one",
				drops.dropped.Load(), total, distinct, most)
			require.NotZero(t, drops.dropped.Load(), "nothing was lost, so the test proves nothing")
			require.NotZero(t, total, "nothing was retransmitted, so the test proves nothing")
			require.LessOrEqual(t, most, maxNacksPerPacket,
				"a lost packet was asked for again after it had been recovered")
		})
	}
}
