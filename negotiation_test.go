package sfu

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSinglePeerConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create room manager and room
	sfuOpts := DefaultOptions()
	roomManager := NewManager(ctx, "test", sfuOpts)
	defer roomManager.Close()

	roomID := roomManager.CreateRoomID()
	roomName := "test-room"
	roomOpts := DefaultRoomOptions()

	testRoom, err := roomManager.NewRoom(roomID, roomName, RoomTypeLocal, roomOpts)
	require.NoError(t, err, "error creating room: %v", err)
	defer testRoom.Close()

	// Create a single peer pair
	pc, client, _, _ := CreatePeerPair(ctx, TestLogger, testRoom, DefaultTestIceServers(), "test-peer", false, false, true)
	defer pc.PeerConnection.Close()

	// Wait for connection to be established
	connectionStateChanged := make(chan webrtc.PeerConnectionState, 10)
	pc.PeerConnection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		t.Logf("Connection state changed to: %s", state)
		select {
		case connectionStateChanged <- state:
		default:
		}
	})

	// Wait for connected state or timeout
	timeout := time.After(15 * time.Second)
	for {
		select {
		case state := <-connectionStateChanged:
			switch state {
			case webrtc.PeerConnectionStateConnected:
				t.Log("Peer connection established successfully")
				goto connected
			case webrtc.PeerConnectionStateFailed:
				t.Fatal("Peer connection failed")
			default:
				t.Logf("Current connection state: %s", state)
			}
		case <-timeout:
			currentState := pc.PeerConnection.ConnectionState()
			t.Fatalf("Timeout waiting for connection, current state: %s", currentState)
		case <-ctx.Done():
			t.Fatal("Test context cancelled")
		}
	}

connected:
	// Verify client is active
	time.Sleep(1 * time.Second) // Allow some time for the client to be set up
	assert.NotNil(t, client)
	assert.Equal(t, ClientStateActive, client.state.Load())
}

func TestFlexFECSDP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create room manager and room
	sfuOpts := DefaultOptions()
	roomManager := NewManager(ctx, "test", sfuOpts)
	defer roomManager.Close()

	roomID := roomManager.CreateRoomID()
	roomName := "test-room"
	roomOpts := DefaultRoomOptions()

	testRoom, err := roomManager.NewRoom(roomID, roomName, RoomTypeLocal, roomOpts)
	require.NoError(t, err, "error creating room: %v", err)
	defer testRoom.Close()

	// Create a peer connection that supports FlexFEC to simulate a client offering FlexFEC
	// The SFU should accept and include FlexFEC in its answer when EnableFlexFEC is true
	mediaEngine := GetMediaEngine()

	// Configure FlexFEC on the simulated client side to offer it
	i := &interceptor.Registry{}
	if err := webrtc.ConfigureFlexFEC03(126, mediaEngine, i); err != nil {
		t.Fatalf("Failed to configure FlexFEC on simulated client: %v", err)
	}

	if err := webrtc.RegisterDefaultInterceptors(mediaEngine, i); err != nil {
		t.Fatalf("Failed to register interceptors on simulated client: %v", err)
	}

	webrtcAPI := webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine), webrtc.WithInterceptorRegistry(i))
	pc, err := webrtcAPI.NewPeerConnection(webrtc.Configuration{
		ICEServers: DefaultTestIceServers(),
	})
	require.NoError(t, err)
	defer pc.Close()

	// Create client with FlexFEC enabled
	id := testRoom.CreateClientID()
	opts := DefaultClientOptions()
	opts.IceTrickle = true
	opts.EnableFlexFEC = true
	client, err := testRoom.AddClient(id, id, opts)
	require.NoError(t, err)

	// Setup ICE candidate handling
	clientPendingCandidates := make([]*webrtc.ICECandidate, 0, 10)
	pendingCandidates := make([]*webrtc.ICECandidate, 0, 10)

	client.OnIceCandidate(func(ctx context.Context, candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		if pc.RemoteDescription() == nil {
			clientPendingCandidates = append(clientPendingCandidates, candidate)
			return
		}
		_ = pc.AddICECandidate(candidate.ToJSON())
	})

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		if client.PeerConnection().pc.RemoteDescription() == nil {
			pendingCandidates = append(pendingCandidates, candidate)
			return
		}
		err = client.PeerConnection().PC().AddICECandidate(candidate.ToJSON())
	})

	// Variables to store SDP for verification
	var receivedOffer webrtc.SessionDescription
	var receivedAnswer webrtc.SessionDescription

	client.onRenegotiation = func(ctx context.Context, offer webrtc.SessionDescription) (answer webrtc.SessionDescription, e error) {
		t.Log("Renegotiation triggered - inspecting SDP")
		receivedOffer = offer

		err = pc.SetRemoteDescription(offer)
		if err != nil {
			return webrtc.SessionDescription{}, err
		}

		answer, _ = pc.CreateAnswer(nil)
		receivedAnswer = answer

		err = pc.SetLocalDescription(answer)
		if err != nil {
			return webrtc.SessionDescription{}, err
		}

		return *pc.LocalDescription(), nil
	}

	// Add video transceiver to trigger negotiation
	pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	})

	// Override OnNegotiationNeeded to perform negotiation
	pc.OnNegotiationNeeded(func() {
		t.Log("OnNegotiationNeeded called")
		negotiate(pc, client, TestLogger, true)

		// Add pending candidates
		if len(clientPendingCandidates) > 0 {
			for _, candidate := range clientPendingCandidates {
				_ = pc.AddICECandidate(candidate.ToJSON())
			}
			clientPendingCandidates = nil
		}
		if len(pendingCandidates) > 0 {
			for _, candidate := range pendingCandidates {
				_ = client.PeerConnection().PC().AddICECandidate(candidate.ToJSON())
			}
			pendingCandidates = nil
		}
	})

	// Wait for negotiation to complete
	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-time.After(200 * time.Millisecond):
			if receivedOffer.SDP != "" && receivedAnswer.SDP != "" {
				goto sdpReceived
			}
		case <-timeout:
			t.Fatal("Timeout waiting for SDP offer and answer")
		case <-ctx.Done():
			t.Fatal("Test context cancelled")
		}
	}

sdpReceived:
	// Verify the client offers FlexFEC (since we configured it to support FlexFEC)
	t.Logf("Client Offer SDP FlexFEC lines:\n%s", extractFlexFECLines(receivedOffer.SDP))

	// The main test: Verify FlexFEC is present in the SFU's answer SDP
	// When a client offers FlexFEC and the SFU has EnableFlexFEC=true,
	// the SFU should accept and include FlexFEC in its answer
	assert.Contains(t, receivedAnswer.SDP, "a=rtpmap:126 flexfec-03", "Answer SDP should contain FlexFEC RTP map")
	assert.Contains(t, receivedAnswer.SDP, "a=fmtp:126", "Answer SDP should contain FlexFEC format parameters")
	t.Logf("FlexFEC successfully found in SFU answer SDP")

	// Log SDP content for verification
	t.Logf("SFU Answer SDP contains FlexFEC lines:\n%s", extractFlexFECLines(receivedAnswer.SDP))
}

// extractFlexFECLines extracts FlexFEC-related lines from SDP for debugging
func extractFlexFECLines(sdp string) string {
	lines := strings.Split(sdp, "\n")
	var flexfecLines []string
	for _, line := range lines {
		if strings.Contains(line, "126") && (strings.Contains(line, "flexfec") || strings.Contains(line, "rtpmap") || strings.Contains(line, "fmtp")) {
			flexfecLines = append(flexfecLines, line)
		}
	}
	return strings.Join(flexfecLines, "\n")
}

func TestOnNegotiationNeededCalledTwice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create room manager and room
	sfuOpts := DefaultOptions()
	roomManager := NewManager(ctx, "test", sfuOpts)
	defer roomManager.Close()

	roomID := roomManager.CreateRoomID()
	roomName := "test-room"
	roomOpts := DefaultRoomOptions()

	testRoom, err := roomManager.NewRoom(roomID, roomName, RoomTypeLocal, roomOpts)
	require.NoError(t, err, "error creating room: %v", err)
	defer testRoom.Close()

	// Create a peer connection manually to override OnNegotiationNeeded
	clientContext, cancelClient := context.WithCancel(ctx)
	defer cancelClient()

	mediaEngine := GetMediaEngine()
	webrtcAPI := webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine))
	pc, err := webrtcAPI.NewPeerConnection(webrtc.Configuration{
		ICEServers: DefaultTestIceServers(),
	})
	require.NoError(t, err)
	defer pc.Close()

	// Create client
	id := testRoom.CreateClientID()
	opts := DefaultClientOptions()
	opts.IceTrickle = true
	client, err := testRoom.AddClient(id, id, opts)
	require.NoError(t, err)

	// Counter for OnNegotiationNeeded calls
	negotiationCount := 0
	var mu sync.Mutex

	clientPendingCandidates := make([]*webrtc.ICECandidate, 0, 10)
	pendingCandidates := make([]*webrtc.ICECandidate, 0, 10)
	client.OnIceCandidate(func(ctx context.Context, candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}

		if pc.RemoteDescription() == nil {
			clientPendingCandidates = append(clientPendingCandidates, candidate)
			return
		}

		_ = pc.AddICECandidate(candidate.ToJSON())
	})

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}

		if client.PeerConnection().pc.RemoteDescription() == nil {
			pendingCandidates = append(pendingCandidates, candidate)
			return
		}

		err = client.PeerConnection().PC().AddICECandidate(candidate.ToJSON())
	})

	client.onRenegotiation = func(ctx context.Context, offer webrtc.SessionDescription) (answer webrtc.SessionDescription, e error) {
		t.Log("Renegotiation triggered")

		err = pc.SetRemoteDescription(offer)
		if err != nil {
			return webrtc.SessionDescription{}, err
		}

		answer, _ = pc.CreateAnswer(nil)
		err = pc.SetLocalDescription(answer)
		if err != nil {
			return webrtc.SessionDescription{}, err
		}

		return *pc.LocalDescription(), nil
	}

	client.OnAllowedRemoteRenegotiation(func() {
		TestLogger.Infof("allowed remote renegotiation")

	})

	// Override OnNegotiationNeeded to count calls and still perform negotiation
	pc.OnNegotiationNeeded(func() {
		mu.Lock()
		negotiationCount++
		currentCount := negotiationCount
		mu.Unlock()
		t.Logf("OnNegotiationNeeded called %d times", currentCount)

		// Still need to call negotiate to perform the actual negotiation
		negotiate(pc, client, TestLogger, true)
		if len(clientPendingCandidates) > 0 {
			for _, candidate := range clientPendingCandidates {
				_ = pc.AddICECandidate(candidate.ToJSON())
			}
			clientPendingCandidates = nil
		}
		if len(pendingCandidates) > 0 {
			for _, candidate := range pendingCandidates {
				_ = client.PeerConnection().PC().AddICECandidate(candidate.ToJSON())
			}
			pendingCandidates = nil
		}
	})

	// Add track to trigger negotiation
	pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	})
	pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	})

	// Wait until the first negotiation is done
	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-time.After(200 * time.Millisecond):
			if negotiationCount > 0 {
				t.Logf("Negotiation needed called %d times", negotiationCount)
				goto negotiationDone
			}
		case <-timeout:
			t.Fatal("Timeout waiting for negotiation needed to be called")
		case <-ctx.Done():
			t.Fatal("Test context cancelled")
		}
	}

negotiationDone:

	// Add more tracks to re-trigger negotiation
	tracks, _ := GetStaticTracks(clientContext, clientContext, "test-peer", false)
	SetPeerConnectionTracks(clientContext, pc, tracks)

	// Wait for the second negotiation to be done
	timeout = time.After(15 * time.Second)
	for {
		select {
		case <-time.After(200 * time.Millisecond):
			if negotiationCount > 1 {
				t.Logf("Negotiation needed called %d times", negotiationCount)
				goto finalCheck
			}
		case <-timeout:
			t.Fatal("Timeout waiting for second negotiation needed to be called")
		case <-ctx.Done():
			t.Fatal("Test context cancelled")
		}
	}

finalCheck:

	// Verify OnNegotiationNeeded was called exactly once
	mu.Lock()
	finalCount := negotiationCount
	mu.Unlock()

	assert.GreaterOrEqual(t, finalCount, 2, "OnNegotiationNeeded should be called at least twice, but was called %d times", finalCount)
}
