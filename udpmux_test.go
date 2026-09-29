package sfu

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// Peers connect through the sharded mux as they did through one socket: ICE
// finds its way to each of them, whichever socket their packets arrive on, and
// what they send gets both there and back.
func TestUDPMuxConnectsPeersAcrossShards(t *testing.T) {
	const peers = 16

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	port := freeUDPPort(t)
	mux := NewUDPMux(ctx, port)
	defer mux.Close()

	serverSettings := webrtc.SettingEngine{}
	serverSettings.SetICEUDPMux(mux.Mux())
	serverSettings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	serverAPI := webrtc.NewAPI(webrtc.WithSettingEngine(serverSettings))

	clientSettings := webrtc.SettingEngine{}
	clientSettings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	clientSettings.SetIncludeLoopbackCandidate(true)
	clientAPI := webrtc.NewAPI(webrtc.WithSettingEngine(clientSettings))

	errs := make(chan error, peers)
	for i := 0; i < peers; i++ {
		go func() { errs <- echoThroughMux(ctx, serverAPI, clientAPI, i, port) }()
	}

	for i := 0; i < peers; i++ {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}

func echoThroughMux(ctx context.Context, serverAPI, clientAPI *webrtc.API, i, muxPort int) error {
	server, err := serverAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return err
	}
	defer server.Close()

	client, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return err
	}
	defer client.Close()

	server.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			_ = dc.SendText("echo " + string(msg.Data))
		})
	})

	dc, err := client.CreateDataChannel("echo", nil)
	if err != nil {
		return err
	}

	want := fmt.Sprintf("echo peer-%d", i)
	got := make(chan string, 1)
	dc.OnOpen(func() { _ = dc.SendText(fmt.Sprintf("peer-%d", i)) })
	dc.OnMessage(func(msg webrtc.DataChannelMessage) { got <- string(msg.Data) })

	offer, err := client.CreateOffer(nil)
	if err != nil {
		return err
	}
	clientGathered := webrtc.GatheringCompletePromise(client)
	if err := client.SetLocalDescription(offer); err != nil {
		return err
	}
	<-clientGathered

	if err := server.SetRemoteDescription(*client.LocalDescription()); err != nil {
		return err
	}
	answer, err := server.CreateAnswer(nil)
	if err != nil {
		return err
	}
	serverGathered := webrtc.GatheringCompletePromise(server)
	if err := server.SetLocalDescription(answer); err != nil {
		return err
	}
	<-serverGathered

	if err := client.SetRemoteDescription(*server.LocalDescription()); err != nil {
		return err
	}

	select {
	case msg := <-got:
		if msg != want {
			return fmt.Errorf("peer %d: got %q, want %q", i, msg, want)
		}

		// And it was the mux that carried it, not a socket of the server's own.
		pair, err := server.SCTP().Transport().ICETransport().GetSelectedCandidatePair()
		if err != nil || pair == nil {
			return fmt.Errorf("peer %d: no selected candidate pair: %v", i, err)
		}
		if int(pair.Local.Port) != muxPort {
			return fmt.Errorf("peer %d: server connected on port %d, not the mux's %d", i, pair.Local.Port, muxPort)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("peer %d: no echo (client ICE %s)", i, client.ICEConnectionState())
	}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()

	c, err := net.ListenUDP("udp4", &net.UDPAddr{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	return c.LocalAddr().(*net.UDPAddr).Port
}
