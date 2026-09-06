package sfu

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// A simulcast track has one read loop per layer and each one reports the track
// ended on its way out, so onEnded is called once per layer rather than once
// per track. Every subscriber's ended handling hangs off it — the bitrate
// claim is dropped there, and so is the sender on the peer connection — so
// firing it three times removed the claim three times (twice against a map it
// had already been taken out of, which logged an error) and marked
// renegotiation needed twice over.
func TestSimulcastTrackOnEndedFiresOncePerLayer(t *testing.T) {
	track := &SimulcastTrack{
		mu:               sync.RWMutex{},
		onEndedCallbacks: make([]func(), 0),
		isEnded:          &atomic.Bool{},
	}

	var calls atomic.Int32
	track.OnEnded(func() { calls.Add(1) })

	// High, mid and low, as they arrive when the publisher goes: the first to
	// notice cancels the track's context, and that is what ends the other two.
	for range 3 {
		track.onEnded()
	}

	require.Equal(t, int32(1), calls.Load())
}

// The layers do not end one after another — the context cancel that ends the
// second and third is the first one's doing, so all three land together. A
// guard that reads a flag and only sets it after running the callbacks is read
// by all three before any of them has set it.
func TestSimulcastTrackOnEndedFiresOnceUnderRace(t *testing.T) {
	track := &SimulcastTrack{
		mu:               sync.RWMutex{},
		onEndedCallbacks: make([]func(), 0),
		isEnded:          &atomic.Bool{},
	}

	var calls atomic.Int32
	track.OnEnded(func() { calls.Add(1) })

	start := make(chan struct{})
	wg := sync.WaitGroup{}

	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			track.onEnded()
		}()
	}

	close(start)
	wg.Wait()

	require.Equal(t, int32(1), calls.Load())
}

// The same guard on the subscriber's copy of the track, which is reachable
// from a track replacement as well as from the track ending.
func TestSimulcastClientTrackOnEndedFiresOnceUnderRace(t *testing.T) {
	clientTrack := &simulcastClientTrack{
		mu:                    sync.RWMutex{},
		isEnded:               &atomic.Bool{},
		onTrackEndedCallbacks: make([]func(), 0),
	}

	var calls atomic.Int32
	clientTrack.OnEnded(func() { calls.Add(1) })

	start := make(chan struct{})
	wg := sync.WaitGroup{}

	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			clientTrack.onEnded()
		}()
	}

	close(start)
	wg.Wait()

	require.Equal(t, int32(1), calls.Load())
}
