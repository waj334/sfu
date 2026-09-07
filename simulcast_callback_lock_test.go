package sfu

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ranWithin reports whether f finished inside d.
//
// A deadlock is not an error the callee can return, so it is measured rather
// than caught. The goroutine that loses is leaked deliberately: it is blocked on
// a mutex nothing will release, which is the very thing being asserted about.
func ranWithin(d time.Duration, f func()) bool {
	done := make(chan struct{})

	go func() {
		f()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// A callback told that a layer has been added is being told about this track,
// so reading this track is the ordinary thing for it to do — sendPLI,
// isTrackActive and GetRemoteTrack all take the track's own mutex. Running the
// callbacks while holding that mutex deadlocks the goroutine adding the layer
// against a lock it holds itself, because a Go RWMutex is not reentrant.
//
// What that cost: the second and third simulcast layers never finished being
// added, and every forwarding decision — getQuality asks isTrackActive — blocked
// behind the same mutex. The publisher's media kept arriving and none of it went
// anywhere, and the subscriber whose track was never delivered sat renegotiating
// against an empty transceiver.
func TestRemoteTrackAddedCallbacksDoNotHoldTheTrackLock(t *testing.T) {
	track := &SimulcastTrack{mu: sync.RWMutex{}}

	var ran atomic.Bool
	track.onRemoteTrackAdded(func(*remoteTrack) {
		// Whatever sendPLI and isTrackActive do, they start here.
		track.mu.RLock()
		track.mu.RUnlock()
		ran.Store(true)
	})

	require.True(t,
		ranWithin(2*time.Second, func() { track.onRemoteTrackAddedCallbacks(nil) }),
		"adding a layer deadlocked against the track's own lock")
	require.True(t, ran.Load())
}

// The same, for the callback that says all three layers have arrived. It is
// raised from the same place, about the same track, a few lines earlier.
func TestTrackCompleteCallbacksDoNotHoldTheTrackLock(t *testing.T) {
	track := &SimulcastTrack{mu: sync.RWMutex{}}

	var ran atomic.Bool
	track.OnTrackComplete(func() {
		track.mu.RLock()
		track.mu.RUnlock()
		ran.Store(true)
	})

	require.True(t,
		ranWithin(2*time.Second, func() { track.onTrackComplete() }),
		"completing a track deadlocked against the track's own lock")
	require.True(t, ran.Load())
}

// A callback that registers another one is the shape the subscriber constructor
// has: it hangs a PLI request off the track while the track is handing out the
// callbacks it already holds. Appending takes the write lock, so it deadlocks
// under a read lock as surely as under a write one — which is why the list is
// copied rather than ranged over in place.
func TestACallbackMayRegisterAnotherCallback(t *testing.T) {
	track := &SimulcastTrack{mu: sync.RWMutex{}}

	track.onRemoteTrackAdded(func(*remoteTrack) {
		track.onRemoteTrackAdded(func(*remoteTrack) {})
	})

	require.True(t,
		ranWithin(2*time.Second, func() { track.onRemoteTrackAddedCallbacks(nil) }),
		"registering a callback from inside one deadlocked")
}
