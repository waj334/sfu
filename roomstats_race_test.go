package sfu

import (
	"sync"
	"testing"

	"github.com/pion/interceptor/pkg/stats"
)

// The crash this guards against is not a data race the runtime tolerates.
// Iterating a map while another goroutine writes it stops the process with a
// fatal error, which no recover up the stack can catch -- so a room reading a
// client's counters while that client's bitrate monitor updates them took the
// whole node down.
//
// The room is handed the very same *TrackStats the client has, so the locks
// have to live on that rather than on the wrapper around it.
func TestTrackStatsSumsAreSafeWhileBeingWritten(t *testing.T) {
	tracks := &TrackStats{
		senders:          make(map[string]stats.Stats),
		senderBitrates:   make(map[string]uint32),
		receivers:        make(map[string]stats.Stats),
		receiverBitrates: make(map[string]uint32),
	}

	// The writer, as the per-client bitrate monitor is: holding only the locks
	// that come with these maps.
	stop := make(chan struct{})
	var writers sync.WaitGroup
	writers.Add(2)

	go func() {
		defer writers.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			tracks.receiverMu.Lock()
			tracks.receiverBitrates[string(rune('a'+i%26))] = uint32(i)
			tracks.receivers[string(rune('a'+i%26))] = stats.Stats{}
			tracks.receiverMu.Unlock()
		}
	}()

	go func() {
		defer writers.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			tracks.senderMu.Lock()
			tracks.senderBitrates[string(rune('a'+i%26))] = uint32(i)
			tracks.senders[string(rune('a'+i%26))] = stats.Stats{}
			tracks.senderMu.Unlock()
		}
	}()

	// The reader, as Room.Stats is.
	for i := 0; i < 20000; i++ {
		tracks.sumReceived()
		tracks.sumSent()
	}

	close(stop)
	writers.Wait()
}
