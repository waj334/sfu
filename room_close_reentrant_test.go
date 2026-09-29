package sfu

import (
	"context"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// A room-closed callback that closes the room again — an extension tearing
// down its own state does exactly that — used to run inside the first Close's
// read lock. With a StopClient queued for the write lock in between, the second
// read lock waited on the writer and the writer on the first read lock, and the
// room hung for good: every join, leave and stats read on the node behind it.
func TestRoomCloseFromItsOwnCallbackWithWriterQueued(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	roomManager := NewManager(ctx, "test", sfuOpts)
	defer roomManager.Close()

	roomOpts := DefaultRoomOptions()
	roomOpts.Codecs = &[]string{webrtc.MimeTypeH264, webrtc.MimeTypeOpus}
	room, err := roomManager.NewRoom(roomManager.CreateRoomID(), "test-room", RoomTypeLocal, roomOpts)
	require.NoError(t, err)

	var reentrantErr error
	room.OnRoomClosed(func(string) {
		// A session teardown arriving now, as it did in production.
		writerDone := make(chan struct{})
		go func() {
			defer close(writerDone)
			_ = room.StopClient("nobody")
		}()
		time.Sleep(50 * time.Millisecond)

		reentrantErr = room.Close()
		<-writerDone
	})

	closed := make(chan error, 1)
	go func() { closed <- room.Close() }()

	select {
	case err := <-closed:
		require.NoError(t, err)
		require.ErrorIs(t, reentrantErr, ErrRoomIsClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Room.Close deadlocked when a close callback closed the room again")
	}
}
