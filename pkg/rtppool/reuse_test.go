package rtppool

import (
	"testing"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

func packetWithExtensions(t *testing.T, seq uint16, ext byte) *rtp.Packet {
	t.Helper()
	p := &rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: seq, SSRC: 1}, Payload: []byte{1, 2, 3}}
	require.NoError(t, p.Header.SetExtension(1, []byte{ext}))
	require.NoError(t, p.Header.SetExtension(2, []byte{ext, ext}))
	return p
}

// A copy returned to the pool and reused must not reach back into the packet
// it was copied from: pooled packets keep their extension storage now, so a
// copy that shared the original's would have the next packet parsed into it
// overwrite the original's extensions.
func TestCopyReusedFromThePoolLeavesTheOriginalAlone(t *testing.T) {
	pool := New()
	original := packetWithExtensions(t, 1, 0xAA)

	copied := pool.CopyPacket(original)
	pool.PutPacket(copied)

	next := packetWithExtensions(t, 2, 0x55)
	raw, err := next.Marshal()
	require.NoError(t, err)
	reused := pool.GetPacket()
	require.NoError(t, reused.Unmarshal(raw))

	require.Equal(t, []byte{0xAA}, original.Header.GetExtension(1))
	require.Equal(t, []byte{0xAA, 0xAA}, original.Header.GetExtension(2))
	require.Equal(t, []byte{0x55}, reused.Header.GetExtension(1))
}

// Growing a copy's extensions (the TWCC interceptor adds one to every packet
// sent) must not write into the original, which every other copy shares.
func TestGrowingACopyLeavesTheOriginalAndOtherCopiesAlone(t *testing.T) {
	pool := New()
	original := packetWithExtensions(t, 1, 0xAA)
	original.Header.Extensions = append(make([]rtp.Extension, 0, 8), original.Header.Extensions...)

	first := pool.CopyPacket(original)
	second := pool.CopyPacket(original)
	require.NoError(t, first.Header.SetExtension(5, []byte{1}))
	require.NoError(t, second.Header.SetExtension(5, []byte{2}))

	require.Nil(t, original.Header.GetExtension(5))
	require.Equal(t, []byte{1}, first.Header.GetExtension(5))
	require.Equal(t, []byte{2}, second.Header.GetExtension(5))
}

// The point of keeping the storage: once warm, copying a packet with
// extensions and parsing one into a pooled packet allocate nothing.
func TestPooledPacketsDoNotAllocateOnceWarm(t *testing.T) {
	pool := New()
	original := packetWithExtensions(t, 1, 0xAA)
	raw, err := original.Marshal()
	require.NoError(t, err)

	// Warm: a packet with storage in the pool. A sync.Pool can be emptied by a
	// collection mid-run, so a stray allocation is allowed.
	pool.PutPacket(pool.CopyPacket(original))

	copies := testing.AllocsPerRun(1000, func() {
		pool.PutPacket(pool.CopyPacket(original))
	})
	parses := testing.AllocsPerRun(1000, func() {
		p := pool.GetPacket()
		_ = p.Unmarshal(raw)
		pool.PutPacket(p)
	})
	require.Less(t, copies, 0.1)
	require.Less(t, parses, 0.1)
}
