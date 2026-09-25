package sfu

import (
	"testing"

	"github.com/pion/logging"
	"github.com/stretchr/testify/require"
)

func TestPacketCachesInOrder(t *testing.T) {
	t.Parallel()
	logger := logging.NewDefaultLoggerFactory().NewLogger("test")
	pc := newPacketCaches(logger)

	for seq := uint16(100); seq < 110; seq++ {
		pc.Add(seq, 100, 1000, 1, 1, 1, 1)
	}

	require.Equal(t, 10, len(pc.caches))
	for i := 0; i < 10; i++ {
		require.Equal(t, uint16(100+i), pc.caches[i].SeqNum)
	}

	require.True(t, pc.IsAllowToUpscaleDownscale(110))
	require.False(t, pc.IsAllowToUpscaleDownscale(105))
}

func TestPacketCachesOutOfOrderAndWrap(t *testing.T) {
	t.Parallel()
	logger := logging.NewDefaultLoggerFactory().NewLogger("test")
	pc := newPacketCaches(logger)

	// Add sequence numbers around the wrap boundary: 65534, 0, 65535, 1
	seqs := []uint16{65534, 0, 65535, 1}
	for _, seq := range seqs {
		pc.Add(seq, 65534, 1000, 1, 1, 1, 1)
	}

	require.Equal(t, 4, len(pc.caches))
	expected := []uint16{65534, 65535, 0, 1}
	for i, exp := range expected {
		require.Equal(t, exp, pc.caches[i].SeqNum)
	}
}

func TestPacketCachesDuplicates(t *testing.T) {
	t.Parallel()
	logger := logging.NewDefaultLoggerFactory().NewLogger("test")
	pc := newPacketCaches(logger)

	pc.Add(10, 10, 1000, 1, 1, 1, 1)
	pc.Add(10, 10, 1000, 1, 1, 1, 1) // duplicate, should be skipped
	require.Equal(t, 1, len(pc.caches))

	_, _, _, err := pc.GetDecision(10, 10, 1, 1)
	require.Equal(t, ErrDuplicate, err)
}

func TestPacketCachesSizeLimit(t *testing.T) {
	t.Parallel()
	logger := logging.NewDefaultLoggerFactory().NewLogger("test")
	pc := newPacketCaches(logger)
	pc.Size = 5 // smaller size for test

	for seq := uint16(1); seq <= 10; seq++ {
		pc.Add(seq, 1, 1000, 1, 1, 1, 1)
	}

	require.Equal(t, 5, len(pc.caches))
	expected := []uint16{6, 7, 8, 9, 10}
	for i, exp := range expected {
		require.Equal(t, exp, pc.caches[i].SeqNum)
	}
}
