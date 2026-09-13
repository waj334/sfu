package sfu

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDuplicateFilterPassesAStreamInOrder(t *testing.T) {
	var f duplicateFilter
	for seq := uint16(100); seq < 3100; seq++ {
		require.False(t, f.seen(seq), "seq %d", seq)
	}
}

func TestDuplicateFilterCatchesTheSamePacketTwice(t *testing.T) {
	var f duplicateFilter
	for seq := uint16(0); seq < 10; seq++ {
		f.seen(seq)
	}

	require.True(t, f.seen(9), "the newest packet again")
	require.True(t, f.seen(3), "an older packet again")
}

// A late packet that never arrived before is not a copy. It is what a
// retransmission looks like when the original was really lost.
func TestDuplicateFilterPassesALatePacketOnce(t *testing.T) {
	var f duplicateFilter
	for seq := uint16(0); seq < 700; seq++ {
		if seq == 5 {
			continue
		}
		f.seen(seq)
	}

	require.False(t, f.seen(5), "first arrival of a lost packet")
	require.True(t, f.seen(5), "and its retransmission after that")
}

// Every packet in the window is still recognised, whatever size the steps were
// that moved it back.
func TestDuplicateFilterRemembersAcrossUnevenSteps(t *testing.T) {
	var f duplicateFilter
	delivered := []uint16{}
	seq := uint16(1)
	for _, step := range []uint16{1, 63, 64, 65, 1, 127, 200, 3, 128} {
		seq += step
		require.False(t, f.seen(seq))
		delivered = append(delivered, seq)
	}

	for _, d := range delivered {
		require.True(t, f.seen(d), "seq %d", d)
	}
}

func TestDuplicateFilterWorksAcrossTheWrap(t *testing.T) {
	var f duplicateFilter
	for seq := uint16(65500); seq != 40; seq++ {
		require.False(t, f.seen(seq), "seq %d", seq)
	}

	require.True(t, f.seen(65530), "a copy from before the wrap")
	require.True(t, f.seen(2), "a copy from after it")
}

// Further back than the window is not dropped: nothing remembers it, and a
// numbering that has started again must not lose its first packets.
func TestDuplicateFilterPassesWhatIsBeyondTheWindow(t *testing.T) {
	var f duplicateFilter
	for seq := uint16(5000); seq < 7000; seq++ {
		f.seen(seq)
	}

	require.False(t, f.seen(10), "a restart far behind")
	require.False(t, f.seen(11))
	require.True(t, f.seen(10), "and the new run is remembered")
}

func TestDuplicateFilterForgetsAfterALongJumpAhead(t *testing.T) {
	var f duplicateFilter
	for seq := uint16(0); seq < 100; seq++ {
		f.seen(seq)
	}

	require.False(t, f.seen(3000))
	require.False(t, f.seen(2999), "a jump past the window leaves nothing recorded behind it")
}
