package sfu

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func resetShedding(t *testing.T) {
	t.Helper()
	shedToMid.Store(0)
	shedToLow.Store(0)
	t.Cleanup(func() {
		shedToMid.Store(0)
		shedToLow.Store(0)
	})
}

// Over the target it sheds a step at a time, no faster than the cooldown, to
// the mid layer for everyone before the low layer for anyone.
func TestShedControllerShedsAStepPerCooldown(t *testing.T) {
	resetShedding(t)
	var lastShed, calmSince time.Time
	target := 25 * time.Millisecond
	now := time.Unix(1000, 0)
	over := func(after time.Duration) {
		now = now.Add(after)
		shedStepFor(now, 100*time.Millisecond, target, &lastShed, &calmSince)
	}

	over(0)
	require.EqualValues(t, 1000, shedToMid.Load(), "first step is at once")

	over(shedTick)
	require.EqualValues(t, 1000, shedToMid.Load(), "inside the cooldown: hold")

	over(shedCooldown)
	require.EqualValues(t, 2000, shedToMid.Load())

	for i := 0; i < 20; i++ {
		over(shedCooldown)
	}
	require.EqualValues(t, shedFull, shedToMid.Load(), "everyone at mid first")
	require.Greater(t, shedToLow.Load(), int32(0), "then some at low")

	for i := 0; i < 20; i++ {
		over(shedCooldown)
	}
	require.EqualValues(t, shedFull, shedToLow.Load(), "and no further than everyone")
}

// Calm for long enough, it gives back, low caps first, a smaller step at a time.
func TestShedControllerRecoversSlowlyLowFirst(t *testing.T) {
	resetShedding(t)
	shedToMid.Store(shedFull)
	shedToLow.Store(1000)
	var lastShed, calmSince time.Time
	target := 25 * time.Millisecond
	now := time.Unix(1000, 0)
	calm := func(after time.Duration) {
		now = now.Add(after)
		shedStepFor(now, time.Millisecond, target, &lastShed, &calmSince)
	}

	calm(0)
	calm(shedCalm - shedTick)
	require.EqualValues(t, 1000, shedToLow.Load(), "not calm for long enough yet")

	calm(shedTick)
	require.EqualValues(t, 500, shedToLow.Load(), "low first, by the smaller step")
	require.EqualValues(t, shedFull, shedToMid.Load())

	calm(shedCalm)
	calm(shedCalm)
	require.EqualValues(t, 0, shedToLow.Load())
	require.EqualValues(t, shedFull-shedRecoverStep, shedToMid.Load(), "then mid")
}

// Between a quarter of the target and the target nothing changes, and a spell
// there restarts the wait for calm.
func TestShedControllerHoldsBetween(t *testing.T) {
	resetShedding(t)
	shedToMid.Store(3000)
	var lastShed, calmSince time.Time
	target := 25 * time.Millisecond
	now := time.Unix(1000, 0)

	shedStepFor(now, time.Millisecond, target, &lastShed, &calmSince)
	now = now.Add(shedCalm - shedTick)
	shedStepFor(now, 15*time.Millisecond, target, &lastShed, &calmSince) // between
	now = now.Add(shedTick)
	shedStepFor(now, time.Millisecond, target, &lastShed, &calmSince)
	require.EqualValues(t, 3000, shedToMid.Load(), "the calm spell restarted")
}

// A viewer's rank is fixed by its id, and caps it once shedding reaches it.
func TestShedCapByRank(t *testing.T) {
	resetShedding(t)
	require.Equal(t, shedRank("viewer-a"), shedRank("viewer-a"))

	require.EqualValues(t, QualityHigh, shedCap(0))

	shedToMid.Store(5000)
	require.EqualValues(t, QualityMid, shedCap(4999))
	require.EqualValues(t, QualityHigh, shedCap(5000))

	shedToLow.Store(2000)
	require.EqualValues(t, QualityLow, shedCap(1999))
	require.EqualValues(t, QualityMid, shedCap(2000))
}

func TestNoteQueueWaitKeepsTheLongest(t *testing.T) {
	egressQueueWaitMax.Store(0)
	noteQueueWait(3 * time.Millisecond)
	noteQueueWait(9 * time.Millisecond)
	noteQueueWait(1 * time.Millisecond)
	require.EqualValues(t, 9*time.Millisecond, egressQueueWaitMax.Swap(0))
}

// The wait is sampled with shedding off too, and shedding stays off.
func TestEgressMonitorSamplesWithSheddingOff(t *testing.T) {
	resetShedding(t)
	egressQueueWaitSeen.Store(0)
	t.Cleanup(func() { egressQueueWaitSeen.Store(0) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runEgressMonitor(ctx, 0)

	noteQueueWait(200 * time.Millisecond)
	require.Eventually(t, func() bool {
		_, _, wait := EgressShedding()
		return wait == 200*time.Millisecond
	}, 3*shedTick, 10*time.Millisecond)

	mid, low, _ := EgressShedding()
	require.Zero(t, mid)
	require.Zero(t, low)
}
