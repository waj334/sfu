package sfu

import (
	"errors"
	"sync"

	"github.com/pion/logging"
)

type packetCaches struct {
	mu     sync.RWMutex
	caches []Cache
	init   bool
	Size   uint16
	log    logging.LeveledLogger
}

type Cache struct {
	SeqNum    uint16
	Timestamp uint32
	BaseSeq   uint16
	// packet TID
	PTID uint8
	// packet SID
	PSID uint8
	// stream TID
	TID uint8
	// stream SID
	SID uint8
}
type OperationType uint8

const (
	KEEPLAYER      = OperationType(0)
	SCALEDOWNLAYER = OperationType(1)
	SCALEUPLAYER   = OperationType(2)
)

var (
	ErrCantDecide = errors.New("can't decide if upscale or downscale the layer")
	ErrDuplicate  = errors.New("packet sequence already exists in the cache")
)

func newPacketCaches(log logging.LeveledLogger) *packetCaches {
	return &packetCaches{
		mu:     sync.RWMutex{},
		caches: make([]Cache, 0, 1024),
		init:   false,
		Size:   1000,
		log:    log,
	}
}

func (p *packetCaches) Add(seqNum, baseSequence uint16, ts uint32, psid, tsid, sid, tid uint8) {
	p.mu.Lock()
	defer func() {
		p.mu.Unlock()

		if uint16(len(p.caches)) > p.Size {
			copy(p.caches, p.caches[1:])
			p.caches = p.caches[:len(p.caches)-1]
		}
	}()

	newCache := Cache{
		SeqNum:    seqNum,
		BaseSeq:   baseSequence,
		Timestamp: ts,
		PSID:      psid,
		PTID:      tsid,
		TID:       tid,
		SID:       sid,
	}

	if len(p.caches) == 0 {
		p.caches = append(p.caches, newCache)

		return
	}

	// add packet in order
Loop:
	for i := len(p.caches) - 1; i >= 0; i-- {
		currentCache := p.caches[i]
		if currentCache.SeqNum == seqNum {
			p.log.Warnf("packet cache: packet sequence ", seqNum, " already exists in the cache, will not adding the packet")

			return
		}

		if (currentCache.SeqNum < seqNum && seqNum-currentCache.SeqNum < uint16SizeHalf) ||
			(currentCache.SeqNum-seqNum > uint16SizeHalf) {
			insertIdx := i + 1
			if insertIdx == len(p.caches) {
				p.caches = append(p.caches, newCache)
			} else {
				p.caches = append(p.caches, Cache{})
				copy(p.caches[insertIdx+1:], p.caches[insertIdx:])
				p.caches[insertIdx] = newCache
			}

			break Loop
		} else if i == 0 {
			p.caches = append(p.caches, Cache{})
			copy(p.caches[1:], p.caches[:len(p.caches)-1])
			p.caches[0] = newCache

			break Loop
		}
	}
}

// This will provide decided sid and tid that can be used for the current packet
// it can return the same sid and tid if the sequence number is in sequence
// it will decide to upscale or downscale the sid and tid based on the sequence number
func (p *packetCaches) GetDecision(currentSeqNum, currentBaseSeq uint16, currentSID, currentTID uint8) (baseSeq uint16, sid uint8, tid uint8, err error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for i := len(p.caches) - 1; i >= 0; i-- {
		currentCache := p.caches[i]
		if currentCache.SeqNum == currentSeqNum {
			return currentBaseSeq, currentSID, currentTID, ErrDuplicate
		}

		// the current packet is not late
		if currentCache.SeqNum < currentSeqNum &&
			currentCache.SeqNum-currentSeqNum > uint16SizeHalf && currentSeqNum-currentCache.SeqNum == 1 {
			// next packet is the next sequence number, allowed to upscale or downscale
			return currentCache.BaseSeq, currentSID, currentTID, nil
		}

		if currentCache.SeqNum < currentSeqNum &&
			currentCache.SeqNum-currentSeqNum > uint16SizeHalf && currentSeqNum-currentCache.SeqNum > 1 {
			// next packet has a gap, can't decide keep the current SID and TID
			return currentCache.BaseSeq, currentCache.SID, currentCache.TID, ErrCantDecide
		}
	}

	// can't decide could be because the cache is empty
	if len(p.caches) == 0 {
		return currentBaseSeq, currentSID, currentTID, ErrCantDecide
	}

	return currentBaseSeq, currentSID, currentTID, ErrCantDecide
}

func (p *packetCaches) IsAllowToUpscaleDownscale(seqNum uint16) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.caches) == 0 {
		return true
	}

	cache := p.caches[len(p.caches)-1]
	return cache.SeqNum < seqNum
}
