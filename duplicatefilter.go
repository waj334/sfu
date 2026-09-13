package sfu

const (
	duplicateWindowWords = 16
	duplicateWindowSize  = duplicateWindowWords * 64
)

// duplicateFilter recognises an RTP sequence number that has already been
// delivered from one remote track.
//
// A publisher's packet can arrive twice, and the second copy is not stopped on
// the way in. SRTP's replay protection only sees the SSRC a packet arrived on,
// and a retransmission arrives on the RTX SSRC; pion then restores it to the
// media stream's SSRC and sequence number and hands it over as an ordinary
// packet. Forwarded, that copy reached every subscriber -- where libwebrtc
// rejected it as an SRTP replay, or, on a simulcast track whose numbering is
// the SFU's own, decoded it twice.
//
// The window is the last 1024 sequence numbers, which is further back than any
// retransmission is asked for: the NACK generator only keeps 512.
//
// Not safe for concurrent use. One track is read by one goroutine.
type duplicateFilter struct {
	started bool
	highest uint16

	// Bit n records whether highest-n has been delivered.
	window [duplicateWindowWords]uint64
}

// seen records a sequence number and reports whether it had been recorded
// already.
func (f *duplicateFilter) seen(seq uint16) bool {
	if !f.started {
		f.started = true
		f.highest = seq
		f.window = [duplicateWindowWords]uint64{}
		f.window[0] = 1

		return false
	}

	// Ahead of everything so far, by less than half the number space.
	if ahead := seq - f.highest; ahead != 0 && ahead < 1<<15 {
		f.shift(int(ahead))
		f.highest = seq
		f.window[0] |= 1

		return false
	}

	behind := int(f.highest - seq)
	if behind >= duplicateWindowSize {
		// Further back than the window reaches, or the numbering has started
		// again. Nothing here can say whether it was delivered before, so it
		// is taken as the start of a new run rather than dropped.
		f.started = false

		return f.seen(seq)
	}

	word, bit := behind/64, uint(behind%64)
	if f.window[word]&(1<<bit) != 0 {
		return true
	}

	f.window[word] |= 1 << bit

	return false
}

// shift moves every recorded bit n places further back.
func (f *duplicateFilter) shift(n int) {
	if n >= duplicateWindowSize {
		f.window = [duplicateWindowWords]uint64{}
		return
	}

	words, bits := n/64, uint(n%64)
	for i := duplicateWindowWords - 1; i >= 0; i-- {
		var v uint64

		if src := i - words; src >= 0 {
			v = f.window[src] << bits
			if bits > 0 && src > 0 {
				v |= f.window[src-1] >> (64 - bits)
			}
		}

		f.window[i] = v
	}
}
