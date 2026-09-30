package rtppool

import (
	"sync"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
)

type RTPPool struct {
	pool          sync.Pool
	PacketManager *PacketManager
}

func New() *RTPPool {
	pm := NewPacketManager()

	return &RTPPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &rtp.Packet{}
			},
		},

		PacketManager: pm,
	}
}

// PutPacket returns p to the pool.
//
// The packet keeps its extension and CSRC storage, emptied, so that the next
// packet parsed or copied into it fills that rather than allocating: once per
// packet a host sends and once per viewer per packet to copy it, which was a
// large share of the node's garbage. A pooled packet always owns that storage
// (Unmarshal appends into its own, CopyPacket copies into it), so nothing
// else is pointing into it.
func (r *RTPPool) PutPacket(p *rtp.Packet) {
	extensions := p.Header.Extensions[:cap(p.Header.Extensions)]
	clear(extensions) // drop the references into the payloads they came from
	csrc := p.Header.CSRC[:0]

	*p = rtp.Packet{}
	p.Header.Extensions = extensions[:0]
	p.Header.CSRC = csrc
	r.pool.Put(p)
}

// CopyPacket creates a copy of the RTP packet using the pool.
// The returned packet MUST be returned to the pool via PutPacket when done.
// WARNING: Do not use the returned packet across goroutine boundaries without additional synchronization.
// WARNING: Do not hold references to the packet after calling PutPacket.
//
// The extensions and CSRCs are copied into the packet's own storage rather than
// shared with p's: a copy that shared them would, back in the pool, let the
// next packet parsed into it overwrite p's, and a copy that grew them (the TWCC
// interceptor adds an extension to every packet sent) could write into p's
// spare capacity, which every other subscriber's copy of p shares too.
func (r *RTPPool) CopyPacket(p *rtp.Packet) *rtp.Packet {
	newPacket := r.GetPacket()
	extensions := newPacket.Header.Extensions[:0]
	csrc := newPacket.Header.CSRC[:0]

	*newPacket = *p
	newPacket.Header.Extensions = append(extensions, p.Header.Extensions...)
	newPacket.Header.CSRC = append(csrc, p.Header.CSRC...)

	return newPacket
}

func (r *RTPPool) GetPayload() *[]byte {
	return r.PacketManager.PayloadPool.Get()
}

func (r *RTPPool) PutPayload(localPayload *[]byte) {
	*localPayload = (*localPayload)[:0]
	r.PacketManager.PayloadPool.Put(localPayload)
}

func (r *RTPPool) NewPacket(header *rtp.Header, payload []byte, attr interceptor.Attributes) *RetainablePacket {
	pkt, err := r.PacketManager.NewPacket(header, payload, attr)
	if err != nil {
		return nil
	}

	return pkt
}

func (r *RTPPool) GetPacket() *rtp.Packet {
	return r.pool.Get().(*rtp.Packet)
}

type BufferPool struct {
	pool *sync.Pool
}

func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: &sync.Pool{
			New: func() interface{} {
				// Initialize with a reasonable default capacity, e.g., 1500 bytes
				buf := make([]byte, 0, 1500)
				return &buf
			},
		},
	}
}

func (r *BufferPool) Get() *[]byte {
	ipayload := r.pool.Get()
	return ipayload.(*[]byte) //nolint:forcetypeassert
}

func (r *BufferPool) Put(localPayload *[]byte) {
	*localPayload = (*localPayload)[:0]

	r.pool.Put(localPayload)
}
