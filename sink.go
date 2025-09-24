package sir

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

const Magic uint32 = 0x53_49_52_00

var Marker = [16]byte{
	0x40, 0x11, 0xDA, 0x70,
	0x80, 0xB0, 0x71, 0xC2,
	// TODO
}

type sink struct {
	w io.Writer
	x Indexer[uint64, []byte]

	n uint64 // Size of the block.
	c Compressor

	t indexTable
}

func NewSink(w io.Writer, x Indexer[uint64, []byte]) Writer[[]byte] {
	v := &sink{
		w: w,
		x: x,
		t: newIndexTable(0),
	}

	return v
}

func (s sink) Write(p []byte) error {
	s.n += uint64(len(p))
	if s.n > math.MaxUint32 {
		return errors.New("block too large")
	}
	if _, err := s.c.Write(p); err != nil {
		return err
	}

	i := s.x(p)
	s.t.tick(i, uint64(len(p)))

	return nil
}

func (s sink) Flush() error {
	if err := s.c.Flush(); err != nil {
		return err
	}

	tail := [20]byte{}
	binary.BigEndian.PutUint32(tail[0:4], uint32(s.n))
	copy(tail[4:20], Marker[:])

	if _, err := s.w.Write(tail[:]); err != nil {
		return err
	}

	s.c.Reset(s.w)
	s.t.tock()

	return nil
}

func (s sink) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}
	return nil
}

type Header struct {
	Compression Compression
	Metadata    []byte
}
