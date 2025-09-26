package sir

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const Magic uint32 = 0x53_49_52_00

var Marker = [16]byte{
	0x48, 0x44, 0x41, 0x59, 0x52, 0x4F, 0x42, 0x4F,
	0x40, 0x11, 0xDA, 0x70, 0x80, 0xB0, 0x71, 0xC2,
}

type sink struct {
	w io.Writer
	x Indexer[uint64, []byte]

	// Total size of the file except for the footer.
	l uint64

	n uint64   // Size of the block.
	b [][]byte // Block.

	cb bytes.Buffer
	c  Compressor

	t indexTable
}

func NewSink(w io.Writer, x Indexer[uint64, []byte]) (Writer[[]byte], error) {
	v := &sink{
		w: w,
		x: x,
		l: HeaderByteSize,
		c: &NopCompressor{},
		t: newIndexTable(HeaderByteSize),
	}

	h := Header{}
	b, err := h.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshal header: %w", err)
	}
	if _, err := w.Write(b); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	v.c.Reset(&v.cb)

	return v, nil
}

func (s *sink) Write(p []byte) error {
	n := uint64(len(p))
	if s.n+n > math.MaxUint32 {
		return errors.New("block too large")
	}

	s.n += 4 + n
	s.b = append(s.b, p)

	sb := [4]byte{}
	binary.LittleEndian.PutUint32(sb[:], uint32(n))
	if _, err := s.c.Write(sb[:]); err != nil {
		return err
	}
	if _, err := s.c.Write(p); err != nil {
		return err
	}

	i := s.x(p)
	s.t.tick(i, uint64(len(p)))

	return nil
}

func (s *sink) Flush() error {
	if s.n == 0 {
		return nil
	}

	if err := s.c.Flush(); err != nil {
		return err
	}

	n := s.cb.Len()
	if n > math.MaxUint32 {
		return errors.New("compressed data too large")
	}

	head := [8]byte{}
	binary.LittleEndian.PutUint32(head[0:4], uint32(n))
	binary.LittleEndian.PutUint32(head[4:8], uint32(s.n))

	if _, err := s.w.Write(head[:]); err != nil {
		return fmt.Errorf("write payload header: %w", err)
	}
	if _, err := io.Copy(s.w, &s.cb); err != nil {
		return fmt.Errorf("write compressed data: %w", err)
	}
	if _, err := s.w.Write(Marker[:]); err != nil {
		return fmt.Errorf("write sync marker: %w", err)
	}

	s.l += 4 + 4 + s.n + uint64(len(Marker))
	s.n = 0
	s.b = s.b[:0]
	s.cb.Reset()
	s.c.Reset(&s.cb)
	s.t.tock()

	return nil
}

func (s *sink) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}

	if s.t.Len() == 0 {
		// Empty file.
		s.l += 24
		seal := [24]byte{}
		copy(seal[8:], Marker[:])
		if _, err := s.w.Write(seal[:]); err != nil {
			return nil
		}

		empty_table := [IndexGroupByteSize]byte{}
		if _, err := s.w.Write(empty_table[:]); err != nil {
			return nil
		}
	} else if err := encodeIndexTable(s.w, s.t); err != nil {
		return err
	}

	footer := [12]byte{}
	binary.LittleEndian.PutUint64(footer[0:8], s.l)
	binary.BigEndian.PutUint32(footer[8:12], Magic)

	if _, err := s.w.Write(footer[:]); err != nil {
		return err
	}
	return nil
}
