package sir

import (
	"encoding/binary"
	"errors"
	"io"
	"iter"
)

const (
	IndexGroupSize     = 63
	IndexGroupByteSize = (8 + 8) + ((IndexGroupSize - 1) * 8)
)

type indexTable struct {
	pos uint64

	slot   *indexSlot
	groups [][]indexSlot
}

type indexSlot struct {
	I uint64
	P uint64
}

func newIndexTable(init uint64) indexTable {
	v := indexTable{
		pos: init,
		groups: [][]indexSlot{
			make([]indexSlot, 1, IndexGroupSize),
		},
	}
	v.slot = &v.groups[0][0]

	return v
}

func (t *indexTable) tick(i uint64, s uint64) {
	if t.slot != nil {
		t.slot.I = i
		t.slot.P = t.pos
		t.slot = nil
	}

	t.pos += s
}

func (t *indexTable) tock() {
	g := t.groups[len(t.groups)-1]
	l := len(g)
	if l == 1 && g[0] == (indexSlot{}) {
		return
	}
	if l == IndexGroupSize {
		g = make([]indexSlot, 0, IndexGroupSize)
		t.groups = append(t.groups, g)
	}

	g = append(g, indexSlot{})
	t.slot = &g[len(g)-1]
	t.groups[len(t.groups)-1] = g
}

func (t *indexTable) iter() iter.Seq2[uint64, []indexSlot] {
	return func(yield func(uint64, []indexSlot) bool) {
		for i, g := range t.groups {
			j := len(g) - 1
			if g[j] == (indexSlot{}) {
				if j == 0 {
					return
				}
				yield(uint64(i), g[:j])
				return
			}
			if !yield(uint64(i), g) {
				return
			}
		}
	}
}

func encodeIndexTable(w io.Writer, t indexTable) error {
	group := [IndexGroupByteSize]byte{}
	for _, g := range t.iter() {
		binary.LittleEndian.PutUint64(group[0:8], g[0].I)
		binary.LittleEndian.PutUint64(group[8:16], g[0].P)

		c := 16
		s_last := g[0]
		for _, s := range g[1:] {
			ds := indexSlot{s.I - s_last.I, s.P - s_last.P}
			binary.LittleEndian.PutUint32(group[c+0:c+4], uint32(ds.I))
			binary.LittleEndian.PutUint32(group[c+4:c+8], uint32(ds.P))
			c += 8
			s_last = s
		}

		if _, err := w.Write(group[:c]); err != nil {
			return err
		}
	}

	return nil
}

func decodeIndexTable(r io.Reader, t *indexTable) error {
	group := [IndexGroupByteSize]byte{}
	for {
		view := group[:]
		n, err := io.ReadFull(r, view)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if !(errors.Is(err, io.ErrUnexpectedEOF)) {
				return err
			}
		}
		if n < 16 || ((n-16)%8 > 0) {
			return io.ErrUnexpectedEOF
		}

		view = view[:n]
		size := (n-16)/8 + 1

		s := indexSlot{}
		s.I = binary.LittleEndian.Uint64(view[0:8])
		s.P = binary.LittleEndian.Uint64(view[8:16])
		t.pos = s.P
		t.tick(s.I, 1)
		t.tock()

		if size == 1 {
			return nil
		}

		view = view[16:]
		for i := range size - 1 {
			o := i * 8
			s.I += uint64(binary.LittleEndian.Uint32(view[o+0 : o+4]))
			s.P += uint64(binary.LittleEndian.Uint32(view[o+4 : o+8]))
			t.pos = s.P
			t.tick(s.I, 1)
			t.tock()
		}

		if size < IndexGroupSize {
			return nil
		}
	}
}
