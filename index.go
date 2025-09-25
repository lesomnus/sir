package sir

import (
	"encoding/binary"
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
