package sir

import "iter"

const IndexGroupSize = 63

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
	t.pos += s
	if t.slot == nil {
		return
	}

	t.slot.I = i
	t.slot.P = t.pos
	t.slot = nil
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
