package sir

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
)

const (
	IndexGroupSize     = 63
	IndexGroupByteSize = (8 + 8) + ((IndexGroupSize - 1) * 8)
	FooterByteSize     = 8 + 4
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

func (t *indexTable) iter() iter.Seq2[int, []indexSlot] {
	return func(yield func(int, []indexSlot) bool) {
		for i, g := range t.groups {
			j := len(g) - 1
			if g[j] == (indexSlot{}) {
				if j == 0 {
					return
				}
				yield(i, g[:j])
				return
			}
			if !yield(i, g) {
				return
			}
		}
	}
}

func (t *indexTable) find(i uint64) (uint64, bool) {
	if len(t.groups) == 0 || len(t.groups[0]) == 0 {
		return 0, false
	}

	s_ := t.groups[0][0]
	for _, g := range t.iter() {
		for _, s := range g {
			if i < s.I {
				return s_.P, true
			}
			s_ = s
		}
	}

	return 0, false
}

// Len returns number of records in the table.
func (t *indexTable) Len() int {
	if len(t.groups) == 0 {
		return 0
	}

	n := 0
	for _, g := range t.groups {
		n += len(g)
	}

	return max(0, n-1)
}

func encodeIndexTable(w io.Writer, t indexTable) error {
	for _, g := range t.iter() {
		group := [IndexGroupByteSize]byte{}
		binary.LittleEndian.PutUint64(group[0:8], g[0].I)
		binary.LittleEndian.PutUint64(group[8:16], g[0].P)

		c := 16
		s_last := g[0]
		for _, s := range g[1:] {
			if s == (indexSlot{}) {
				break
			}

			ds := indexSlot{s.I - s_last.I, s.P - s_last.P}
			binary.LittleEndian.PutUint32(group[c+0:c+4], uint32(ds.I))
			binary.LittleEndian.PutUint32(group[c+4:c+8], uint32(ds.P))
			c += 8
			s_last = s
		}

		if _, err := w.Write(group[:]); err != nil {
			return err
		}
	}

	return nil
}

func decodeIndexTable(r io.Reader, t *indexTable) error {
	for {
		group := [IndexGroupByteSize]byte{}
		if _, err := io.ReadFull(r, group[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if !(errors.Is(err, io.ErrUnexpectedEOF)) {
				return err
			}
		}

		size, err := feedIndexTable(group[:], t)
		if err != nil {
			return err
		}
		if size < IndexGroupSize {
			return nil
		}
	}
}

func feedIndexTable(b []byte, t *indexTable) (int, error) {
	n := len(b)
	if n < 16 || ((n-16)%8 > 0) {
		return 0, io.ErrUnexpectedEOF
	}

	size := (n-16)/8 + 1

	s := indexSlot{}
	s.I = binary.LittleEndian.Uint64(b[0:8])
	s.P = binary.LittleEndian.Uint64(b[8:16])
	t.pos = s.P
	t.tick(s.I, 1)
	t.tock()

	if size == 1 {
		return 1, nil
	}

	b = b[16:]
	for i := range size - 1 {
		o := i * 8
		di := uint64(binary.LittleEndian.Uint32(b[o+0 : o+4]))
		dp := uint64(binary.LittleEndian.Uint32(b[o+4 : o+8]))
		if di == 0 && dp == 0 {
			return i + 1, nil
		}

		s.I += di
		s.P += dp

		t.pos = s.P
		t.tick(s.I, 1)
		t.tock()
	}

	return size, nil
}

func scanIndexTable(r io.ReadSeeker) (t indexTable, index_table_offset int64, err_ error) {
	const EpilogueByteSize = len(Marker) + IndexGroupByteSize

	epilogue_offset, err := r.Seek(-int64(EpilogueByteSize+FooterByteSize), io.SeekEnd)
	if err != nil {
		err_ = fmt.Errorf("seek epilogue: %w", err)
		return
	}

	buff := make([]byte, EpilogueByteSize+FooterByteSize)
	if _, err := io.ReadFull(r, buff); err != nil {
		err_ = fmt.Errorf("read footer: %w", err)
		return
	}

	footer := buff[EpilogueByteSize:]
	if binary.BigEndian.Uint32(footer[8:12]) != Magic {
		err_ = errors.New("magic not found at the end of the file")
		return
	}

	group_last_offset := epilogue_offset + int64(len(Marker))
	group_last := buff[len(Marker):EpilogueByteSize]
	index_table_offset = int64(binary.LittleEndian.Uint32(footer[:8]))
	if group_last_offset == int64(index_table_offset) {
		// There is single index group.
		if !bytes.Equal(Marker[:], buff[:len(Marker)]) {
			err_ = errors.New("sync marker for the last block not found")
			return
		}

		t = newIndexTable(0)
		feedIndexTable(group_last, &t)
		return
	}
	if (index_table_offset-group_last_offset)%IndexGroupByteSize > 0 {
		err_ = errors.New("invalid size of index table")
		return
	}
	if _, err := r.Seek(index_table_offset-int64(len(Marker)), io.SeekStart); err != nil {
		err_ = fmt.Errorf("seek index table: %w", err)
		return
	}

	size := (index_table_offset - group_last_offset) / IndexGroupByteSize
	buff = make([]byte, len(Marker)+IndexGroupByteSize)
	if _, err := io.ReadFull(r, buff); err != nil {
		err_ = fmt.Errorf("read first index group: %w", err)
		return
	}
	if !bytes.Equal(Marker[:], buff[:len(Marker)]) {
		err_ = errors.New("sync marker for the last block not found")
		return
	}

	buff = buff[len(Marker):]

	t = newIndexTable(0)
	feedIndexTable(buff, &t)

	for range size - 1 {
		if _, err := io.ReadFull(r, buff); err != nil {
			err_ = fmt.Errorf("read index group: %w", err)
			return
		}

		feedIndexTable(buff, &t)
	}
	feedIndexTable(group_last, &t)
	return
}
