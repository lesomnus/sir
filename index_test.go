package sir

import (
	"bytes"
	"encoding/binary"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexTable(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		_, _, ok := next()
		x.False(ok)
	})
	t.Run("tock with no tick", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tock()

		next, stop := iter.Pull2(v.iter())
		defer stop()

		_, _, ok := next()
		x.False(ok)
	})
	t.Run("tock twice with no tick", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tock()
		v.tock()

		next, stop := iter.Pull2(v.iter())
		defer stop()

		_, _, ok := next()
		x.False(ok)
	})
	t.Run("tick twice", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tick(1000, 1)
		v.tick(2000, 1)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal([]indexSlot{
			{1000, 10},
		}, g)

		_, _, ok = next()
		x.False(ok)
	})
	t.Run("end with tock", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tick(1000, 1)
		v.tick(2000, 1)
		v.tock()

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal([]indexSlot{
			{1000, 10},
		}, g)

		_, _, ok = next()
		x.False(ok)
	})
	t.Run("tick after tock", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tick(1000, 1)
		v.tick(2000, 1)
		v.tock()
		v.tick(3000, 1)
		v.tick(4000, 1)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal([]indexSlot{
			{1000, 10},
			{3000, 12},
		}, g)

		_, _, ok = next()
		x.False(ok)
	})
	t.Run("tock twice", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tick(1000, 1)
		v.tick(2000, 1)
		v.tock()
		v.tick(3000, 1)
		v.tick(4000, 1)
		v.tock()

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal([]indexSlot{
			{1000, 10},
			{3000, 12},
		}, g)

		_, _, ok = next()
		x.False(ok)
	})
	t.Run("fit to group", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		for i := range IndexGroupSize {
			v.tick(uint64(1000+1000*(i*2)), 1)
			v.tick(uint64(1000+1000*(i*2+1)), 1)
			v.tock()
		}

		answer := make([]indexSlot, IndexGroupSize)
		for i := range IndexGroupSize {
			answer[i] = indexSlot{
				I: uint64(1000 + 1000*(i*2)),
				P: uint64(10 + (i * 2)),
			}
		}

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal(answer, g)

		_, _, ok = next()
		x.False(ok)
	})
	t.Run("two groups", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		for i := range IndexGroupSize + 10 {
			v.tick(uint64(1000+1000*(i*2)), 1)
			v.tick(uint64(1000+1000*(i*2+1)), 1)
			v.tock()
		}

		answer := make([]indexSlot, IndexGroupSize+10)
		for i := range IndexGroupSize + 10 {
			answer[i] = indexSlot{
				I: uint64(1000 + 1000*(i*2)),
				P: uint64(10 + (i * 2)),
			}
		}

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal(answer[:IndexGroupSize], g)

		i, g, ok = next()
		x.True(ok)
		x.Equal(1, i)
		x.Equal(answer[IndexGroupSize:], g)
	})
}

func TestEncodeIndexTable(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, v)
		x.NoError(err)

		data := b.Bytes()
		x.Equal([]byte(nil), data)
	})
	t.Run("tock with no tick", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tock()

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, v)
		x.NoError(err)

		data := b.Bytes()
		x.Equal([]byte(nil), data)
	})
	t.Run("tock twice with no tick", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tock()
		v.tock()

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, v)
		x.NoError(err)

		data := b.Bytes()
		x.Equal([]byte(nil), data)
	})
	t.Run("tick twice", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tick(1000, 1)
		v.tick(2000, 1)

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, v)
		x.NoError(err)

		data := b.Bytes()
		x.Equal(uint64(1000), binary.LittleEndian.Uint64(data[0:8]))
		x.Equal(uint64(10), binary.LittleEndian.Uint64(data[8:16]))
	})
	t.Run("end with tock", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tick(1000, 1)
		v.tick(2000, 1)
		v.tock()

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, v)
		x.NoError(err)

		data := b.Bytes()
		x.Equal(uint64(1000), binary.LittleEndian.Uint64(data[0:8]))
		x.Equal(uint64(10), binary.LittleEndian.Uint64(data[8:16]))
	})
	t.Run("tick after tock", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tick(1000, 1)
		v.tick(2000, 1)
		v.tock()
		v.tick(3000, 1)
		v.tick(4000, 1)

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, v)
		x.NoError(err)

		data := b.Bytes()
		x.Equal(uint64(1000), binary.LittleEndian.Uint64(data[0:8]))
		x.Equal(uint64(10), binary.LittleEndian.Uint64(data[8:16]))
		x.Equal(uint32(2000), binary.LittleEndian.Uint32(data[16:20]))
		x.Equal(uint32(2), binary.LittleEndian.Uint32(data[20:24]))
	})
	t.Run("tock twice", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tick(1000, 1)
		v.tick(2000, 1)
		v.tock()
		v.tick(3000, 1)
		v.tick(4000, 1)
		v.tock()

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, v)
		x.NoError(err)

		data := b.Bytes()
		x.Equal(uint64(1000), binary.LittleEndian.Uint64(data[0:8]))
		x.Equal(uint64(10), binary.LittleEndian.Uint64(data[8:16]))
		x.Equal(uint32(2000), binary.LittleEndian.Uint32(data[16:20]))
		x.Equal(uint32(2), binary.LittleEndian.Uint32(data[20:24]))
	})
	t.Run("fit to group", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		for i := range IndexGroupSize {
			v.tick(uint64(1000+1000*(i*2)), 1)
			v.tick(uint64(1000+1000*(i*2+1)), 1)
			v.tock()
		}

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, v)
		x.NoError(err)

		data := b.Bytes()
		x.Equal(IndexGroupByteSize, len(data))
		x.Equal(uint64(1000), binary.LittleEndian.Uint64(data[0:8]))
		x.Equal(uint64(10), binary.LittleEndian.Uint64(data[8:16]))
		for i := range IndexGroupSize - 1 {
			a := 16 + (i * 8)
			b := a + 4
			c := a + 8

			x.Equal(uint32(2000), binary.LittleEndian.Uint32(data[a:b]))
			x.Equal(uint32(2), binary.LittleEndian.Uint32(data[b:c]))
		}
	})
	t.Run("two groups", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		for i := range IndexGroupSize + 10 {
			v.tick(uint64(1000+1000*(i*2)), 1)
			v.tick(uint64(1000+1000*(i*2+1)), 1)
			v.tock()
		}

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, v)
		x.NoError(err)

		data := b.Bytes()
		x.Equal(IndexGroupByteSize*2, len(data))
		x.Equal(uint64(1000), binary.LittleEndian.Uint64(data[0:8]))
		x.Equal(uint64(10), binary.LittleEndian.Uint64(data[8:16]))
		for i := range IndexGroupSize - 1 {
			a := 16 + (i * 8)
			b := a + 4
			c := a + 8

			x.Equal(uint32(2000), binary.LittleEndian.Uint32(data[a:b]))
			x.Equal(uint32(2), binary.LittleEndian.Uint32(data[b:c]))
		}

		data = data[IndexGroupByteSize:]
		x.Equal(uint64(127000), binary.LittleEndian.Uint64(data[0:8]))
		x.Equal(uint64(136), binary.LittleEndian.Uint64(data[8:16]))
		for i := range 9 {
			a := 16 + (i * 8)
			b := a + 4
			c := a + 8

			x.Equal(uint32(2000), binary.LittleEndian.Uint32(data[a:b]))
			x.Equal(uint32(2), binary.LittleEndian.Uint32(data[b:c]))
		}
	})
}

func TestDecodeIndexTable(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		x := require.New(t)

		u := newIndexTable(10)

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, u)
		x.NoError(err)

		v := newIndexTable(0)
		err = decodeIndexTable(b, &v)
		x.NoError(err)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		_, _, ok := next()
		x.False(ok)
	})
	t.Run("tock with no tick", func(t *testing.T) {
		x := require.New(t)

		u := newIndexTable(10)
		u.tock()

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, u)
		x.NoError(err)

		v := newIndexTable(0)
		err = decodeIndexTable(b, &v)
		x.NoError(err)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		_, _, ok := next()
		x.False(ok)
	})
	t.Run("tock twice with no tick", func(t *testing.T) {
		x := require.New(t)

		u := newIndexTable(10)
		u.tock()
		u.tock()

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, u)
		x.NoError(err)

		v := newIndexTable(0)
		err = decodeIndexTable(b, &v)
		x.NoError(err)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		_, _, ok := next()
		x.False(ok)
	})
	t.Run("tick twice", func(t *testing.T) {
		x := require.New(t)

		u := newIndexTable(10)
		u.tick(1000, 1)
		u.tick(2000, 1)

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, u)
		x.NoError(err)

		v := newIndexTable(0)
		err = decodeIndexTable(b, &v)
		x.NoError(err)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal([]indexSlot{
			{1000, 10},
		}, g)

		_, _, ok = next()
		x.False(ok)
	})
	t.Run("end with tock", func(t *testing.T) {
		x := require.New(t)

		u := newIndexTable(10)
		u.tick(1000, 1)
		u.tick(2000, 1)
		u.tock()

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, u)
		x.NoError(err)

		v := newIndexTable(0)
		err = decodeIndexTable(b, &v)
		x.NoError(err)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal([]indexSlot{
			{1000, 10},
		}, g)

		_, _, ok = next()
		x.False(ok)
	})
	t.Run("tick after tock", func(t *testing.T) {
		x := require.New(t)

		u := newIndexTable(10)
		u.tick(1000, 1)
		u.tick(2000, 1)
		u.tock()
		u.tick(3000, 1)
		u.tick(4000, 1)

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, u)
		x.NoError(err)

		v := newIndexTable(0)
		err = decodeIndexTable(b, &v)
		x.NoError(err)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal([]indexSlot{
			{1000, 10},
			{3000, 12},
		}, g)

		_, _, ok = next()
		x.False(ok)
	})
	t.Run("tock twice", func(t *testing.T) {
		x := require.New(t)

		u := newIndexTable(10)
		u.tick(1000, 1)
		u.tick(2000, 1)
		u.tock()
		u.tick(3000, 1)
		u.tick(4000, 1)
		u.tock()

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, u)
		x.NoError(err)

		v := newIndexTable(0)
		err = decodeIndexTable(b, &v)
		x.NoError(err)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal([]indexSlot{
			{1000, 10},
			{3000, 12},
		}, g)

		_, _, ok = next()
		x.False(ok)
	})
	t.Run("fit to group", func(t *testing.T) {
		x := require.New(t)

		u := newIndexTable(10)
		for i := range IndexGroupSize {
			u.tick(uint64(1000+1000*(i*2)), 1)
			u.tick(uint64(1000+1000*(i*2+1)), 1)
			u.tock()
		}

		answer := make([]indexSlot, IndexGroupSize)
		for i := range IndexGroupSize {
			answer[i] = indexSlot{
				I: uint64(1000 + 1000*(i*2)),
				P: uint64(10 + (i * 2)),
			}
		}

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, u)
		x.NoError(err)

		v := newIndexTable(0)
		err = decodeIndexTable(b, &v)
		x.NoError(err)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal(answer, g)

		_, _, ok = next()
		x.False(ok)
	})
	t.Run("two groups", func(t *testing.T) {
		x := require.New(t)

		u := newIndexTable(10)
		for i := range IndexGroupSize + 10 {
			u.tick(uint64(1000+1000*(i*2)), 1)
			u.tick(uint64(1000+1000*(i*2+1)), 1)
			u.tock()
		}

		answer := make([]indexSlot, IndexGroupSize+10)
		for i := range IndexGroupSize + 10 {
			answer[i] = indexSlot{
				I: uint64(1000 + 1000*(i*2)),
				P: uint64(10 + (i * 2)),
			}
		}

		b := &bytes.Buffer{}
		err := encodeIndexTable(b, u)
		x.NoError(err)

		v := newIndexTable(0)
		err = decodeIndexTable(b, &v)
		x.NoError(err)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(0, i)
		x.Equal(answer[:IndexGroupSize], g)

		i, g, ok = next()
		x.True(ok)
		x.Equal(1, i)
		x.Equal(answer[IndexGroupSize:], g)
	})
}
