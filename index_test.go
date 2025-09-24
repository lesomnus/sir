package sir

import (
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
	t.Run("tick twice", func(t *testing.T) {
		x := require.New(t)

		v := newIndexTable(10)
		v.tick(1000, 1)
		v.tick(2000, 1)

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(uint64(0), i)
		x.Equal([]indexSlot{
			{1000, 11},
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
		x.Equal(uint64(0), i)
		x.Equal([]indexSlot{
			{1000, 11},
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
		x.Equal(uint64(0), i)
		x.Equal([]indexSlot{
			{1000, 11},
			{3000, 13},
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
		x.Equal(uint64(0), i)
		x.Equal([]indexSlot{
			{1000, 11},
			{3000, 13},
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
				P: uint64(10 + (1 + (i * 2))),
			}
		}

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(uint64(0), i)
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
				P: uint64(10 + (1 + (i * 2))),
			}
		}

		next, stop := iter.Pull2(v.iter())
		defer stop()

		i, g, ok := next()
		x.True(ok)
		x.Equal(uint64(0), i)
		x.Equal(answer[:IndexGroupSize], g)

		i, g, ok = next()
		x.True(ok)
		x.Equal(uint64(1), i)
		x.Equal(answer[IndexGroupSize:], g)
	})
}
