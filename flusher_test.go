package sir_test

import (
	"io"
	"testing"

	"github.com/lesomnus/sir"
	"github.com/stretchr/testify/require"
)

func TestByCount(t *testing.T) {
	t.Run("flushed if the number of written records reached to cap in the block", func(t *testing.T) {
		x := require.New(t)

		s, w := sir.Mem(sir.AutoFirst[int])
		w = sir.ByCount(w, 3)
		defer w.Close()

		w.Write([]int{1, 2, 3})
		w.Write([]int{4, 5, 6, 7})
		w.Write([]int{8, 9})
		w.Write([]int{10, 11})

		r := s.Reader(0)

		vs, err := r.Next()
		x.NoError(err)
		x.Equal([][]int{{1, 2, 3}}, vs)

		vs, err = r.Next()
		x.NoError(err)
		x.Equal([][]int{{4, 5, 6, 7}}, vs)

		vs, err = r.Next()
		x.NoError(err)
		x.Equal([][]int{{8, 9}, {10, 11}}, vs)
	})
	t.Run("manual flush reset the count", func(t *testing.T) {
		x := require.New(t)

		s, w := sir.Mem(sir.AutoFirst[int])
		w = sir.ByCount(w, 3)
		defer w.Close()

		w.Write([]int{1, 2})
		w.Flush()
		w.Write([]int{3, 4})
		w.Write([]int{5, 6})

		r := s.Reader(0)

		vs, err := r.Next()
		x.NoError(err)
		x.Equal([][]int{{1, 2}}, vs)

		vs, err = r.Next()
		x.NoError(err)
		x.Equal([][]int{{3, 4}, {5, 6}}, vs)
	})
	t.Run("write empty slice does nothing", func(t *testing.T) {
		_, w := sir.Mem(sir.AutoFirst[int])
		w = sir.ByCount(w, 3)
		defer w.Close()

		err := w.Write([]int{})
		require.ErrorIs(t, err, io.ErrNoProgress)
	})
}
