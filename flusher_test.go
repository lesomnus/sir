package sir_test

import (
	"io"
	"testing"
	"time"

	"github.com/lesomnus/sir"
	"github.com/stretchr/testify/require"
)

func TestByCount(t *testing.T) {
	t.Run("flushed if the number of written records reached to cap in the block", func(t *testing.T) {
		x := require.New(t)

		s, w := sir.Mem(sir.AutoFirst[int])
		w = sir.ByCount(w, 3, func(vs []int) int { return len(vs) })
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
		w = sir.ByCount(w, 3, func(vs []int) int { return len(vs) })
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
		w = sir.ByCount(w, 3, func(vs []int) int { return len(vs) })
		defer w.Close()

		err := w.Write([]int{})
		require.ErrorIs(t, err, io.ErrNoProgress)
	})
}

func TestByTimeout(t *testing.T) {
	// TODO: can I use synctest?

	const GP = 100 * time.Millisecond
	const ET = 10 * time.Millisecond

	around := func(t *testing.T, expected time.Duration, actual time.Duration) {
		t.Helper()
		require.GreaterOrEqual(t, expected, actual-ET/2)
		require.LessOrEqual(t, expected, actual+ET)
	}

	t.Run("flush at the given interval", func(t *testing.T) {
		x := require.New(t)

		s, w := sir.Mem(sir.Auto[int])
		w = sir.ByTimeout(w, GP)
		defer w.Close()

		t0 := time.Now()
		r := s.Reader(0)

		w.Write(0)
		v, err := r.Next()
		dt := time.Since(t0)
		around(t, dt, GP)
		x.NoError(err)
		x.Equal([]int{0}, v)

		w.Write(1)
		v, err = r.Next()
		dt = time.Since(t0)
		around(t, dt, 2*GP)
		x.NoError(err)
		x.Equal([]int{1}, v)

	})
	t.Run("manual flush reset the timer", func(t *testing.T) {
		x := require.New(t)

		s, w := sir.Mem(sir.Auto[int])
		w = sir.ByTimeout(w, GP)
		defer w.Close()

		t0 := time.Now()
		r := s.Reader(0)

		w.Write(0)
		time.Sleep(GP / 2)
		w.Flush()
		r.Next()

		w.Write(1)
		v, err := r.Next()
		dt := time.Since(t0)
		around(t, dt, GP/2+GP)
		x.NoError(err)
		x.Equal([]int{1}, v)
	})
}
