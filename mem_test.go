package sir_test

import (
	"testing"
	"time"

	"github.com/lesomnus/sir"
	"github.com/stretchr/testify/require"
)

func TestMem(t *testing.T) {
	const GP = 100 * time.Millisecond

	t.Run("write and read", func(t *testing.T) {
		s, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		w.Write(1)
		w.Write(2)
		w.Write(3)
		w.Flush()

		vs, ok := s.Reader(0).Next()
		require.True(t, ok)
		require.Equal(t, []int{1, 2, 3}, vs)
	})
	t.Run("write fails if index is not increased", func(t *testing.T) {
		s, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		w.Write(2)

		ok := w.Write(1)
		require.False(t, ok)

		w.Write(3)
		w.Flush()

		vs, ok := s.Reader(0).Next()
		require.True(t, ok)
		require.Equal(t, []int{2, 3}, vs)
	})
	t.Run("write multiple blocks", func(t *testing.T) {
		s, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		w.Write(1)
		w.Write(2)
		w.Write(3)
		w.Flush()

		w.Write(4)
		w.Write(5)
		w.Write(6)
		w.Flush()

		r := s.Reader(0)
		vs, ok := r.Next()
		require.True(t, ok)
		require.Equal(t, []int{1, 2, 3}, vs)

		vs, ok = r.Next()
		require.True(t, ok)
		require.Equal(t, []int{4, 5, 6}, vs)
	})
	t.Run("read from the middle", func(t *testing.T) {
		s, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		w.Write(1)
		w.Write(2)
		w.Write(3)
		w.Flush()

		w.Write(4)
		w.Write(5)
		w.Write(6)
		w.Flush()

		w.Write(7)
		w.Write(8)
		w.Write(9)
		w.Flush()

		r := s.Reader(4)
		vs, ok := r.Next()
		require.True(t, ok)
		require.Equal(t, []int{4, 5, 6}, vs)

		vs, ok = r.Next()
		require.True(t, ok)
		require.Equal(t, []int{7, 8, 9}, vs)
	})
	t.Run("each reader maintains their own cursor on the stream", func(t *testing.T) {
		s, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		w.Write(1)
		w.Write(2)
		w.Write(3)
		w.Flush()

		w.Write(4)
		w.Write(5)
		w.Write(6)
		w.Flush()

		w.Write(7)
		w.Write(8)
		w.Write(9)
		w.Flush()

		r1 := s.Reader(4)
		vs, ok := r1.Next()
		require.True(t, ok)
		require.Equal(t, []int{4, 5, 6}, vs)

		r2 := s.Reader(0)
		vs, ok = r2.Next()
		require.True(t, ok)
		require.Equal(t, []int{1, 2, 3}, vs)

		vs, ok = r1.Next()
		require.True(t, ok)
		require.Equal(t, []int{7, 8, 9}, vs)

		vs, ok = r2.Next()
		require.True(t, ok)
		require.Equal(t, []int{4, 5, 6}, vs)

		vs, ok = r2.Next()
		require.True(t, ok)
		require.Equal(t, []int{7, 8, 9}, vs)
	})
	t.Run("reader can read even if the stream is closed", func(t *testing.T) {
		s, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		w.Write(1)
		w.Write(2)
		w.Write(3)
		w.Flush()

		w.Write(4)
		w.Write(5)
		w.Write(6)
		w.Flush()

		w.Close()

		r := s.Reader(0)
		vs, ok := r.Next()
		require.True(t, ok)
		require.Equal(t, []int{1, 2, 3}, vs)

		vs, ok = r.Next()
		require.True(t, ok)
		require.Equal(t, []int{4, 5, 6}, vs)
	})
	t.Run("flush does nothing if there is no data in the block", func(t *testing.T) {
		s, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		r := s.Reader(0)
		t0 := time.Now()

		go func() {
			<-time.After(GP)
			w.Close()
		}()

		w.Flush()
		_, ok := r.Next()
		require.False(t, ok)

		dt := time.Since(t0)
		require.GreaterOrEqual(t, dt, GP)
	})
	t.Run("close flushes", func(t *testing.T) {
		s, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		r := s.Reader(0)

		w.Write(1)
		w.Write(2)
		w.Write(3)
		w.Close()

		vs, ok := r.Next()
		require.True(t, ok)
		require.Equal(t, []int{1, 2, 3}, vs)
	})
	t.Run("write on closed stream returns false", func(t *testing.T) {
		_, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		ok := w.Write(1)
		require.True(t, ok)

		w.Close()
		ok = w.Write(1)
		require.False(t, ok)
	})
	t.Run("read on closed stream returns false", func(t *testing.T) {
		s, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		r := s.Reader(0)

		w.Write(1)
		w.Write(2)
		w.Write(3)
		w.Flush()
		w.Close()

		_, ok := r.Next()
		require.True(t, ok)

		_, ok = r.Next()
		require.False(t, ok)
	})
	t.Run("close of the writer unblocks the blocked reader", func(t *testing.T) {
		s, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		r := s.Reader(0)
		t0 := time.Now()

		go func() {
			<-time.After(GP)
			w.Close()
		}()

		_, ok := r.Next()
		require.False(t, ok)

		dt := time.Since(t0)
		require.GreaterOrEqual(t, dt, GP)
	})
	t.Run("reader blocked until flush", func(t *testing.T) {
		s, w := sir.Mem(sir.Auto[int])
		defer w.Close()

		r := s.Reader(0)
		t0 := time.Now()

		w.Write(42)
		go func() {
			<-time.After(GP)
			w.Flush()
		}()

		r.Next()

		dt := time.Since(t0)
		require.GreaterOrEqual(t, dt, GP)
	})
}
