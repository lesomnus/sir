package sir_test

import (
	"strconv"
	"testing"

	"github.com/lesomnus/sir"
	"github.com/stretchr/testify/require"
)

func TestTap(t *testing.T) {
	t.Run("given function is invoked whenever the value is written", func(t *testing.T) {
		x := require.New(t)

		_, w := sir.Mem(sir.Auto[int])

		vs := []int{}
		w = sir.Tap(w, func(v int) {
			vs = append(vs, v)
		})
		defer w.Close()

		w.Write(1)
		x.Equal([]int{1}, vs)
		w.Write(2)
		x.Equal([]int{1, 2}, vs)
		w.Write(3)
		x.Equal([]int{1, 2, 3}, vs)
	})
}

func TestTransform(t *testing.T) {
	t.Run("transforms input data", func(t *testing.T) {
		s, w_ := sir.Mem(sir.Auto[int])
		w := sir.Transform(w_, func(v string) int {
			v_, _ := strconv.Atoi(v)
			return v_
		})
		defer w.Close()

		w.Write("1")
		w.Write("2")
		w.Write("3")
		w.Flush()

		vs, err := s.Reader(0).Next()
		require.NoError(t, err)
		require.Equal(t, []int{1, 2, 3}, vs)
	})
}
