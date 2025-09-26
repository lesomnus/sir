package sir_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/lesomnus/sir"
	"github.com/stretchr/testify/require"
)

func TestFile(t *testing.T) {
	z := func(v uint32) []byte {
		b := [4]byte{}
		binary.LittleEndian.PutUint32(b[:], v)
		return b[:]
	}

	t.Run("no records", withFile(
		func(o sir.Writer[[]byte]) {},
		func(x *require.Assertions, s sir.Stream[uint64, []byte]) {
			r := s.Reader(0)

			_, err := r.Next()
			x.ErrorIs(err, io.EOF)
		},
	))
	t.Run("flush once", withFile(
		func(o sir.Writer[[]byte]) {
			o.Write(z(1))
			o.Write(z(2))
			o.Flush()
		},
		func(x *require.Assertions, s sir.Stream[uint64, []byte]) {
			r := s.Reader(0)

			vs, err := r.Next()
			x.NoError(err)
			x.Equal([][]byte{z(1), z(2)}, vs)

			_, err = r.Next()
			x.ErrorIs(err, io.EOF)
		},
	))
	t.Run("flush twice", withFile(
		func(o sir.Writer[[]byte]) {
			o.Write(z(1))
			o.Write(z(2))
			o.Flush()
			o.Write(z(3))
			o.Write(z(4))
			o.Flush()
		},
		func(x *require.Assertions, s sir.Stream[uint64, []byte]) {
			r := s.Reader(0)

			vs, err := r.Next()
			x.NoError(err)
			x.Equal([][]byte{z(1), z(2)}, vs)

			vs, err = r.Next()
			x.NoError(err)
			x.Equal([][]byte{z(3), z(4)}, vs)

			_, err = r.Next()
			x.ErrorIs(err, io.EOF)
		},
	))
	t.Run("fit to index group", withFile(
		func(o sir.Writer[[]byte]) {
			for i := range sir.IndexGroupSize {
				o.Write(z(uint32(i*2 + 1)))
				o.Write(z(uint32(i*2 + 2)))
				o.Flush()
			}
		},
		func(x *require.Assertions, s sir.Stream[uint64, []byte]) {
			r := s.Reader(0)

			for i := range sir.IndexGroupSize {
				vs, err := r.Next()
				x.NoError(err)
				x.Equal([][]byte{
					z(uint32(i*2 + 1)),
					z(uint32(i*2 + 2)),
				}, vs)
			}

			vs, err := r.Next()
			fmt.Printf("vs: %v\n", vs)
			x.ErrorIs(err, io.EOF)
		},
	))
	t.Run("two index groups", withFile(
		func(o sir.Writer[[]byte]) {
			for i := range sir.IndexGroupSize + 10 {
				o.Write(z(uint32(i*2 + 1)))
				o.Write(z(uint32(i*2 + 2)))
				o.Flush()
			}
		},
		func(x *require.Assertions, s sir.Stream[uint64, []byte]) {
			r := s.Reader(0)

			for i := range sir.IndexGroupSize + 10 {
				vs, err := r.Next()
				x.NoError(err)
				x.Equal([][]byte{
					z(uint32(i*2 + 1)),
					z(uint32(i*2 + 2)),
				}, vs)
			}

			vs, err := r.Next()
			fmt.Printf("vs: %v\n", vs)
			x.ErrorIs(err, io.EOF)
		},
	))
}

func withFile(fw func(o sir.Writer[[]byte]), fr func(x *require.Assertions, s sir.Stream[uint64, []byte])) func(t *testing.T) {
	return func(t *testing.T) {
		x := require.New(t)

		f := &bytes.Buffer{}
		o, err := sir.NewSink(f, func(v []byte) uint64 { return uint64(binary.LittleEndian.Uint32(v)) })
		x.NoError(err)

		fw(o)

		err = o.Close()
		x.NoError(err)

		b := f.Bytes()
		s, err := sir.OpenFile(func() (io.ReadSeeker, error) {
			return bytes.NewReader(b), nil
		})
		x.NoError(err)

		fr(x, s)
	}
}
