package sir

import (
	"io"
)

type Compression byte

const (
	Plain     Compression = iota
	Deflate   Compression = 0x01
	Brotili   Compression = 0x02
	LZ4       Compression = 0x03
	Snappy    Compression = 0x04
	Zstandard Compression = 0x05
)

type Compressor interface {
	io.WriteCloser
	Flush() error
	Reset(w io.Writer)
}

type NopCompressor struct {
	w io.Writer
}

func (c *NopCompressor) Write(p []byte) (int, error) {
	return c.w.Write(p)
}
func (*NopCompressor) Close() error {
	return nil
}
func (*NopCompressor) Flush() error {
	return nil
}
func (c *NopCompressor) Reset(w io.Writer) {
	c.w = w
}
