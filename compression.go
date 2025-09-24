package sir

import "io"

type Compression int

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
}

func (NopCompressor) Write(p []byte) (int, error) {
	return len(p), nil
}
func (NopCompressor) Close() error {
	return nil
}
func (NopCompressor) Flush() error {
	return nil
}
func (NopCompressor) Reset(w io.Writer) {}
