package sir

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type fileCtx struct {
	h Header
	t indexTable

	open func() (io.ReadSeeker, error)
}

func OpenFile(open func() (io.ReadSeeker, error)) (Stream[uint64, []byte], error) {
	f, err := open()
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	if f, ok := f.(io.Closer); ok {
		defer f.Close()
	}

	h, err := ReadHeader(f)
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	var t indexTable
	if h.IndexTableOffset == 0 {
		if t, h.IndexTableOffset, err = scanIndexTable(f); err != nil {
			return nil, fmt.Errorf("scan index table: %w", err)
		}
	} else if _, err := f.Seek(int64(h.IndexTableOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek index table: %w", err)
	} else {
		var r io.Reader = f
		if h.ContentLength > 0 {
			r = io.LimitReader(f, h.ContentLength-h.IndexTableOffset-FooterByteSize)
		}
		if err := decodeIndexTable(r, &t); err != nil {
			return nil, fmt.Errorf("decode index table: %w", err)
		}
	}

	return &fileCtx{
		h: h,
		t: t,

		open: open,
	}, nil
}

type file struct {
	r io.Reader
}

func (f *fileCtx) Reader(index uint64) Reader[[]byte] {
	r, err := f.open()
	if err != nil {
		return errReader[[]byte]{err}
	}

	p, ok := f.t.find(index)
	if !ok {
		p = uint64(f.h.FirstBlockOffset)
	}
	if _, err := r.Seek(int64(p), io.SeekStart); err != nil {
		return errReader[[]byte]{err}
	}

	size := f.h.IndexTableOffset - f.h.FirstBlockOffset
	return &file{io.LimitReader(r, size)}
}

func (f *file) Next() ([][]byte, error) {
	head := [8]byte{}
	if _, err := io.ReadFull(f.r, head[:]); err != nil {
		return nil, err
	}

	size_b := int(binary.LittleEndian.Uint32(head[0:4]))
	// size_c := binary.LittleEndian.Uint32(head[4:8])
	// TODO: decompress

	buff := make([]byte, size_b+len(Marker))
	if _, err := io.ReadFull(f.r, buff); err != nil {
		return nil, err
	}
	if !bytes.Equal(Marker[:], buff[size_b:]) {
		return nil, errors.New("sync marker not found")
	}
	if size_b == 0 {
		// Sealing block.
		return nil, io.EOF
	}

	vs := [][]byte{}
	pos := 0
	for pos < size_b {
		size := binary.LittleEndian.Uint32(buff[pos:])
		next := pos + 4 + int(size)
		vs = append(vs, buff[pos+4:next])
		pos = next
	}

	return vs, nil
}

func (f *file) Close() error {
	if r, ok := f.r.(io.Closer); ok {
		return r.Close()
	}
	return nil
}
