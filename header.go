package sir

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	HeaderByteSize = 0x20
)

type Header struct {
	Compression      Compression
	ContentLength    int64
	IndexTableOffset int64
	FirstBlockOffset int64
	Metadata         []byte
}

func ReadHeader(r io.Reader) (Header, error) {
	data := [HeaderByteSize]byte{}
	if _, err := io.ReadFull(r, data[:]); err != nil {
		return Header{}, err
	}

	v := Header{}
	if err := v.UnmarshalBinary(data[:]); err != nil {
		return Header{}, err
	}

	return v, nil
}

func (h Header) MarshalBinary() ([]byte, error) {
	size := HeaderByteSize + len(h.Metadata)
	return h.AppendBinary(make([]byte, 0, size))
}

func (h Header) AppendBinary(b []byte) ([]byte, error) {
	if h.ContentLength != 0 && int(h.ContentLength) < HeaderByteSize+len(h.Metadata) {
		return nil, errors.New("invalid content length")
	}
	if h.FirstBlockOffset == 0 {
		h.FirstBlockOffset = HeaderByteSize + int64(len(h.Metadata))
	} else if h.FirstBlockOffset < HeaderByteSize || h.FirstBlockOffset-HeaderByteSize != int64(len(h.Metadata)) {
		return nil, errors.New("invalid first block offset")
	}
	if h.IndexTableOffset != 0 && h.IndexTableOffset < h.FirstBlockOffset {
		return nil, errors.New("invalid index table offset")
	}

	b = binary.BigEndian.AppendUint32(b, Magic)
	b = append(b, 0x01)
	b = append(b, byte(h.Compression))
	b = binary.LittleEndian.AppendUint16(b, 0)
	b = binary.LittleEndian.AppendUint64(b, 0)
	b = binary.LittleEndian.AppendUint64(b, 0)
	b = binary.LittleEndian.AppendUint64(b, uint64(h.FirstBlockOffset))
	b = append(b, h.Metadata...)
	return b, nil
}

func (h *Header) UnmarshalBinary(b []byte) error {
	if len(b) < HeaderByteSize {
		return errors.New("header too short")
	}
	if binary.BigEndian.Uint32(b[:4]) != Magic {
		return errors.New("magic not found")
	}
	if v := b[4]; v != 0x01 {
		return fmt.Errorf("unsupported version: %d", v)
	}

	h.ContentLength = int64(binary.LittleEndian.Uint64(b[0x08:0x10]))
	h.IndexTableOffset = int64(binary.LittleEndian.Uint64(b[0x10:0x18]))
	h.FirstBlockOffset = int64(binary.LittleEndian.Uint64(b[0x18:0x20]))
	if len(b) < int(h.FirstBlockOffset) {
		return io.ErrUnexpectedEOF
	}
	h.Metadata = b[0x20:h.FirstBlockOffset]

	return nil
}
