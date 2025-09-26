package sir

import (
	"io"
	"sync"

	"golang.org/x/exp/constraints"
)

type memBlock[K constraints.Ordered, T any] struct {
	index K
	data  []T
	next  *memBlock[K, T]
}

type mem[K constraints.Ordered, T any] struct {
	x Indexer[K, T]
	k K

	m sync.Mutex
	c *sync.Cond

	head *memBlock[K, T]
	tail *memBlock[K, T]

	closed bool
}

func Mem[K constraints.Ordered, T any](indexer Indexer[K, T]) (Stream[K, T], Writer[T]) {
	b := &memBlock[K, T]{}
	s := &mem[K, T]{
		x:    indexer,
		head: b,
		tail: b,
	}
	s.c = sync.NewCond(&s.m)

	return s, s
}

func (s *mem[K, T]) Write(v T) error {
	s.m.Lock()
	defer s.m.Unlock()
	if s.closed {
		return io.ErrClosedPipe
	}

	k := s.x(v)
	if k < s.k {
		return io.ErrNoProgress
	}
	s.k = k

	var z K
	if s.tail.index == z {
		s.tail.index = k
	}
	s.tail.data = append(s.tail.data, v)
	return nil
}

func (s *mem[K, T]) Close() error {
	s.m.Lock()
	defer s.m.Unlock()
	s.closed = true
	s.flush()
	s.c.Broadcast()
	return nil
}

func (s *mem[K, T]) flush() bool {
	if len(s.tail.data) == 0 {
		// Nothing to flush.
		return false
	}

	b := &memBlock[K, T]{}
	s.tail.next = b
	s.tail = b
	return true
}

func (s *mem[K, T]) Flush() error {
	s.m.Lock()
	defer s.m.Unlock()
	if s.closed {
		return io.ErrClosedPipe
	}

	if s.flush() {
		s.c.Broadcast()
	}
	return nil
}

type memReader[K constraints.Ordered, T any] struct {
	s *mem[K, T]
	b *memBlock[K, T]
}

func (s *mem[K, T]) Reader(index K) Reader[T] {
	s.m.Lock()
	defer s.m.Unlock()

	curr := s.head
	next := s.head.next
	for next != nil {
		if len(next.data) == 0 {
			break
		}
		if index < next.index {
			break
		}

		curr = next
		next = next.next
	}

	return &memReader[K, T]{s, curr}
}

func (r *memReader[K, T]) Next() ([]T, error) {
	r.s.m.Lock()
	defer r.s.m.Unlock()
	if r.b.next == nil {
		if r.s.closed {
			return nil, io.EOF
		}
		r.s.c.Wait()
	}
	if len(r.b.data) == 0 {
		if !r.s.closed {
			panic("no data")
		}

		return nil, io.EOF
	}

	vs := r.b.data
	r.b = r.b.next
	return vs, nil
}

func (r *memReader[K, T]) Close() error {
	return nil
}
