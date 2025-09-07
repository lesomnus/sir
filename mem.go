package sir

import (
	"io"
	"sync"

	"golang.org/x/exp/constraints"
)

type block[T any] struct {
	data []T
	next *block[T]
}

type mem[K constraints.Ordered, T any] struct {
	x Indexer[K, T]
	l *K

	m sync.Mutex
	c *sync.Cond

	head *block[T]
	tail *block[T]

	closed bool
}

func Mem[K constraints.Ordered, T any](indexer Indexer[K, T]) (Stream[K, T], Writer[T]) {
	b := &block[T]{}
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

	l := s.x(v)
	if s.l != nil && l < *s.l {
		return io.ErrNoProgress
	}
	s.l = &l

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

	b := &block[T]{}
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
	b *block[T]
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

		i := s.x(next.data[0])
		if index < i {
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
