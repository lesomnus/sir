package sir

import "golang.org/x/exp/constraints"

type Stream[K constraints.Ordered, T any] interface {
	Reader(index K) Reader[T]
}

type Writer[T any] interface {
	Write(v T) error
	Flush() error
	Close() error
}

type Reader[T any] interface {
	Next() ([]T, error)
}
