package sir

import "golang.org/x/exp/constraints"

type Indexer[K constraints.Ordered, T any] func(v T) K

type Stream[K constraints.Ordered, T any] interface {
	Reader(index K) Reader[T]
}

type Writer[T any] interface {
	Write(v T) bool
	Flush()
	Close()
}

type Reader[T any] interface {
	Next() ([]T, bool)
}

func Auto[T constraints.Ordered](v T) T {
	return v
}
