package sir

import "golang.org/x/exp/constraints"

type Indexer[K constraints.Ordered, T any] func(v T) K

func Auto[T constraints.Ordered](v T) T {
	return v
}

func AutoFirst[T constraints.Ordered](vs []T) T {
	return vs[0]
}
