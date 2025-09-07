package sir

type byCount[T any] struct {
	Writer[[]T]
	c int
	s int
}

func ByCount[T any](w Writer[[]T], cap int) Writer[[]T] {
	return &byCount[T]{w, cap, 0}
}

func (w *byCount[T]) Write(v []T) bool {
	if !w.Writer.Write(v) {
		return false
	}

	w.s += len(v)
	if w.s >= w.c {
		w.Flush()
		w.s = 0
	}
	return true
}
