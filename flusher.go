package sir

type byCount[T any] struct {
	Writer[[]T]
	c int
	s int
}

func ByCount[T any](w Writer[[]T], cap int) Writer[[]T] {
	return &byCount[T]{w, cap, 0}
}

func (w *byCount[T]) Write(vs []T) bool {
	n := len(vs)
	if n == 0 {
		return false
	}
	if !w.Writer.Write(vs) {
		return false
	}

	w.s += n
	if w.s >= w.c {
		w.Flush()
	}
	return true
}

func (w *byCount[T]) Flush() {
	w.Writer.Flush()
	w.s = 0
}
