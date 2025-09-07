package sir

import "io"

type byCount[T any] struct {
	Writer[[]T]
	c int
	s int
}

func ByCount[T any](w Writer[[]T], cap int) Writer[[]T] {
	return &byCount[T]{w, cap, 0}
}

func (w *byCount[T]) Write(vs []T) error {
	n := len(vs)
	if n == 0 {
		return io.ErrNoProgress
	}
	if err := w.Writer.Write(vs); err != nil {
		return err
	}

	w.s += n
	if w.s >= w.c {
		w.Flush()
	}
	return nil
}

func (w *byCount[T]) Flush() error {
	if err := w.Writer.Flush(); err != nil {
		return err
	}

	w.s = 0
	return nil
}
