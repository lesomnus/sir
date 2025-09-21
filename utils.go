package sir

type tap[T any] struct {
	Writer[T]
	f func(v T)
}

func Tap[T any](w Writer[T], f func(v T)) Writer[T] {
	return tap[T]{w, f}
}

func (w tap[T]) Write(v T) error {
	w.f(v)
	return w.Writer.Write(v)
}

type transform[T any, U any] struct {
	Writer[U]
	f func(v T) U
}

func Transform[T any, U any](w Writer[U], f func(v T) U) Writer[T] {
	return transform[T, U]{w, f}
}

func (w transform[T, U]) Write(v T) error {
	v_ := w.f(v)
	return w.Writer.Write(v_)
}
