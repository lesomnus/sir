package sir

import (
	"io"
	"sync/atomic"
	"time"
)

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

type byTimeout[T any] struct {
	Writer[T]
	d atomic.Int64
}

func ByTimeout[T any](w Writer[T], d time.Duration) Writer[T] {
	p := d.Milliseconds()
	if p == 0 {
		panic("too short")
	}

	w_ := &byTimeout[T]{Writer: w}
	w_.d.Store(time.Now().UnixMilli())
	go func() {
		for {
			prev := w_.d.Load()
			curr := time.Now().UnixMilli()

			dt := curr - prev
			r := p - dt
			if r > 0 {
				time.Sleep(time.Duration(r) * time.Millisecond)
				continue
			}

			if err := w_.Flush(); err != nil {
				return
			}
		}
	}()

	return w_
}

func (w *byTimeout[T]) Flush() error {
	err := w.Writer.Flush()
	if err != nil {
		return err
	}

	w.d.Store(time.Now().UnixMilli())
	return nil
}
