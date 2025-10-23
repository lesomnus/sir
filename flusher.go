package sir

import (
	"io"
	"sync/atomic"
	"time"
)

type immediate[T any] struct {
	Writer[T]
}

func Immediate[T any](w Writer[T]) Writer[T] {
	return immediate[T]{w}
}

func (w immediate[T]) Write(v T) error {
	if err := w.Writer.Write(v); err != nil {
		return err
	}

	w.Writer.Flush()
	return nil
}

type byCount[T any] struct {
	Writer[T]
	c int
	s int
	m func(v T) int
}

func ByCount[T any](w Writer[T], cap int, meter func(v T) int) Writer[T] {
	if meter == nil {
		meter = func(v T) int { return 1 }
	}
	return &byCount[T]{w, cap, 0, meter}
}

func (w *byCount[T]) Write(v T) error {
	n := w.m(v)
	if n == 0 {
		return io.ErrNoProgress
	}
	if err := w.Writer.Write(v); err != nil {
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
