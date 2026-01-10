package main

import (
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/Priyanshu23/FlashLogGo/segments"
)

var ErrWALClosed = os.ErrClosed

type WALWriter struct {
	ch     chan *Log
	wg     sync.WaitGroup
	closed atomic.Bool
	sm     segments.SegmentsWriter
}

func NewWALWriter(buffer int, sm segments.SegmentsWriter) *WALWriter {
	w := &WALWriter{
		ch: make(chan *Log, buffer),
		sm: sm,
	}
	go w.loop()
	return w
}

func (w *WALWriter) Write(l *Log) error {
	if w.closed.Load() {
		return ErrWALClosed
	}

	w.wg.Add(1)
	defer w.wg.Done()

	select {
	case w.ch <- l:
		return nil
	default:
		w.ch <- l
		return nil
	}
}

func (w *WALWriter) Close() {
	if w.closed.Swap(true) {
		return
	}

	go func() {
		w.wg.Wait()
		close(w.ch)
		_ = w.sm.Close()
	}()
}

func (w *WALWriter) loop() {
	for l := range w.ch {
		_ = w.sm.Write(l.Size(), func(out io.Writer) {
			_ = l.Encode(out)
		})
	}
}
