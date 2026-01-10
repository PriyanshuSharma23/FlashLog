package main

import (
	"os"
	"sync"
)

var ErrWALClosed = os.ErrClosed

type WALWriter struct {
	mu     sync.Mutex
	ch     chan *walRequest
	done   chan struct{}
	closed bool
	f      *os.File // TODO: Replace this with segment manager
	wg     sync.WaitGroup
}

type walRequest struct {
	log  *Log
	done chan error
}

func NewWALWriter(buffer int) *WALWriter {
	f, _ := os.Create("wal.log")

	w := &WALWriter{
		ch:   make(chan *walRequest, buffer),
		done: make(chan struct{}),
		f:    f,
	}

	go w.loop()
	return w
}

func (w *WALWriter) Write(l *Log) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrWALClosed
	}
	w.wg.Add(1)
	w.mu.Unlock()

	defer w.wg.Done()

	req := &walRequest{log: l, done: make(chan error, 1)}

	select {
	case w.ch <- req:
		return <-req.done
	case <-w.done:
		return ErrWALClosed
	}
}

func (w *WALWriter) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()

	w.wg.Wait()
	close(w.ch)
	<-w.done
	w.f.Close()
}

func (w *WALWriter) loop() {
	defer close(w.done)

	for req := range w.ch {
		err := req.log.Encode(w.f)
		if err == nil {
			w.f.Sync()
		}
		req.done <- err
	}
}
