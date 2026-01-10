package main

import (
	"os"
	"sync"

	"github.com/Priyanshu23/FlashLogGo/segmentmanager"
)

var ErrWALClosed = os.ErrClosed

type WALWriter struct {
	mu     sync.Mutex
	ch     chan *walRequest
	done   chan struct{}
	closed bool
	sm     segmentmanager.SegmentManager
	wg     sync.WaitGroup
}

type walRequest struct {
	log  *Log
	done chan error
}

func NewWALWriter(sm segmentmanager.SegmentManager, chanBuffer int) *WALWriter {
	w := &WALWriter{
		ch:   make(chan *walRequest, chanBuffer),
		done: make(chan struct{}),
		sm:   sm,
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
	w.sm.Close() // TODO: handle error
}

func (w *WALWriter) loop() {
	defer close(w.done)

	for req := range w.ch {
		out, err := w.sm.Active(req.log.Size())
		if err != nil {
			req.done <- err
			continue
		}

		err = req.log.Encode(out)
		if err == nil {
			w.sm.Sync()
		}
		req.done <- err
	}
}
