package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var ErrWALClosed = os.ErrClosed

const WalFilePath = "WAL.log"

type WALWriter struct {
	ch     chan *Log
	done   chan struct{}
	wg     sync.WaitGroup
	closed atomic.Bool
	f      *os.File
}

func NewWALWriter(buffer int, dir string) (*WALWriter, error) {
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	f, err := os.OpenFile(filepath.Join(dir, WalFilePath), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Seek to end to append new entries (can't use O_APPEND as it breaks seek-back-and-update CRC logic)
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to seek to end of WAL file: %w", err)
	}

	w := &WALWriter{
		ch:   make(chan *Log, buffer),
		done: make(chan struct{}),
		f:    f,
	}

	w.wg.Add(1)
	go w.loop()

	return w, nil
}

func (w *WALWriter) Write(l *Log) error {
	select {
	case w.ch <- l:
		return nil
	case <-w.done:
		return ErrWALClosed
	}
}

func (w *WALWriter) Close() {
	if w.closed.Swap(true) {
		return
	}

	close(w.done)
	w.wg.Wait()
	_ = w.f.Close()
}

func (w *WALWriter) loop() {
	defer w.wg.Done()

	for {
		select {
		case l := <-w.ch:
			err := l.Encode(w.f)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to write WAL: %v\n", err)
			}
			_ = w.f.Sync()
		case <-w.done:
			// Drain remaining items in channel before exiting
			for {
				select {
				case l := <-w.ch:
					err := l.Encode(w.f)
					if err != nil {
						fmt.Fprintf(os.Stderr, "failed to write WAL: %v\n", err)
					}
					_ = w.f.Sync()
				default:
					return
				}
			}
		}
	}
}
