package main

import (
	"io"
	"iter"
	"os"
	"path/filepath"
)

type WALReader struct {
	f *os.File
}

func NewWALReader(dir string) (*WALReader, error) {
	f, err := os.OpenFile(filepath.Join(dir, WalFilePath), os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}

	return &WALReader{f: f}, nil
}

func (w *WALReader) Read() (*Log, error) {
	return Decode(w.f)
}

func (w *WALReader) Iter() iter.Seq2[Log, error] {
	return func(yield func(Log, error) bool) {
		for {
			log, err := Decode(w.f)
			if err == io.EOF {
				return
			}
			if err != nil {
				yield(Log{}, err)
				return
			}
			if !yield(*log, nil) {
				return
			}
		}
	}
}

func (w *WALReader) Reset() error {
	_, err := w.f.Seek(0, io.SeekStart)
	return err
}

func (w *WALReader) Close() error {
	return w.f.Close()
}
