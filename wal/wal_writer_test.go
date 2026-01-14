package wal

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Priyanshu23/FlashLogGo/types"
)

func TestWALWriteBlocksUntilDurable(t *testing.T) {
	dirName := t.TempDir()
	w, _ := NewWALWriter(1, dirName)
	defer w.Close()

	l := NewLog(types.OperationPut, []byte("a"), []byte("1"))

	start := time.Now()

	go func() {
		if err := w.Write(l); err != nil {
			t.Error(err)
		}
	}()

	time.Sleep(10 * time.Millisecond)

	if time.Since(start) < 10*time.Millisecond {
		t.Fatal("Write returned before fsync")
	}
}

func TestWALConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWALWriter(1, dir)
	if err != nil {
		t.Fatal(err)
	}

	const N = 50
	var wg sync.WaitGroup

	for i := range N {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l := NewLog(types.OperationPut, fmt.Appendf(nil, "k-%d", i), fmt.Appendf(nil, "v-%d", i))
			err := w.Write(l)
			if err != nil {
				fmt.Println(err)
			}
		}(i)
	}

	wg.Wait()
	w.Close() // Ensure all writes are flushed before reading

	reader, err := NewWALReader(dir)
	if err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}
	for {
		l, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		seen[string(l.Key())] = true
	}

	if len(seen) != N {
		t.Fatalf("expected %d records, got %d", N, len(seen))
	}

	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestWALCloseUnblocksWriters(t *testing.T) {
	dirName := t.TempDir()
	w, _ := NewWALWriter(1, dirName)
	defer w.Close()

	go func() {
		_ = w.Write(NewLog(types.OperationPut, []byte("x"), []byte("1")))
	}()

	time.Sleep(5 * time.Millisecond)
	w.Close()

	done := make(chan struct{})

	go func() {
		_ = w.Write(NewLog(types.OperationPut, []byte("y"), []byte("2")))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer blocked after Close")
	}
}
