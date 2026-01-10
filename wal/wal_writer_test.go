package main

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

func TestWALWriteBlocksUntilDurable(t *testing.T) {
	dirName := t.TempDir()
	wal, _ := NewWALWriter(1, dirName)
	defer wal.Close()

	l := &Log{
		op:    OperationPut,
		key:   []byte("a"),
		value: []byte("1"),
	}

	start := time.Now()

	go func() {
		if err := wal.Write(l); err != nil {
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
	wal, err := NewWALWriter(1, dir)
	if err != nil {
		t.Fatal(err)
	}

	const N = 50
	var wg sync.WaitGroup

	for i := range N {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l := &Log{
				op:    OperationPut,
				key:   []byte(fmt.Sprintf("k-%d", i)),
				value: []byte(fmt.Sprintf("v-%d", i)),
			}
			err := wal.Write(l)
			if err != nil {
				fmt.Println(err)
			}
		}(i)
	}

	wg.Wait()
	wal.Close() // Ensure all writes are flushed before reading

	reader, err := NewWALReader(dir)
	defer reader.Close()

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

		seen[string(l.key)] = true
	}

	if len(seen) != N {
		t.Fatalf("expected %d records, got %d", N, len(seen))
	}
}

func TestWALCloseUnblocksWriters(t *testing.T) {
	dirName := t.TempDir()
	wal, _ := NewWALWriter(1, dirName)
	defer wal.Close()

	go func() {
		_ = wal.Write(&Log{op: OperationPut, key: []byte("x"), value: []byte("1")})
	}()

	time.Sleep(5 * time.Millisecond)
	wal.Close()

	done := make(chan struct{})

	go func() {
		_ = wal.Write(&Log{op: OperationPut, key: []byte("y"), value: []byte("2")})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer blocked after Close")
	}
}
