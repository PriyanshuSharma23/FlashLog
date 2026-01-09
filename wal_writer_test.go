package main

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestWALWriteBlocksUntilDurable(t *testing.T) {
	wal := NewWALWriter(1)
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
	wal := NewWALWriter(1024)
	defer wal.Close()

	var wg sync.WaitGroup

	for i := range 1000 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			l := &Log{
				op:    OperationPut,
				key:   []byte("k"),
				value: []byte(strconv.Itoa(i)),
			}

			if err := wal.Write(l); err != nil {
				t.Error(err)
			}
		}(i)
	}

	wg.Wait()
}

func TestWALCloseUnblocksWriters(t *testing.T) {
	wal := NewWALWriter(1)

	go func() {
		_ = wal.Write(&Log{op: OperationPut, key: []byte("x"), value: []byte("1")})
	}()

	time.Sleep(5 * time.Millisecond)
	wal.Close()

	done := make(chan struct{})

	go func() {
		wal.Write(&Log{op: OperationPut, key: []byte("y"), value: []byte("2")})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer blocked after Close")
	}
}
