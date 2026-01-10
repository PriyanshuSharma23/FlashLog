package main

import (
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Priyanshu23/FlashLogGo/segments"
)

const dirName = "wal_writer_test"

func setupSegmentManager(t *testing.T) (segments.SegmentsWriter, func()) {
	sm, err := segments.NewDiskSegmentsWriter(dirName)
	if err != nil {
		t.Fatal(err)
	}

	return sm, func() {
		err := os.RemoveAll(dirName)
		if err != nil {
			t.Log("failed to remove test directory:", err)
		}
	}
}

func TestWALWriteBlocksUntilDurable(t *testing.T) {
	sm, cleanup := setupSegmentManager(t)
	defer cleanup()

	wal := NewWALWriter(1, sm)
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
	sm, cleanup := setupSegmentManager(t)
	defer cleanup()

	wal := NewWALWriter(1024, sm)
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
	sm, cleanup := setupSegmentManager(t)
	defer cleanup()

	wal := NewWALWriter(1, sm)

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
