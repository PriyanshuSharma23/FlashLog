package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"
)

func withTempWAL(t *testing.T, fn func(f *os.File)) {
	f, err := os.CreateTemp("", "wal-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()
	fn(f)
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		log  *Log
	}{
		{"small", &Log{op: OperationPut, key: []byte("a"), value: []byte("b")}},
		{"empty", &Log{op: OperationDelete, key: []byte{}, value: []byte{}}},
		{"binary", &Log{op: OperationPut, key: []byte{0, 1, 2, 3}, value: []byte{9, 8, 7}}},
		{"large", &Log{op: OperationPut, key: bytes.Repeat([]byte("k"), 1024), value: bytes.Repeat([]byte("v"), 2048)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withTempWAL(t, func(f *os.File) {
				if err := tt.log.Encode(f); err != nil {
					t.Fatal(err)
				}
				f.Seek(0, io.SeekStart)

				got, err := Decode(f)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}

				if got.op != tt.log.op ||
					!bytes.Equal(got.key, tt.log.key) ||
					!bytes.Equal(got.value, tt.log.value) {
					t.Fatalf("mismatch")
				}
			})
		})
	}
}

func TestDecodeDetectsCorruption(t *testing.T) {
	withTempWAL(t, func(f *os.File) {
		l := &Log{op: OperationPut, key: []byte("key"), value: []byte("value")}
		if err := l.Encode(f); err != nil {
			t.Fatal(err)
		}

		// Flip one bit
		f.Seek(-1, io.SeekEnd)
		b := []byte{0}
		f.Read(b)
		b[0] ^= 0xFF
		f.Seek(-1, io.SeekEnd)
		f.Write(b)

		f.Seek(0, io.SeekStart)
		if _, err := Decode(f); err != ErrCorruptWAL {
			t.Fatalf("expected ErrCorruptWAL, got %v", err)
		}
	})
}

func TestDecodeDetectsTruncation(t *testing.T) {
	l := &Log{op: OperationPut, key: []byte("key"), value: []byte("value")}

	lTotalLength := uint32(4 + 4 + 1 + 4 + len(l.key) + 4 + len(l.value))

	for i := 1; i < int(lTotalLength); i++ {
		withTempWAL(t, func(f *os.File) {
			if err := l.Encode(f); err != nil {
				t.Fatal(err)
			}

			f.Truncate(int64(i))
			f.Seek(0, io.SeekStart)

			if _, err := Decode(f); err != io.EOF {
				t.Fatalf("expected EOF at %d, got %v", i, err)
			}
		})
	}
}

func TestDecodeMultipleRecords(t *testing.T) {
	withTempWAL(t, func(f *os.File) {
		records := []*Log{
			{op: OperationPut, key: []byte("a"), value: []byte("1")},
			{op: OperationPut, key: []byte("b"), value: []byte("2")},
			{op: OperationDelete, key: []byte("a"), value: nil},
		}

		for _, r := range records {
			if err := r.Encode(f); err != nil {
				t.Fatal(err)
			}
		}

		f.Seek(0, io.SeekStart)
		for i, want := range records {
			got, err := Decode(f)
			if err != nil {
				t.Fatalf("record %d: %v", i, err)
			}
			if got.op != want.op ||
				!bytes.Equal(got.key, want.key) ||
				!bytes.Equal(got.value, want.value) {
				t.Fatalf("record %d mismatch", i)
			}
		}

		if _, err := Decode(f); err != io.EOF {
			t.Fatalf("expected EOF, got %v", err)
		}
	})
}

func TestRejectsInsaneLength(t *testing.T) {
	withTempWAL(t, func(f *os.File) {
		binary.Write(f, binary.LittleEndian, uint32(0x11111111))
		binary.Write(f, binary.LittleEndian, uint32(0xFFFFFFFF))
		f.Seek(0, io.SeekStart)

		if _, err := Decode(f); err != ErrCorruptWAL {
			t.Fatalf("expected ErrCorruptWAL, got %v", err)
		}
	})
}
