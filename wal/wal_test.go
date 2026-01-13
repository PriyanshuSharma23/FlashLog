package wal

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
	defer func() {
		_ = os.Remove(f.Name())
	}()
	defer func() {
		_ = f.Close()
	}()
	fn(f)
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		log  *Log
	}{
		{"small", NewLog(OperationPut, []byte("a"), []byte("b"))},
		{"empty", NewLog(OperationDelete, []byte{}, []byte{})},
		{"binary", NewLog(OperationPut, []byte{0, 1, 2, 3}, []byte{9, 8, 7})},
		{"large", NewLog(OperationPut, bytes.Repeat([]byte("k"), 1024), bytes.Repeat([]byte("v"), 2048))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withTempWAL(t, func(f *os.File) {
				if err := tt.log.Encode(f); err != nil {
					t.Fatal(err)
				}
				_, _ = f.Seek(0, io.SeekStart)

				got, err := Decode(f)
				if err != nil {
					t.Fatalf("decode error: %v", err)
				}

				if got.Op() != tt.log.Op() ||
					!bytes.Equal(got.Key(), tt.log.Key()) ||
					!bytes.Equal(got.Value(), tt.log.Value()) {
					t.Fatalf("mismatch")
				}
			})
		})
	}
}

func TestDecodeDetectsCorruption(t *testing.T) {
	withTempWAL(t, func(f *os.File) {
		l := NewLog(OperationPut, []byte("key"), []byte("value"))
		if err := l.Encode(f); err != nil {
			t.Fatal(err)
		}

		// Flip one bit
		_, _ = f.Seek(-1, io.SeekEnd)
		b := []byte{0}
		_, _ = f.Read(b)
		b[0] ^= 0xFF
		_, _ = f.Seek(-1, io.SeekEnd)
		_, _ = f.Write(b)

		_, _ = f.Seek(0, io.SeekStart)
		if _, err := Decode(f); err != ErrCorruptWAL {
			t.Fatalf("expected ErrCorruptWAL, got %v", err)
		}
	})
}

func TestDecodeDetectsTruncation(t *testing.T) {
	l := NewLog(OperationPut, []byte("key"), []byte("value"))

	lTotalLength := uint32(4 + 4 + 1 + 4 + len(l.Key()) + 4 + len(l.Value()))

	for i := 1; i < int(lTotalLength); i++ {
		withTempWAL(t, func(f *os.File) {
			if err := l.Encode(f); err != nil {
				t.Fatal(err)
			}

			_ = f.Truncate(int64(i))
			_, _ = f.Seek(0, io.SeekStart)

			if _, err := Decode(f); err != io.EOF {
				t.Fatalf("expected EOF at %d, got %v", i, err)
			}
		})
	}
}

func TestDecodeMultipleRecords(t *testing.T) {
	withTempWAL(t, func(f *os.File) {
		records := []*Log{
			NewLog(OperationPut, []byte("a"), []byte("1")),
			NewLog(OperationPut, []byte("b"), []byte("2")),
			NewLog(OperationDelete, []byte("a"), nil),
		}

		for _, r := range records {
			if err := r.Encode(f); err != nil {
				t.Fatal(err)
			}
		}

		_, _ = f.Seek(0, io.SeekStart)
		for i, want := range records {
			got, err := Decode(f)
			if err != nil {
				t.Fatalf("record %d: %v", i, err)
			}
			if got.Op() != want.Op() ||
				!bytes.Equal(got.Key(), want.Key()) ||
				!bytes.Equal(got.Value(), want.Value()) {
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
		_ = binary.Write(f, binary.LittleEndian, uint32(0x11111111))
		_ = binary.Write(f, binary.LittleEndian, uint32(0xFFFFFFFF))
		_, _ = f.Seek(0, io.SeekStart)

		if _, err := Decode(f); err != ErrCorruptWAL {
			t.Fatalf("expected ErrCorruptWAL, got %v", err)
		}
	})
}
