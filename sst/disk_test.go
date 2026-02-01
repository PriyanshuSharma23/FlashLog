package sst

import (
	"os"
	"path"
	"testing"
)

func TestDataBlockReadWrite(t *testing.T) {
	tempDir := t.TempDir()

	pathName := path.Join(tempDir, filename)

	_, err := os.Create(pathName)
	if err != nil {
		t.Fatal(err)
	}

	writer, err := NewDiskSSTWriter(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Write([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write([]byte("key2"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write([]byte("key3"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write([]byte("key4"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write([]byte("key5"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write([]byte("key6"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Flush()
	if err != nil {
		t.Fatal(err)
	}
}
