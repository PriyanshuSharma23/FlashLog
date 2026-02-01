package sst

import (
	"testing"

	"github.com/Priyanshu23/FlashLogGo/types"
)

func TestDataBlockReadWrite(t *testing.T) {
	dir := "check_sst"
	writer, err := NewDiskSSTWriter(dir)
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Write(types.OperationPut, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write(types.OperationPut, []byte("key2"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write(types.OperationPut, []byte("key3"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write(types.OperationPut, []byte("key4"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write(types.OperationPut, []byte("key5"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Write(types.OperationPut, []byte("key6"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Flush()
	if err != nil {
		t.Fatal(err)
	}
}
