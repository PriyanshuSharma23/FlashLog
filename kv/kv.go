package kv

import (
	"encoding/binary"
	"fmt"

	"github.com/Priyanshu23/FlashLogGo/memtable"
	"github.com/Priyanshu23/FlashLogGo/sst"
)

const (
	dataDir    = "data"
	maxEntries = 10 // TODO: Fix this
)

type DB interface {
	Get(k string) (string, bool)
	Put(k string, v string)
	Delete(k string)
}

type Record struct {
	Value   []byte
	Deleted bool
}

func (r *Record) ToBytes() []byte {
	valueBytes := []byte(r.Value)

	buf := make([]byte, 1+4+len(valueBytes))

	if r.Deleted {
		buf[0] = 1
	} else {
		buf[0] = 0
	}

	binary.BigEndian.PutUint32(buf[1:5], uint32(len(valueBytes)))

	copy(buf[5:], valueBytes)

	return buf
}

type KV struct {
	m   memtable.Memtable[string, Record]
	sst sst.SSTWriter
}

func NewKV() (*KV, error) {
	m := memtable.NewSkipListMemtable[string, Record]()
	s, err := sst.NewDiskSSTWriter(dataDir)
	if err != nil {
		return nil, err
	}
	return &KV{m, s}, nil
}

func (kv *KV) Get(k string) ([]byte, bool) {
	record, fnd := kv.m.Get(k)
	if !fnd {
		// kv.sst.LookUp(k) // TODO: Implement look up mechanism
		return nil, false
	}

	if record.Deleted {
		return nil, false
	}
	return record.Value, true
}

func (kv *KV) flushMemTable() error {
	fmt.Println("Memtable size exceeded max limit\nFlushing table...")
	for item := range kv.m.Iterator() {
		fmt.Println("Record:", item)
		err := kv.sst.Write([]byte(item.Key), []byte(item.Value.ToBytes()))

		if err != nil {
			return fmt.Errorf("failed to write into SST: %w", err)
		}
	}

	err := kv.sst.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush memtable: %w", err)
	}

	return nil
}

func (kv *KV) Put(k string, v []byte) error {
	fmt.Printf("Inserting %s | current size: %d", k, kv.m.Size())
	if kv.m.Size() == maxEntries {
		err := kv.flushMemTable()
		if err != nil {
			return err
		}
		kv.m = memtable.NewSkipListMemtable[string, Record]()
	}

	kv.m.Put(k, Record{
		Value:   v,
		Deleted: false,
	})

	return nil
}

func (kv *KV) Delete(k string) {
	fmt.Println("Deleting key", k)
	kv.m.Put(k, Record{
		Value:   nil,
		Deleted: true,
	})
}
