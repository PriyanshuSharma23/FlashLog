// Package memtable provides an in-memory, ordered key–value store implemented using a skip list.
package memtable

import "iter"

type ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 |
		~string
}

type Record[K ordered, V any] struct {
	Key   K
	Value V
}

type Memtable[K ordered, V any] interface {
	Put(key K, value V)
	Get(key K) V
	Delete(key K)
	Iterator() iter.Seq[Record[K, V]]
}
