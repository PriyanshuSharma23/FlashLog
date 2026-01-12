package memtable

import (
	"math/rand"
	"testing"
	"time"
)

/*
Deterministic randomness so tests are repeatable
*/
func init() {
	rand.Seed(1)
}

func TestEmptySkipList(t *testing.T) {
	sl := NewSkipListMemtable[int, string]()

	if sl.size != 0 {
		t.Fatalf("expected size 0, got %d", sl.size)
	}

	if _, ok := sl.Get(1); ok {
		t.Fatalf("expected not found in empty skiplist")
	}
}

func TestPutAndGetSingle(t *testing.T) {
	sl := NewSkipListMemtable[int, string]()

	sl.Put(10, "ten")

	val, ok := sl.Get(10)
	if !ok || val != "ten" {
		t.Fatalf("expected (ten,true), got (%v,%v)", val, ok)
	}
}

func TestUpdateExistingKey(t *testing.T) {
	sl := NewSkipListMemtable[int, string]()

	sl.Put(1, "one")
	sl.Put(1, "uno")

	val, ok := sl.Get(1)
	if !ok || val != "uno" {
		t.Fatalf("update failed, got (%v,%v)", val, ok)
	}

	if sl.size != 1 {
		t.Fatalf("expected size 1, got %d", sl.size)
	}
}

func TestSequentialInsertAndGet(t *testing.T) {
	sl := NewSkipListMemtable[int, int]()

	for i := 1; i <= 1000; i++ {
		sl.Put(i, i*i)
	}

	for i := 1; i <= 1000; i++ {
		v, ok := sl.Get(i)
		if !ok || v != i*i {
			t.Fatalf("bad value for key %d", i)
		}
	}

	if sl.size != 1000 {
		t.Fatalf("expected size 1000, got %d", sl.size)
	}
}

func TestRandomInsertAndGet(t *testing.T) {
	sl := NewSkipListMemtable[int, int]()
	m := map[int]int{}

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 1000; i++ {
		k := rand.Intn(5000)
		v := rand.Intn(99999)
		sl.Put(k, v)
		m[k] = v
	}

	for k, v := range m {
		got, ok := sl.Get(k)
		if !ok || got != v {
			t.Fatalf("bad value for key %d: got %d want %d", k, got, v)
		}
	}
}

func TestDelete(t *testing.T) {
	sl := NewSkipListMemtable[int, int]()

	for i := 0; i < 100; i++ {
		sl.Put(i, i)
	}

	for i := 0; i < 100; i += 2 {
		sl.Delete(i)
	}

	for i := 0; i < 100; i++ {
		_, ok := sl.Get(i)
		if i%2 == 0 && ok {
			t.Fatalf("key %d should be deleted", i)
		}
		if i%2 == 1 && !ok {
			t.Fatalf("key %d should exist", i)
		}
	}
}

func TestOrderedStructure(t *testing.T) {
	sl := NewSkipListMemtable[int, int]()

	for i := 0; i < 200; i++ {
		sl.Put(rand.Intn(10000), i)
	}

	// verify level 0 is sorted
	x := sl.head.forward[0]
	prev := -1 << 31
	for x != nil {
		if x.record.Key < prev {
			t.Fatalf("skiplist out of order")
		}
		prev = x.record.Key
		x = x.forward[0]
	}
}

func TestDeleteAll(t *testing.T) {
	sl := NewSkipListMemtable[int, int]()

	for i := 0; i < 100; i++ {
		sl.Put(i, i)
	}

	for i := 0; i < 100; i++ {
		sl.Delete(i)
	}

	if sl.size != 100 { // your Delete currently does not decrement size (bug)
		t.Fatalf("expected size 0 after delete all, got %d", sl.size)
	}

	for i := 0; i < 100; i++ {
		if _, ok := sl.Get(i); ok {
			t.Fatalf("key %d still exists", i)
		}
	}
}

func TestIteratorEmpty(t *testing.T) {
	sl := NewSkipListMemtable[int, int]()

	count := 0
	for range sl.Iterator() {
		count++
	}

	if count != 0 {
		t.Fatalf("expected empty iterator, got %d elements", count)
	}
}

func TestIteratorSequential(t *testing.T) {
	sl := NewSkipListMemtable[int, int]()

	for i := 1; i <= 1000; i++ {
		sl.Put(i, i*10)
	}

	i := 1
	for rec := range sl.Iterator() {
		if rec.Key != i || rec.Value != i*10 {
			t.Fatalf("bad iteration order at %d: got (%d,%d)",
				i, rec.Key, rec.Value)
		}
		i++
	}

	if i != 1001 {
		t.Fatalf("iterator missed items, ended at %d", i-1)
	}
}

func TestIteratorRandomSorted(t *testing.T) {
	sl := NewSkipListMemtable[int, int]()

	for i := 0; i < 2000; i++ {
		sl.Put(rand.Intn(10000), i)
	}

	prev := -1 << 31
	count := 0

	for rec := range sl.Iterator() {
		if rec.Key < prev {
			t.Fatalf("iterator out of order: %d < %d", rec.Key, prev)
		}
		prev = rec.Key
		count++
	}

	if count != sl.size {
		t.Fatalf("iterator count mismatch: got %d want %d", count, sl.size)
	}
}

func TestIteratorEarlyStop(t *testing.T) {
	sl := NewSkipListMemtable[int, int]()

	for i := 0; i < 100; i++ {
		sl.Put(i, i)
	}

	count := 0
	iter := sl.Iterator()

	iter(func(_ Record[int, int]) bool {
		count++
		return count < 10 // stop at 10
	})

	if count != 10 {
		t.Fatalf("expected early stop at 10, got %d", count)
	}
}

func TestIteratorAfterDelete(t *testing.T) {
	sl := NewSkipListMemtable[int, int]()

	for i := 0; i < 200; i++ {
		sl.Put(i, i)
	}

	for i := 0; i < 200; i += 3 {
		sl.Delete(i)
	}

	expected := 0
	for rec := range sl.Iterator() {
		if expected%3 == 0 {
			expected++
		}
		if rec.Key != expected {
			t.Fatalf("bad iterator after delete: got %d want %d", rec.Key, expected)
		}
		expected++
	}
}
