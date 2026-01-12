package memtable

import (
	"fmt"
	"iter"
	"math/rand"
)

const maxLevel = 32

type skipListNode[K ordered, V any] struct {
	record  Record[K, V]
	forward []*skipListNode[K, V]
}

func NewSkipListNode[K ordered, V any](key K, value V, levels int) *skipListNode[K, V] {
	forward := make([]*skipListNode[K, V], levels+1)
	return &skipListNode[K, V]{
		record:  Record[K, V]{key, value},
		forward: forward,
	}
}

type SkipList[K ordered, V any] struct {
	head   *skipListNode[K, V]
	levels int
	size   int
}

func NewSkipListMemtable[K ordered, V any]() *SkipList[K, V] {
	sl := SkipList[K, V]{
		head:   NewSkipListNode(*new(K), *new(V), 0),
		levels: -1,
		size:   0,
	}

	return &sl
}

func (sl *SkipList[K, V]) Get(key K) (V, bool) {
	curr := sl.head

	for level := sl.levels; level >= 0; level-- {
		for {
			if curr.forward[level] == nil || curr.forward[level].record.Key > key {
				break
			} else if curr.forward[level] != nil && curr.forward[level].record.Key == key {
				return curr.forward[level].record.Value, true
			} else {
				curr = curr.forward[level]
			}
		}
	}

	return *new(V), false
}

func getRandomLevel() int {
	level := 0

	for rand.Int31()&1 == 0 && level < maxLevel {
		level++
	}

	return level
}

func (sl *SkipList[K, V]) adjustLevels(level int) {
	temp := sl.head.forward

	sl.head = NewSkipListNode(*new(K), *new(V), level)
	sl.levels = level

	copy(sl.head.forward, temp)
}

func (sl *SkipList[K, V]) Put(key K, value V) {
	newLevel := getRandomLevel()

	if newLevel > sl.levels {
		sl.adjustLevels(newLevel)
	}

	newNode := NewSkipListNode(key, value, newLevel)
	updates := make([]*skipListNode[K, V], sl.levels+1)

	x := sl.head

	for level := sl.levels; level >= 0; level-- {
		for x.forward[level] != nil && x.forward[level].record.Key < key {
			x = x.forward[level]
		}

		updates[level] = x
	}

	if x.forward[0] != nil && x.forward[0].record.Key == key {
		x.forward[0].record.Value = value
		return
	}

	for level := 0; level <= newLevel; level++ {
		newNode.forward[level] = updates[level].forward[level]
		updates[level].forward[level] = newNode
	}

	sl.size++
}

func (sl *SkipList[K, V]) Delete(key K) {
	x := sl.head

	for level := sl.levels; level >= 0; level-- {
		for {
			if x.forward[level] == nil || x.forward[level].record.Key > key {
				break
			} else if x.forward[level].record.Key == key {
				x.forward[level] = x.forward[level].forward[level]
			} else {
				x = x.forward[level]
			}
		}
	}

	for sl.levels > 0 && sl.head.forward[sl.levels] == nil {
		sl.levels--
		sl.head.forward = sl.head.forward[:sl.levels+1]
	}
}

func (sl *SkipList[K, V]) Iterator() iter.Seq[Record[K, V]] {
	return func(yield func(Record[K, V]) bool) {
		curr := sl.head
		for curr.forward[0] != nil {
			if !yield(curr.forward[0].record) {
				break
			}
			curr = curr.forward[0]
		}
	}
}

func (sl *SkipList[K, V]) print() {
	if sl.size == 0 {
		return
	}

	fmt.Println("===================================")
	fmt.Printf("SkipList size=%d levels=%d\n", sl.size, sl.levels)
	fmt.Println("===================================")

	// Print each level top-down
	for level := sl.levels; level >= 0; level-- {
		fmt.Printf("Level %2d: ", level)

		x := sl.head.forward[level]
		for x != nil {
			fmt.Printf("%v ", x.record.Key)
			x = x.forward[level]
		}
		fmt.Println()
	}
	fmt.Println("===================================")
}
