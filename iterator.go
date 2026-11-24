package lotusdb

import (
	"bytes"
	"container/heap"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
)

// baseIterator.
type baseIterator interface {
	// Rewind seek the first key in the iterator.
	Rewind()
	// Seek move the iterator to the key which is
	// greater(less when reverse is true) than or equal to the specified key.
	Seek(key []byte)
	// Next moves the iterator to the next key.
	Next()
	// Key get the current key.
	Key() []byte
	// Value get the current value.
	Value() any
	// Valid returns whether the iterator is exhausted.
	Valid() bool
	// Close the iterator.
	Close() error
}

// Iterator holds a heap and a set of iterators that implement the baseIterator interface.
type Iterator struct {
	h    iterHeap
	pos  map[*singleIter]int // current position of each member in h
	itrs []*singleIter       // all snapshot iterators, used for rebuilding the heap
	db   *DB
}

// Rewind seek the first key in the iterator.
func (mi *Iterator) Rewind() {
	for _, v := range mi.itrs {
		v.iter.Rewind()
	}
	mi.rebuildHeap(mi.itrs)
}

// Seek move the iterator to the key which is
// greater(less when reverse is true) than or equal to the specified key.
func (mi *Iterator) Seek(key []byte) {
	seekItrs := make([]*singleIter, 0)
	for _, v := range mi.itrs {
		v.iter.Seek(key)
		if v.iter.Valid() {
			seekItrs = append(seekItrs, v)
		}
	}
	mi.rebuildHeap(seekItrs)
}

// rebuildHeap rebuilds the merged heap from the given valid single iterators
// and resynchronizes the position map with the new heap.
func (mi *Iterator) rebuildHeap(validItrs []*singleIter) {
	h := iterHeap(validItrs)
	heap.Init(&h)
	mi.h = h
	mi.pos = make(map[*singleIter]int, h.Len())
	for i, itr := range h {
		mi.pos[itr] = i
	}
}

// remove drops an exhausted iterator from the heap and deletes its position.
func (mi *Iterator) remove(itr *singleIter) {
	idx, ok := mi.pos[itr]
	if !ok {
		return
	}
	heap.Remove(&mi.h, idx)
	delete(mi.pos, itr)
	mi.syncPositions()
}

// syncPositions rebuilds the position map from the current heap layout. It is
// simpler and safer than chasing every swap inside container/heap.
func (mi *Iterator) syncPositions() {
	for i, member := range mi.h {
		mi.pos[member] = i
	}
}

// advance moves one single iterator forward and keeps its heap entry
// consistent, removing it from the heap once it is exhausted.
func (mi *Iterator) advance(itr *singleIter) {
	itr.iter.Next()
	if itr.iter.Valid() {
		idx, ok := mi.pos[itr]
		if !ok {
			return
		}
		heap.Fix(&mi.h, idx)
		mi.syncPositions()
		return
	}
	mi.remove(itr)
}

// cleanKey removes every occurrence of oldKey from the other iterators, so a
// key is exposed by the merged view exactly once. The iterator that currently
// owns the top of the heap (identified by rank) always has the freshest value
// for that key because memtable iterators have higher ranks than index ones.
func (mi *Iterator) cleanKey(oldKey []byte, rank int) {
	for _, itr := range mi.itrs {
		if itr.rank == rank || !itr.iter.Valid() ||
			!bytes.Equal(itr.iter.Key(), oldKey) {
			continue
		}
		// A higher rank can never share the heap top's key, since it would have
		// been ordered first by iterHeap.Less. Skip every duplicate entry; the
		// heap position map is updated inside advance/remove.
		for itr.iter.Valid() && bytes.Equal(itr.iter.Key(), oldKey) {
			mi.advance(itr)
		}
	}
}

// Next moves the iterator to the next key.
func (mi *Iterator) Next() {
	if mi.h.Len() == 0 {
		return
	}

	// Get the top item from the heap
	topIter := mi.h[0]
	mi.cleanKey(topIter.iter.Key(), topIter.rank)

	// Move to the next key and update the heap
	mi.advance(topIter)
}

// Key get the current key.
func (mi *Iterator) Key() []byte {
	return mi.h[0].iter.Key()
}

// Value get the current value.
func (mi *Iterator) Value() []byte {
	topIter := mi.h[0]
	switch topIter.iType {
	case BptreeItr:
		keyPos := new(KeyPosition)
		keyPos.key = topIter.iter.Key()
		keyPos.partition = uint32(mi.db.vlog.getKeyPartition(topIter.iter.Key()))
		keyPos.position = wal.DecodeChunkPosition(topIter.iter.Value().([]byte))
		record, err := mi.db.vlog.read(keyPos)
		if err != nil {
			panic(err)
		}
		return record.value
	case MemItr:
		return topIter.iter.Value().(y.ValueStruct).Value
	default:
		panic("iType not support")
	}
}

// Valid returns whether the iterator is exhausted.
func (mi *Iterator) Valid() bool {
	if mi.h.Len() == 0 {
		return false
	}
	topIter := mi.h[0]
	if topIter.iType == MemItr && topIter.iter.Value().(y.ValueStruct).Meta == LogRecordDeleted {
		mi.cleanKey(topIter.iter.Key(), topIter.rank)
		mi.advance(topIter)
		return mi.Valid()
	}
	return true
}

// Close the iterator.
func (mi *Iterator) Close() error {
	for _, itr := range mi.itrs {
		if err := itr.iter.Close(); err != nil {
			return err
		}
	}
	return nil
}

// NewIterator returns a new iterator.
// The iterator will iterate all the keys in DB.
// It's the caller's responsibility to call Close when iterator is no longer
// used, otherwise resources will be leaked.
// The iterator is not goroutine-safe, you should not use the same iterator
// concurrently from multiple goroutines.
func (db *DB) NewIterator(options IteratorOptions) (*Iterator, error) {
	if db.options.IndexType == Hash {
		return nil, ErrDBIteratorUnsupportedTypeHASH
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	itrs := make([]*singleIter, 0, db.options.PartitionNum+len(db.immuMems)+1)
	closeItrs := func() {
		for _, itr := range itrs {
			_ = itr.iter.Close()
		}
	}
	rank := 0
	index, ok := db.index.(*BPTree)
	if !ok {
		panic("index type not support")
	}

	for i := 0; i < db.options.PartitionNum; i++ {
		tx, err := index.trees[i].Begin(false)
		if err != nil {
			closeItrs()
			return nil, err
		}
		itr := newBptreeIterator(
			tx,
			options,
		)
		itr.Rewind()
		// is empty
		if !itr.Valid() {
			_ = itr.Close()
			continue
		}
		itrs = append(itrs, &singleIter{
			iType:   BptreeItr,
			options: options,
			rank:    rank,
			iter:    itr,
		})
		rank++
	}
	// Snapshot the memtable set under db.mu. The skiplist behind each
	// memtableIterator is ref-counted and stays alive for the whole iterator
	// lifetime, even if the memtable is flushed and removed from db.immuMems.
	memtableList := make([]*memtable, 0, len(db.immuMems)+1)
	memtableList = append(memtableList, db.immuMems...)
	memtableList = append(memtableList, db.activeMem)
	for i := 0; i < len(memtableList); i++ {
		itr := newMemtableIterator(options, memtableList[i])
		itr.Rewind()
		// is empty
		if !itr.Valid() {
			_ = itr.Close()
			continue
		}
		itrs = append(itrs, &singleIter{
			iType:   MemItr,
			options: options,
			rank:    rank,
			iter:    itr,
		})
		rank++
	}

	mergedIter := &Iterator{
		itrs: itrs,
		db:   db,
	}
	mergedIter.rebuildHeap(itrs)
	return mergedIter, nil
}
