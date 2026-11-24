package lotusdb

import (
	"bytes"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIteratorScanDuringFlush(t *testing.T) {
	options := DefaultOptions
	options.AutoCompactSupport = false
	options.PartitionNum = 1
	path, err := os.MkdirTemp("", "db-test-iter-flush")
	require.NoError(t, err)
	options.DirPath = path

	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	keyCount := 100
	for i := 0; i < keyCount; i++ {
		key := []byte("key-" + strconv.Itoa(i))
		require.NoError(t, db.Put(key, []byte("value-"+strconv.Itoa(i))))
	}

	iter, err := db.NewIterator(IteratorOptions{Reverse: false})
	require.NoError(t, err)

	seen := make(map[string]int)
	iter.Rewind()
	steps := 0
	for iter.Valid() {
		seen[string(iter.Key())]++
		iter.Next()
		steps++
		// flush the only memtable in the middle of the scan.
		if steps == keyCount/2 {
			db.flushMemtable(db.activeMem)
		}
	}
	require.NoError(t, iter.Close())

	assert.Equal(t, keyCount, len(seen), "each key must be returned exactly once")
	for key, times := range seen {
		assert.Equal(t, 1, times, "key %s returned more than once", key)
	}

	// reverse scan with a flush in the middle must also be duplicate-free.
	revIter, err := db.NewIterator(IteratorOptions{Reverse: true})
	require.NoError(t, err)
	revSeen := make(map[string]int)
	revIter.Rewind()
	steps = 0
	for revIter.Valid() {
		revSeen[string(revIter.Key())]++
		revIter.Next()
		steps++
		if steps == keyCount/2 {
			// no memtable left after the first flush; write more data into the
			// new active memtable and flush it while scanning.
			for j := 0; j < 20; j++ {
				extraKey := []byte("extra-" + strconv.Itoa(j))
				require.NoError(t, db.Put(extraKey, []byte("v")))
			}
			db.flushMemtable(db.activeMem)
		}
	}
	require.NoError(t, revIter.Close())
	for key, times := range revSeen {
		assert.Equal(t, 1, times, "reverse scan returned %s more than once", key)
	}

	// a fresh iterator after all flushes must see every key exactly once and
	// return them in byte order.
	freshIter, err := db.NewIterator(IteratorOptions{Reverse: false})
	require.NoError(t, err)
	freshSeen := make(map[string]int)
	var prev []byte
	freshIter.Rewind()
	for freshIter.Valid() {
		key := freshIter.Key()
		freshSeen[string(key)]++
		if prev != nil {
			assert.True(t, bytes.Compare(prev, key) < 0, "keys out of order: %s then %s", prev, key)
		}
		prev = append(prev[:0], key...)
		freshIter.Next()
	}
	require.NoError(t, freshIter.Close())
	assert.Equal(t, keyCount+20, len(freshSeen))
	for key, times := range freshSeen {
		assert.Equal(t, 1, times, "fresh iterator returned %s more than once", key)
	}

	// Seek on a snapshot iterator must stay duplicate-free when a flush lands
	// between Seek and the end of the scan.
	seekIter, err := db.NewIterator(IteratorOptions{Reverse: false})
	require.NoError(t, err)
	seekSeen := make(map[string]int)
	seekIter.Seek([]byte("key-50"))
	steps = 0
	for seekIter.Valid() {
		seekSeen[string(seekIter.Key())]++
		seekIter.Next()
		steps++
		if steps == 10 {
			db.flushMemtable(db.activeMem)
		}
	}
	require.NoError(t, seekIter.Close())
	for key, times := range seekSeen {
		assert.Equal(t, 1, times, "seek scan returned %s more than once", key)
	}
}

func TestIteratorConcurrentFlush(t *testing.T) {
	options := DefaultOptions
	options.AutoCompactSupport = false
	options.PartitionNum = 2
	path, err := os.MkdirTemp("", "db-test-iter-concurrent-flush")
	require.NoError(t, err)
	options.DirPath = path

	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	keyCount := 200
	for i := 0; i < keyCount; i++ {
		require.NoError(t, db.Put(
			[]byte("key-"+strconv.Itoa(i)),
			[]byte("value-"+strconv.Itoa(i)),
		))
	}

	iter, err := db.NewIterator(IteratorOptions{Reverse: false})
	require.NoError(t, err)

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		db.flushMemtable(db.activeMem)
	}()

	seen := make(map[string]int)
	iter.Rewind()
	for iter.Valid() {
		seen[string(iter.Key())]++
		iter.Next()
	}
	require.NoError(t, iter.Close())
	<-flushDone

	assert.Equal(t, keyCount, len(seen))
	for key, times := range seen {
		assert.Equal(t, 1, times, "key %s returned more than once", key)
	}

	// a brand new iterator must see the same key set after the flush
	freshIter, err := db.NewIterator(IteratorOptions{Reverse: false})
	require.NoError(t, err)
	freshSeen := make(map[string]int)
	freshIter.Rewind()
	for freshIter.Valid() {
		freshSeen[string(freshIter.Key())]++
		freshIter.Next()
	}
	require.NoError(t, freshIter.Close())
	assert.Equal(t, seen, freshSeen)
}

func TestIteratorRepeatedFlushWhileScanning(t *testing.T) {
	options := DefaultOptions
	options.AutoCompactSupport = false
	options.PartitionNum = 1
	path, err := os.MkdirTemp("", "db-test-iter-repeated-flush")
	require.NoError(t, err)
	options.DirPath = path

	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	rounds := 5
	keysPerRound := 50
	totalKeys := rounds * keysPerRound
	for round := 0; round < rounds; round++ {
		for j := 0; j < keysPerRound; j++ {
			key := "key-" + strconv.Itoa(round) + "-" + strconv.Itoa(j)
			require.NoError(t, db.Put([]byte(key), []byte("v")))
		}
	}

	iter, err := db.NewIterator(IteratorOptions{Reverse: false})
	require.NoError(t, err)

	seen := make(map[string]int)
	iter.Rewind()
	flushes := 0
	for iter.Valid() {
		seen[string(iter.Key())]++
		iter.Next()
		// keep flushing the active memtable while the scan is in progress.
		if len(seen)%30 == 0 && flushes < rounds {
			db.flushMemtable(db.activeMem)
			flushes++
		}
	}
	require.NoError(t, iter.Close())

	assert.Equal(t, totalKeys, len(seen), "snapshot view must expose every key once")
	for key, times := range seen {
		assert.Equal(t, 1, times, "key %s returned more than once", key)
	}
}
