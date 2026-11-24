package lotusdb

import (
	"bytes"
	"io/fs"
	"os"
	"sync"
	"testing"

	"github.com/bwmarrin/snowflake"
	arenaskl "github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openMemtableWithoutWal builds a memtable backed only by the in-memory skip
// list. It lets concurrency tests exercise the skiplist directly, without WAL
// file I/O noise.
func openMemtableWithoutWal(t *testing.T, memSize uint32) *memtable {
	t.Helper()
	return &memtable{
		options: memtableOptions{memSize: memSize},
		//nolint:gomnd // same headroom factor as openMemtable
		skl: arenaskl.NewSkiplist(int64(float64(memSize) * 1.5)),
	}
}

// putRecord writes a single record (normal or tombstone) to the skip list.
// This mirrors what memtable.putBatch does after the WAL write.
func (mt *memtable) putRecord(record *LogRecord) {
	mt.skl.Put(y.KeyWithTs(record.Key, 0),
		y.ValueStruct{Value: record.Value, Meta: record.Type})
}

// allSklKeys walks the base level of the skip list from head to tail.
// A broken chain would make the iterator stop early and silently drop every
// key after the broken link, so the full key set is the invariant we assert.
func (mt *memtable) allSklKeys(t *testing.T) [][]byte {
	t.Helper()
	iter := mt.skl.NewIterator()
	defer func() { _ = iter.Close() }()

	var keys [][]byte
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, y.ParseKey(iter.Key()))
	}
	return keys
}

// TestMemtableConcurrentPutDeleteSameKey is the regression test for a broken
// lock-free chain: one goroutine inserts keys while another deletes (writes a
// tombstone for) the very same keys at the same time.
//
// Insert and delete must be able to run concurrently without a global mutex,
// and the skip list chain must never be severed. When the storm finishes every
// key still has to be reachable from the head, both through point lookups and
// through a full base-level scan.
func TestMemtableConcurrentPutDeleteSameKey(t *testing.T) {
	const keyCount = 2000
	const rounds = 20

	table := openMemtableWithoutWal(t, DefaultOptions.MemtableSize)

	// Surround the contended key range with fixed sentinel keys. If a link in
	// the middle is ever corrupted, the tail sentinel becomes unreachable and
	// the final scan fails instead of quietly passing on a partial list.
	headSentinel := []byte("aaa-sentinel-head")
	tailSentinel := []byte("zzz-sentinel-tail")
	table.putRecord(&LogRecord{Key: headSentinel, Value: []byte("h")})
	table.putRecord(&LogRecord{Key: tailSentinel, Value: []byte("t")})

	start := make(chan struct{})
	var wg sync.WaitGroup

	// Inserter: keeps (re)writing normal values for every key.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for round := 0; round < rounds; round++ {
			for i := 0; i < keyCount; i++ {
				key := util.GetTestKey(int64(i))
				table.putRecord(&LogRecord{
					Key:   key,
					Value: util.RandomValue(128),
					Type:  LogRecordNormal,
				})
			}
		}
	}()

	// Deleter: marks the same keys deleted on the same lock-free path.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for round := 0; round < rounds; round++ {
			for i := 0; i < keyCount; i++ {
				key := util.GetTestKey(int64(i))
				table.putRecord(&LogRecord{Key: key, Type: LogRecordDeleted})
			}
		}
	}()

	// Reader: concurrently does point lookups and must never observe a missing
	// node for a key that is actually linked (tombstone vs value both count as
	// present). It also keeps the iterator path exercised under contention.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for round := 0; round < rounds; round++ {
			for i := 0; i < keyCount; i += 7 {
				_, _ = table.get(util.GetTestKey(int64(i)))
			}
			_ = table.allSklKeys(t)
		}
	}()

	close(start)
	wg.Wait()

	// Chain integrity: the full scan must return exactly keyCount keys plus the
	// two sentinels, in strictly sorted order, with no duplicates.
	keys := table.allSklKeys(t)
	require.Len(t, keys, keyCount+2, "chain broken: some nodes became unreachable")

	assert.Equal(t, headSentinel, keys[0])
	assert.Equal(t, tailSentinel, keys[len(keys)-1])
	for i := 1; i < len(keys); i++ {
		assert.Equal(t, bytes.Compare(keys[i-1], keys[i]), -1,
			"keys not strictly sorted around index %d", i)
	}

	// Point lookup integrity: every key is reachable and reports a coherent
	// state (either a live value or a tombstone), never a false "not found".
	for i := 0; i < keyCount; i++ {
		key := util.GetTestKey(int64(i))
		// skl.Get returns a zero ValueStruct only when the node cannot be
		// found. Tombstones carry Meta=LogRecordDeleted instead, so presence is
		// established through Meta rather than value length.
		vs := table.skl.Get(y.KeyWithTs(key, 0))
		require.Contains(t, []byte{LogRecordNormal, LogRecordDeleted}, vs.Meta,
			"key %q lost from the chain after concurrent put/delete", string(key))
		// The memtable-level getter must keep resolving the key as well.
		deleted, _ := table.get(key)
		assert.Equal(t, vs.Meta == LogRecordDeleted, deleted)
	}

	// Sanity check on the two sentinels as well.
	deletedHead, headVal := table.get(headSentinel)
	assert.False(t, deletedHead)
	assert.Equal(t, []byte("h"), headVal)
	deletedTail, tailVal := table.get(tailSentinel)
	assert.False(t, deletedTail)
	assert.Equal(t, []byte("t"), tailVal)
}

// TestMemtableConcurrentPutDeleteDisjointKeys covers the common production
// shape of the workload described in the issue: hundreds of thousands of
// mixed inserts and deletes spread across many goroutines and keys. It asserts
// that the aggregate set of live + deleted nodes stays fully linked.
func TestMemtableConcurrentPutDeleteDisjointKeys(t *testing.T) {
	const goroutines = 8
	const perGoroutine = 1000

	table := openMemtableWithoutWal(t, DefaultOptions.MemtableSize)
	start := make(chan struct{})
	var wg sync.WaitGroup

	totalKeys := goroutines * perGoroutine
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			base := g * perGoroutine
			for i := 0; i < perGoroutine; i++ {
				key := util.GetTestKey(int64(base + i))
				record := &LogRecord{Key: key, Value: util.RandomValue(64)}
				if i%3 == 0 {
					record.Type = LogRecordDeleted
					record.Value = nil
				}
				table.putRecord(record)
			}
		}(g)
	}

	close(start)
	wg.Wait()

	keys := table.allSklKeys(t)
	require.Len(t, keys, totalKeys, "chain broken: node count mismatch")

	seen := make(map[string]struct{}, totalKeys)
	for _, key := range keys {
		seen[string(key)] = struct{}{}
	}
	assert.Len(t, seen, totalKeys, "duplicate or missing node after concurrent writes")
}

func TestMemtableOpen(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-open")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}

	t.Run("open memtable", func(t *testing.T) {
		var table *memtable
		table, err = openMemtable(opts)
		assert.NotNil(t, table)
		require.NoError(t, err)
		err = table.close()
		assert.NoError(t, err)
	})
}

func TestMemtableOpenAll(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-open-all")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	var table *memtable
	for i := 0; i < DefaultOptions.MemtableNums; i++ {
		opts := memtableOptions{
			dirPath:         path,
			tableID:         uint32(i),
			memSize:         DefaultOptions.MemtableSize,
			walBytesPerSync: DefaultOptions.BytesPerSync,
			walSync:         DefaultBatchOptions.Sync,
		}
		table, err = openMemtable(opts)
		require.NoError(t, err)
		assert.NotNil(t, table)
		err = table.close()
		require.NoError(t, err)
	}

	t.Run("test open all memtables", func(t *testing.T) {
		var tables []*memtable
		var opts = DefaultOptions
		opts.DirPath = path
		tables, err = openAllMemtables(opts)
		require.NoError(t, err)
		assert.NotNil(t, tables)
		for _, table := range tables {
			err = table.close()
			assert.NoError(t, err)
		}
	})
}

func TestMemTablePutAllKindsEntries(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-put-all-kinds-entries")
	require.NoError(t, err)
	assert.NotEqual(t, "", path)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)

	logs := []*LogRecord{
		{Key: []byte("key 0"), Value: []byte("value 0"), Type: LogRecordNormal},
		{Key: nil, Value: []byte("value 1"), Type: LogRecordNormal},
		{Key: []byte("key 2"), Value: nil, Type: LogRecordNormal},
		{Key: []byte("key 0"), Value: nil, Type: LogRecordDeleted},
	}

	type args struct {
		entry map[string]*LogRecord
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"normal key-value", args{map[string]*LogRecord{string(logs[0].Key): logs[0]}}, false},
		{"nil key", args{map[string]*LogRecord{string(logs[1].Key): logs[1]}}, false},
		{"nil value", args{map[string]*LogRecord{string(logs[2].Key): logs[2]}}, false},
		{"delete key-value", args{map[string]*LogRecord{string(logs[3].Key): logs[3]}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = table.putBatch(tt.args.entry, node.Generate(), writeOpts)
			assert.NoError(t, err)
		})
	}

	err = table.close()
	assert.NoError(t, err)
}

func TestMemTablePutBatch(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-put-batch")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)

	pendingWrites := make(map[string]*LogRecord)
	val := util.RandomValue(512)
	for i := 0; i < 1000; i++ {
		log := &LogRecord{Key: util.GetTestKey(int64(i)), Value: val}
		pendingWrites[string(log.Key)] = log
	}

	t.Run("test memory table put batch", func(t *testing.T) {
		err = table.putBatch(pendingWrites, node.Generate(), writeOpts)
		assert.NoError(t, err)
	})

	err = table.close()
	assert.NoError(t, err)
}

func TestMemTablePutBatchReopen(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-put-batch-reopen")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)

	pendingWrites := make(map[string]*LogRecord)
	val := util.RandomValue(512)
	for i := 0; i < 1000; i++ {
		log := &LogRecord{Key: util.GetTestKey(int64(i)), Value: val}
		pendingWrites[string(log.Key)] = log
	}

	err = table.putBatch(pendingWrites, node.Generate(), writeOpts)
	require.NoError(t, err)

	t.Run("test memory table put batch after reopening", func(t *testing.T) {
		err = table.close()
		require.NoError(t, err)
		table, err = openMemtable(opts)
		require.NoError(t, err)
		err = table.putBatch(pendingWrites, node.Generate(), writeOpts)
		require.NoError(t, err)
	})

	err = table.close()
	assert.NoError(t, err)
}

func TestMemTableGet(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-get")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)

	writeLogs := map[string]*LogRecord{
		"key 0": {Key: []byte("key 0"), Value: []byte("value 0"), Type: LogRecordNormal},
		"":      {Key: nil, Value: []byte("value 1"), Type: LogRecordNormal},
		"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordNormal},
	}
	deleteLogs := map[string]*LogRecord{
		"key 0": {Key: []byte("key 0"), Value: []byte(""), Type: LogRecordDeleted},
		"":      {Key: nil, Value: []byte(""), Type: LogRecordDeleted},
		"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordDeleted},
	}

	err = table.putBatch(writeLogs, node.Generate(), writeOpts)
	require.NoError(t, err)
	t.Run("get existing log", func(t *testing.T) {
		for keyStr, log := range writeLogs {
			del, value := table.get([]byte(keyStr))
			assert.False(t, del)
			assert.Equal(t, value, log.Value)
		}
	})

	err = table.putBatch(deleteLogs, node.Generate(), writeOpts)
	require.NoError(t, err)
	t.Run("get deleted log", func(t *testing.T) {
		for keyStr, log := range deleteLogs {
			del, value := table.get([]byte(keyStr))
			assert.True(t, del)
			assert.Equal(t, value, log.Value)
		}
	})

	err = table.close()
	assert.NoError(t, err)
}

func TestMemTableGetReopen(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-get-reopen")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)
	t.Run("get existing log reopen", func(t *testing.T) {
		var table *memtable
		table, err = openMemtable(opts)
		require.NoError(t, err)
		writeLogs := map[string]*LogRecord{
			"key 0": {Key: []byte("key 0"), Value: []byte("value 0"), Type: LogRecordNormal},
			"":      {Key: nil, Value: []byte("value 1"), Type: LogRecordNormal},
			"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordNormal},
		}
		err = table.putBatch(writeLogs, node.Generate(), writeOpts)
		require.NoError(t, err)
		err = table.close()
		require.NoError(t, err)

		table, err = openMemtable(opts)
		require.NoError(t, err)
		for keyStr, log := range writeLogs {
			del, value := table.get([]byte(keyStr))
			assert.False(t, del)
			assert.Equal(t, log.Value, value)
		}

		err = table.close()
		assert.NoError(t, err)
	})
	t.Run("get deleted log reopen", func(t *testing.T) {
		var table *memtable
		table, err = openMemtable(opts)
		require.NoError(t, err)
		deleteLogs := map[string]*LogRecord{
			"key 0": {Key: []byte("key 0"), Value: []byte(""), Type: LogRecordDeleted},
			"":      {Key: nil, Value: []byte(""), Type: LogRecordDeleted},
			"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordDeleted},
		}
		err = table.putBatch(deleteLogs, node.Generate(), writeOpts)
		require.NoError(t, err)

		for keyStr, log := range deleteLogs {
			del, value := table.get([]byte(keyStr))
			assert.True(t, del)
			assert.Equal(t, log.Value, value)
		}

		err = table.close()
		require.NoError(t, err)

		table, err = openMemtable(opts)
		require.NoError(t, err)
		for keyStr, log := range deleteLogs {
			del, value := table.get([]byte(keyStr))
			assert.True(t, del)
			assert.Equal(t, log.Value, value)
		}

		err = table.close()
		assert.NoError(t, err)
	})
}

func TestMemTableDelWal(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-delete-wal")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	t.Run("test memtable delete wal", func(t *testing.T) {
		var entries []fs.DirEntry
		err = table.deleteWAl()
		require.NoError(t, err)
		entries, err = os.ReadDir(path)
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	err = table.close()
	assert.NoError(t, err)
}

func TestMemTableSync(t *testing.T) {
	var err error
	path, err := os.MkdirTemp("", "memtable-test-sync")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)

	pendingWrites := make(map[string]*LogRecord)
	val := util.RandomValue(512)
	for i := 0; i < 1000; i++ {
		log := &LogRecord{Key: util.GetTestKey(int64(i)), Value: val}
		pendingWrites[string(log.Key)] = log
	}
	err = table.putBatch(pendingWrites, node.Generate(), writeOpts)
	require.NoError(t, err)

	t.Run("test memtable delete wal", func(t *testing.T) {
		err = table.sync()
		assert.NoError(t, err)
	})

	err = table.close()
	assert.NoError(t, err)
}

func TestMemtableClose(t *testing.T) {
	var err error
	path, err := os.MkdirTemp("", "memtable-test-close")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}

	table, err := openMemtable(opts)
	require.NoError(t, err)

	t.Run("open memtable", func(t *testing.T) {
		err = table.close()
		assert.NoError(t, err)
	})
}

func TestNewMemtableIterator(t *testing.T) {
	path, err := os.MkdirTemp("", "memtable-test-iterator-new")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}

	table, err := openMemtable(opts)
	defer func() {
		err = table.close()
		assert.NoError(t, err)
	}()
	require.NoError(t, err)

	options := IteratorOptions{
		Reverse: false,
	}
	iter := newMemtableIterator(options, table)
	require.NoError(t, err)

	err = iter.Close()
	assert.NoError(t, err)
}

func Test_memtableIterator(t *testing.T) {
	var err error
	path, err := os.MkdirTemp("", "memtable-test-iterator-rewind")
	require.NoError(t, err)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	opts := memtableOptions{
		dirPath:         path,
		tableID:         0,
		memSize:         DefaultOptions.MemtableSize,
		walBytesPerSync: DefaultOptions.BytesPerSync,
		walSync:         DefaultBatchOptions.Sync,
	}
	table, err := openMemtable(opts)
	require.NoError(t, err)

	writeOpts := WriteOptions{
		Sync:       false,
		DisableWal: false,
	}
	node, err := snowflake.NewNode(1)
	require.NoError(t, err)
	writeLogs := map[string]*LogRecord{
		"key 0": {Key: []byte("key 0"), Value: []byte("value 0"), Type: LogRecordNormal},
		"key 1": {Key: nil, Value: []byte("value 1"), Type: LogRecordNormal},
		"key 2": {Key: []byte("key 2"), Value: []byte(""), Type: LogRecordNormal},
	}
	writeLogs2 := map[string]*LogRecord{
		"abc 0": {Key: []byte("abc 0"), Value: []byte("value 0"), Type: LogRecordNormal},
		"key 3": {Key: nil, Value: []byte("key 3"), Type: LogRecordNormal},
		"abc 1": {Key: []byte("abc 1"), Value: []byte(""), Type: LogRecordNormal},
	}
	err = table.putBatch(writeLogs, node.Generate(), writeOpts)
	require.NoError(t, err)

	iteratorOptions := IteratorOptions{
		Reverse: false,
	}
	itr := newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	var prev []byte
	itr.Rewind()
	for itr.Valid() {
		currKey := itr.Key()
		assert.True(t, prev == nil || bytes.Compare(prev, currKey) == -1)
		assert.Equal(t, writeLogs[string(currKey)].Value, itr.Value().(y.ValueStruct).Value)
		assert.Equal(t, writeLogs[string(currKey)].Type, itr.Value().(y.ValueStruct).Meta)
		prev = currKey
		itr.Next()
	}
	err = itr.Close()
	require.NoError(t, err)

	iteratorOptions.Reverse = true
	prev = nil
	itr = newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	itr.Rewind()
	for itr.Valid() {
		currKey := itr.Key()
		assert.True(t, prev == nil || bytes.Compare(prev, currKey) == 1)
		prev = currKey
		assert.Equal(t, writeLogs[string(currKey)].Value, itr.Value().(y.ValueStruct).Value)
		assert.Equal(t, writeLogs[string(currKey)].Type, itr.Value().(y.ValueStruct).Meta)
		itr.Next()
	}
	err = itr.Close()
	require.NoError(t, err)

	iteratorOptions.Reverse = false
	itr = newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	itr.Seek([]byte("key 0"))
	assert.Equal(t, []byte("key 0"), itr.Key())
	itr.Seek([]byte("key 4"))
	assert.False(t, itr.Valid())

	itr.Seek([]byte("aye 2"))
	assert.Equal(t, []byte("key 0"), itr.Key())
	err = itr.Close()
	require.NoError(t, err)

	iteratorOptions.Reverse = true
	itr = newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	itr.Seek([]byte("key 4"))
	assert.Equal(t, []byte("key 2"), itr.Key())

	itr.Seek([]byte("key 2"))
	assert.Equal(t, []byte("key 2"), itr.Key())

	itr.Seek([]byte("aye 2"))
	assert.False(t, itr.Valid())

	err = itr.Close()
	require.NoError(t, err)

	// prefix
	err = table.putBatch(writeLogs2, node.Generate(), writeOpts)
	require.NoError(t, err)

	iteratorOptions.Reverse = false
	iteratorOptions.Prefix = []byte("not valid")
	itr = newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	itr.Rewind()
	assert.False(t, itr.Valid())
	err = itr.Close()
	require.NoError(t, err)

	iteratorOptions.Reverse = false
	iteratorOptions.Prefix = []byte("abc")
	itr = newMemtableIterator(iteratorOptions, table)
	require.NoError(t, err)
	itr.Rewind()
	for itr.Valid() {
		assert.True(t, bytes.HasPrefix(itr.Key(), iteratorOptions.Prefix))
		assert.Equal(t, writeLogs2[string(itr.Key())].Value, itr.Value().(y.ValueStruct).Value)
		itr.Next()
	}
	err = itr.Close()
	assert.NoError(t, err)
}
