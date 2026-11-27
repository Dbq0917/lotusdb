package lotusdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/rosedblabs/diskhash"
	"github.com/rosedblabs/wal"
	"golang.org/x/sync/errgroup"
)

// diskhash requires fixed-size value
// so we set the slotValueLength to `binary.MaxVarintLen32*3 + binary.MaxVarintLen64`.
// This is the maximum length after wal.chunkPosition encoding.
const slotValueLength = binary.MaxVarintLen32*3 + binary.MaxVarintLen64

// HashTable is the diskhash index implementation.
// see: https://github.com/rosedblabs/diskhash
type HashTable struct {
	options indexOptions
	tables  []*diskhash.Table
}

// openHashIndex open a diskhash for each partition.
// The partition number is specified by the index options.
func openHashIndex(options indexOptions) (*HashTable, error) {
	tables := make([]*diskhash.Table, options.partitionNum)

	for i := 0; i < options.partitionNum; i++ {
		dishHashOptions := diskhash.DefaultOptions
		dishHashOptions.DirPath = filepath.Join(options.dirPath, fmt.Sprintf(indexFileExt, i))
		dishHashOptions.SlotValueLength = slotValueLength
		table, err := diskhash.Open(dishHashOptions)
		if err != nil {
			return nil, err
		}
		tables[i] = table
	}

	return &HashTable{
		options: options,
		tables:  tables,
	}, nil
}

// PutBatch put batch records to index.
func (ht *HashTable) PutBatch(positions []*KeyPosition, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error) {
	if len(positions) == 0 {
		return nil, nil
	}
	partitionRecords := make([][]*KeyPosition, ht.options.partitionNum)
	matchKeys := make([][]diskhash.MatchKeyFunc, ht.options.partitionNum)
	for i, pos := range positions {
		p := pos.partition
		partitionRecords[p] = append(partitionRecords[p], pos)
		matchKeys[p] = append(matchKeys[p], matchKeyFunc[i])
	}

	g, ctx := errgroup.WithContext(context.Background())
	for i := range partitionRecords {
		partition := i
		if len(partitionRecords[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			// get the hashtable instance for this partition
			table := ht.tables[partition]
			matchKey := matchKeys[partition]
			for i, record := range partitionRecords[partition] {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if len(record.key) == 0 {
						return ErrKeyIsEmpty
					}
					encPos := record.position.EncodeFixedSize()
					if err := table.Put(record.key, encPos, matchKey[i]); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return nil, g.Wait()
}

// Get chunk position by key.
func (ht *HashTable) Get(key []byte, matchKeyFunc ...diskhash.MatchKeyFunc) (*KeyPosition, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	p := ht.options.getKeyPartition(key)
	table := ht.tables[p]
	err := table.Get(key, matchKeyFunc[0])
	if err != nil {
		return nil, err
	}
	//nolint:nilnil // hashTable will not use keyPosition, so return nil, nil
	return nil, nil
}

// DeleteBatch delete batch records from index.
func (ht *HashTable) DeleteBatch(keys [][]byte, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	partitionKeys := make([][][]byte, ht.options.partitionNum)
	matchKeys := make([][]*diskhash.MatchKeyFunc, ht.options.partitionNum)
	for i, key := range keys {
		p := ht.options.getKeyPartition(key)
		partitionKeys[p] = append(partitionKeys[p], key)
		matchKeys[p] = append(matchKeys[p], &matchKeyFunc[i])
	}
	g, ctx := errgroup.WithContext(context.Background())
	for i := range partitionKeys {
		partition := i
		if len(partitionKeys[partition]) == 0 {
			continue
		}
		g.Go(func() error {
			table := ht.tables[partition]
			matchKey := matchKeys[partition]
			for i, key := range partitionKeys[partition] {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if len(key) == 0 {
						return ErrKeyIsEmpty
					}
					if err := table.Delete(key, *matchKey[i]); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return nil, g.Wait()
}

// Sync sync index data to disk.
func (ht *HashTable) Sync() error {
	for _, table := range ht.tables {
		err := table.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close close index.
func (ht *HashTable) Close() error {
	for _, table := range ht.tables {
		err := table.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// The hash table files are created by github.com/rosedblabs/diskhash,
// which does not expose any iteration API. To support iterators on the
// hash index, the table files are scanned directly, so the file layout
// of diskhash is mirrored here.
const (
	hashPrimaryFileName  = "HASH.PRIMARY"
	hashOverflowFileName = "HASH.OVERFLOW"
	hashSlotsPerBucket   = 31
	hashKeyLen           = 4
	hashNextOffsetLen    = 8
)

// hashBucketSize is the size in bytes of a bucket in the diskhash table files.
func hashBucketSize() int64 {
	return hashSlotsPerBucket*(hashKeyLen+slotValueLength) + hashNextOffsetLen
}

// forEachSlot walks all the buckets of the specified partition table, and
// invokes fn with the value of every occupied slot, every live key in the
// partition is visited exactly once.
//
// The caller must guarantee that the table files are not being written
// while scanning. In LotusDB this is done by holding db.flushLock and
// db.mu, which blocks flush, compaction and writes.
func (ht *HashTable) forEachSlot(partition int, fn func(slotValue []byte) error) error {
	dirPath := filepath.Join(ht.options.dirPath, fmt.Sprintf(indexFileExt, partition))
	primaryFile, err := os.Open(filepath.Join(dirPath, hashPrimaryFileName))
	if err != nil {
		return err
	}
	defer func() { _ = primaryFile.Close() }()

	overflowFile, err := os.Open(filepath.Join(dirPath, hashOverflowFileName))
	if err != nil {
		return err
	}
	defer func() { _ = overflowFile.Close() }()

	bucketSize := hashBucketSize()
	stat, err := primaryFile.Stat()
	if err != nil {
		return err
	}

	// scanBucket reads the bucket at offset from file, invokes fn for every
	// occupied slot, and returns the offset of the next overflow bucket.
	scanBucket := func(file *os.File, offset int64) (int64, error) {
		buf := make([]byte, bucketSize)
		if _, err := file.ReadAt(buf, offset); err != nil {
			return 0, err
		}
		for i := 0; i < hashSlotsPerBucket; i++ {
			slot := buf[i*(hashKeyLen+slotValueLength):]
			if binary.LittleEndian.Uint32(slot[:hashKeyLen]) == 0 {
				// all slots after an empty slot are empty
				break
			}
			value := make([]byte, slotValueLength)
			copy(value, slot[hashKeyLen:hashKeyLen+slotValueLength])
			if err := fn(value); err != nil {
				return 0, err
			}
		}
		return int64(binary.LittleEndian.Uint64(buf[bucketSize-hashNextOffsetLen:])), nil
	}

	// the first bucket of the primary file is a dummy header, skip it.
	for offset := bucketSize; offset+bucketSize <= stat.Size(); offset += bucketSize {
		nextOffset, err := scanBucket(primaryFile, offset)
		if err != nil {
			return err
		}
		// follow the overflow bucket chain
		for nextOffset != 0 {
			if nextOffset, err = scanBucket(overflowFile, nextOffset); err != nil {
				return err
			}
		}
	}
	return nil
}

// hashKV is a key/position pair collected from the hash index.
type hashKV struct {
	key   []byte
	value []byte // encoded chunk position of the key in the value log
}

// hashIndexIterator implements baseIterator.
// It iterates over a sorted snapshot of all the key/position pairs that
// are collected from one hash index partition when the iterator is
// created, so no extra data structure is maintained on the write path.
type hashIndexIterator struct {
	options IteratorOptions
	pairs   []hashKV // sorted by key in ascending order
	idx     int
}

// newHashIndexIterator collects all the key/position pairs from the
// specified hash index partition, and sorts them by key.
// The hash table only stores the hash of the key, so the raw key is
// read back from the value log by the position stored in the slot.
func newHashIndexIterator(db *DB, partition int, options IteratorOptions) (*hashIndexIterator, error) {
	hashIndex, ok := db.index.(*HashTable)
	if !ok {
		panic("index type not support")
	}
	var pairs []hashKV
	err := hashIndex.forEachSlot(partition, func(slotValue []byte) error {
		keyPos := &KeyPosition{
			partition: uint32(partition),
			position:  wal.DecodeChunkPosition(slotValue),
		}
		record, err := db.vlog.read(keyPos)
		if err != nil {
			return err
		}
		if record == nil {
			return nil
		}
		pairs = append(pairs, hashKV{key: record.key, value: slotValue})
		return nil
	})
	if err != nil {
		return nil, err
	}
	// sort the snapshot, so that it can be merged with the memtable
	// iterators by the heap.
	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].key, pairs[j].key) < 0
	})
	return &hashIndexIterator{options: options, pairs: pairs}, nil
}

// Rewind seek the first key in the iterator.
func (hi *hashIndexIterator) Rewind() {
	if hi.options.Reverse {
		hi.idx = len(hi.pairs) - 1
	} else {
		hi.idx = 0
	}
	hi.skipToPrefix()
}

// Seek move the iterator to the key which is
// greater(less when reverse is true) than or equal to the specified key.
func (hi *hashIndexIterator) Seek(key []byte) {
	if hi.options.Reverse {
		// the last key which is less than or equal to the specified key
		hi.idx = sort.Search(len(hi.pairs), func(i int) bool {
			return bytes.Compare(hi.pairs[i].key, key) > 0
		}) - 1
	} else {
		// the first key which is greater than or equal to the specified key
		hi.idx = sort.Search(len(hi.pairs), func(i int) bool {
			return bytes.Compare(hi.pairs[i].key, key) >= 0
		})
	}
	hi.skipToPrefix()
}

// Next moves the iterator to the next key.
func (hi *hashIndexIterator) Next() {
	if hi.options.Reverse {
		hi.idx--
	} else {
		hi.idx++
	}
	hi.skipToPrefix()
}

// skipToPrefix moves the iterator in the iteration direction until the
// current key has the specified prefix or the iterator is exhausted.
func (hi *hashIndexIterator) skipToPrefix() {
	if len(hi.options.Prefix) == 0 {
		return
	}
	for hi.Valid() && !bytes.HasPrefix(hi.pairs[hi.idx].key, hi.options.Prefix) {
		if hi.options.Reverse {
			hi.idx--
		} else {
			hi.idx++
		}
	}
}

// Key get the current key.
func (hi *hashIndexIterator) Key() []byte {
	return hi.pairs[hi.idx].key
}

// Value get the current value.
func (hi *hashIndexIterator) Value() any {
	return hi.pairs[hi.idx].value
}

// Valid returns whether the iterator is exhausted.
func (hi *hashIndexIterator) Valid() bool {
	return hi.idx >= 0 && hi.idx < len(hi.pairs)
}

// Close the iterator, the snapshot is in memory, nothing to release.
func (hi *hashIndexIterator) Close() error {
	return nil
}

// MatchKeyFunc Set nil if do not need keyPos or value.
func MatchKeyFunc(db *DB, key []byte, keyPos **KeyPosition, value *[]byte) func(slot diskhash.Slot) (bool, error) {
	return func(slot diskhash.Slot) (bool, error) {
		chunkPosition := wal.DecodeChunkPosition(slot.Value)
		checkKeyPos := &KeyPosition{
			key:       key,
			partition: uint32(db.vlog.getKeyPartition(key)),
			position:  chunkPosition,
		}
		valueLogRecord, err := db.vlog.read(checkKeyPos)
		if err != nil {
			return false, err
		}
		if valueLogRecord == nil {
			return false, nil
		}
		if !bytes.Equal(valueLogRecord.key, key) {
			return false, nil
		}
		if keyPos != nil {
			*keyPos = checkKeyPos
		}
		if value != nil {
			*value = valueLogRecord.value
		}
		return true, nil
	}
}
