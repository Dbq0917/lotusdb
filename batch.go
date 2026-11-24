package lotusdb

import (
	"errors"
	"fmt"
	"sync"

	"github.com/bwmarrin/snowflake"
	"github.com/rosedblabs/diskhash"
)

// Batch is a batch operations of the database.
// If readonly is true, you can only get data from the batch by Get method.
// An error will be returned if you try to use Put or Delete method.
//
// If readonly is false, you can use Put and Delete method to write data to the batch.
// The data will be written to the database when you call Commit method.
//
// Batch is not a transaction, it does not guarantee isolation.
// But it can guarantee atomicity, consistency and durability(if the Sync options is true).
type Batch struct {
	db            *DB
	pendingWrites map[string]*LogRecord
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool
	batchID       *snowflake.Node
}

// NewBatch creates a new Batch instance.
func (db *DB) NewBatch(options BatchOptions) *Batch {
	batch := &Batch{
		db:        db,
		options:   options,
		committed: false,
	}
	if !options.ReadOnly {
		batch.pendingWrites = make(map[string]*LogRecord)
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchID = node
	}
	return batch
}

func makeBatch() interface{} {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &Batch{
		options: DefaultBatchOptions,
		batchID: node,
	}
}

func (b *Batch) init(rdonly, sync bool, disableWal bool, db *DB) *Batch {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.options.DisableWal = disableWal
	b.db = db
	return b
}

func (b *Batch) withPendingWrites() {
	b.pendingWrites = make(map[string]*LogRecord)
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = nil
	b.committed = false
}

// Put adds a key-value pair to the batch for writing.
func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// write to pendingWrites
	b.pendingWrites[string(key)] = &LogRecord{
		Key:   key,
		Value: value,
		Type:  LogRecordNormal,
	}
	b.mu.Unlock()

	return nil
}

// readFromDB looks up a key in the committed data (memtables, index and value
// log) outside of any pending writes. It only takes a short read lock around
// the memtable access instead of holding the DB lock for the whole batch
// lifecycle, so a read between NewBatch and Commit cannot deadlock.
func (b *Batch) readFromDB(key []byte) (value []byte, deleted bool, err error) {
	b.db.mu.RLock()
	// get from memtables
	tables := b.db.getMemTables()
	for _, table := range tables {
		tableDeleted, tableValue := table.get(key)
		if tableDeleted {
			b.db.mu.RUnlock()
			return nil, true, nil
		}
		if len(tableValue) != 0 {
			b.db.mu.RUnlock()
			return tableValue, false, nil
		}
	}
	b.db.mu.RUnlock()

	// get from index
	var hashValue []byte
	var matchKey func(diskhash.Slot) (bool, error)
	if b.db.options.IndexType == Hash {
		matchKey = MatchKeyFunc(b.db, key, nil, &hashValue)
	}

	position, err := b.db.index.Get(key, matchKey)
	if err != nil {
		return nil, false, err
	}

	if b.db.options.IndexType == Hash {
		if hashValue == nil {
			return nil, false, ErrKeyNotFound
		}
		return hashValue, false, nil
	}
	if position == nil {
		return nil, false, ErrKeyNotFound
	}
	record, err := b.db.vlog.read(position)
	if err != nil {
		return nil, false, err
	}
	return record.value, false, nil
}

// Get retrieves the value associated with a given key from the batch.
func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, ErrDBClosed
	}

	// get from pendingWrites
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			if record.Type == LogRecordDeleted {
				b.mu.RUnlock()
				return nil, ErrKeyNotFound
			}
			b.mu.RUnlock()
			return record.Value, nil
		}
		b.mu.RUnlock()
	}

	value, deleted, err := b.readFromDB(key)
	if err != nil {
		return nil, err
	}
	if deleted {
		return nil, ErrKeyNotFound
	}
	return value, nil
}

// Delete marks a key for deletion in the batch.
func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	b.pendingWrites[string(key)] = &LogRecord{
		Key:  key,
		Type: LogRecordDeleted,
	}
	b.mu.Unlock()

	return nil
}

// Exist checks if the key exists in the database.
func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed {
		return false, ErrDBClosed
	}

	// check if the key exists in pendingWrites
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			b.mu.RUnlock()
			return record.Type != LogRecordDeleted, nil
		}
		b.mu.RUnlock()
	}

	_, deleted, err := b.readFromDB(key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return !deleted, nil
}

// Commit commits the batch, if the batch is readonly or empty, it will return directly.
//
// It will iterate the pendingWrites and write the data to the database,
// then write a record to indicate the end of the batch to guarantee atomicity.
// Finally, it will write the index.
func (b *Batch) Commit() error {
	if b.db.closed {
		return ErrDBClosed
	}

	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// check if committed
	if b.committed {
		return ErrBatchCommitted
	}

	// The DB write lock is only held for the actual commit critical section,
	// not for the whole lifecycle of the batch, so reads between NewBatch and
	// Commit are not blocked by the batch's own write lock.
	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	// wait for memtable space
	if err := b.db.waitMemtableSpace(); err != nil {
		return err
	}
	batchID := b.batchID.Generate()
	// call memtable put batch
	err := b.db.activeMem.putBatch(b.pendingWrites, batchID, b.options.WriteOptions)
	if err != nil {
		return err
	}

	b.committed = true
	return nil
}
