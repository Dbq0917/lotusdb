package lotusdb

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T, indexType IndexType) *DB {
	t.Helper()
	options := DefaultOptions
	path, err := os.MkdirTemp("", "db-test-batch")
	require.NoError(t, err)
	options.DirPath = path
	options.IndexType = indexType
	db, err := Open(options)
	require.NoError(t, err)
	return db
}

// TestBatchReadBetweenNewBatchAndCommit reproduces the original deadlock:
// a write batch used to hold the DB write lock from NewBatch until Commit,
// so any read (from the same goroutine or another one) performed in between
// blocked forever. Reads must now complete and the batch must still commit
// atomically.
func TestBatchReadBetweenNewBatchAndCommit(t *testing.T) {
	for _, indexType := range []IndexType{BTree, Hash} {
		t.Run(indexTypeName(indexType), func(t *testing.T) {
			db := openTestDB(t, indexType)
			defer destroyDB(db)

			require.NoError(t, db.Put([]byte("committed-key"), []byte("committed-value")))

			batch := db.NewBatch(DefaultBatchOptions)
			require.NoError(t, batch.Put([]byte("pending-key"), []byte("pending-value")))

			// Pending writes are visible inside the batch.
			pending, err := batch.Get([]byte("pending-key"))
			require.NoError(t, err)
			assert.Equal(t, []byte("pending-value"), pending)

			// Reads of committed data must not be blocked by the open batch.
			done := make(chan struct{})
			go func() {
				defer close(done)
				value, getErr := db.Get([]byte("committed-key"))
				assert.NoError(t, getErr)
				assert.Equal(t, []byte("committed-value"), value)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("db.Get blocked while a write batch is open")
			}

			// A read from the same goroutine must not self-deadlock either.
			value, err := batch.Get([]byte("committed-key"))
			require.NoError(t, err)
			assert.Equal(t, []byte("committed-value"), value)

			require.NoError(t, batch.Commit())

			value, err = db.Get([]byte("pending-key"))
			require.NoError(t, err)
			assert.Equal(t, []byte("pending-value"), value)

			// A committed batch cannot be committed twice.
			assert.ErrorIs(t, batch.Commit(), ErrBatchCommitted)
		})
	}
}

// TestBatchDeleteAndExistBetweenNewBatchAndCommit checks delete semantics and
// Exist while a batch is still open.
func TestBatchDeleteAndExistBetweenNewBatchAndCommit(t *testing.T) {
	db := openTestDB(t, BTree)
	defer destroyDB(db)

	require.NoError(t, db.Put([]byte("key-1"), []byte("value-1")))
	require.NoError(t, db.Put([]byte("key-2"), []byte("value-2")))

	batch := db.NewBatch(DefaultBatchOptions)
	require.NoError(t, batch.Delete([]byte("key-1")))

	exist, err := batch.Exist([]byte("key-1"))
	require.NoError(t, err)
	assert.False(t, exist)

	exist, err = batch.Exist([]byte("key-2"))
	require.NoError(t, err)
	assert.True(t, exist)

	exist, err = batch.Exist([]byte("missing-key"))
	require.NoError(t, err)
	assert.False(t, exist)

	_, err = batch.Get([]byte("key-1"))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	require.NoError(t, batch.Commit())

	_, err = db.Get([]byte("key-1"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// TestBatchConcurrentReadersAndWriters makes sure that narrowing the lock
// scope keeps concurrent batch usage safe and free of deadlocks.
func TestBatchConcurrentReadersAndWriters(t *testing.T) {
	db := openTestDB(t, BTree)
	defer destroyDB(db)

	const writers = 4
	const readers = 4
	const iterations = 200

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				batch := db.NewBatch(DefaultBatchOptions)
				key := []byte("key")
				_ = batch.Put(key, []byte("value"))
				if _, err := batch.Get(key); err != nil {
					t.Errorf("batch.Get failed: %v", err)
					return
				}
				if err := batch.Commit(); err != nil {
					t.Errorf("batch.Commit failed: %v", err)
					return
				}
			}
		}(i)
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = db.Get([]byte("key"))
			}
		}()
	}

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent batch readers and writers deadlocked")
	}

	value, err := db.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
}

func indexTypeName(indexType IndexType) string {
	if indexType == Hash {
		return "Hash"
	}
	return "BTree"
}
