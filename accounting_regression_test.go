package lotusdb

import (
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/lotusdblabs/lotusdb/v2/util"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValueLogWriteBatchCountsOnlyWrittenEntries(t *testing.T) {
	options := valueLogOptions{
		dirPath:         t.TempDir(),
		segmentSize:     MB,
		partitionNum:    1,
		hashKeyFunction: DefaultOptions.KeyHashFunction,
	}

	vlog, err := openValueLog(options)
	require.NoError(t, err)

	successfulRecords := []*ValueLogRecord{
		{uid: uuid.New(), key: []byte("key-1"), value: []byte("value-1")},
		{uid: uuid.New(), key: []byte("key-2"), value: []byte("value-2")},
		{uid: uuid.New(), key: []byte("key-3"), value: []byte("value-3")},
	}
	positions, err := vlog.writeBatch(successfulRecords)
	require.NoError(t, err)
	require.Len(t, positions, len(successfulRecords))
	assert.Equal(t, uint32(3), atomic.LoadUint32(&vlog.totalNumber))

	sizeBeforeFailure, err := util.DirSize(options.dirPath)
	require.NoError(t, err)

	failedRecords := make([]*ValueLogRecord, 16)
	for i := range failedRecords {
		failedRecords[i] = &ValueLogRecord{
			uid:   uuid.New(),
			key:   []byte("oversized-key"),
			value: make([]byte, 128*KB),
		}
	}
	_, err = vlog.writeBatch(failedRecords)
	require.ErrorIs(t, err, wal.ErrPendingSizeTooLarge)

	assert.Equal(t, uint32(3), atomic.LoadUint32(&vlog.totalNumber))
	sizeAfterFailure, err := util.DirSize(options.dirPath)
	require.NoError(t, err)
	assert.Equal(t, sizeBeforeFailure, sizeAfterFailure)

	require.NoError(t, vlog.close())
}

func TestFailedBatchDoesNotInflateCompactionThreshold(t *testing.T) {
	options := DefaultOptions
	options.DirPath = t.TempDir()
	options.PartitionNum = 1
	options.MemtableSize = MB
	options.ValueLogFileSize = MB
	options.AutoCompactSupport = false
	options.AdvisedCompactionRate = 0.3
	options.ForceCompactionRate = 0.5

	db, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(db)

	validRecords := make([]*ValueLogRecord, 10)
	for i := range validRecords {
		validRecords[i] = &ValueLogRecord{
			uid:   uuid.New(),
			key:   []byte{byte('a' + i)},
			value: make([]byte, 32*KB),
		}
	}
	positions, err := db.vlog.writeBatch(validRecords)
	require.NoError(t, err)
	require.Len(t, positions, len(validRecords))
	require.NoError(t, db.vlog.sync())

	failedRecords := make([]*ValueLogRecord, 100)
	for i := range failedRecords {
		failedRecords[i] = &ValueLogRecord{
			uid:   uuid.New(),
			key:   []byte("failed-key"),
			value: make([]byte, 32*KB),
		}
	}
	_, err = db.vlog.writeBatch(failedRecords)
	require.ErrorIs(t, err, wal.ErrPendingSizeTooLarge)
	require.Equal(t, uint32(10), atomic.LoadUint32(&db.vlog.totalNumber))

	for i := 0; i < 5; i++ {
		db.vlog.setDeprecated(0, validRecords[i].uid)
	}

	db.compactChan = make(chan deprecatedState, 1)
	db.options.AutoCompactSupport = true
	db.sendThresholdState()
	select {
	case state := <-db.compactChan:
		assert.Equal(t, ThresholdState(ArriveForceThreshold), state.thresholdState)
	default:
		t.Fatal("expected correct total count to reach force compaction threshold")
	}
	db.options.AutoCompactSupport = false

	sizeBeforeCompaction, err := util.DirSize(options.DirPath)
	require.NoError(t, err)
	require.NoError(t, db.CompactWithDeprecatedtable())
	sizeAfterCompaction, err := util.DirSize(options.DirPath)
	require.NoError(t, err)
	assert.Less(t, sizeAfterCompaction, sizeBeforeCompaction)
	assert.Equal(t, uint32(5), atomic.LoadUint32(&db.vlog.totalNumber))

	require.NoError(t, db.Close())

	reopenedDB, err := Open(options)
	require.NoError(t, err)
	defer destroyDB(reopenedDB)
	assert.Equal(t, uint32(5), atomic.LoadUint32(&reopenedDB.vlog.totalNumber))
}
