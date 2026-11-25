package lotusdb

import (
	"bytes"
	"errors"
	"io"
	"os"
	"reflect"

	"github.com/rosedblabs/diskhash"
	"github.com/rosedblabs/wal"
)

const partitionMigrationBatchSize = 4096
const partitionMigrationMaxBatchBytes = 16 << 20

// migratePartitionLayout rewrites all live index entries and value-log records
// into a staging directory using newPartitionNum. The active database directory
// is only switched after the complete new generation has been synced.
//
// Memtables are intentionally not rewritten: their WAL records are keyed but
// not hash-partitioned, and they take precedence over the index on reads.
func migratePartitionLayout(options Options, oldPartitionNum, newPartitionNum int) (liveEntries uint32, backupDir string, err error) {
	if oldPartitionNum <= 0 || newPartitionNum <= 0 {
		return 0, "", ErrInvalidDBMetadata
	}
	if oldPartitionNum == newPartitionNum {
		return 0, "", nil
	}

	stageDir, err := os.MkdirTemp(options.DirPath, partitionMigrationStagePrefix+"*")
	if err != nil {
		return 0, "", err
	}
	defer func() {
		_ = os.RemoveAll(stageDir)
	}()

	liveEntries, err = buildMigratedPartitionStage(options, oldPartitionNum, newPartitionNum, stageDir)
	if err != nil {
		return 0, "", err
	}

	backupDir, err = replacePartitionFiles(options.DirPath, stageDir, oldPartitionNum)
	if err != nil {
		return 0, "", err
	}

	return liveEntries, backupDir, nil
}

func buildMigratedPartitionStage(options Options, oldPartitionNum, newPartitionNum int, stageDir string) (liveEntries uint32, err error) {
	oldIndex, err := openIndex(indexOptions{
		indexType:       options.IndexType,
		dirPath:         options.DirPath,
		partitionNum:    oldPartitionNum,
		keyHashFunction: options.KeyHashFunction,
	})
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = oldIndex.Close()
	}()

	oldVLog, err := openValueLog(valueLogOptions{
		dirPath:               options.DirPath,
		segmentSize:           options.ValueLogFileSize,
		partitionNum:          uint32(oldPartitionNum),
		hashKeyFunction:       options.KeyHashFunction,
		compactBatchCapacity:  options.CompactBatchCapacity,
		deprecatedtableNumber: 0,
		totalNumber:           0,
	})
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = oldVLog.close()
	}()

	newIndex, err := openIndex(indexOptions{
		indexType:       options.IndexType,
		dirPath:         stageDir,
		partitionNum:    newPartitionNum,
		keyHashFunction: options.KeyHashFunction,
	})
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = newIndex.Close()
	}()

	newVLog, err := openValueLog(valueLogOptions{
		dirPath:               stageDir,
		segmentSize:           options.ValueLogFileSize,
		partitionNum:          uint32(newPartitionNum),
		hashKeyFunction:       options.KeyHashFunction,
		compactBatchCapacity:  options.CompactBatchCapacity,
		deprecatedtableNumber: 0,
		totalNumber:           0,
	})
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = newVLog.close()
	}()

	pendingRecords := make([]*ValueLogRecord, 0, partitionMigrationBatchSize)
	pendingBytes := int64(0)
	maxBatchBytes := int64(partitionMigrationMaxBatchBytes)
	if quarterSegmentSize := options.ValueLogFileSize / 4; quarterSegmentSize > 0 && quarterSegmentSize < maxBatchBytes {
		maxBatchBytes = quarterSegmentSize
	}
	flushPendingRecords := func() error {
		if len(pendingRecords) == 0 {
			return nil
		}

		positions, writeErr := newVLog.writeBatch(pendingRecords)
		if writeErr != nil {
			return writeErr
		}

		matchKeys := make([]diskhash.MatchKeyFunc, 0)
		if options.IndexType == Hash {
			matchKeys = make([]diskhash.MatchKeyFunc, len(positions))
			for i := range positions {
				matchKeys[i] = migrationMatchKeyFunc(newVLog, positions[i].key, nil, nil)
			}
		}
		if _, writeErr = newIndex.PutBatch(positions, matchKeys...); writeErr != nil {
			return writeErr
		}
		if writeErr = newIndex.Sync(); writeErr != nil {
			return writeErr
		}
		if writeErr = newVLog.sync(); writeErr != nil {
			return writeErr
		}

		liveEntries += uint32(len(pendingRecords))
		pendingRecords = pendingRecords[:0]
		pendingBytes = 0
		return nil
	}

	for partition := 0; partition < oldPartitionNum; partition++ {
		reader := oldVLog.walFiles[partition].NewReader()
		for {
			chunk, position, readErr := reader.Next()
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					break
				}
				return 0, readErr
			}

			record := decodeValueLogRecord(chunk)
			if record == nil {
				return 0, ErrKeyNotFound
			}

			live, liveErr := migrationRecordIsLive(oldIndex, options.IndexType, record.key, partition, position)
			if liveErr != nil {
				return 0, liveErr
			}
			if !live {
				continue
			}

			recordBytes := int64(len(chunk))
			if len(pendingRecords) > 0 &&
				(len(pendingRecords) >= partitionMigrationBatchSize || pendingBytes+recordBytes > maxBatchBytes) {
				if err = flushPendingRecords(); err != nil {
					return 0, err
				}
			}
			pendingRecords = append(pendingRecords, record)
			pendingBytes += recordBytes
			if len(pendingRecords) >= partitionMigrationBatchSize || pendingBytes >= maxBatchBytes {
				if err = flushPendingRecords(); err != nil {
					return 0, err
				}
			}
		}
	}

	if err = flushPendingRecords(); err != nil {
		return 0, err
	}
	if err = newIndex.Sync(); err != nil {
		return 0, err
	}
	if err = newVLog.sync(); err != nil {
		return 0, err
	}

	return liveEntries, nil
}

func migrationRecordIsLive(index Index, indexType IndexType, key []byte, partition int, position *wal.ChunkPosition) (bool, error) {
	if indexType == Hash {
		matched := false
		matchKey := func(slot diskhash.Slot) (bool, error) {
			indexPosition := wal.DecodeChunkPosition(slot.Value)
			if !reflect.DeepEqual(indexPosition, position) {
				return false, nil
			}
			matched = true
			return true, nil
		}
		if _, err := index.Get(key, matchKey); err != nil {
			return false, err
		}
		return matched, nil
	}

	keyPosition, err := index.Get(key)
	if err != nil {
		return false, err
	}
	return keyPosition != nil &&
		keyPosition.partition == uint32(partition) &&
		reflect.DeepEqual(keyPosition.position, position), nil
}

func migrationMatchKeyFunc(vlog *valueLog, key []byte, keyPos **KeyPosition, value *[]byte) diskhash.MatchKeyFunc {
	return func(slot diskhash.Slot) (bool, error) {
		chunkPosition := wal.DecodeChunkPosition(slot.Value)
		checkKeyPos := &KeyPosition{
			key:       key,
			partition: uint32(vlog.getKeyPartition(key)),
			position:  chunkPosition,
		}
		valueLogRecord, err := vlog.read(checkKeyPos)
		if err != nil {
			return false, err
		}
		if valueLogRecord == nil || !bytes.Equal(valueLogRecord.key, key) {
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
