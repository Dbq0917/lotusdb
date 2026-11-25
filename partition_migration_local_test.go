package lotusdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalPartitionMetadataMigration(t *testing.T) {
	indexTypes := []IndexType{BTree, Hash}
	newPartitionNums := []int{1, 2, 7}

	for _, indexType := range indexTypes {
		for _, newPartitionNum := range newPartitionNums {
			testName := fmt.Sprintf("index-%d-to-%d-partitions", indexType, newPartitionNum)
			t.Run(testName, func(t *testing.T) {
				dir := t.TempDir()
				options := DefaultOptions
				options.DirPath = dir
				options.IndexType = indexType
				options.PartitionNum = 3

				db, err := Open(options)
				require.NoError(t, err)

				const keyCount = 120
				keys := make([][]byte, keyCount)
				for i := 0; i < keyCount; i++ {
					keys[i] = []byte(fmt.Sprintf("partition-key-%04d", i))
					require.NoError(t, db.Put(keys[i], []byte("v1-"+string(keys[i]))))
				}
				db.flushMemtable(db.activeMem)

				for i := 0; i < keyCount; i += 3 {
					require.NoError(t, db.Put(keys[i], []byte("v2-"+string(keys[i]))))
				}
				db.flushMemtable(db.activeMem)
				require.NoError(t, db.Close())

				options.PartitionNum = newPartitionNum
				db, err = Open(options)
				require.NoError(t, err)

				assertLocalMigratedValues(t, db, keys)
				require.NoError(t, db.Compact())
				assertLocalMigratedValues(t, db, keys)

				for i := 0; i < keyCount; i += 2 {
					require.NoError(t, db.Put(keys[i], []byte("v3-"+string(keys[i]))))
				}
				db.flushMemtable(db.activeMem)
				if indexType == BTree {
					require.NoError(t, db.CompactWithDeprecatedtable())
					assertLocalFinalValues(t, db, keys)
				}
				require.NoError(t, db.Close())

				db, err = Open(options)
				require.NoError(t, err)
				defer func() { _ = db.Close() }()

				assertLocalFinalValues(t, db, keys)
			})
		}
	}
}

func assertLocalMigratedValues(t *testing.T, db *DB, keys [][]byte) {
	t.Helper()
	for i, key := range keys {
		expected := "v1-" + string(key)
		if i%3 == 0 {
			expected = "v2-" + string(key)
		}
		value, err := db.Get(key)
		require.NoError(t, err, "key %s", key)
		require.Equal(t, expected, string(value))
	}
}

func assertLocalFinalValues(t *testing.T, db *DB, keys [][]byte) {
	t.Helper()
	for i, key := range keys {
		expected := "v1-" + string(key)
		if i%3 == 0 {
			expected = "v2-" + string(key)
		}
		if i%2 == 0 {
			expected = "v3-" + string(key)
		}
		value, err := db.Get(key)
		require.NoError(t, err, "key %s", key)
		require.Equal(t, expected, string(value))
	}
}

func TestLocalPartitionMetadataCreatedAndValidated(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.PartitionNum = 4

	db, err := Open(options)
	require.NoError(t, err)
	require.NoError(t, db.Put([]byte("meta-key"), []byte("meta-value")))
	db.flushMemtable(db.activeMem)
	require.NoError(t, db.Close())

	metadata, exists, err := loadDBMetadata(dir)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 4, metadata.PartitionNum)

	options.PartitionNum = 4
	db, err = Open(options)
	require.NoError(t, err)
	value, err := db.Get([]byte("meta-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("meta-value"), value)
	require.NoError(t, db.Close())
}

func TestLocalMemtableSurvivesPartitionChange(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.PartitionNum = 3

	db, err := Open(options)
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("mem-key-%02d", i))
		require.NoError(t, db.Put(key, []byte("value")))
	}
	require.NoError(t, db.Close())

	options.PartitionNum = 5
	db, err = Open(options)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("mem-key-%02d", i))
		value, getErr := db.Get(key)
		require.NoError(t, getErr)
		require.Equal(t, []byte("value"), value)
	}
}

func TestLocalLegacyPartitionMetadataDetection(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.PartitionNum = 3

	db, err := Open(options)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("legacy-key-%02d", i))
		require.NoError(t, db.Put(key, []byte("legacy-value")))
	}
	db.flushMemtable(db.activeMem)
	require.NoError(t, db.Close())
	require.NoError(t, os.Remove(dbMetadataPath(dir)))

	detected, found, err := detectPartitionNumOnDisk(dir)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 3, detected)

	options.PartitionNum = 5
	db, err = Open(options)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("legacy-key-%02d", i))
		value, getErr := db.Get(key)
		require.NoError(t, getErr)
		require.Equal(t, []byte("legacy-value"), value)
	}

	metadata, exists, err := loadDBMetadata(dir)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 5, metadata.PartitionNum)
}

func TestLocalInterruptedPartitionMigrationRestoresBackup(t *testing.T) {
	dir := t.TempDir()
	options := DefaultOptions
	options.DirPath = dir
	options.PartitionNum = 3

	db, err := Open(options)
	require.NoError(t, err)
	keys := make([][]byte, 40)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("recovery-key-%02d", i))
		require.NoError(t, db.Put(keys[i], []byte("recovery-value")))
	}
	db.flushMemtable(db.activeMem)
	require.NoError(t, db.Close())

	backupDir, err := os.MkdirTemp(dir, partitionMigrationBackupPrefix+"*")
	require.NoError(t, err)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	moved := 0
	for _, entry := range entries {
		if !isPartitionDataFileName(entry.Name()) {
			continue
		}
		require.NoError(t, os.Rename(
			filepath.Join(dir, entry.Name()),
			filepath.Join(backupDir, entry.Name()),
		))
		moved++
	}
	require.Greater(t, moved, 0)

	partialIndex := filepath.Join(dir, fmt.Sprintf(indexFileExt, 9))
	require.NoError(t, os.WriteFile(partialIndex, []byte("partial-index"), 0o600))
	partialVLog := filepath.Join(dir, "000000001.VLOG.9")
	require.NoError(t, os.WriteFile(partialVLog, []byte("partial-vlog"), 0o600))

	options.PartitionNum = 6
	storedPartitionNum, err := resolvePartitionNum(options)
	require.NoError(t, err)
	require.Equal(t, 3, storedPartitionNum)

	_, err = os.Stat(backupDir)
	require.True(t, os.IsNotExist(err))

	db, err = Open(options)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	for _, key := range keys {
		value, getErr := db.Get(key)
		require.NoError(t, getErr)
		require.Equal(t, []byte("recovery-value"), value)
	}
}
