package lotusdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const (
	dbMetadataFileName = "DBMETA"
	dbMetadataVersion  = 1

	partitionMigrationStagePrefix  = ".partition-migration-"
	partitionMigrationBackupPrefix = ".partition-backup-"
)

var (
	indexPartitionFileNamePattern = regexp.MustCompile(`^INDEX\.(\d+)$`)
	vlogPartitionFileNamePattern  = regexp.MustCompile(`^\d+\.VLOG\.(\d+)(?:\.temp)?$`)
)

// dbMetadata is the database-owned metadata. Unlike Options, this file is the
// source of truth for the on-disk layout and must not be replaced by a value
// from the caller's configuration.
type dbMetadata struct {
	Version      int `json:"version"`
	PartitionNum int `json:"partition_num"`
}

func dbMetadataPath(dirPath string) string {
	return filepath.Join(dirPath, dbMetadataFileName)
}

func loadDBMetadata(dirPath string) (*dbMetadata, bool, error) {
	content, err := os.ReadFile(dbMetadataPath(dirPath))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	var metadata dbMetadata
	if err = json.Unmarshal(content, &metadata); err != nil {
		return nil, true, fmt.Errorf("%w: decode %s: %v", ErrInvalidDBMetadata, dbMetadataFileName, err)
	}
	if metadata.Version != dbMetadataVersion {
		return nil, true, fmt.Errorf("%w: unsupported version %d", ErrInvalidDBMetadata, metadata.Version)
	}
	if metadata.PartitionNum <= 0 {
		return nil, true, fmt.Errorf("%w: partition number %d", ErrInvalidDBMetadata, metadata.PartitionNum)
	}

	return &metadata, true, nil
}

func writeDBMetadata(dirPath string, partitionNum int) error {
	metadata := dbMetadata{Version: dbMetadataVersion, PartitionNum: partitionNum}
	content, err := json.Marshal(&metadata)
	if err != nil {
		return err
	}
	return writeFileAtomic(dbMetadataPath(dirPath), append(content, '\n'))
}

func writeFileAtomic(path string, content []byte) error {
	dirPath := filepath.Dir(path)
	tempFile, err := os.CreateTemp(dirPath, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()

	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()

	if _, err = tempFile.Write(content); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err = tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err = tempFile.Close(); err != nil {
		return err
	}
	if err = os.Rename(tempPath, path); err != nil {
		return err
	}
	cleanup = false
	return syncDirectory(dirPath)
}

func syncDirectory(dirPath string) error {
	dir, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// detectPartitionNumOnDisk reconstructs the partition count for a database
// created before partition count was stored in DBMETA.
func detectPartitionNumOnDisk(dirPath string) (int, bool, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return 0, false, err
	}

	maxPartition := -1
	found := false
	for _, entry := range entries {
		name := entry.Name()
		partition := -1

		if matches := indexPartitionFileNamePattern.FindStringSubmatch(name); matches != nil {
			partition, err = strconv.Atoi(matches[1])
			if err != nil {
				return 0, false, err
			}
		} else if matches := vlogPartitionFileNamePattern.FindStringSubmatch(name); matches != nil {
			partition, err = strconv.Atoi(matches[1])
			if err != nil {
				return 0, false, err
			}
		}

		if partition >= 0 {
			found = true
			if partition > maxPartition {
				maxPartition = partition
			}
		}
	}

	if !found {
		return 0, false, nil
	}
	return maxPartition + 1, true, nil
}

// resolvePartitionNum returns the database-owned partition count. A missing
// metadata file is created from either existing data files or the configured
// value for a new/empty database.
func resolvePartitionNum(options Options) (int, error) {
	if metadata, exists, err := loadDBMetadata(options.DirPath); err != nil {
		return 0, err
	} else if exists {
		if err = recoverInterruptedPartitionMigration(options.DirPath, metadata.PartitionNum); err != nil {
			return 0, err
		}
		if diskPartitionNum, diskFound, detectErr := detectPartitionNumOnDisk(options.DirPath); detectErr != nil {
			return 0, detectErr
		} else if diskFound && diskPartitionNum != metadata.PartitionNum {
			return 0, fmt.Errorf("%w: metadata records %d partitions but disk contains %d",
				ErrInvalidDBMetadata, metadata.PartitionNum, diskPartitionNum)
		}
		return metadata.PartitionNum, nil
	}

	partitionNum, found, err := detectPartitionNumOnDisk(options.DirPath)
	if err != nil {
		return 0, err
	}
	if !found {
		partitionNum = options.PartitionNum
	}
	if err = writeDBMetadata(options.DirPath, partitionNum); err != nil {
		return 0, err
	}
	return partitionNum, nil
}

type fileMove struct {
	from string
	to   string
}

func applyFileMoves(moves []fileMove) (int, error) {
	for i, move := range moves {
		if err := os.Rename(move.from, move.to); err != nil {
			return i, err
		}
	}
	return len(moves), nil
}

func rollbackFileMoves(moves []fileMove, completed int) {
	for i := completed - 1; i >= 0; i-- {
		_ = os.Rename(moves[i].to, moves[i].from)
	}
}

func isPartitionDataFileName(name string) bool {
	return indexPartitionFileNamePattern.MatchString(name) ||
		vlogPartitionFileNamePattern.MatchString(name)
}

// recoverInterruptedPartitionMigration makes opening deterministic after a
// process crash. An old metadata partition count means the new layout was not
// committed, so backup files are restored. If metadata already records the
// backed-up generation's successor, the backup is obsolete and is removed.
func recoverInterruptedPartitionMigration(dirPath string, committedPartitionNum int) error {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		switch {
		case strings.HasPrefix(name, partitionMigrationStagePrefix):
			if err = os.RemoveAll(filepath.Join(dirPath, name)); err != nil {
				return err
			}
		case strings.HasPrefix(name, partitionMigrationBackupPrefix):
			backupDir := filepath.Join(dirPath, name)
			backupPartitionNum, backupFound, detectErr := detectPartitionNumOnDisk(backupDir)
			if detectErr != nil {
				return detectErr
			}

			// A backup of the committed generation has a different partition
			// count from current metadata. A backup of the uncommitted old
			// generation has the same count as metadata.
			if backupFound && backupPartitionNum == committedPartitionNum {
				if err = restorePartitionBackup(dirPath, backupDir); err != nil {
					return err
				}
			}
			if err = os.RemoveAll(backupDir); err != nil {
				return err
			}
		}
	}

	return syncDirectory(dirPath)
}

func restorePartitionBackup(dirPath, backupDir string) error {
	rootEntries, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}
	for _, entry := range rootEntries {
		if isPartitionDataFileName(entry.Name()) {
			if err = os.RemoveAll(filepath.Join(dirPath, entry.Name())); err != nil {
				return err
			}
		}
	}

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !isPartitionDataFileName(entry.Name()) {
			continue
		}
		destination := filepath.Join(dirPath, entry.Name())
		if err = os.Rename(filepath.Join(backupDir, entry.Name()), destination); err != nil {
			return err
		}
	}
	return syncDirectory(dirPath)
}

func replacePartitionFiles(dirPath, stageDir string, oldPartitionNum int) (string, error) {
	backupDir, err := os.MkdirTemp(dirPath, partitionMigrationBackupPrefix+"*")
	if err != nil {
		return "", err
	}

	var oldMoves []fileMove
	for partition := 0; partition < oldPartitionNum; partition++ {
		indexName := fmt.Sprintf(indexFileExt, partition)
		indexPath := filepath.Join(dirPath, indexName)
		if _, statErr := os.Lstat(indexPath); statErr == nil {
			oldMoves = append(oldMoves, fileMove{
				from: indexPath,
				to:   filepath.Join(backupDir, indexName),
			})
		} else if !errors.Is(statErr, os.ErrNotExist) {
			_ = os.RemoveAll(backupDir)
			return "", statErr
		}

		vlogNames, listErr := listVLogPartitionFiles(dirPath, partition)
		if listErr != nil {
			_ = os.RemoveAll(backupDir)
			return "", listErr
		}
		for _, vlogName := range vlogNames {
			oldMoves = append(oldMoves, fileMove{
				from: filepath.Join(dirPath, vlogName),
				to:   filepath.Join(backupDir, vlogName),
			})
		}
	}

	completedOld, err := applyFileMoves(oldMoves)
	if err != nil {
		rollbackFileMoves(oldMoves, completedOld)
		_ = os.RemoveAll(backupDir)
		return "", err
	}

	stageEntries, err := os.ReadDir(stageDir)
	if err != nil {
		rollbackFileMoves(oldMoves, len(oldMoves))
		_ = os.RemoveAll(backupDir)
		return "", err
	}
	var newMoves []fileMove
	for _, entry := range stageEntries {
		if isPartitionDataFileName(entry.Name()) {
			newMoves = append(newMoves, fileMove{
				from: filepath.Join(stageDir, entry.Name()),
				to:   filepath.Join(dirPath, entry.Name()),
			})
		}
	}

	completedNew, err := applyFileMoves(newMoves)
	if err != nil {
		rollbackFileMoves(newMoves, completedNew)
		rollbackFileMoves(oldMoves, len(oldMoves))
		_ = os.RemoveAll(backupDir)
		return "", err
	}

	if err = syncDirectory(dirPath); err != nil {
		return "", err
	}
	return backupDir, nil
}

func listVLogPartitionFiles(dirPath string, partition int) ([]string, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	pattern := regexp.MustCompile(fmt.Sprintf(`^\d+\.VLOG\.%d(?:\.temp)?$`, partition))
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && pattern.MatchString(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	return names, nil
}
