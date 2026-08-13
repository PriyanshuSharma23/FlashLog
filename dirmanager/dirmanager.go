// package dirmanager
//
// # Usecase
// I want this to be the manager of the file layouts and directories
// present in the repository
//
// # Constructs
//  1. WAL file management: I want to have a WAL file to store operations for crash recovery
//     1.1. Functions of the WAL file: Provide functionalities
package dirmanager

import (
	"fmt"
	"os"
	"path"
)

const (
	WAL_PATH      = "WAL.log"
	SST_PREFIX    = "sst-"
	SST_EXTENSION = ".sst"
	SST_DIR       = "sst"
	MANIFEST_FILE = "MANIFEST"
)

type Manager interface {
	WALFile() (*os.File, error)
	TruncateWAL() error
	SSTFile() (*os.File, error)
	CreateSSTFile() (*os.File, error)
}

type DiskManager struct {
	basePath string
}

func initializeDir(dirPath string) error {
	pathStat, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dirPath, 0755)
			if err != nil {
				return fmt.Errorf("failed to create base dir: %w", err)
			}
		} else {
			return fmt.Errorf("failed to load stat for path %s: %w", dirPath, err)
		}
	} else if !pathStat.IsDir() {
		return fmt.Errorf("dirPath already exists as a file: %w", err)
	}

	return nil
}

func NewDiskManager(basePath string) (*DiskManager, error) {
	err := initializeDir(basePath)
	if err != nil {
		return nil, err
	}

	return &DiskManager{
		basePath,
	}, nil
}

func (dm *DiskManager) WALFile() (*os.File, error) {
	walFilePath := path.Join(dm.basePath, WAL_PATH)
	return os.OpenFile(walFilePath, os.O_WRONLY|os.O_CREATE, 0644)
}

func (dm *DiskManager) TruncateWAL() error {
	walFilePath := path.Join(dm.basePath, WAL_PATH)
	return os.Truncate(walFilePath, 0)
}

func (dm *DiskManager) SSTFile() (*os.File, error) {
	return nil, nil
}

func (dm *DiskManager) CreateSSTFile() (*os.File, error) {
	return nil, nil
}
