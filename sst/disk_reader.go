package sst

import (
	"os"
	"path/filepath"
)

type diskSSTReader struct {
	dir  string
	file *os.File
}

func NewDiskSSTReader(dir string) (*diskSSTReader, error) {
	path := filepath.Join(dir, filename)
	file, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}

	return &diskSSTReader{
		dir:  dir,
		file: file,
	}, nil
}

func (r *diskSSTReader) readDataBlock() dataBlock {
	panic("not implemented")
}

func (r *diskSSTReader) LoadFile() *File {
	return nil
}
