package segmentmanager

import (
	"io"
	"os"
)

const (
	MaxSegmentSize = 16 * 1024 * 1024 // 16MB
)

// SegmentManager Interface to expose the active file and rotate the segment
type SegmentManager interface {
	Active(n int) (io.Writer, error)
	Sync()
	RotateSegment() error
	Close() error
}

type DiskSegmentManager struct {
	active   *os.File
	activeID int
	dir      string
}

func NewDiskSegmentManager(dir string) *DiskSegmentManager {
	return &DiskSegmentManager{}
}

func (s *DiskSegmentManager) Active(n int) (io.Writer, error) {
	return nil, nil
}

func (s *DiskSegmentManager) RotateSegment() error {
	return nil
}

func (s *DiskSegmentManager) Sync() {}

func (s *DiskSegmentManager) Close() error {
	return nil
}
