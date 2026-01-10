package segmentmanager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	DefaultMaxDiskSegmentSize = 16 * 1024 * 1024 // 16MB
	DefaultDiskLogFileExt     = ".log"
)

type diskSegmentManager struct {
	active         *os.File
	activeID       int
	dir            string
	logFileExt     string
	maxSegmentSize int
}

func isDirectoryValid(path string) error {
	fileInfo, err := os.Stat(path)

	if err == nil {
		if fileInfo.IsDir() {
			return nil
		}
		return fmt.Errorf("path exists but is not a directory: %s", path)
	}

	return err
}

func initializeEmptySegmentDir(dir, logFileExt string) (*diskSegmentManager, error) {
	sm := &diskSegmentManager{
		activeID:   0,
		dir:        dir,
		logFileExt: logFileExt,
		active:     nil,
	}

	if err := sm.RotateSegment(); err != nil {
		return nil, fmt.Errorf("failed to craete first segment: %w", err)
	}

	return sm, nil
}

func NewDiskSegmentManager(dir string, logFileExt string) (*diskSegmentManager, error) {
	if logFileExt == "" {
		logFileExt = DefaultDiskLogFileExt
	}

	if err := isDirectoryValid(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(dir, 0o755); err != nil {
				return nil, err
			}

			return initializeEmptySegmentDir(dir, logFileExt)
		}

		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	segmentEntries := SegmentEntries{}

	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}

		ext := filepath.Ext(entry.Name())

		if ext != logFileExt {
			continue
		}

		matches := segmentFileNamePattern.FindStringSubmatch(entry.Name())
		if len(matches) != 2 {
			continue
		}

		id, err := strconv.Atoi(matches[1])
		if err != nil {
			continue
		}

		segmentEntries = append(segmentEntries, segmentEntry{
			id:   id,
			name: entry.Name(),
		})
	}

	if len(segmentEntries) == 0 {
		return initializeEmptySegmentDir(dir, logFileExt)
	}

	sort.Sort(segmentEntries)

	if ok := validateSegmentEntries(segmentEntries); !ok {
		return nil, errors.New("invalid segment entries")
	}

	latestID := segmentEntries[len(segmentEntries)-1].id

	sm := &diskSegmentManager{
		logFileExt: logFileExt,
		dir:        dir,
		activeID:   latestID,
		active:     nil,
	}

	activeFile, err := os.OpenFile(sm.idToPath(latestID), os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open active file: %w", err)
	}

	sm.active = activeFile

	return sm, nil
}

func validateSegmentEntries(entries SegmentEntries) bool {
	if len(entries) == 0 {
		return true
	}

	if len(entries) == entries[len(entries)-1].id {
		return true
	}

	return false
}

func (s *diskSegmentManager) idToPath(id int) string {
	filename := fmt.Sprintf("segment-%04d%s", id, s.logFileExt)
	return filepath.Join(s.dir, filename)
}

func (s *diskSegmentManager) RotateSegment() error {
	s.activeID++
	newSegmentFilePath := s.idToPath(s.activeID)

	file, err := os.Create(newSegmentFilePath)
	if err != nil {
		return err
	}

	s.active = file

	return nil
}

func (s *diskSegmentManager) Active(n int) (io.Writer, error) {
	return nil, nil
}

func (s *diskSegmentManager) Sync() {}

func (s *diskSegmentManager) Close() error {
	return nil
}
