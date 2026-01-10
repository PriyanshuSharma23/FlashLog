package segmentmanager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
)

const (
	defaultMaxDiskSegmentSize = 16 * 1024 * 1024
	diskLogFileExt            = ".log"
)

var segmentFileNamePattern = regexp.MustCompile(`^segment-(\d+)\.log$`)

type diskSegmentManager struct {
	mu             sync.Mutex
	active         *os.File
	activeID       int
	dir            string
	logFileExt     string
	maxSegmentSize int64
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

func initializeEmptySegmentDir(baseSM *diskSegmentManager) (*diskSegmentManager, error) {
	if err := baseSM.RotateSegment(); err != nil {
		return nil, fmt.Errorf("failed to craete first segment: %w", err)
	}

	return baseSM, nil
}

type DiskSegmentManagerOption func(sm *diskSegmentManager)

func WithMaxSegmentSize(maxSegmentSize int64) DiskSegmentManagerOption {
	return func(sm *diskSegmentManager) {
		sm.maxSegmentSize = maxSegmentSize
	}
}

func NewDiskSegmentManager(dir string, options ...DiskSegmentManagerOption) (*diskSegmentManager, error) {
	sm := &diskSegmentManager{
		activeID:       0,
		dir:            dir,
		logFileExt:     diskLogFileExt,
		active:         nil,
		maxSegmentSize: defaultMaxDiskSegmentSize,
	}

	for _, option := range options {
		option(sm)
	}

	if err := isDirectoryValid(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}

			return initializeEmptySegmentDir(sm)
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

		if ext != sm.logFileExt {
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
		return initializeEmptySegmentDir(sm)
	}

	sort.Sort(segmentEntries)

	if ok := validateSegmentEntries(segmentEntries); !ok {
		return nil, errors.New("invalid segment entries")
	}

	sm.activeID = segmentEntries[len(segmentEntries)-1].id

	activeFile, err := os.OpenFile(sm.idToPath(sm.activeID), os.O_APPEND|os.O_RDWR, 0o644)
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

	for i, e := range entries {
		if e.id != i+1 {
			return false
		}
	}

	return true
}

func (s *diskSegmentManager) idToPath(id int) string {
	filename := fmt.Sprintf("segment-%04d%s", id, s.logFileExt)
	return filepath.Join(s.dir, filename)
}

func (s *diskSegmentManager) RotateSegment() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil {
		err := s.active.Close()
		if err != nil {
			return fmt.Errorf("failed to close previous segment: %w", err)
		}
	}

	s.activeID++
	newSegmentFilePath := s.idToPath(s.activeID)

	file, err := os.Create(newSegmentFilePath)
	if err != nil {
		return err
	}

	s.active = file

	return nil
}

func (s *diskSegmentManager) WriteActive(n int, fn func(w io.Writer)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if int64(n) > s.maxSegmentSize {
		return fmt.Errorf("n too large: %d", n)
	}

	if s.active == nil {
		return fmt.Errorf("active file not initialized")
	}

	stat, err := s.active.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat active file: %w", err)
	}

	if stat.Size()+int64(n) > s.maxSegmentSize {
		if err := s.RotateSegment(); err != nil {
			return fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	fn(s.active)

	err = s.active.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync active file: %w", err)
	}

	return nil
}

func (s *diskSegmentManager) Sync() error {
	if s.active == nil {
		panic("active file not initialized")
	}

	err := s.active.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync active file: %w", err)
	}

	return nil
}

func (s *diskSegmentManager) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.active.Close()
	if err != nil {
		return fmt.Errorf("failed to close active file: %w", err)
	}
	return nil
}
