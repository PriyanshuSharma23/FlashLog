// Package segmentmanager provides an interface for writing logs into rotating segments.
// The user of this module only sees an Active() method to write into; all segment
// rotation functionality is handled internally by this package.
package segmentmanager

import (
	"io"
	"regexp"
)

var segmentFileNamePattern = regexp.MustCompile(`^segment-(\d+)\.log$`)

type SegmentManager interface {
	Active(n int) (io.Writer, error)
	Sync()
	RotateSegment() error
	Close() error
}

type segmentEntry struct {
	id   int
	name string
}

type SegmentEntries []segmentEntry

func (a SegmentEntries) Len() int           { return len(a) }
func (a SegmentEntries) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SegmentEntries) Less(i, j int) bool { return a[i].id < a[j].id }
