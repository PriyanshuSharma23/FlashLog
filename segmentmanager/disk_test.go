package segmentmanager

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const dirName = "./segments"

func setupDiskTests(t *testing.T, options ...DiskSegmentManagerOption) (sm *diskSegmentManager, cleanup func(...bool)) {
	sm, err := NewDiskSegmentManager(dirName, options...)
	if err != nil {
		t.Fatal("failed to create disk segment manager", err)
	}

	return sm, func(skip ...bool) {
		if len(skip) > 0 && skip[0] {
			return
		}
		err := os.RemoveAll(dirName)
		if err != nil {
			t.Log("Failed to clean up segments dir")
		}
	}
}

func TestWithOptionInitializers(t *testing.T) {
	sm, cleanup := setupDiskTests(t, WithLogFileExt(".dog"), WithMaxSegmentSize(10))
	defer cleanup()

	if sm.logFileExt != ".dog" {
		t.Fatal("expected .dog", "got", sm.logFileExt)
	}

	if sm.maxSegmentSize != 10 {
		t.Fatal("expected 10", "got", sm.maxSegmentSize)
	}
}

func TestInitializeEmptyDirDiskSegmentManager(t *testing.T) {
	sm, cleanup := setupDiskTests(t)
	defer cleanup()

	if sm.activeID != 1 {
		t.Fatal("active id not set")
	}

	entries, err := os.ReadDir(dirName)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 1 {
		t.Log("Entries", entries)
		t.Fatal("expected one entry", "got", len(entries))
	}

	if entries[0].Name() != "segment-0001.log" {
		t.Fatal("expected segment-0001.log", "got", entries[0].Name())
	}
}

func TestExistingDirDiskStateManager(t *testing.T) {
	sm, cleanup := setupDiskTests(t)
	defer cleanup()

	initializeDir := func() {
		file, err := os.Create(dirName + "/segment-0001.log")
		if err != nil {
			t.Fatal(err)
		}

		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}

	initializeDir()

	if sm.activeID != 1 {
		t.Fatal("active id not set")
	}

	if !strings.Contains(sm.active.Name(), "segment-0001.log") {
		t.Fatal("expected segment-0001.log", "got", sm.active.Name())
	}
}

func TestDiskGetActiveFileWithoutRotation(t *testing.T) {
	sm, cleanup := setupDiskTests(t, WithMaxSegmentSize(100))
	defer cleanup()

	file, err := sm.Active(50)
	if err != nil {
		t.Fatal(err)
	}

	_, err = fmt.Fprintf(file, "whats up")
	if err != nil {
		t.Fatal(err)
	}

	filename := filepath.Join(dirName, "segment-0001.log")

	segementFileContent, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	if string(segementFileContent) != "whats up" {
		t.Fatal("expected whats up", "got", string(segementFileContent))
	}
}

func TestDisGetActiveFileWithRotation(t *testing.T) {
	tests := []struct {
		name           string
		content        string
		iterations     int
		maxSegmentSize int
		expectedFiles  int
	}{
		{"2 writes per file", "hello", 50, 10, 25},
		{"Content size greater than half", "hello", 50, 8, 50},
		{"content size exual to max segment size", "hello", 50, 5, 50},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sm, cleanup := setupDiskTests(t, WithMaxSegmentSize(int64(test.maxSegmentSize)))
			defer cleanup()

			for i := 0; i < test.iterations; i++ {
				out, err := sm.Active(len(test.content))
				if err != nil {
					t.Fatal(err)
				}

				_, err = fmt.Fprint(out, test.content)
				if err != nil {
					t.Fatal(err)
				}

				err = sm.Sync()
				if err != nil {
					t.Fatal(err)
				}
			}

			entries, err := os.ReadDir(dirName)
			if err != nil {
				t.Fatal(err)
			}

			if len(entries) != test.expectedFiles {
				t.Fatal("expected", test.expectedFiles, "got", len(entries))
			}
		})
	}
}
