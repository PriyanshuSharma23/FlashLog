package segments

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const dirName = "./segments"

func setupDiskTests(t *testing.T, options ...DiskSegmentsWriterOption) (sm *diskSegmentsWriter, cleanup func(...bool)) {
	sm, err := NewDiskSegmentsWriter(dirName, options...)
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
	sm, cleanup := setupDiskTests(t, WithMaxSegmentSize(10))
	defer cleanup()

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

	err := sm.Write(50, func(w io.Writer) {
		_, err := fmt.Fprintf(w, "whats up")
		if err != nil {
			t.Fatal(err)
		}
	})
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

func TestDiskGetActiveFileWithRotation(t *testing.T) {
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
				err := sm.Write(len(test.content), func(w io.Writer) {
					_, err := fmt.Fprint(w, test.content)
					if err != nil {
						t.Fatal(err)
					}
				})
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

func TestConcurrentDiskSegmentWrites(t *testing.T) {
	sm, cleanup := setupDiskTests(t, WithMaxSegmentSize(100))
	defer cleanup()

	content := "whats up"

	var wg sync.WaitGroup

	wg.Go(func() {
		_ = sm.Write(len(content), func(w io.Writer) {
			_, _ = fmt.Fprint(w, content)
		})
	})

	wg.Go(func() {
		_ = sm.Write(len(content), func(w io.Writer) {
			_, _ = fmt.Fprint(w, content)
		})
	})

	wg.Wait()

	fileName := filepath.Join("segments", "segment-0001.log")
	file, err := os.Open(fileName)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = file.Close()
	}()

	fileContent, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	if string(fileContent) != "whats upwhats up" {
		t.Fatal("expected whats upwhats up", "got", string(content))
	}
}
