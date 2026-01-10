package segmentmanager

import (
	"os"
	"strings"
	"testing"
)

func TestInitializeEmptyDirDiskSegmentManager(t *testing.T) {
	dirName := "./segments"

	defer func() {
		err := os.RemoveAll(dirName)
		if err != nil {
			t.Log("Failed to clean up segments dir")
		}
	}()

	sm, err := NewDiskSegmentManager(dirName, "")
	if err != nil {
		t.Fatal(err)
	}

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
	dirName := "./segments"

	defer func() {
		err := os.RemoveAll(dirName)
		if err != nil {
			t.Log("Failed to clean up segments dir")
		}
	}()

	initializeDir := func() {
		if err := os.Mkdir(dirName, 0o755); err != nil {
			t.Fatal(err)
		}

		file, err := os.Create(dirName + "/segment-0001.log")
		if err != nil {
			t.Fatal(err)
		}

		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}

	initializeDir()

	sm, err := NewDiskSegmentManager(dirName, "")
	if err != nil {
		t.Fatal(err)
	}

	if sm.activeID != 1 {
		t.Fatal("active id not set")
	}

	if !strings.Contains(sm.active.Name(), "segment-0001.log") {
		t.Fatal("expected segment-0001.log", "got", sm.active.Name())
	}
}
