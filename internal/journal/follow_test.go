package journal

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// follow reads what ReadFrom returns from offset, as strings.
func follow(t *testing.T, path string, offset int64, limit int) ([]string, int64) {
	t.Helper()
	var got []string
	next, err := ReadFrom(path, offset, limit, func(p []byte) error {
		got = append(got, string(p))
		return nil
	})
	if err != nil {
		t.Fatalf("ReadFrom(%d): %v", offset, err)
	}
	return got, next
}

func TestReadFromFollowsAGrowingJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	writeRecords(t, path, "one", "two")

	got, next := follow(t, path, 0, 0)
	if !slices.Equal(got, []string{"one", "two"}) {
		t.Fatalf("first read = %q", got)
	}

	writeRecords(t, path, "three")
	got, next = follow(t, path, next, 0)
	if !slices.Equal(got, []string{"three"}) {
		t.Fatalf("second read = %q, want only the new record", got)
	}

	got, again := follow(t, path, next, 0)
	if len(got) != 0 || again != next {
		t.Fatalf("read at the end = %q, %d; want nothing and the same position %d", got, again, next)
	}
}

func TestReadFromHonoursTheLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	writeRecords(t, path, "a", "b", "c")

	got, next := follow(t, path, 0, 2)
	if !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("limited read = %q", got)
	}
	got, _ = follow(t, path, next, 2)
	if !slices.Equal(got, []string{"c"}) {
		t.Fatalf("next read = %q", got)
	}
}

func TestReadFromWaitsForARecordBeingWrittenAndNeverTruncates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	writeRecords(t, path, "one")
	full := filepath.Join(t.TempDir(), "full.wal")
	writeRecords(t, full, "one", "second record")
	complete, err := os.ReadFile(full)
	if err != nil {
		t.Fatal(err)
	}
	_, start := follow(t, path, 0, 0)

	// The writer has written the second record byte by byte: at every cut, the follower
	// sees nothing new and leaves the file alone; once complete, it reads the record.
	for cut := int(start) + 1; cut < len(complete); cut++ {
		if err := os.WriteFile(path, complete[:cut], 0o600); err != nil {
			t.Fatal(err)
		}
		got, next := follow(t, path, start, 0)
		if len(got) != 0 || next != start {
			t.Fatalf("cut %d: read %q and moved to %d before the record was complete", cut, got, next)
		}
		if info, _ := os.Stat(path); info.Size() != int64(cut) {
			t.Fatalf("cut %d: the follower changed the file size to %d", cut, info.Size())
		}
	}
	if err := os.WriteFile(path, complete, 0o600); err != nil {
		t.Fatal(err)
	}
	got, next := follow(t, path, start, 0)
	if !slices.Equal(got, []string{"second record"}) || next != int64(len(complete)) {
		t.Fatalf("after completion: %q at %d", got, next)
	}
}

func TestReadFromReportsCorruptionAndAShrunkFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	writeRecords(t, path, "one", "two", "three")
	raw, _ := os.ReadFile(path)
	raw[headerSize] ^= 0xff // damage the first payload, which has records after it
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrom(path, 0, 0, func([]byte) error { return nil }); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}

	if _, err := ReadFrom(path, int64(len(raw))+10, 0, func([]byte) error { return nil }); !errors.Is(err, ErrShrunk) {
		t.Fatalf("err = %v, want ErrShrunk", err)
	}
}

func TestReadFromAMissingJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "none.wal")
	if next, err := ReadFrom(path, 0, 0, func([]byte) error { return nil }); err != nil || next != 0 {
		t.Fatalf("ReadFrom(missing, 0) = %d, %v", next, err)
	}
	if _, err := ReadFrom(path, 100, 0, func([]byte) error { return nil }); err == nil {
		t.Fatal("a follower that had read 100 bytes must notice the journal disappeared")
	}
}

func TestReadFromStopsAtACallbackError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	writeRecords(t, path, "one", "two")
	boom := errors.New("boom")
	calls := 0
	next, err := ReadFrom(path, 0, 0, func([]byte) error {
		calls++
		if calls == 2 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) || calls != 2 {
		t.Fatalf("err = %v after %d calls", err, calls)
	}
	if next != headerSize+3 {
		t.Fatalf("next = %d, want the offset of the record that failed (%d)", next, headerSize+3)
	}
}
