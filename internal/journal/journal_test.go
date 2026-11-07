package journal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func writeRecords(t *testing.T, path string, records ...string) {
	t.Helper()
	w, err := Open(path, SyncEveryCommit)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if err := w.Add([]byte(r)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func readAll(t *testing.T, path string) ([]string, Stats, error) {
	t.Helper()
	var got []string
	st, err := Replay(path, func(p []byte) error {
		got = append(got, string(p))
		return nil
	})
	return got, st, err
}

func TestRoundTripAcrossSessions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	writeRecords(t, path, "one", "two")
	writeRecords(t, path, "three", "") // reopening appends; empty payloads are fine
	got, st, err := readAll(t, path)
	if err != nil || !slices.Equal(got, []string{"one", "two", "three", ""}) {
		t.Fatalf("got %q, %v", got, err)
	}
	if st.Records != 4 || st.TruncatedBytes != 0 {
		t.Errorf("stats = %+v", st)
	}
}

func TestMissingFileIsEmpty(t *testing.T) {
	got, st, err := readAll(t, filepath.Join(t.TempDir(), "none.wal"))
	if err != nil || len(got) != 0 || st.Records != 0 {
		t.Fatalf("got %q, %+v, %v", got, st, err)
	}
}

// A crash can stop a write anywhere inside the last record. Every cut point must recover
// the records before it, remove the partial one, and leave a journal that accepts appends.
func TestTornTailIsTruncatedAtEveryCutPoint(t *testing.T) {
	dir := t.TempDir()
	full := filepath.Join(dir, "full.wal")
	writeRecords(t, full, "first", "second", "third record")
	raw, err := os.ReadFile(full)
	if err != nil {
		t.Fatal(err)
	}
	lastStart := len(raw) - headerSize - len("third record")
	for cut := lastStart + 1; cut < len(raw); cut++ {
		path := filepath.Join(dir, fmt.Sprintf("cut%d.wal", cut))
		if err := os.WriteFile(path, raw[:cut], 0o600); err != nil {
			t.Fatal(err)
		}
		got, st, err := readAll(t, path)
		if err != nil || !slices.Equal(got, []string{"first", "second"}) {
			t.Fatalf("cut at %d: got %q, %v", cut, got, err)
		}
		if st.TruncatedBytes != int64(cut-lastStart) {
			t.Fatalf("cut at %d: truncated %d bytes", cut, st.TruncatedBytes)
		}
		writeRecords(t, path, "after")
		if got, _, _ := readAll(t, path); !slices.Equal(got, []string{"first", "second", "after"}) {
			t.Fatalf("cut at %d: after append got %q", cut, got)
		}
	}
}

func TestGarbledLastRecordIsTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	writeRecords(t, path, "keep", "lost")
	raw, _ := os.ReadFile(path)
	raw[len(raw)-1] ^= 0xff
	os.WriteFile(path, raw, 0o600)
	got, st, err := readAll(t, path)
	if err != nil || !slices.Equal(got, []string{"keep"}) || st.TruncatedBytes == 0 {
		t.Fatalf("got %q, %+v, %v", got, st, err)
	}
}

func TestCorruptionInTheMiddleIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	writeRecords(t, path, "first", "second", "third")
	raw, _ := os.ReadFile(path)
	raw[headerSize+1] ^= 0xff // inside "first"
	os.WriteFile(path, raw, 0o600)
	_, _, err := readAll(t, path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if after, _ := os.ReadFile(path); len(after) != len(raw) {
		t.Error("a corrupt journal must not be modified")
	}
}

func TestReplayStopsOnCallbackError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	writeRecords(t, path, "a", "b")
	boom := errors.New("boom")
	_, err := Replay(path, func([]byte) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestRecordTooLarge(t *testing.T) {
	w, err := Open(filepath.Join(t.TempDir(), "x.wal"), SyncNever)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Add(make([]byte, MaxRecord+1)); err == nil {
		t.Fatal("oversized record accepted")
	}
}
