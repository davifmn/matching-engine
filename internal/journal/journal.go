// Package journal is an append-only, checksummed log: the engine's write-ahead log.
//
// A file is a sequence of records:
//
//	| payload length: uint32 LE | CRC-32C of the payload: uint32 LE | payload |
//
// Records are written in groups (one write and one fsync per group, docs/adr/0004).
// A crash in the middle of a write can leave only the last record incomplete; Replay
// detects that and cuts it off. Damage anywhere else is reported as ErrCorrupt.
package journal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// SyncMode says when committed records are forced to stable storage.
type SyncMode int

const (
	// SyncEveryCommit fsyncs once per Commit, i.e. once per group of records.
	SyncEveryCommit SyncMode = iota
	// SyncNever leaves flushing to the operating system: a process crash loses nothing,
	// a machine crash can lose the last records. For tests, benchmarks and replicas.
	SyncNever
)

const (
	headerSize = 8
	// MaxRecord is the largest payload accepted.
	MaxRecord = 1 << 20
)

var (
	// ErrCorrupt means a damaged record is followed by more data, so it is not a torn
	// tail and cutting it off would lose acknowledged commands.
	ErrCorrupt = errors.New("journal: corrupt record")

	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

// Writer appends records to a journal file.
type Writer struct {
	f    *os.File
	buf  []byte
	mode SyncMode
	size int64
}

// Open opens the journal at path for appending, creating it (and syncing its directory)
// if needed. Call Replay first, so a torn tail from a previous crash is gone.
func Open(path string, mode SyncMode) (*Writer, error) {
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if errors.Is(statErr, os.ErrNotExist) && mode != SyncNever {
		if err := syncDir(filepath.Dir(path)); err != nil {
			f.Close()
			return nil, err
		}
	}
	return &Writer{f: f, mode: mode, size: st.Size()}, nil
}

// Add buffers one record. Nothing reaches the file until Commit.
func (w *Writer) Add(payload []byte) error {
	if len(payload) > MaxRecord {
		return fmt.Errorf("journal: record of %d bytes exceeds %d", len(payload), MaxRecord)
	}
	w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(len(payload)))
	w.buf = binary.LittleEndian.AppendUint32(w.buf, crc32.Checksum(payload, castagnoli))
	w.buf = append(w.buf, payload...)
	return nil
}

// Commit writes the buffered records with a single write and, in SyncEveryCommit mode,
// fsyncs. When it returns nil the records are durable.
func (w *Writer) Commit() error {
	if len(w.buf) == 0 {
		return nil
	}
	n, err := w.f.Write(w.buf)
	w.size += int64(n)
	w.buf = w.buf[:0]
	if err != nil {
		return fmt.Errorf("journal: write: %w", err)
	}
	if w.mode == SyncEveryCommit {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("journal: fsync: %w", err)
		}
	}
	return nil
}

// Size returns the file size, committed records included.
func (w *Writer) Size() int64 { return w.size }

// Close commits what is buffered and closes the file.
func (w *Writer) Close() error {
	err := w.Commit()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// Stats describes a replay.
type Stats struct {
	Records        int
	Bytes          int64 // size of the valid part of the file
	TruncatedBytes int64 // torn tail removed
}

// Replay calls fn with every record of the journal at path, in order. A missing file is
// an empty journal. A torn last record (cut short, or failing its checksum right at the
// end of the file) is removed by truncating the file. The payload passed to fn is only
// valid during the call.
func Replay(path string, fn func(payload []byte) error) (Stats, error) {
	var st Stats
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return st, err
	}
	size := info.Size()

	r := bufio.NewReaderSize(f, 1<<20)
	var header [headerSize]byte
	payload := make([]byte, 0, 256)
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if err == io.EOF {
				return st, nil // clean end
			}
			if err == io.ErrUnexpectedEOF {
				return truncate(path, st, size) // header cut short
			}
			return st, err
		}
		n := int64(binary.LittleEndian.Uint32(header[0:4]))
		sum := binary.LittleEndian.Uint32(header[4:8])
		end := st.Bytes + headerSize + n
		if end > size {
			return truncate(path, st, size) // payload cut short
		}
		if n > MaxRecord {
			return st, fmt.Errorf("%w at offset %d: length %d", ErrCorrupt, st.Bytes, n)
		}
		payload = payload[:n]
		if _, err := io.ReadFull(r, payload); err != nil {
			return st, err
		}
		if crc32.Checksum(payload, castagnoli) != sum {
			if end == size {
				return truncate(path, st, size) // last record garbled by the crash
			}
			return st, fmt.Errorf("%w at offset %d: checksum mismatch", ErrCorrupt, st.Bytes)
		}
		if err := fn(payload); err != nil {
			return st, fmt.Errorf("journal: record %d at offset %d: %w", st.Records, st.Bytes, err)
		}
		st.Records++
		st.Bytes = end
	}
}

func truncate(path string, st Stats, size int64) (Stats, error) {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return st, err
	}
	defer f.Close()
	if err := f.Truncate(st.Bytes); err != nil {
		return st, err
	}
	st.TruncatedBytes = size - st.Bytes
	return st, f.Sync()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
