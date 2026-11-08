package dropcopy

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// cursor remembers, per instrument, the last command whose trades were all published.
// It lives in a small file, replaced atomically (write, fsync, rename, fsync the
// directory), so after a crash it holds either the old or the new position.
type cursor struct {
	path string
}

func newCursor(dir, symbol string) cursor {
	return cursor{path: filepath.Join(dir, symbol+".cursor")}
}

// load returns 0 when there is no cursor yet (nothing published).
func (c cursor) load() (uint64, error) {
	raw, err := os.ReadFile(c.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	seq, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cursor %s: %w", c.path, err)
	}
	return seq, nil
}

func (c cursor) save(seq uint64) error {
	tmp := c.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(strconv.FormatUint(seq, 10) + "\n"); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, c.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(c.path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
