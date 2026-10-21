package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ErrDataDirInUse reports a WAL that another store holds open: another
// process, or this one, already writes to the data directory. Two writers
// on one WAL diverge without an error (ASQL-11).
var ErrDataDirInUse = errors.New("wal: the data directory is in use")

// dirLock is an exclusive lock on .<base name>.lock, next to the segments,
// held while a store is open. The name does not start with the base name, so
// nothing that lists segments by it sees the lock. The operating system releases it when the process exits, so a crash
// leaves nothing to clean up by hand.
type dirLock struct {
	file *os.File
}

func acquireDirLock(basePath string) (*dirLock, error) {
	path := filepath.Join(filepath.Dir(basePath), "."+filepath.Base(basePath)+".lock")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open lock file: %w", err)
	}
	if err := lockFile(file); err != nil {
		owner := ""
		if data, readErr := os.ReadFile(path); readErr == nil {
			if pid, convErr := strconv.Atoi(strings.TrimSpace(string(data))); convErr == nil {
				owner = fmt.Sprintf(" by process %d", pid)
			}
		}
		_ = file.Close()
		return nil, fmt.Errorf("%w: %s is locked%s; stop the other server or embedded engine using it first", ErrDataDirInUse, path, owner)
	}
	// Who holds it, for the error the next opener gets.
	if err := file.Truncate(0); err == nil {
		_, _ = file.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0)
	}
	return &dirLock{file: file}, nil
}

func (l *dirLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close() // closing releases the lock
	l.file = nil
	return err
}
