//go:build windows

package wal

import (
	"os"

	"golang.org/x/sys/windows"
)

// lockFile takes an exclusive lock on the first byte without waiting.
func lockFile(file *os.File) error {
	var overlapped windows.Overlapped
	return windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)
}
