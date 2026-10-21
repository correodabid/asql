//go:build unix

package wal

import (
	"os"

	"golang.org/x/sys/unix"
)

// lockFile takes an exclusive flock without waiting. flock locks belong to
// the open file, so a second open in the same process conflicts too.
func lockFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}
