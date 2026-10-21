//go:build !unix && !windows

package wal

import "os"

// lockFile does nothing on platforms without file locks.
func lockFile(*os.File) error { return nil }
