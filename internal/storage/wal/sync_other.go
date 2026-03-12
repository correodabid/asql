//go:build !darwin

package wal

import "os"

// PlatformSync performs a platform-optimized fsync.
func PlatformSync(f *os.File) error {
	return f.Sync()
}
