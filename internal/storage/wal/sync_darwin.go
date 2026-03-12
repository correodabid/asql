//go:build darwin

package wal

import (
	"os"
	"syscall"
)

// F_BARRIERFSYNC is macOS fcntl command 85. It provides write-ordering
// guarantees (writes issued before the barrier are persisted before writes
// issued after) without forcing a full device cache flush like F_FULLFSYNC.
// This is the same mechanism SQLite uses on macOS for its default journal mode.
//
// Trade-off: survives process crashes but not sudden power loss. Acceptable
// for development on macOS; production deployments use Linux where fdatasync
// is both fast and fully durable.
const fBarrierFsync = 85

// PlatformSync performs a platform-optimized fsync. On macOS it uses
// F_BARRIERFSYNC for write-ordering without a full device cache flush.
// On other platforms it falls back to the standard f.Sync() (fdatasync).
func PlatformSync(f *os.File) error {
	conn, err := f.SyscallConn()
	if err != nil {
		return f.Sync() // fallback to F_FULLFSYNC
	}
	var syncErr error
	err = conn.Control(func(fd uintptr) {
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, fBarrierFsync, 0)
		if errno != 0 {
			syncErr = errno
		}
	})
	if err != nil {
		return err
	}
	return syncErr
}
