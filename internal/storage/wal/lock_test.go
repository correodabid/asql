package wal

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestASecondStoreOnTheSameWALFails (ASQL-11): two writers on one WAL
// diverge without an error, so the second open fails, in the same process
// as in another, and the WAL opens again once the first store is closed.
func TestASecondStoreOnTheSameWALFails(t *testing.T) {
	base := filepath.Join(t.TempDir(), "asql.wal")
	first, err := NewSegmentedLogStore(base, AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewSegmentedLogStore(base, AlwaysSync{}); !errors.Is(err, ErrDataDirInUse) || !strings.Contains(err.Error(), fmt.Sprintf("process %d", os.Getpid())) {
		t.Fatalf("second open: %v, want ErrDataDirInUse naming this process", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := NewSegmentedLogStore(base, AlwaysSync{})
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	_ = again.Close()
}

// TestDataDirLockHolder is the other process of the next test: it opens the
// WAL, reports it, and waits to be killed.
func TestDataDirLockHolder(t *testing.T) {
	base := os.Getenv("ASQL_LOCK_TEST_WAL")
	if base == "" {
		t.Skip("run by TestAnotherProcessHoldingTheWALIsRefused")
	}
	if _, err := NewSegmentedLogStore(base, AlwaysSync{}); err != nil {
		fmt.Println("open failed:", err)
		os.Exit(1)
	}
	fmt.Println("holding")
	select {}
}

// TestAnotherProcessHoldingTheWALIsRefused: a process that holds the WAL
// keeps others out, and its lock goes when it dies, even when killed without
// closing anything.
func TestAnotherProcessHoldingTheWALIsRefused(t *testing.T) {
	base := filepath.Join(t.TempDir(), "asql.wal")
	holder := exec.Command(os.Args[0], "-test.run=^TestDataDirLockHolder$")
	holder.Env = append(os.Environ(), "ASQL_LOCK_TEST_WAL="+base)
	out, err := holder.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Process.Kill(); _ = holder.Wait() })
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "holding" {
		t.Fatalf("holder: %q, %v", line, err)
	}

	if _, err := NewSegmentedLogStore(base, AlwaysSync{}); !errors.Is(err, ErrDataDirInUse) || !strings.Contains(err.Error(), fmt.Sprintf("process %d", holder.Process.Pid)) {
		t.Fatalf("open while another process holds the WAL: %v, want ErrDataDirInUse naming process %d", err, holder.Process.Pid)
	}

	if err := holder.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = holder.Wait()
	store, err := NewSegmentedLogStore(base, AlwaysSync{})
	if err != nil {
		t.Fatalf("open after the holder was killed: %v", err)
	}
	_ = store.Close()
}
