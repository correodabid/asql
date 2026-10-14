package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Storage persists Raft's hard state — currentTerm and votedFor.
//
// Every time a node updates its term or casts a vote these values MUST be
// written to stable storage before any RPC is sent or any response returned §5.2, §5.4.
//
// The implementation uses a tiny JSON file (raft-state.json) written atomically
// via a write-to-temp + rename pattern so no partial writes are ever observable.
type Storage interface {
	// SaveState writes currentTerm and votedFor to durable storage.
	SaveState(ctx context.Context, term uint64, votedFor string) error
	// LoadState reads the last persisted currentTerm and votedFor.
	// Returns term=0, votedFor="" when no state has been persisted yet (fresh node).
	LoadState(ctx context.Context) (term uint64, votedFor string, err error)
}

// CommitIndexStore is implemented by storages that also persist a lower
// bound of the commit index. Raft does not require it for safety (the commit
// index is volatile in the paper) but ASQL needs it on restart: the engine
// only replays WAL entries known to be committed, because the WAL tail may
// hold entries a former leader wrote but never replicated. Saving a value
// below the true commit index is always safe; the rest is re-learned from
// the leader. The value is written off the commit path.
type CommitIndexStore interface {
	SaveCommitIndex(ctx context.Context, index uint64) error
	// LoadCommitIndex returns ok=false when no commit index was ever saved
	// (a fresh node, or one written before the commit index was persisted).
	LoadCommitIndex(ctx context.Context) (index uint64, ok bool, err error)
}

type fileStorage struct {
	path string // absolute path to the state file, e.g. "/data/raft-state.json"
}

type raftState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// NewFileStorage returns a Storage implementation backed by a JSON file at path.
// The containing directory must already exist.
func NewFileStorage(path string) Storage {
	return &fileStorage{path: path}
}

// SaveState atomically writes term and votedFor to disk.
func (s *fileStorage) SaveState(_ context.Context, term uint64, votedFor string) error {
	data, err := json.Marshal(raftState{Term: term, VotedFor: votedFor})
	if err != nil {
		return fmt.Errorf("raft storage: marshal: %w", err)
	}
	return writeFileAtomic(s.path, data)
}

type raftCommit struct {
	CommitIndex uint64 `json:"commit_index"`
}

// commitPath is the commit-index file next to the state file.
func (s *fileStorage) commitPath() string {
	return filepath.Join(filepath.Dir(s.path), "raft-commit.json")
}

// SaveCommitIndex atomically writes the commit index to disk.
func (s *fileStorage) SaveCommitIndex(_ context.Context, index uint64) error {
	data, err := json.Marshal(raftCommit{CommitIndex: index})
	if err != nil {
		return fmt.Errorf("raft storage: marshal commit index: %w", err)
	}
	return writeFileAtomic(s.commitPath(), data)
}

// LoadCommitIndex reads the commit index from disk.
func (s *fileStorage) LoadCommitIndex(_ context.Context) (uint64, bool, error) {
	data, err := os.ReadFile(s.commitPath())
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("raft storage: read commit index: %w", err)
	}
	var c raftCommit
	if err := json.Unmarshal(data, &c); err != nil {
		return 0, false, fmt.Errorf("raft storage: unmarshal commit index: %w", err)
	}
	return c.CommitIndex, true, nil
}

// writeFileAtomic writes data to path via a synced temp file and a rename, so
// no partial write is ever observable.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".raft-state-*.tmp")
	if err != nil {
		return fmt.Errorf("raft storage: create temp: %w", err)
	}
	defer func() {
		// Best-effort cleanup of temp file on error path.
		_ = os.Remove(tmp.Name())
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("raft storage: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("raft storage: sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("raft storage: close temp: %w", err)
	}

	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("raft storage: rename: %w", err)
	}
	return nil
}

// LoadState reads currentTerm and votedFor from disk.
func (s *fileStorage) LoadState(_ context.Context) (uint64, string, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return 0, "", nil // fresh node
	}
	if err != nil {
		return 0, "", fmt.Errorf("raft storage: read: %w", err)
	}

	var st raftState
	if err := json.Unmarshal(data, &st); err != nil {
		return 0, "", fmt.Errorf("raft storage: unmarshal: %w", err)
	}
	return st.Term, st.VotedFor, nil
}

// memStorage is an in-memory Storage used in unit tests.
// persistLocked intentionally releases n.mu before calling SaveState to avoid
// holding the Raft lock during I/O, so SaveState may be called concurrently
// from multiple goroutines and must protect its own state.
type memStorage struct {
	mu          sync.Mutex
	term        uint64
	votedFor    string
	commitIndex uint64
	commitSaved bool
}

// NewMemStorage returns a non-durable in-memory Storage for tests.
func NewMemStorage() Storage {
	return &memStorage{}
}

func (m *memStorage) SaveState(_ context.Context, term uint64, votedFor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term = term
	m.votedFor = votedFor
	return nil
}

func (m *memStorage) LoadState(_ context.Context) (uint64, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.term, m.votedFor, nil
}

func (m *memStorage) SaveCommitIndex(_ context.Context, index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitIndex = index
	m.commitSaved = true
	return nil
}

func (m *memStorage) LoadCommitIndex(_ context.Context) (uint64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitIndex, m.commitSaved, nil
}
