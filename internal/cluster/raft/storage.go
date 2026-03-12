package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

	dir := filepath.Dir(s.path)
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

	if err := os.Rename(tmp.Name(), s.path); err != nil {
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
type memStorage struct {
	term     uint64
	votedFor string
}

// NewMemStorage returns a non-durable in-memory Storage for tests.
func NewMemStorage() Storage {
	return &memStorage{}
}

func (m *memStorage) SaveState(_ context.Context, term uint64, votedFor string) error {
	m.term = term
	m.votedFor = votedFor
	return nil
}

func (m *memStorage) LoadState(_ context.Context) (uint64, string, error) {
	return m.term, m.votedFor, nil
}
