package raft

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/correodabid/asql/internal/platform/clock"
)

type stubLog struct {
	lastIndex uint64
	terms     map[uint64]uint64
}

func (s *stubLog) LastIndex() uint64 { return s.lastIndex }
func (s *stubLog) LastTerm() uint64 {
	if s.lastIndex == 0 {
		return 0
	}
	return s.terms[s.lastIndex]
}
func (s *stubLog) Term(index uint64) (uint64, error) {
	term, ok := s.terms[index]
	if !ok {
		return 0, ErrIndexOutOfRange
	}
	return term, nil
}
func (s *stubLog) Entries(_ context.Context, _, _ uint64) ([]Entry, error) { return nil, nil }
func (s *stubLog) AppendLeader(_ context.Context, _ uint64, _, _ string, _ []byte) (Entry, error) {
	return Entry{}, errors.New("not implemented")
}
func (s *stubLog) AppendLeaderBatch(_ context.Context, _ uint64, _ []BatchRecord) ([]Entry, error) {
	return nil, errors.New("not implemented")
}
func (s *stubLog) AppendFollower(_ context.Context, _ []Entry) error {
	return errors.New("not implemented")
}
func (s *stubLog) TruncateAfter(_ context.Context, _ uint64) error {
	return errors.New("not implemented")
}

func newTestNodeForAdvance(log Log) *RaftNode {
	return &RaftNode{
		cfg: Config{
			NodeID:  "n1",
			Peers:   []Peer{{NodeID: "n2"}, {NodeID: "n3"}},
			Log:     log,
			Storage: NewMemStorage(),
			Clock:   clock.Realtime{},
			Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		matchIndex: map[string]uint64{"n2": 0, "n3": 0},
	}
}

func TestAdvanceCommitIndexLockedCommitsHighestCurrentTermQuorum(t *testing.T) {
	log := &stubLog{
		lastIndex: 10,
		terms: map[uint64]uint64{
			7:  3,
			10: 2,
		},
	}
	n := newTestNodeForAdvance(log)
	n.commitIndex = 5
	n.matchIndex["n2"] = 10
	n.matchIndex["n3"] = 7

	n.advanceCommitIndexLocked(3)

	if n.commitIndex != 7 {
		t.Fatalf("commitIndex=%d want 7", n.commitIndex)
	}
}

func TestAdvanceCommitIndexLockedSkipsOldTermQuorum(t *testing.T) {
	log := &stubLog{
		lastIndex: 10,
		terms: map[uint64]uint64{
			10: 2,
		},
	}
	n := newTestNodeForAdvance(log)
	n.commitIndex = 5
	n.matchIndex["n2"] = 10
	n.matchIndex["n3"] = 9

	n.advanceCommitIndexLocked(3)

	if n.commitIndex != 5 {
		t.Fatalf("commitIndex=%d want 5", n.commitIndex)
	}
}
