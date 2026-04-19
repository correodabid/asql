package raft

import (
	"context"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
)

func TestWALLogMatchEntriesPrefixSkipsWholeMatchingRanges(t *testing.T) {
	store := &heartbeatTestWALStore{}
	appendTerms := func(term uint64, count int) {
		for i := 0; i < count; i++ {
			if _, err := store.Append(context.Background(), ports.WALRecord{Term: term, Type: "MUTATION", TxID: "x"}); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
	}
	appendTerms(1, 5)
	appendTerms(2, 4)
	appendTerms(3, 3)

	log, err := NewWALLog(context.Background(), store)
	if err != nil {
		t.Fatalf("NewWALLog: %v", err)
	}
	matcher, ok := log.(overlapMatcher)
	if !ok {
		t.Fatal("walLog should implement overlapMatcher")
	}

	entries := []Entry{
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
		{Index: 6, Term: 2},
		{Index: 7, Term: 2},
		{Index: 8, Term: 2},
		{Index: 9, Term: 2},
		{Index: 10, Term: 3},
		{Index: 11, Term: 3},
		{Index: 12, Term: 3},
		{Index: 13, Term: 3},
	}

	appendFrom, conflictIndex, err := matcher.MatchEntriesPrefix(entries)
	if err != nil {
		t.Fatalf("MatchEntriesPrefix: %v", err)
	}
	if appendFrom != 10 {
		t.Fatalf("appendFrom=%d want 10", appendFrom)
	}
	if conflictIndex != 0 {
		t.Fatalf("conflictIndex=%d want 0", conflictIndex)
	}
}

func TestWALLogMatchEntriesPrefixDetectsConflict(t *testing.T) {
	store := &heartbeatTestWALStore{}
	for i := 0; i < 6; i++ {
		term := uint64(1)
		if i >= 3 {
			term = 2
		}
		if _, err := store.Append(context.Background(), ports.WALRecord{Term: term, Type: "MUTATION", TxID: "x"}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	log, err := NewWALLog(context.Background(), store)
	if err != nil {
		t.Fatalf("NewWALLog: %v", err)
	}
	matcher := log.(overlapMatcher)
	entries := []Entry{{Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 9}, {Index: 5, Term: 9}}

	appendFrom, conflictIndex, err := matcher.MatchEntriesPrefix(entries)
	if err != nil {
		t.Fatalf("MatchEntriesPrefix: %v", err)
	}
	if appendFrom != 2 {
		t.Fatalf("appendFrom=%d want 2", appendFrom)
	}
	if conflictIndex != 4 {
		t.Fatalf("conflictIndex=%d want 4", conflictIndex)
	}
}
