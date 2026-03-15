package executor

import "testing"

func TestOverlayIndexForInsertFlattensHashChainsAtThreshold(t *testing.T) {
	base := &indexState{
		name:     "idx_pk",
		column:   "id",
		kind:     "hash",
		buckets:  map[string][]int{"base": {1}},
		baseSize: 0,
	}

	depth := adaptiveHashOverlayMaxDepth(base.baseSize)
	current := base
	for i := 0; i < depth; i++ {
		current = &indexState{
			name:        base.name,
			column:      base.column,
			kind:        base.kind,
			buckets:     map[string][]int{string(rune('a' + (i % 26))): {i + 2}},
			parent:      current,
			cachedDepth: current.cachedDepth + 1,
			baseSize:    base.baseSize,
		}
	}

	next := overlayIndexForInsert(current, false)
	if next.parent == nil {
		t.Fatal("expected overlay parent")
	}
	if next.cachedDepth != 2 {
		t.Fatalf("expected compacted overlay depth 2, got %d", next.cachedDepth)
	}
	if next.parent.parent == nil {
		t.Fatal("expected hash compaction to retain the base under a compacted tier")
	}
	if next.parent.parent.parent != nil {
		t.Fatal("expected hash compaction to collapse the chain to compacted tier plus base")
	}
	if got := len(next.parent.lookupBucket("base")); got != 1 {
		t.Fatalf("expected flattened parent to retain base bucket, got %d rows", got)
	}
	if !next.parent.isCompacted {
		t.Fatal("expected parent tier to be marked compacted")
	}
}
