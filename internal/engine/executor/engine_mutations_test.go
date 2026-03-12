package executor

import "testing"

func TestOverlayIndexForInsertFlattensHashChainsAtThreshold(t *testing.T) {
	base := &indexState{
		name:     "idx_pk",
		column:   "id",
		kind:     "hash",
		buckets:  map[string][]int{"base": {1}},
		baseSize: 1,
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
	if next.cachedDepth != 1 {
		t.Fatalf("expected flattened parent depth 1, got %d", next.cachedDepth)
	}
	if next.parent.parent != nil {
		t.Fatal("expected hash compaction to flatten the chain to a single base level")
	}
	if got := len(next.parent.lookupBucket("base")); got != 1 {
		t.Fatalf("expected flattened parent to retain base bucket, got %d rows", got)
	}
}