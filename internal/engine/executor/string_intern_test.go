package executor

import "testing"

func alphaToken(i int) string {
	b := [8]byte{'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a'}
	for pos := len(b) - 1; pos >= 0; pos-- {
		b[pos] = byte('a' + (i % 26))
		i /= 26
	}
	return string(b[:])
}

func resetStringInternerForTest() {
	globalStringIntern.mu.Lock()
	defer globalStringIntern.mu.Unlock()
	globalStringIntern.table = make(map[string]string, 1024)
}

func TestShouldInternStringRejectsUUIDLikeValues(t *testing.T) {
	resetStringInternerForTest()
	if shouldInternString("550e8400-e29b-41d4-a716-446655440000") {
		t.Fatal("expected UUID-like string to bypass interning")
	}
	if got := internString("550e8400-e29b-41d4-a716-446655440000"); got != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("unexpected interned UUID value: %q", got)
	}
	globalStringIntern.mu.RLock()
	defer globalStringIntern.mu.RUnlock()
	if len(globalStringIntern.table) != 0 {
		t.Fatalf("expected UUID-like strings to not be retained, got %d entries", len(globalStringIntern.table))
	}
}

func TestInternStringCapsGlobalTableGrowth(t *testing.T) {
	resetStringInternerForTest()
	for i := 0; i < stringInternMaxEntries+1024; i++ {
		internString(alphaToken(i))
	}
	globalStringIntern.mu.RLock()
	defer globalStringIntern.mu.RUnlock()
	if len(globalStringIntern.table) > stringInternMaxEntries {
		t.Fatalf("expected interner cap %d, got %d", stringInternMaxEntries, len(globalStringIntern.table))
	}
	if len(globalStringIntern.table) == 0 {
		t.Fatal("expected some low-cardinality strings to be interned")
	}
}

func TestShouldInternStringRejectsHighCardinalityIdentifiers(t *testing.T) {
	resetStringInternerForTest()
	if shouldInternString("Batch_2026-03-10_123456") {
		t.Fatal("expected identifier-like string to bypass interning")
	}
	if !shouldInternString("approved") {
		t.Fatal("expected categorical value to remain internable")
	}
}