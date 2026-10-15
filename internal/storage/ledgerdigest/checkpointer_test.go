package ledgerdigest

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeSource struct {
	mu        sync.Mutex
	committed uint64
	missing   map[uint64]bool
}

func (s *fakeSource) set(lsn uint64) {
	s.mu.Lock()
	s.committed = lsn
	s.mu.Unlock()
}

func (s *fakeSource) source() Source {
	return Source{
		CommittedLSN: func() uint64 {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.committed
		},
		ChainAt: func(lsn uint64) ([32]byte, bool) {
			if s.missing[lsn] {
				return [32]byte{}, false
			}
			return [32]byte{byte(lsn), byte(lsn >> 8)}, true
		},
		RecordAt: func(_ context.Context, lsn uint64) (uint64, uint64, error) {
			return 7, lsn * 10, nil
		},
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newTestCheckpointer(t *testing.T, src *fakeSource, clock *fakeClock) (*Checkpointer, *Log, *Ed25519Signer) {
	t.Helper()
	signer := testSigner(t)
	l, err := OpenLog(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	c, err := NewCheckpointer(CheckpointerConfig{
		NodeID: "n1", Signer: signer, Log: l, Source: src.source(),
		Interval: time.Second, EveryLSNs: 100, Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, l, signer
}

func TestCheckpointSignsTheCommittedHeadOnlyWhenItAdvances(t *testing.T) {
	src := &fakeSource{}
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c, l, signer := newTestCheckpointer(t, src, clock)
	ctx := context.Background()

	if _, ok, _ := c.Checkpoint(ctx); ok {
		t.Fatal("nothing committed yet: no digest expected")
	}
	src.set(5)
	d, ok, err := c.Checkpoint(ctx)
	if err != nil || !ok {
		t.Fatalf("checkpoint: %v %v", ok, err)
	}
	if d.LSN != 5 || d.Chain != [32]byte{5} || d.Term != 7 || d.Timestamp != 50 || d.NodeID != "n1" || !d.SignedAt.Equal(clock.Now()) {
		t.Fatalf("unexpected digest %+v", d)
	}
	keys := Keyring{}
	keys.Add(signer.PublicKey())
	if err := d.Verify(keys); err != nil {
		t.Fatal(err)
	}
	if latest, _ := l.Latest(); latest.LSN != 5 {
		t.Fatalf("digest not logged: latest=%d", latest.LSN)
	}
	if _, ok, _ := c.Checkpoint(ctx); ok {
		t.Fatal("head did not advance: no digest expected")
	}
}

func TestCheckpointTriggers(t *testing.T) {
	src := &fakeSource{}
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c, _, _ := newTestCheckpointer(t, src, clock)
	src.set(1)
	if _, ok, _ := c.Checkpoint(context.Background()); !ok {
		t.Fatal("expected a first digest")
	}

	src.set(50)
	if c.due() {
		t.Fatal("neither the interval nor the LSN count elapsed")
	}
	clock.advance(time.Second)
	if !c.due() {
		t.Fatal("interval elapsed: a digest is due")
	}
	clock.advance(-time.Second)
	src.set(101)
	if !c.due() {
		t.Fatal("100 LSNs committed: a digest is due")
	}
}

func TestCheckpointDefersWhenTheChainValueIsUnavailable(t *testing.T) {
	src := &fakeSource{missing: map[uint64]bool{9: true}}
	clock := &fakeClock{now: time.Unix(1000, 0)}
	c, _, _ := newTestCheckpointer(t, src, clock)
	src.set(9)
	if _, ok, err := c.Checkpoint(context.Background()); ok || err != nil {
		t.Fatalf("expected a deferred digest, got ok=%v err=%v", ok, err)
	}
	src.set(10)
	if d, ok, _ := c.Checkpoint(context.Background()); !ok || d.LSN != 10 {
		t.Fatalf("expected a digest at LSN 10, got %v %d", ok, d.LSN)
	}
}

func TestCheckpointerRunSignsAndFinishesWithTheFinalHead(t *testing.T) {
	src := &fakeSource{}
	signer := testSigner(t)
	l, err := OpenLog(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c, err := NewCheckpointer(CheckpointerConfig{
		NodeID: "n1", Signer: signer, Log: l, Source: src.source(),
		Interval: 10 * time.Millisecond, EveryLSNs: 1 << 40,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()

	src.set(3)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if d, ok := l.Latest(); ok && d.LSN == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no digest signed by Run")
		}
		time.Sleep(5 * time.Millisecond)
	}
	src.set(4)
	cancel()
	<-done
	if d, _ := l.Latest(); d.LSN != 4 {
		t.Fatalf("final digest at LSN %d, want 4", d.LSN)
	}

	// A new checkpointer continues after the logged digests.
	c2, err := NewCheckpointer(CheckpointerConfig{Signer: signer, Log: l, Source: src.source()})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := c2.Checkpoint(context.Background()); ok {
		t.Fatal("head already signed: no digest expected")
	}
}
