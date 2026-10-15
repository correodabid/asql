package ledgerdigest

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func appendDigests(t *testing.T, l *Log, signer *Ed25519Signer, lsns ...uint64) {
	t.Helper()
	for _, lsn := range lsns {
		d, err := Sign(signer, Digest{LSN: lsn, Chain: [32]byte{byte(lsn)}, NodeID: "n1", SignedAt: time.Unix(int64(lsn), 0)})
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Append(d); err != nil {
			t.Fatalf("append LSN %d: %v", lsn, err)
		}
	}
}

func TestLogRotatesAndFindsCoveringDigest(t *testing.T) {
	dir := t.TempDir()
	signer := testSigner(t)
	l, err := OpenLog(dir, 600) // a few digests per file
	if err != nil {
		t.Fatal(err)
	}
	lsns := []uint64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	appendDigests(t, l, signer, lsns...)
	files, _ := filepath.Glob(filepath.Join(dir, "digests-*.jsonl"))
	if len(files) < 3 {
		t.Fatalf("expected the log to rotate, got %d files", len(files))
	}

	for lsn, want := range map[uint64]uint64{1: 10, 10: 10, 11: 20, 55: 60, 100: 100} {
		d, ok, err := l.Covering(lsn)
		if err != nil || !ok || d.LSN != want {
			t.Fatalf("Covering(%d) = %d, %v, %v; want %d", lsn, d.LSN, ok, err, want)
		}
	}
	if _, ok, _ := l.Covering(101); ok {
		t.Fatal("no digest covers an LSN past the latest one")
	}
	if err := l.Append(Digest{LSN: 100}); err == nil {
		t.Fatal("expected a non-increasing LSN to be rejected")
	}
	_ = l.Close()

	l, err = OpenLog(dir, 600)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if d, ok := l.Latest(); !ok || d.LSN != 100 {
		t.Fatalf("latest after reopen = %d, %v", d.LSN, ok)
	}
	keys := Keyring{}
	keys.Add(signer.PublicKey())
	d, _, _ := l.Covering(42)
	if err := d.Verify(keys); err != nil {
		t.Fatalf("digest read back from the log does not verify: %v", err)
	}
	appendDigests(t, l, signer, 110)
}

func TestLogTruncatesATornTail(t *testing.T) {
	dir := t.TempDir()
	signer := testSigner(t)
	l, err := OpenLog(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	appendDigests(t, l, signer, 1, 2)
	_ = l.Close()

	files, _ := filepath.Glob(filepath.Join(dir, "digests-*.jsonl"))
	f, err := os.OpenFile(files[0], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"version":1,"lsn":3,"cha`)
	_ = f.Close()

	l, err = OpenLog(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if d, ok := l.Latest(); !ok || d.LSN != 2 {
		t.Fatalf("latest after a torn append = %d, %v; want 2", d.LSN, ok)
	}
	appendDigests(t, l, signer, 3)
	if d, ok, err := l.Covering(3); err != nil || !ok || d.LSN != 3 {
		t.Fatalf("Covering(3) after recovery = %d, %v, %v", d.LSN, ok, err)
	}
}
