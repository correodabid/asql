package ledgerdigest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	logFilePrefix = "digests-"
	logFileSuffix = ".jsonl"

	// DefaultMaxLogFileSize is the size at which the digest log rotates to a
	// new file. Digests are evidence, so rotated files are kept.
	DefaultMaxLogFileSize = 16 << 20
)

// Log is the append-only sidecar log of signed digests, one JSON object per
// line, in files named by the LSN of their first digest. Digest LSNs are
// strictly increasing across the whole log.
//
// The log lives next to the WAL, so it is no more trustworthy than the data
// directory: its value comes from copies published elsewhere.
type Log struct {
	mu      sync.Mutex
	dir     string
	maxSize int64
	files   []logFile
	cur     *os.File
	curSize int64
	latest  *Digest
	closed  bool
}

type logFile struct {
	firstLSN uint64
	path     string
}

// OpenLog opens or creates the digest log in dir. A torn last line, left by
// a crash during an append, is truncated away.
func OpenLog(dir string, maxFileSize int64) (*Log, error) {
	if maxFileSize <= 0 {
		maxFileSize = DefaultMaxLogFileSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("digest log: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("digest log: %w", err)
	}
	l := &Log{dir: dir, maxSize: maxFileSize}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, logFilePrefix) || !strings.HasSuffix(name, logFileSuffix) {
			continue
		}
		first, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, logFilePrefix), logFileSuffix), 10, 64)
		if err != nil {
			continue
		}
		l.files = append(l.files, logFile{firstLSN: first, path: filepath.Join(dir, name)})
	}
	sort.Slice(l.files, func(i, j int) bool { return l.files[i].firstLSN < l.files[j].firstLSN })
	if len(l.files) == 0 {
		return l, nil
	}

	last := l.files[len(l.files)-1]
	f, err := os.OpenFile(last.path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("digest log: %w", err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("digest log: %w", err)
	}
	good, latest, err := scanTail(data)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("digest log %s: %w", last.path, err)
	}
	if good < int64(len(data)) {
		if err := f.Truncate(good); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("digest log: truncate torn tail: %w", err)
		}
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("digest log: %w", err)
	}
	l.cur, l.curSize, l.latest = f, good, latest
	if l.latest == nil && len(l.files) > 1 {
		// The newest file was empty: the latest digest is in the one before.
		if err := l.scanFile(l.files[len(l.files)-2].path, func(d Digest) bool { l.latest = &d; return true }); err != nil {
			return nil, err
		}
	}
	return l, nil
}

// scanTail returns the length of data that holds complete, valid lines and
// the last digest in it. Only the final line may be torn.
func scanTail(data []byte) (int64, *Digest, error) {
	var latest *Digest
	var off int64
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			break // torn: no newline
		}
		var d Digest
		if err := json.Unmarshal(data[:i], &d); err != nil {
			if i+1 == len(data) {
				break // torn: last line is garbage
			}
			return 0, nil, fmt.Errorf("corrupt digest at offset %d: %w", off, err)
		}
		latest = &d
		off += int64(i + 1)
		data = data[i+1:]
	}
	return off, latest, nil
}

// Append adds d to the log and syncs it. d.LSN must exceed the latest
// digest's LSN.
func (l *Log) Append(d Digest) error {
	line, err := json.Marshal(d)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("digest log is closed")
	}
	if l.latest != nil && d.LSN <= l.latest.LSN {
		return fmt.Errorf("digest log: LSN %d does not follow %d", d.LSN, l.latest.LSN)
	}
	if l.cur == nil || (l.curSize > 0 && l.curSize+int64(len(line)) > l.maxSize) {
		if err := l.rotate(d.LSN); err != nil {
			return err
		}
	}
	if _, err := l.cur.Write(line); err != nil {
		return fmt.Errorf("digest log: %w", err)
	}
	if err := l.cur.Sync(); err != nil {
		return fmt.Errorf("digest log: %w", err)
	}
	l.curSize += int64(len(line))
	l.latest = &d
	return nil
}

func (l *Log) rotate(firstLSN uint64) error {
	path := filepath.Join(l.dir, fmt.Sprintf("%s%020d%s", logFilePrefix, firstLSN, logFileSuffix))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("digest log: %w", err)
	}
	if dir, err := os.Open(l.dir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	if l.cur != nil {
		_ = l.cur.Close()
	}
	l.cur, l.curSize = f, 0
	l.files = append(l.files, logFile{firstLSN: firstLSN, path: path})
	return nil
}

// Latest returns the newest digest.
func (l *Log) Latest() (Digest, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.latest == nil {
		return Digest{}, false
	}
	return *l.latest, true
}

// Covering returns the oldest digest with LSN >= lsn: the first signed state
// that includes the record at lsn.
func (l *Log) Covering(lsn uint64) (Digest, bool, error) {
	l.mu.Lock()
	files := append([]logFile(nil), l.files...)
	l.mu.Unlock()

	start := sort.Search(len(files), func(i int) bool { return files[i].firstLSN > lsn }) - 1
	if start < 0 {
		start = 0
	}
	for _, f := range files[start:] {
		var found *Digest
		err := l.scanFile(f.path, func(d Digest) bool {
			if d.LSN >= lsn {
				found = &d
				return false
			}
			return true
		})
		if err != nil {
			return Digest{}, false, err
		}
		if found != nil {
			return *found, true, nil
		}
	}
	return Digest{}, false, nil
}

// scanFile calls fn for each digest in path until fn returns false. A torn
// last line is skipped.
func (l *Log) scanFile(path string, fn func(Digest) bool) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("digest log: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 4096), 1<<20)
	for sc.Scan() {
		var d Digest
		if err := json.Unmarshal(sc.Bytes(), &d); err != nil {
			continue
		}
		if !fn(d) {
			return nil
		}
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("digest log %s: %w", path, err)
	}
	return nil
}

// Close closes the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	if l.cur == nil {
		return nil
	}
	err := l.cur.Close()
	l.cur = nil
	return err
}
