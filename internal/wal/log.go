package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ErrClosed is returned by Append once the log has been closed.
var ErrClosed = errors.New("wal: log is closed")

// Log is an append-only file with group commit.
//
// Durability model: an Append is *not* durable when it returns. It is durable
// when Wait reports success for the batch id Append handed back. Splitting the
// two is the whole point — a caller holds the queue mutex while appending, and
// releases it before waiting, so every writer that shows up during one fsync
// piles into the next batch and rides out on a single fsync with everyone
// else. Throughput then scales with concurrency instead of being pinned to the
// disk's serial fsync rate, and no caller is ever told "committed" before the
// bytes are actually on the platter.
//
// Failure model: a failed write or fsync is fatal and sticky. We do not know
// what did or did not reach the disk, so every subsequent Append and Wait
// returns the same error and the queue above us stops accepting work. Failing
// closed is the only honest option; the alternative is accepting messages we
// cannot persist.
type Log struct {
	path string

	mu   sync.Mutex
	cond *sync.Cond
	f    *os.File

	buf      []byte // records accumulated for the next batch
	pending  int    // records in buf, for stats
	nextSeq  uint64 // batch id that buffered records will belong to
	flushed  uint64 // highest batch id fully written and fsynced
	fatal    error  // sticky write/fsync failure
	fatalSeq uint64 // batch id that failed
	closed   bool
	size     int64

	signal chan struct{}
	done   chan struct{}
	wg     sync.WaitGroup

	stats Stats
}

// Stats is a snapshot of write-path behaviour. The demo dashboard graphs these
// to make group commit visible: as concurrency rises, records-per-fsync rises
// while fsyncs-per-second stays roughly flat.
type Stats struct {
	Records      uint64        // records appended and synced
	Batches      uint64        // fsyncs performed
	Bytes        int64         // bytes written
	LastBatch    int           // records in the most recent batch
	MaxBatch     int           // largest batch seen
	LastSync     time.Duration // duration of the most recent write+fsync
	TotalSync    time.Duration // cumulative time spent in write+fsync
	SizeBytes    int64         // current log size on disk
	PendingBytes int           // bytes buffered but not yet synced
}

// Open opens (or creates) a log for appending. size is the verified length of
// the file as established by Replay; the file is truncated to it first, which
// is how a torn tail record is discarded.
func Open(path string, size int64) (*Log, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() != size {
		// Discard a torn tail. Done before any append so we never write good
		// records after a partial one.
		if err := f.Truncate(size); err != nil {
			f.Close()
			return nil, fmt.Errorf("wal: truncating %s to %d: %w", path, size, err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	}
	// fsync the directory so the file's existence survives a power cut, not
	// just its contents.
	if err := syncDir(filepath.Dir(path)); err != nil {
		f.Close()
		return nil, err
	}

	l := &Log{
		path:    path,
		f:       f,
		nextSeq: 1,
		size:    size,
		signal:  make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	l.cond = sync.NewCond(&l.mu)
	l.wg.Add(1)
	go l.flusher()
	return l, nil
}

// Append buffers a record and returns the batch id it will be committed in.
// It does not block on I/O. The record is durable only after Wait(batch)
// returns nil.
func (l *Log) Append(t Type, payload []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fatal != nil {
		return 0, l.fatal
	}
	if l.closed {
		return 0, ErrClosed
	}
	l.buf = Encode(l.buf, t, payload)
	l.pending++
	batch := l.nextSeq
	l.kick()
	return batch, nil
}

// kick wakes the flusher. Must be called with l.mu held. The channel is
// buffered so a wakeup is never lost: if the flusher is mid-fsync the signal
// sits in the buffer and it loops again immediately after.
func (l *Log) kick() {
	select {
	case l.signal <- struct{}{}:
	default:
	}
}

// Wait blocks until the given batch is on disk. It reports the sticky fatal
// error if this batch (or an earlier one, which would mean ours never got
// written) failed.
func (l *Log) Wait(batch uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.flushed < batch && l.fatal == nil {
		l.cond.Wait()
	}
	if l.fatal != nil && l.fatalSeq <= batch {
		return l.fatal
	}
	return nil
}

// Sync blocks until everything appended so far is durable. Used by compaction
// and shutdown, not on the hot path.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		if l.fatal != nil {
			return l.fatal
		}
		if len(l.buf) == 0 && l.flushed+1 == l.nextSeq {
			return nil
		}
		if len(l.buf) > 0 {
			l.kick()
		}
		l.cond.Wait()
	}
}

// flusher owns all writes to the file. Exactly one goroutine touches the fd,
// so batches reach the disk in the order their ids were handed out.
func (l *Log) flusher() {
	defer l.wg.Done()
	for {
		select {
		case <-l.signal:
			l.flushOnce()
		case <-l.done:
			l.flushOnce() // final drain
			return
		}
	}
}

func (l *Log) flushOnce() {
	l.mu.Lock()
	if len(l.buf) == 0 || l.fatal != nil {
		l.mu.Unlock()
		return
	}
	data := l.buf
	records := l.pending
	batch := l.nextSeq
	l.buf = nil
	l.pending = 0
	l.nextSeq++
	l.mu.Unlock()

	start := time.Now()
	err := l.writeSync(data)
	elapsed := time.Since(start)

	l.mu.Lock()
	if err != nil {
		l.fatal = err
		l.fatalSeq = batch
	} else {
		l.size += int64(len(data))
		l.stats.Records += uint64(records)
		l.stats.Batches++
		l.stats.Bytes += int64(len(data))
		l.stats.LastBatch = records
		if records > l.stats.MaxBatch {
			l.stats.MaxBatch = records
		}
		l.stats.LastSync = elapsed
		l.stats.TotalSync += elapsed
	}
	l.flushed = batch
	l.cond.Broadcast()
	l.mu.Unlock()
}

func (l *Log) writeSync(data []byte) error {
	if _, err := l.f.Write(data); err != nil {
		return fmt.Errorf("wal: write %s: %w", l.path, err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync %s: %w", l.path, err)
	}
	return nil
}

// Stats returns a copy of the current write-path statistics.
func (l *Log) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.stats
	s.SizeBytes = l.size
	s.PendingBytes = len(l.buf)
	return s
}

// Size reports the number of bytes durably written.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

// Reopen swaps in the file now living at the log's path, discarding the old
// descriptor. Compaction calls this after atomically renaming a snapshot over
// the live log. The caller must guarantee no concurrent Append (in this
// codebase every Append happens under the owning queue's mutex, and compaction
// holds that mutex).
func (l *Log) Reopen(size int64) error {
	if err := l.Sync(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if err := l.f.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		// The log is now unusable; make that loud rather than limping on.
		l.fatal = fmt.Errorf("wal: reopen %s: %w", l.path, err)
		l.fatalSeq = 0
		l.cond.Broadcast()
		return l.fatal
	}
	l.f = f
	l.size = size
	return nil
}

// Close drains buffered records, fsyncs, and closes the file.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()

	close(l.done)
	l.wg.Wait()

	l.mu.Lock()
	defer l.mu.Unlock()
	l.cond.Broadcast()
	err := l.f.Close()
	if l.fatal != nil {
		return l.fatal
	}
	return err
}

// syncDir fsyncs a directory so that creates and renames within it are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
