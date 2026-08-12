package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// CorruptionError means the log contains a record we cannot trust. Startup
// refuses on this error rather than skipping the record: a queue that silently
// drops data it cannot read is worse than a queue that will not start, because
// only one of those two failures is visible to a human.
type CorruptionError struct {
	Path   string
	Offset int64  // byte offset of the record that failed
	Reason string // what specifically was wrong
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("wal: corrupt record in %s at byte offset %d: %s "+
		"(refusing to start; inspect with `artie-queue verify -dir <dir>`, "+
		"or discard everything from this offset onward with "+
		"`artie-queue repair -dir <dir> -queue <name> -truncate-at %d`)",
		e.Path, e.Offset, e.Reason, e.Offset)
}

// TornTail describes a record that the file physically ends in the middle of.
// This is the normal artifact of a crash during a write, and it is safe to
// discard because no reader was ever told the record committed: an Append is
// only reported durable after its whole batch is written and fsynced.
type TornTail struct {
	Offset int64 // where the incomplete record starts
	Bytes  int64 // how many bytes are being discarded
}

// ReplayResult reports where a log ends and whether a torn tail was found.
type ReplayResult struct {
	// ValidBytes is the length of the prefix of the file that is fully
	// verified. The log is truncated to this before any new record is written.
	ValidBytes int64
	Records    int
	Torn       *TornTail // nil when the file ended on a clean record boundary
}

// Replay reads every record in order and hands it to fn.
//
// Corruption policy, in one place:
//
//   - The file ends mid-record (a partial header, or a length that runs past
//     EOF): torn tail. Report it so the caller can warn, truncate, continue.
//     This is unambiguous — the record cannot be complete, because the file
//     is not long enough to hold it.
//   - A complete record whose checksum does not match, or whose type is not
//     recognised, anywhere in the file including the very last record: real
//     corruption. Return CorruptionError naming the byte offset. We do not
//     guess that a bad checksum at the tail was "probably" a torn write; that
//     guess is exactly how a queue silently loses a record.
//
// fn is called only for records that have passed their checksum.
func Replay(path string, fn func(Record) error) (ReplayResult, error) {
	var res ReplayResult

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return res, nil // brand new queue
		}
		return res, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return res, err
	}
	size := st.Size()

	br := bufio.NewReaderSize(f, 1<<16)
	var off int64
	var hdr [HeaderSize]byte

	for {
		remaining := size - off
		if remaining == 0 {
			res.ValidBytes = off
			return res, nil // clean end
		}
		if remaining < HeaderSize {
			res.ValidBytes = off
			res.Torn = &TornTail{Offset: off, Bytes: remaining}
			return res, nil
		}
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return res, fmt.Errorf("wal: reading header at %d in %s: %w", off, path, err)
		}
		length := int64(binary.LittleEndian.Uint32(hdr[0:4]))
		want := binary.LittleEndian.Uint32(hdr[4:8])

		if length > remaining-HeaderSize {
			// The record claims more bytes than the file has left. Whether the
			// length field itself was half-written or the payload was, the
			// conclusion is the same: this record never finished being written.
			res.ValidBytes = off
			res.Torn = &TornTail{Offset: off, Bytes: remaining}
			return res, nil
		}
		if length > MaxRecordSize {
			return res, &CorruptionError{Path: path, Offset: off,
				Reason: fmt.Sprintf("record length %d exceeds maximum %d", length, MaxRecordSize)}
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(br, payload); err != nil {
			return res, fmt.Errorf("wal: reading payload at %d in %s: %w", off, path, err)
		}
		if got := checksum(hdr[:], payload); got != want {
			return res, &CorruptionError{Path: path, Offset: off,
				Reason: fmt.Sprintf("checksum mismatch (header says %#08x, computed %#08x over %d payload bytes)", want, got, length)}
		}
		t := Type(hdr[8])
		if !t.valid() {
			return res, &CorruptionError{Path: path, Offset: off,
				Reason: fmt.Sprintf("unknown record type %d", hdr[8])}
		}

		if err := fn(Record{Type: t, Payload: payload, Offset: off}); err != nil {
			return res, fmt.Errorf("wal: applying %s record at offset %d: %w", t, off, err)
		}

		res.Records++
		off += HeaderSize + length
	}
}

// Writer writes a fresh log file in one pass and installs it atomically. It is
// used for compaction snapshots and for hand-building logs in tests.
type Writer struct {
	tmp  string
	f    *os.File
	bw   *bufio.Writer
	size int64
}

// NewWriter creates (or truncates) tmp for writing.
func NewWriter(tmp string) (*Writer, error) {
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{tmp: tmp, f: f, bw: bufio.NewWriterSize(f, 1<<16)}, nil
}

// Append writes one record.
func (w *Writer) Append(t Type, payload []byte) error {
	rec := Encode(nil, t, payload)
	if _, err := w.bw.Write(rec); err != nil {
		return err
	}
	w.size += int64(len(rec))
	return nil
}

// Size reports the bytes written so far.
func (w *Writer) Size() int64 { return w.size }

// CommitAs flushes, fsyncs, and atomically renames the temp file over final.
// The sequence — write, fsync the data, rename, fsync the directory — is what
// makes the swap crash-safe: after it, a reader sees either the whole old log
// or the whole new one, never a half-written snapshot.
func (w *Writer) CommitAs(final string) error {
	if err := w.bw.Flush(); err != nil {
		w.f.Close()
		return err
	}
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(w.tmp, final); err != nil {
		return err
	}
	return syncDir(dirOf(final))
}

// Abort removes the temp file after a failed snapshot.
func (w *Writer) Abort() {
	w.f.Close()
	os.Remove(w.tmp)
}

func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			if i == 0 {
				return "/"
			}
			return p[:i]
		}
	}
	return "."
}
