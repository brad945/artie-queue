package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func tempLog(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "wal.log")
}

// collect replays a log and returns the records it contains.
func collect(t *testing.T, path string) ([]Record, ReplayResult, error) {
	t.Helper()
	var recs []Record
	res, err := Replay(path, func(r Record) error {
		cp := r
		cp.Payload = append([]byte(nil), r.Payload...)
		recs = append(recs, cp)
		return nil
	})
	return recs, res, err
}

func writeRecords(t *testing.T, path string, payloads ...string) *Log {
	t.Helper()
	l, err := Open(path, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, p := range payloads {
		b, err := l.Append(TypeEnqueue, []byte(p))
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := l.Wait(b); err != nil {
			t.Fatalf("wait: %v", err)
		}
	}
	return l
}

func TestRoundTrip(t *testing.T) {
	path := tempLog(t)
	l := writeRecords(t, path, "alpha", "beta", "gamma")
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	recs, res, err := collect(t, path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Torn != nil {
		t.Fatalf("unexpected torn tail: %+v", res.Torn)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	for i, want := range []string{"alpha", "beta", "gamma"} {
		if string(recs[i].Payload) != want {
			t.Errorf("record %d = %q, want %q", i, recs[i].Payload, want)
		}
		if recs[i].Type != TypeEnqueue {
			t.Errorf("record %d type = %s, want ENQUEUE", i, recs[i].Type)
		}
	}
	// Offsets must be exact: they are what a corruption report points a human at.
	if recs[0].Offset != 0 {
		t.Errorf("first offset = %d, want 0", recs[0].Offset)
	}
	if want := int64(HeaderSize + 5); recs[1].Offset != want {
		t.Errorf("second offset = %d, want %d", recs[1].Offset, want)
	}
}

func TestEmptyAndMissingLog(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.log")
	recs, res, err := collect(t, missing)
	if err != nil || len(recs) != 0 || res.ValidBytes != 0 {
		t.Fatalf("missing log: got %d records, %+v, err %v", len(recs), res, err)
	}

	empty := tempLog(t)
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if recs, res, err := collect(t, empty); err != nil || len(recs) != 0 || res.Torn != nil {
		t.Fatalf("empty log: got %d records, %+v, err %v", len(recs), res, err)
	}
}

// A crash mid-write leaves a record the file ends inside. That is unambiguous:
// the record cannot be complete, and no client was ever told it committed, so
// startup truncates and carries on.
func TestTornTailIsTruncated(t *testing.T) {
	// Cut points below HeaderSize leave a partial header; at or above it leave
	// an intact header whose payload never arrived. Both are torn tails, and
	// they exercise different branches of the reader.
	for _, cut := range []int{1, 5, 9, HeaderSize, HeaderSize + 2} {
		t.Run(fmt.Sprintf("partial-%d-bytes", cut), func(t *testing.T) {
			path := tempLog(t)
			l := writeRecords(t, path, "alpha", "beta")
			l.Close()

			good, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			goodLen := int64(len(good))

			// Append the first `cut` bytes of a third record.
			third := Encode(nil, TypeEnqueue, []byte("gamma-interrupted"))
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(third[:cut]); err != nil {
				t.Fatal(err)
			}
			f.Close()

			recs, res, err := collect(t, path)
			if err != nil {
				t.Fatalf("replay should tolerate a torn tail, got %v", err)
			}
			if res.Torn == nil {
				t.Fatal("expected a torn tail to be reported")
			}
			if res.Torn.Offset != goodLen {
				t.Errorf("torn offset = %d, want %d", res.Torn.Offset, goodLen)
			}
			if res.ValidBytes != goodLen {
				t.Errorf("valid bytes = %d, want %d", res.ValidBytes, goodLen)
			}
			if len(recs) != 2 {
				t.Fatalf("got %d records, want the 2 complete ones", len(recs))
			}

			// Reopening truncates the torn bytes and appends cleanly after them.
			l2, err := Open(path, res.ValidBytes)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			b, _ := l2.Append(TypeEnqueue, []byte("delta"))
			if err := l2.Wait(b); err != nil {
				t.Fatal(err)
			}
			l2.Close()

			recs, res, err = collect(t, path)
			if err != nil {
				t.Fatalf("replay after recovery: %v", err)
			}
			if res.Torn != nil {
				t.Errorf("torn tail should be gone, got %+v", res.Torn)
			}
			if len(recs) != 3 || string(recs[2].Payload) != "delta" {
				t.Fatalf("after recovery got %d records, last = %q", len(recs), recs[len(recs)-1].Payload)
			}
		})
	}
}

// A complete record whose checksum does not match is real corruption. We
// refuse to start and name the byte offset — we never skip the record and
// carry on, because a silently dropped record is a data loss nobody sees.
func TestMidLogCorruptionRefuses(t *testing.T) {
	path := tempLog(t)
	l := writeRecords(t, path, "alpha", "beta", "gamma")
	l.Close()

	recs, _, err := collect(t, path)
	if err != nil {
		t.Fatal(err)
	}
	badOffset := recs[1].Offset

	// Flip a bit inside the second record's payload.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[badOffset+HeaderSize] ^= 0x40
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err = collect(t, path)
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("want CorruptionError, got %v", err)
	}
	if ce.Offset != badOffset {
		t.Errorf("corruption offset = %d, want %d", ce.Offset, badOffset)
	}
}

// The ambiguous case, and the one the policy is really about: the *last*
// record is complete but its checksum is wrong. It could be bit rot, or it
// could be a torn write that happened to land on a record boundary. We refuse
// rather than guess, because guessing "probably torn" silently discards data.
func TestTailCorruptionAlsoRefuses(t *testing.T) {
	path := tempLog(t)
	l := writeRecords(t, path, "alpha", "beta", "gamma")
	l.Close()

	recs, _, err := collect(t, path)
	if err != nil {
		t.Fatal(err)
	}
	last := recs[len(recs)-1].Offset

	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0x01
	os.WriteFile(path, data, 0o644)

	_, _, err = collect(t, path)
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("want CorruptionError for a bad checksum at the tail, got %v", err)
	}
	if ce.Offset != last {
		t.Errorf("corruption offset = %d, want %d", ce.Offset, last)
	}
}

// The length field is what tells the reader where the next record starts, so
// it is inside the checksum. Corrupting it must be detected rather than
// silently re-framing everything that follows.
func TestCorruptLengthFieldIsDetected(t *testing.T) {
	path := tempLog(t)
	l := writeRecords(t, path, "alpha", "beta", "gamma")
	l.Close()

	recs, _, _ := collect(t, path)
	target := recs[1].Offset

	data, _ := os.ReadFile(path)
	data[target] = byte(len("beta") - 1) // shrink the claimed length
	os.WriteFile(path, data, 0o644)

	_, _, err := collect(t, path)
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("a corrupt length field must be detected, got %v", err)
	}
	if ce.Offset != target {
		t.Errorf("offset = %d, want %d", ce.Offset, target)
	}
}

// Regression test for the nastiest case in the format.
//
// Corrupt a length field so the record claims to run past the end of the file.
// With only a whole-record checksum that is indistinguishable from a torn
// write, so the reader would truncate — silently discarding every valid,
// already-acknowledged record behind it and reporting only a warning. The
// header checksum is what makes this detectable: the framing is damaged, so it
// is corruption, not a crash artifact.
func TestInflatedLengthIsCorruptionNotATornTail(t *testing.T) {
	path := tempLog(t)
	l := writeRecords(t, path, "alpha", "beta", "gamma", "delta", "epsilon")
	l.Close()

	recs, _, err := collect(t, path)
	if err != nil {
		t.Fatal(err)
	}
	target := recs[2].Offset

	data, _ := os.ReadFile(path)
	// Inflate the high byte of the length field: now it runs past EOF.
	data[target+3] = 0xff
	os.WriteFile(path, data, 0o644)

	recs, res, err := collect(t, path)
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("a corrupt length that overruns the file must be reported as corruption, got err=%v torn=%+v after %d records",
			err, res.Torn, len(recs))
	}
	if ce.Offset != target {
		t.Errorf("offset = %d, want %d", ce.Offset, target)
	}
	if res.Torn != nil {
		t.Errorf("must not be reported as a torn tail: %+v", res.Torn)
	}
}

func TestUnknownRecordTypeRefuses(t *testing.T) {
	path := tempLog(t)
	l := writeRecords(t, path, "alpha")
	l.Close()

	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write(Encode(nil, Type(99), []byte("who?")))
	f.Close()

	_, _, err := collect(t, path)
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("want CorruptionError for unknown type, got %v", err)
	}
}

// Group commit: many concurrent writers must all be durable when their Wait
// returns, and should have shared far fewer fsyncs than there were records.
func TestGroupCommitBatchesConcurrentWriters(t *testing.T) {
	path := tempLog(t)
	l, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	const writers, each = 16, 25
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < each; j++ {
				b, err := l.Append(TypeEnqueue, []byte(fmt.Sprintf("w%d-%d", i, j)))
				if err != nil {
					t.Errorf("append: %v", err)
					return
				}
				if err := l.Wait(b); err != nil {
					t.Errorf("wait: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	stats := l.Stats()
	if stats.Records != writers*each {
		t.Errorf("synced records = %d, want %d", stats.Records, writers*each)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	recs, res, err := collect(t, path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(recs) != writers*each {
		t.Fatalf("replayed %d records, want %d", len(recs), writers*each)
	}
	if res.Torn != nil {
		t.Errorf("unexpected torn tail")
	}
	if stats.Batches >= uint64(writers*each) {
		t.Errorf("group commit did nothing: %d fsyncs for %d records", stats.Batches, stats.Records)
	}
	t.Logf("%d records committed in %d fsyncs (%.1f records/fsync, max batch %d)",
		stats.Records, stats.Batches, float64(stats.Records)/float64(stats.Batches), stats.MaxBatch)
}

// Everything a caller was told is durable must survive, in order, with no
// duplicates — the property replay depends on.
func TestAppendOrderIsPreserved(t *testing.T) {
	path := tempLog(t)
	l, _ := Open(path, 0)
	const n = 500
	for i := 0; i < n; i++ {
		if _, err := l.Append(TypeEnqueue, []byte(fmt.Sprintf("%04d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	l.Close()

	recs, _, err := collect(t, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != n {
		t.Fatalf("got %d records, want %d", len(recs), n)
	}
	for i, r := range recs {
		if want := fmt.Sprintf("%04d", i); string(r.Payload) != want {
			t.Fatalf("record %d = %q, want %q", i, r.Payload, want)
		}
	}
}

func TestWriterInstallIsAtomic(t *testing.T) {
	dir := t.TempDir()
	final := filepath.Join(dir, "wal.log")
	l := writeRecords(t, final, "old-1", "old-2")
	l.Close()

	w, err := NewWriter(final + ".compact")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(TypeMeta, []byte(`{"name":"q"}`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(TypeEnqueue, []byte("new-1")); err != nil {
		t.Fatal(err)
	}
	size := w.Size()
	installed, err := w.Install(final)
	if err != nil {
		t.Fatal(err)
	}
	if !installed {
		t.Fatal("Install reported the rename did not happen, but returned no error")
	}

	recs, res, err := collect(t, final)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].Type != TypeMeta || string(recs[1].Payload) != "new-1" {
		t.Fatalf("snapshot did not replace the log: %+v", recs)
	}
	if res.ValidBytes != size {
		t.Errorf("valid bytes = %d, want %d", res.ValidBytes, size)
	}
	if _, err := os.Stat(final + ".compact"); !os.IsNotExist(err) {
		t.Errorf("temp file should be gone after rename")
	}
}
