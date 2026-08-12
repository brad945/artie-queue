package wal

import (
	"fmt"
	"sync"
	"testing"
)

// A write or fsync failure is fatal and sticky. We cannot know what reached
// the disk, so the log stops accepting records instead of pretending the next
// one might be fine. Failing closed is the only honest option: the alternative
// is telling clients their messages are durable when they are not.
func TestWriteFailureIsStickyAndFatal(t *testing.T) {
	path := tempLog(t)
	l, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	good, err := l.Append(TypeEnqueue, []byte("committed"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Wait(good); err != nil {
		t.Fatalf("first append should have succeeded: %v", err)
	}

	// Close the descriptor out from under the flusher: every write syscall
	// from here on fails, which is the shape of a disk that has gone away.
	l.mu.Lock()
	l.f.Close()
	l.mu.Unlock()

	doomed, err := l.Append(TypeEnqueue, []byte("never lands"))
	if err == nil {
		err = l.Wait(doomed)
	}
	if err == nil {
		t.Fatal("the log reported a record durable after its file was closed")
	}
	t.Logf("write failure surfaced as: %v", err)

	// Sticky: no further records are accepted.
	if _, err := l.Append(TypeEnqueue, []byte("after")); err == nil {
		t.Error("the log accepted a record after a fatal error")
	}
	if err := l.Sync(); err == nil {
		t.Error("Sync reported success after a fatal error")
	}

	// A batch that was already durable before the failure must not be
	// retroactively reported as failed — it really is on disk.
	if err := l.Wait(good); err != nil {
		t.Errorf("a batch that committed before the failure now reports %v", err)
	}
}

// Everything a writer was told is durable must still be there, in order, after
// a concurrent burst — no interleaving, no duplicates, no gaps.
func TestConcurrentAppendsAreNotInterleaved(t *testing.T) {
	path := tempLog(t)
	l, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	const writers, each = 12, 40
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				b, err := l.Append(TypeEnqueue, []byte(fmt.Sprintf("w%02d-i%02d", w, i)))
				if err != nil {
					t.Errorf("append: %v", err)
					return
				}
				if err := l.Wait(b); err != nil {
					t.Errorf("wait: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	seen := make(map[string]int)
	perWriter := make(map[int][]int)
	recs, res, err := collect(t, path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if res.Torn != nil {
		t.Fatalf("torn tail after a clean close: %+v", res.Torn)
	}
	for _, r := range recs {
		var w, i int
		if _, err := fmt.Sscanf(string(r.Payload), "w%02d-i%02d", &w, &i); err != nil {
			t.Fatalf("record payload was corrupted or spliced: %q", r.Payload)
		}
		seen[string(r.Payload)]++
		perWriter[w] = append(perWriter[w], i)
	}
	if len(recs) != writers*each {
		t.Fatalf("replayed %d records, want %d", len(recs), writers*each)
	}
	for payload, n := range seen {
		if n != 1 {
			t.Errorf("record %q appears %d times", payload, n)
		}
	}
	// Each writer's own records must appear in the order that writer wrote
	// them, even though batches interleave writers.
	for w, indices := range perWriter {
		if len(indices) != each {
			t.Errorf("writer %d has %d records, want %d", w, len(indices), each)
		}
		for i, got := range indices {
			if got != i {
				t.Errorf("writer %d record %d is out of order (saw %d)", w, i, got)
				break
			}
		}
	}
}

// Close must drain whatever is still buffered rather than dropping it.
func TestCloseFlushesBufferedRecords(t *testing.T) {
	path := tempLog(t)
	l, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	const n = 64
	for i := 0; i < n; i++ {
		if _, err := l.Append(TypeEnqueue, []byte(fmt.Sprintf("r%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// No Wait, no Sync: everything may still be sitting in the buffer.
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	recs, res, err := collect(t, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != n {
		t.Fatalf("Close dropped buffered records: %d of %d survived", len(recs), n)
	}
	if res.Torn != nil {
		t.Errorf("torn tail after Close: %+v", res.Torn)
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close returned %v, want nil", err)
	}
}
