package queue

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// reopen closes a queue and replays it from its log, which is what a restart
// does. Every recovery assertion below goes through this.
func reopen(t *testing.T, q *Queue, root string) *Queue {
	t.Helper()
	name := q.Config().Name
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := Load(root, name, t.Logf)
	if err != nil {
		t.Fatalf("load after restart: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	return reopened
}

func TestRestartKeepsUnackedAndForgetsAcked(t *testing.T) {
	q, root := mustQueue(t, Config{Ordering: FIFO})
	for i := 0; i < 10; i++ {
		enq(t, q, fmt.Sprintf("m%02d", i), 0, 0)
	}
	leased, err := q.Dequeue(5, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	acked := map[string]bool{}
	for _, m := range leased[:3] {
		if err := q.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
		acked[string(m.Payload)] = true
	}

	q2 := reopen(t, q, root)

	s := q2.Stats()
	if s.Total != 7 {
		t.Fatalf("after restart total = %d, want 7", s.Total)
	}
	// Messages that were leased but never acked come back ready. That is the
	// same outcome their lease expiring would have produced.
	if s.Ready != 7 || s.InFlight != 0 {
		t.Errorf("after restart ready = %d in-flight = %d, want 7/0", s.Ready, s.InFlight)
	}
	got := drain(t, q2, 20)
	if len(got) != 7 {
		t.Fatalf("recovered %d messages, want 7", len(got))
	}
	for _, p := range got {
		if acked[p] {
			t.Errorf("acked message %q came back after restart", p)
		}
	}
}

func TestRestartPreservesOrderingForEveryMode(t *testing.T) {
	modes := []struct {
		name string
		cfg  Config
	}{
		{"fifo", Config{Ordering: FIFO}},
		{"lifo", Config{Ordering: LIFO}},
		{"priority-fifo", Config{Ordering: FIFO, PriorityEnabled: true}},
		{"priority-lifo", Config{Ordering: LIFO, PriorityEnabled: true}},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			// Order the queue would produce with no restart at all.
			ref, _ := mustQueue(t, mode.cfg)
			for i := 0; i < 12; i++ {
				enq(t, ref, fmt.Sprintf("m%02d", i), i%4, 0)
			}
			want := drain(t, ref, 20)

			q, root := mustQueue(t, mode.cfg)
			for i := 0; i < 12; i++ {
				enq(t, q, fmt.Sprintf("m%02d", i), i%4, 0)
			}
			q2 := reopen(t, q, root)
			got := drain(t, q2, 20)

			if !reflect.DeepEqual(got, want) {
				t.Errorf("order after restart = %v, want %v", got, want)
			}
		})
	}
}

func TestRestartPreservesAttemptsDelayAndDLQ(t *testing.T) {
	q, root := mustQueue(t, Config{Ordering: FIFO, MaxAttempts: 3})
	enq(t, q, "flaky", 0, 0)
	enq(t, q, "poison", 0, 0)
	enq(t, q, "later", 0, 10*time.Second)

	// One nack on "flaky", enough nacks on "poison" to dead-letter it.
	for i := 0; i < 4; i++ {
		msgs, err := q.Dequeue(2, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if string(m.Payload) == "flaky" && m.Attempts >= 1 {
				continue // leave it leased-then-restarted at attempts 1
			}
			if err := q.Nack(m.ID); err != nil {
				t.Fatal(err)
			}
		}
		if q.Stats().DeadLettered == 1 {
			break
		}
	}
	if s := q.Stats(); s.DeadLettered != 1 {
		t.Fatalf("expected poison in the DLQ before restart, stats %+v", s)
	}

	q2 := reopen(t, q, root)

	s := q2.Stats()
	if s.DeadLettered != 1 {
		t.Errorf("dead-lettered count = %d after restart, want 1", s.DeadLettered)
	}
	// Payload is rendered as a JSON value, so the raw bytes "poison" come back
	// quoted.
	if dead := q2.DLQ(10); len(dead) != 1 || string(dead[0].Payload) != `"poison"` || dead[0].Attempts != 3 {
		t.Errorf("dlq after restart = %+v", dead)
	}
	if s.Delayed != 1 {
		t.Errorf("delayed count = %d after restart, want 1 (the 10s message)", s.Delayed)
	}
	msgs, _ := q2.Dequeue(5, time.Minute)
	if len(msgs) != 1 || string(msgs[0].Payload) != "flaky" {
		t.Fatalf("expected only flaky to be ready, got %d messages", len(msgs))
	}
	if msgs[0].Attempts == 0 {
		t.Error("attempt count was lost across the restart")
	}
}

func TestRestartPreservesDedupWindow(t *testing.T) {
	q, root := mustQueue(t, Config{Ordering: FIFO})
	first, _ := q.Enqueue([]byte("once"), 0, 0, "key-1")

	q2 := reopen(t, q, root)

	again, err := q2.Enqueue([]byte("once"), 0, 0, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate {
		t.Fatal("dedup index did not survive the restart")
	}
	if again.ID != first.ID {
		t.Errorf("duplicate returned %s, want the original %s", again.ID, first.ID)
	}
}

// ---------------------------------------------------------------------------
// Compaction.
// ---------------------------------------------------------------------------

func TestCompactionShrinksTheLogAndPreservesState(t *testing.T) {
	q, root := mustQueue(t, Config{Ordering: FIFO, PriorityEnabled: true})
	for i := 0; i < 200; i++ {
		enq(t, q, fmt.Sprintf("m%03d", i), i%5, 0)
	}
	// Consume and ack most of them so the log is mostly dead weight.
	msgs, err := q.Dequeue(180, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range msgs {
		if err := q.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}

	before := q.Stats()
	if err := q.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	after := q.Stats()

	if after.Log.SizeBytes >= before.Log.SizeBytes {
		t.Errorf("log did not shrink: %d -> %d bytes", before.Log.SizeBytes, after.Log.SizeBytes)
	}
	if after.Total != before.Total {
		t.Errorf("compaction changed live message count: %d -> %d", before.Total, after.Total)
	}

	// Everything still works after compaction, and survives a restart.
	enq(t, q, "post-compaction", 9, 0)
	q2 := reopen(t, q, root)
	if got, want := q2.Stats().Total, after.Total+1; got != want {
		t.Fatalf("after restart total = %d, want %d", got, want)
	}
	head := drain(t, q2, 1)
	if len(head) != 1 || head[0] != "post-compaction" {
		t.Errorf("head after compaction+restart = %v, want the priority-9 message", head)
	}
}

// Compacting a fully drained queue must not reset the sequence counter: Seq is
// what every ordering decision is made on, so a reset would reorder new
// messages against old ones.
func TestCompactionPreservesSequenceCounter(t *testing.T) {
	q, root := mustQueue(t, Config{Ordering: FIFO})
	for i := 0; i < 10; i++ {
		enq(t, q, fmt.Sprintf("m%d", i), 0, 0)
	}
	msgs, _ := q.Dequeue(10, time.Minute)
	for _, m := range msgs {
		if err := q.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if s := q.Stats(); s.Total != 0 {
		t.Fatalf("queue should be empty, got %+v", s)
	}
	nextSeq := q.Stats().NextSeq

	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	q2 := reopen(t, q, root)

	if got := q2.Stats().NextSeq; got != nextSeq {
		t.Errorf("next seq = %d after compacting an empty queue, want %d", got, nextSeq)
	}
	res, err := q2.Enqueue([]byte("after"), 0, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if res.Seq < nextSeq {
		t.Errorf("new message got seq %d, which collides with already-acked history (next was %d)", res.Seq, nextSeq)
	}
}

func TestCompactionPreservesDedupEntriesForAckedMessages(t *testing.T) {
	q, root := mustQueue(t, Config{Ordering: FIFO})
	first, _ := q.Enqueue([]byte("x"), 0, 0, "dedup-key")
	msgs, _ := q.Dequeue(1, time.Minute)
	if err := q.Ack(msgs[0].ID); err != nil {
		t.Fatal(err)
	}
	// The message is gone but its dedup entry must be carried into the snapshot.
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	q2 := reopen(t, q, root)

	again, _ := q2.Enqueue([]byte("x"), 0, 0, "dedup-key")
	if !again.Duplicate {
		t.Fatal("dedup entry for an acked message was lost by compaction")
	}
	if again.ID != first.ID {
		t.Errorf("duplicate returned %s, want %s", again.ID, first.ID)
	}
}

func TestLoadRefusesCorruptLog(t *testing.T) {
	q, root := mustQueue(t, Config{Ordering: FIFO})
	name := q.Config().Name
	for i := 0; i < 5; i++ {
		enq(t, q, fmt.Sprintf("m%d", i), 0, 0)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(root, name, logFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(root, name, t.Logf); err == nil {
		t.Fatal("Load accepted a corrupt log; it must refuse to start")
	} else {
		t.Logf("refused as expected: %v", err)
	}

	// And the manager must refuse too, rather than booting without this queue.
	if _, err := OpenManager(root, t.Logf); err == nil {
		t.Fatal("OpenManager started despite a corrupt queue log")
	}
}
