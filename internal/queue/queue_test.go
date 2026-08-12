package queue

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func mustQueue(t *testing.T, cfg Config) (*Queue, string) {
	t.Helper()
	if cfg.Name == "" {
		cfg.Name = "test"
	}
	root := t.TempDir()
	q, err := Create(root, cfg, t.Logf)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q, root
}

func enq(t *testing.T, q *Queue, payload string, priority int, delay time.Duration) string {
	t.Helper()
	res, err := q.Enqueue([]byte(payload), priority, delay, "")
	if err != nil {
		t.Fatalf("enqueue %q: %v", payload, err)
	}
	return res.ID
}

// drain dequeues up to n messages and returns their payloads in delivery order.
func drain(t *testing.T, q *Queue, n int) []string {
	t.Helper()
	msgs, err := q.Dequeue(n, time.Minute)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, string(m.Payload))
	}
	return out
}

// ---------------------------------------------------------------------------
// Ordering: one comparator, six queue types.
// ---------------------------------------------------------------------------

func TestOrderingModes(t *testing.T) {
	// Each message is (payload, priority). Enqueued in listed order.
	type msg struct {
		name string
		prio int
	}
	input := []msg{{"a", 1}, {"b", 5}, {"c", 1}, {"d", 5}}

	cases := []struct {
		name string
		cfg  Config
		want []string
	}{
		{
			name: "fifo",
			cfg:  Config{Ordering: FIFO},
			want: []string{"a", "b", "c", "d"},
		},
		{
			name: "lifo",
			cfg:  Config{Ordering: LIFO},
			want: []string{"d", "c", "b", "a"},
		},
		{
			// Priority is primary; FIFO orders within a priority level.
			name: "priority-fifo",
			cfg:  Config{Ordering: FIFO, PriorityEnabled: true},
			want: []string{"b", "d", "a", "c"},
		},
		{
			// Same priority grouping, reversed inside each level.
			name: "priority-lifo",
			cfg:  Config{Ordering: LIFO, PriorityEnabled: true},
			want: []string{"d", "b", "c", "a"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := mustQueue(t, tc.cfg)
			for _, m := range input {
				enq(t, q, m.name, m.prio, 0)
			}
			got := drain(t, q, len(input))
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("order = %v, want %v", got, tc.want)
			}
		})
	}
}

// Delay is not a fifth comparator: it is a gate in front of the same one. Once
// a delayed message becomes visible it takes its normal place in whatever
// ordering the queue is configured for.
func TestDelayedVariantsRespectTheSameComparator(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want []string
	}{
		{"delayed-fifo", Config{Ordering: FIFO}, []string{"a", "b", "delayed-c", "d"}},
		{"delayed-lifo", Config{Ordering: LIFO}, []string{"d", "delayed-c", "b", "a"}},
		{"delayed-priority-fifo", Config{Ordering: FIFO, PriorityEnabled: true}, []string{"b", "d", "a", "delayed-c"}},
		{"delayed-priority-lifo", Config{Ordering: LIFO, PriorityEnabled: true}, []string{"d", "b", "delayed-c", "a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := mustQueue(t, tc.cfg)
			enq(t, q, "a", 1, 0)
			enq(t, q, "b", 5, 0)
			enq(t, q, "delayed-c", 1, 60*time.Millisecond)
			enq(t, q, "d", 5, 0)

			// Before it is visible, the delayed message must not appear at all.
			early := drain(t, q, 10)
			for _, p := range early {
				if p == "delayed-c" {
					t.Fatal("delayed message was delivered before VisibleAt")
				}
			}
			if len(early) != 3 {
				t.Fatalf("got %d messages before the delay elapsed, want 3", len(early))
			}
			// Put them back so the full ordering can be checked in one pass.
			for _, m := range early {
				_ = m
			}
			// Nack everything we took so the comparator sees all four together.
			for _, id := range inFlightIDs(q) {
				if err := q.Nack(id); err != nil {
					t.Fatalf("nack: %v", err)
				}
			}

			waitFor(t, time.Second, func() bool { return q.Stats().Delayed == 0 })

			got := drain(t, q, 10)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("order = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDelayedMessageIsNotVisibleEarlyAndIsPromptlyVisible(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO})
	const delay = 250 * time.Millisecond
	start := time.Now()
	enq(t, q, "later", 0, delay)

	if got := drain(t, q, 5); len(got) != 0 {
		t.Fatalf("message delivered %v before its delay elapsed", delay)
	}
	if s := q.Stats(); s.Delayed != 1 || s.Ready != 0 {
		t.Fatalf("stats = delayed %d ready %d, want 1/0", s.Delayed, s.Ready)
	}

	// Poll tightly so the assertion is about promptness, not about our sleep.
	var elapsed time.Duration
	for {
		if got := drain(t, q, 5); len(got) == 1 {
			elapsed = time.Since(start)
			break
		}
		if time.Since(start) > 2*time.Second {
			t.Fatal("delayed message never became visible")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if elapsed < delay {
		t.Errorf("delivered after %v, before the %v delay", elapsed, delay)
	}
	if slack := elapsed - delay; slack > 50*time.Millisecond {
		t.Errorf("delivered %v late; promotion should be timer-driven, not polled", slack)
	}
}

func TestPriorityAgingRescuesAStarvedMessage(t *testing.T) {
	q, _ := mustQueue(t, Config{
		Ordering:        FIFO,
		PriorityEnabled: true,
		AgingIntervalMS: 40,
		AgingMaxBoost:   10,
	})
	enq(t, q, "starved", 0, 0)
	// Keep a stream of higher-priority work in front of it.
	for i := 0; i < 5; i++ {
		enq(t, q, fmt.Sprintf("urgent-%d", i), 3, 0)
	}
	if got := drain(t, q, 1); got[0] == "starved" {
		t.Fatal("low-priority message should be behind the urgent ones initially")
	}
	for _, id := range inFlightIDs(q) {
		q.Nack(id)
	}

	// After enough aging intervals its effective priority overtakes them.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		msgs, err := q.Dequeue(1, time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) == 1 && string(msgs[0].Payload) == "starved" {
			return // aging worked
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("aging never promoted the starved message to the head of the queue")
}

// ---------------------------------------------------------------------------
// Lease-and-ack delivery.
// ---------------------------------------------------------------------------

func TestLeasedMessageIsInvisibleToOtherConsumers(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO})
	enq(t, q, "only", 0, 0)

	first, err := q.Dequeue(1, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("first dequeue: %v %d", err, len(first))
	}
	second, err := q.Dequeue(1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatal("a leased message was handed to a second consumer")
	}
	if s := q.Stats(); s.InFlight != 1 || s.Ready != 0 {
		t.Fatalf("stats = in-flight %d ready %d, want 1/0", s.InFlight, s.Ready)
	}
	if err := q.Ack(first[0].ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if s := q.Stats(); s.Total != 0 {
		t.Fatalf("acked message still present: %+v", s)
	}
}

func TestVisibilityTimeoutRedelivers(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO})
	enq(t, q, "retry-me", 0, 0)

	first, _ := q.Dequeue(1, 60*time.Millisecond)
	if len(first) != 1 || first[0].Attempts != 0 {
		t.Fatalf("first delivery: %d messages, attempts %d", len(first), first[0].Attempts)
	}

	var second []*Message
	waitFor(t, 2*time.Second, func() bool {
		second, _ = q.Dequeue(1, time.Minute)
		return len(second) == 1
	})
	if second[0].ID != first[0].ID {
		t.Errorf("redelivered a different message: %s vs %s", second[0].ID, first[0].ID)
	}
	if second[0].Attempts != 1 {
		t.Errorf("attempts = %d after one expiry, want 1", second[0].Attempts)
	}
}

func TestNackRequeuesImmediatelyAndCountsAnAttempt(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO})
	enq(t, q, "nack-me", 0, 0)

	first, _ := q.Dequeue(1, time.Minute)
	if err := q.Nack(first[0].ID); err != nil {
		t.Fatalf("nack: %v", err)
	}
	second, _ := q.Dequeue(1, time.Minute)
	if len(second) != 1 || second[0].ID != first[0].ID {
		t.Fatal("nacked message was not immediately available again")
	}
	if second[0].Attempts != 1 {
		t.Errorf("attempts = %d, want 1", second[0].Attempts)
	}
}

func TestAckAndNackRejectMessagesThatAreNotLeased(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO})
	id := enq(t, q, "ready", 0, 0)

	if err := q.Ack(id); err != ErrNotInFlight {
		t.Errorf("ack of a ready message = %v, want ErrNotInFlight", err)
	}
	if err := q.Nack(id); err != ErrNotInFlight {
		t.Errorf("nack of a ready message = %v, want ErrNotInFlight", err)
	}
	if err := q.Ack("no-such-id"); err != ErrNotFound {
		t.Errorf("ack of an unknown id = %v, want ErrNotFound", err)
	}
}

func TestExhaustedAttemptsDeadLetter(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO, MaxAttempts: 3})
	enq(t, q, "poison", 0, 0)

	for i := 0; i < 3; i++ {
		msgs, err := q.Dequeue(1, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 1 {
			t.Fatalf("attempt %d: got no message (stats %+v)", i+1, q.Stats())
		}
		if msgs[0].Attempts != i {
			t.Errorf("attempt %d: Attempts = %d, want %d", i+1, msgs[0].Attempts, i)
		}
		if err := q.Nack(msgs[0].ID); err != nil {
			t.Fatal(err)
		}
	}

	s := q.Stats()
	if s.DeadLettered != 1 {
		t.Fatalf("dlq = %d, want 1 after %d failed attempts", s.DeadLettered, s.MaxAttempts)
	}
	if s.Ready != 0 || s.InFlight != 0 {
		t.Errorf("dead-lettered message is still live: %+v", s)
	}
	dead := q.DLQ(10)
	if len(dead) != 1 || dead[0].Attempts != 3 || dead[0].State != "dead" {
		t.Errorf("dlq contents = %+v", dead)
	}
	if got := drain(t, q, 5); len(got) != 0 {
		t.Errorf("dead-lettered message was delivered again: %v", got)
	}
}

func TestVisibilityExpiryEventuallyDeadLetters(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO, MaxAttempts: 2, DefaultVisibilityTimeoutMS: 40})
	enq(t, q, "never-acked", 0, 0)

	// Deliver it, then never ack; expiry alone must drive it to the DLQ.
	waitFor(t, 5*time.Second, func() bool {
		q.Dequeue(1, 40*time.Millisecond)
		return q.Stats().DeadLettered == 1
	})
	if s := q.Stats(); s.Counters.Expired < 2 {
		t.Errorf("lease expiries = %d, want at least 2", s.Counters.Expired)
	}
}

// ---------------------------------------------------------------------------
// Dedup.
// ---------------------------------------------------------------------------

func TestDedupRejectsRetriedEnqueueAndReturnsOriginalID(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO})

	first, err := q.Enqueue([]byte("payment-42"), 0, 0, "txn-42")
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate {
		t.Fatal("first enqueue reported as duplicate")
	}
	second, err := q.Enqueue([]byte("payment-42"), 0, 0, "txn-42")
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate {
		t.Fatal("retried enqueue was accepted as a new message")
	}
	if second.ID != first.ID {
		t.Errorf("duplicate returned id %s, want the original %s", second.ID, first.ID)
	}
	if s := q.Stats(); s.Total != 1 {
		t.Errorf("queue holds %d messages, want 1", s.Total)
	}

	// The window outlives the message: a producer retrying after the consumer
	// already acked must still be rejected.
	msgs, _ := q.Dequeue(1, time.Minute)
	if err := q.Ack(msgs[0].ID); err != nil {
		t.Fatal(err)
	}
	third, _ := q.Enqueue([]byte("payment-42"), 0, 0, "txn-42")
	if !third.Duplicate {
		t.Error("dedup entry did not outlive the acked message")
	}
	if s := q.Stats(); s.Counters.Duplicates != 2 {
		t.Errorf("duplicates counter = %d, want 2", s.Counters.Duplicates)
	}
}

func TestDedupWindowExpires(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO, DedupWindowMS: 50})
	first, _ := q.Enqueue([]byte("x"), 0, 0, "k")
	time.Sleep(80 * time.Millisecond)
	second, _ := q.Enqueue([]byte("x"), 0, 0, "k")
	if second.Duplicate {
		t.Fatal("dedup entry outlived its window")
	}
	if second.ID == first.ID {
		t.Fatal("expected a genuinely new message")
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func waitFor(t *testing.T, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", limit)
}

func inFlightIDs(q *Queue) []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, 0, q.inflight.Len())
	for _, m := range q.inflight.items {
		out = append(out, m.ID)
	}
	return out
}
