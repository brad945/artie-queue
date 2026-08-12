package queue

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/brad945/artie-queue/internal/wal"
)

// Compaction must not thrash. A queue whose live state is already bigger than
// the compaction threshold can never get below it, so a trigger of "log >=
// threshold" alone would rewrite the entire log on every timer tick forever —
// burning disk bandwidth to reclaim nothing.
func TestCompactionDoesNotThrashWhenLiveStateExceedsThreshold(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO})

	payload := make([]byte, 1024)
	for i := 0; i < 200; i++ {
		if _, err := q.Enqueue(payload, 0, 0, ""); err != nil {
			t.Fatal(err)
		}
	}
	// Threshold far below the size of the live data itself.
	q.SetCompactThreshold(50 << 10)
	if got := q.Stats().Log.SizeBytes; got < 50<<10 {
		t.Fatalf("test setup is wrong: log is %d bytes, threshold is %d", got, 50<<10)
	}

	// Keep the timer loop busy so compaction gets every chance to re-fire.
	if _, err := q.Enqueue([]byte("tick"), 0, 40*time.Millisecond, ""); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)

	got := q.Stats().Counters.Compactions
	if got > 2 {
		t.Errorf("compaction thrashed: %d compactions in ~1.2s with no reclaimable space", got)
	}
	t.Logf("%d compaction(s) over 1.2s with live state above the threshold", got)

	// And it must still compact when there really is garbage to reclaim.
	msgs, err := q.Dequeue(190, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range msgs {
		if err := q.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	before := q.Stats().Log.SizeBytes
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	if after := q.Stats().Log.SizeBytes; after >= before {
		t.Errorf("explicit compaction did not reclaim: %d -> %d", before, after)
	}
}

// A crash between creating a queue's log file and committing its META record
// leaves a zero-byte log. That is not corruption — nothing was ever written,
// so nothing was ever acknowledged — and it must not stop the server from
// starting, or one interrupted create would take every other queue down too.
func TestIncompleteQueueDoesNotBrickStartup(t *testing.T) {
	root := t.TempDir()

	good, err := Create(root, Config{Name: "good", Ordering: FIFO}, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := good.Enqueue([]byte("keep me"), 0, 0, ""); err != nil {
		t.Fatal(err)
	}
	good.Close()

	// The remains of an interrupted create.
	halfDir := filepath.Join(root, "halfmade")
	if err := os.MkdirAll(halfDir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(halfDir, logFileName))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	mgr, err := OpenManager(root, t.Logf)
	if err != nil {
		t.Fatalf("startup refused over an empty log, taking healthy queues down with it: %v", err)
	}
	defer mgr.Close()

	if _, ok := mgr.Get("good"); !ok {
		t.Error("the healthy queue was not recovered")
	}
	if _, ok := mgr.Get("halfmade"); ok {
		t.Error("a queue with no META record was loaded as if it were real")
	}
	if st, ok := mgr.Get("good"); ok && st.Stats().Total != 1 {
		t.Errorf("healthy queue recovered %d messages, want 1", st.Stats().Total)
	}

	// The name must still be usable: re-creating over an empty log is safe.
	if _, err := mgr.Create(Config{Name: "halfmade", Ordering: LIFO}); err != nil {
		t.Errorf("could not re-create the interrupted queue: %v", err)
	}
	if _, ok := mgr.Get("halfmade"); !ok {
		t.Error("re-created queue is not registered")
	}

	// A *torn* META is the same situation as an empty file — the create never
	// committed anything — and must be treated identically. Checking file size
	// alone is not enough: a partially written META is non-empty but holds no
	// committed record, and getting this wrong leaves the name skipped at boot
	// yet unusable by Create, i.e. permanently dead.
	fullMeta := func() []byte {
		cfg := Config{Name: "orders", Ordering: FIFO, MaxAttempts: 3}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		meta, err := encodeConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return wal.Encode(nil, wal.TypeMeta, meta)
	}()

	for _, prefix := range []int{1, 5, wal.HeaderSize, wal.HeaderSize + 3, len(fullMeta) - 1} {
		t.Run(fmt.Sprintf("torn-meta-%d-bytes", prefix), func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "orders")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, logFileName), fullMeta[:prefix], 0o644); err != nil {
				t.Fatal(err)
			}

			if _, err := Load(root, "orders", t.Logf); !errors.Is(err, ErrIncompleteQueue) {
				t.Errorf("Load of a %d-byte torn META = %v, want ErrIncompleteQueue", prefix, err)
			}
			mgr, err := OpenManager(root, t.Logf)
			if err != nil {
				t.Fatalf("startup refused over a torn META: %v", err)
			}
			defer mgr.Close()

			// The whole point: the name must be reusable.
			q, err := mgr.Create(Config{Name: "orders", Ordering: FIFO, MaxAttempts: 3})
			if err != nil {
				t.Fatalf("could not create over a %d-byte torn META: %v", prefix, err)
			}
			if _, err := q.Enqueue([]byte("works"), 0, 0, ""); err != nil {
				t.Fatalf("re-created queue does not work: %v", err)
			}
			if got := q.Stats().Total; got != 1 {
				t.Errorf("re-created queue holds %d messages, want 1", got)
			}
		})
	}

	// Creating over a log that exists and is *corrupt* must refuse rather than
	// silently overwrite: that file may hold committed data.
	t.Run("create-over-corrupt-log-refuses", func(t *testing.T) {
		root := t.TempDir()
		q, err := Create(root, Config{Name: "real", Ordering: FIFO}, t.Logf)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := q.Enqueue([]byte("valuable"), 0, 0, ""); err != nil {
			t.Fatal(err)
		}
		q.Close()

		p := filepath.Join(root, "real", logFileName)
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		data[len(data)/2] ^= 0xff
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}

		if _, err := Create(root, Config{Name: "real", Ordering: FIFO}, t.Logf); err == nil {
			t.Fatal("Create overwrote a corrupt log instead of refusing")
		}
		if st, err := os.Stat(p); err != nil || st.Size() != int64(len(data)) {
			t.Errorf("the corrupt log was modified: %v %v", st, err)
		}
	})

	// A log with real records but no META is a different story: that is a
	// malformed log, and it must still refuse.
	badDir := filepath.Join(root, "headless")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile(filepath.Join(root, "good", logFileName))
	if err != nil {
		t.Fatal(err)
	}
	// Strip the META record by keeping only the tail of the log.
	if err := os.WriteFile(filepath.Join(badDir, logFileName), src[len(src)/2:], 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenManager(root, t.Logf); err == nil {
		t.Error("a log with records but no META was accepted")
	} else if errors.Is(err, ErrIncompleteQueue) {
		t.Errorf("a malformed log was misreported as merely incomplete: %v", err)
	}
}

// Counters must mean the same thing regardless of whether the log has been
// compacted. They count what this process did, so a restart resets them —
// but a restart *after a compaction* must not report something different from
// a restart without one.
func TestCountersAreConsistentAcrossCompaction(t *testing.T) {
	newDeadLettered := func(t *testing.T, compact bool) Counters {
		t.Helper()
		q, root := mustQueue(t, Config{Ordering: FIFO, MaxAttempts: 1})
		for i := 0; i < 3; i++ {
			enq(t, q, fmt.Sprintf("m%d", i), 0, 0)
		}
		msgs, err := q.Dequeue(3, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if err := q.Nack(m.ID); err != nil { // MaxAttempts 1: straight to the DLQ
				t.Fatal(err)
			}
		}
		if got := q.Stats().Counters.DeadLettered; got != 3 {
			t.Fatalf("live dead-lettered counter = %d, want 3", got)
		}
		if compact {
			if err := q.Compact(); err != nil {
				t.Fatal(err)
			}
		}
		q2 := reopen(t, q, root)
		if got := q2.Stats().DeadLettered; got != 3 {
			t.Fatalf("dlq depth after restart = %d, want 3", got)
		}
		return q2.Stats().Counters
	}

	plain := newDeadLettered(t, false)
	compacted := newDeadLettered(t, true)

	if plain != compacted {
		t.Errorf("counters depend on whether compaction ran:\n without compaction %+v\n with compaction    %+v", plain, compacted)
	}
	// And after a restart they read zero, because this process did none of it.
	if plain.DeadLettered != 0 || plain.Acked != 0 || plain.Enqueued != 0 {
		t.Errorf("counters after a restart = %+v, want all zero (they count this process's work)", plain)
	}
}

// A queue whose log has failed should go quiet, not spin. The expired-lease
// loop cannot make progress once appends fail, and if the timer kept being set
// to an already-past deadline the background goroutine would wake every
// millisecond forever.
func TestFailedQueueDoesNotSpin(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO, DefaultVisibilityTimeoutMS: 20})
	enq(t, q, "leased", 0, 0)
	if _, err := q.Dequeue(1, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	q.mu.Lock()
	q.failed = errors.New("simulated log failure")
	q.mu.Unlock()

	// Count how many times the background loop runs over a window in which the
	// lease deadline is long past.
	var ticks int
	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(400 * time.Millisecond)
		for time.Now().Before(deadline) {
			if q.tick().IsZero() {
				ticks++
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	<-done

	// Every tick on a failed queue must report "nothing scheduled" (zero time),
	// which parks the timer instead of re-arming it for a past deadline.
	if ticks == 0 {
		t.Fatal("tick never reported an idle schedule on a failed queue")
	}
	if next := q.tick(); !next.IsZero() {
		t.Errorf("a failed queue scheduled another wakeup at %v", next)
	}
}

// Compaction replaces the live log file underneath a running queue. It runs
// under the queue mutex, after draining the write buffer, so no append can be
// in flight while the file is swapped — but that is an argument, and this is
// the test.
//
// Producers and consumers hammer the queue while compaction runs in a tight
// loop; afterwards the queue is restarted from whatever is on disk and every
// acknowledgement the server issued has to still hold.
func TestCompactionUnderConcurrentLoadLosesNothing(t *testing.T) {
	q, root := mustQueue(t, Config{Ordering: FIFO, MaxAttempts: 10})
	// Never let the background trigger fire; this test drives compaction itself.
	q.SetCompactThreshold(1 << 40)

	var (
		mu       sync.Mutex
		accepted = map[string]bool{}
		acked    = map[string]bool{}
	)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				res, err := q.Enqueue([]byte(fmt.Sprintf("p%d-%04d", p, i)), i%5, 0, "")
				if err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
				mu.Lock()
				accepted[res.ID] = true
				mu.Unlock()
			}
		}(p)
	}

	for c := 0; c < 2; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				msgs, err := q.Dequeue(4, time.Minute)
				if err != nil {
					t.Errorf("dequeue: %v", err)
					return
				}
				for _, m := range msgs {
					if err := q.Ack(m.ID); err != nil {
						t.Errorf("ack: %v", err)
						return
					}
					mu.Lock()
					acked[m.ID] = true
					mu.Unlock()
				}
			}
		}()
	}

	var compactions int
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := q.Compact(); err != nil {
				t.Errorf("compact: %v", err)
				return
			}
			compactions++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	time.Sleep(700 * time.Millisecond)
	close(stop)
	wg.Wait()

	mu.Lock()
	nAccepted, nAcked := len(accepted), len(acked)
	mu.Unlock()
	if compactions < 5 || nAccepted < 100 || nAcked == 0 {
		t.Fatalf("test did not generate enough contention: %d compactions, %d accepted, %d acked",
			compactions, nAccepted, nAcked)
	}

	// Restart from disk: the only thing that survives is what the log holds.
	q2 := reopen(t, q, root)
	survivors := make(map[string]bool)
	q2.mu.Lock()
	for id := range q2.byID {
		survivors[id] = true
	}
	q2.mu.Unlock()

	var lost, resurrected int
	for id := range accepted {
		switch {
		case acked[id] && survivors[id]:
			resurrected++
		case !acked[id] && !survivors[id]:
			lost++
		}
	}
	if lost > 0 {
		t.Errorf("%d confirmed messages were lost across %d compactions", lost, compactions)
	}
	if resurrected > 0 {
		t.Errorf("%d acked messages came back after compaction", resurrected)
	}
	if want := nAccepted - nAcked; len(survivors) != want {
		t.Errorf("recovered %d messages, want %d", len(survivors), want)
	}
	t.Logf("%d compactions during %d enqueues and %d acks; %d messages recovered intact",
		compactions, nAccepted, nAcked, len(survivors))
}

// checkInvariants asserts the structural rules the whole engine rests on.
// Must be called with q.mu held.
func checkInvariants(t *testing.T, q *Queue, when string) {
	t.Helper()

	counted := 0
	for _, m := range q.ready.items {
		if m.state != stateReady {
			t.Fatalf("%s: message %s is in the ready heap but its state is %s", when, m.ID, m.state)
		}
		counted++
	}
	for _, m := range q.delayed.items {
		if m.state != stateDelayed {
			t.Fatalf("%s: message %s is in the delayed heap but its state is %s", when, m.ID, m.state)
		}
		counted++
	}
	for _, m := range q.inflight.items {
		if m.state != stateInFlight {
			t.Fatalf("%s: message %s is in the lease heap but its state is %s", when, m.ID, m.state)
		}
		counted++
	}
	for _, m := range q.dlq {
		if m.state != stateDead {
			t.Fatalf("%s: message %s is in the DLQ but its state is %s", when, m.ID, m.state)
		}
		counted++
	}
	if counted != len(q.byID) {
		t.Fatalf("%s: %d messages across the structures but %d in the index — one is in two places or none",
			when, counted, len(q.byID))
	}

	// Heap indices must point back at the right slot, or arbitrary removal
	// corrupts the heap.
	for i, m := range q.ready.items {
		if m.index != i {
			t.Fatalf("%s: ready heap index mismatch at %d (message %s says %d)", when, i, m.ID, m.index)
		}
	}
	for i, m := range q.delayed.items {
		if m.index != i {
			t.Fatalf("%s: delayed heap index mismatch at %d (message %s says %d)", when, i, m.ID, m.index)
		}
	}
	for i, m := range q.inflight.items {
		if m.index != i {
			t.Fatalf("%s: lease heap index mismatch at %d (message %s says %d)", when, i, m.ID, m.index)
		}
	}

	// Heap property: no child may sort before its parent.
	for i := 1; i < q.ready.Len(); i++ {
		if q.ready.Less(i, (i-1)/2) {
			t.Fatalf("%s: ready heap invariant violated at index %d", when, i)
		}
	}
	for i := 1; i < q.delayed.Len(); i++ {
		if q.delayed.Less(i, (i-1)/2) {
			t.Fatalf("%s: delayed heap invariant violated at index %d", when, i)
		}
	}
	for i := 1; i < q.inflight.Len(); i++ {
		if q.inflight.Less(i, (i-1)/2) {
			t.Fatalf("%s: lease heap invariant violated at index %d", when, i)
		}
	}
}

// Hammer every operation concurrently with short timeouts, so promotion, lease
// expiry, aging and dead-lettering all fire while the structures are being
// mutated — and check the structural invariants continuously while it happens.
func TestStructuralInvariantsUnderConcurrentLoad(t *testing.T) {
	q, _ := mustQueue(t, Config{
		Ordering:                   FIFO,
		PriorityEnabled:            true,
		MaxAttempts:                3,
		AgingIntervalMS:            5,
		AgingMaxBoost:              4,
		DefaultVisibilityTimeoutMS: 15,
		DedupWindowMS:              200,
	})

	var wg sync.WaitGroup
	deadline := time.Now().Add(3 * time.Second)
	running := func() bool { return time.Now().Before(deadline) }

	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed*7+1))
			for running() {
				switch rng.IntN(5) {
				case 0, 1:
					dedup := ""
					if rng.IntN(4) == 0 {
						dedup = fmt.Sprintf("d%d", rng.IntN(20))
					}
					q.Enqueue([]byte(`{"n":1}`), rng.IntN(4),
						time.Duration(rng.IntN(20))*time.Millisecond, dedup)
				case 2:
					msgs, _ := q.Dequeue(rng.IntN(3)+1, time.Duration(rng.IntN(20))*time.Millisecond)
					for _, m := range msgs {
						switch rng.IntN(3) {
						case 0:
							q.Ack(m.ID)
						case 1:
							q.Nack(m.ID)
						}
					}
				case 3:
					q.Peek(10)
					q.DLQ(10)
				default:
					q.Stats()
				}
			}
		}(uint64(w) + 1)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for running() {
			q.mu.Lock()
			checkInvariants(t, q, "under load")
			q.mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()

	q.mu.Lock()
	checkInvariants(t, q, "final")
	t.Logf("final state: ready=%d delayed=%d in-flight=%d dlq=%d indexed=%d",
		q.ready.Len(), q.delayed.Len(), q.inflight.Len(), len(q.dlq), len(q.byID))
	q.mu.Unlock()

	if s := q.Stats(); !s.Healthy {
		t.Errorf("queue went unhealthy under load: %s", s.Error)
	}
}
