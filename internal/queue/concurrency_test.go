package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// N producers, M consumers. The properties that must hold:
//   - every message that was accepted is delivered at least once
//   - no message is leased by two consumers at the same time
//   - counters add up
//
// Run under -race, which is where the interesting failures show up.
func TestConcurrentProducersAndConsumers(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO, PriorityEnabled: true, MaxAttempts: 10})

	const (
		producers = 8
		perProd   = 60
		consumers = 4
		total     = producers * perProd
	)

	var (
		mu       sync.Mutex
		enqueued = map[string]bool{}
		acked    = map[string]bool{}
		leased   = map[string]bool{} // currently held by some consumer
	)

	var prodWG sync.WaitGroup
	for p := 0; p < producers; p++ {
		prodWG.Add(1)
		go func(p int) {
			defer prodWG.Done()
			for i := 0; i < perProd; i++ {
				body := fmt.Sprintf("p%d-i%02d", p, i)
				res, err := q.Enqueue([]byte(body), i%3, 0, "")
				if err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
				mu.Lock()
				enqueued[res.ID] = true
				mu.Unlock()
			}
		}(p)
	}

	done := make(chan struct{})
	var consWG sync.WaitGroup
	for c := 0; c < consumers; c++ {
		consWG.Add(1)
		go func() {
			defer consWG.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				msgs, err := q.Dequeue(5, 30*time.Second)
				if err != nil {
					t.Errorf("dequeue: %v", err)
					return
				}
				if len(msgs) == 0 {
					time.Sleep(time.Millisecond)
					continue
				}
				for _, m := range msgs {
					mu.Lock()
					if leased[m.ID] {
						mu.Unlock()
						t.Errorf("message %s was leased by two consumers at once", m.ID)
						return
					}
					leased[m.ID] = true
					mu.Unlock()

					if err := q.Ack(m.ID); err != nil {
						t.Errorf("ack %s: %v", m.ID, err)
						return
					}

					mu.Lock()
					delete(leased, m.ID)
					if acked[m.ID] {
						mu.Unlock()
						t.Errorf("message %s was acked twice", m.ID)
						return
					}
					acked[m.ID] = true
					mu.Unlock()
				}
			}
		}()
	}

	prodWG.Wait()
	waitFor(t, 30*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(acked) == total
	})
	close(done)
	consWG.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(enqueued) != total {
		t.Fatalf("enqueued %d distinct ids, want %d", len(enqueued), total)
	}
	for id := range enqueued {
		if !acked[id] {
			t.Fatalf("message %s was accepted but never delivered", id)
		}
	}
	s := q.Stats()
	if s.Total != 0 {
		t.Errorf("queue not empty at the end: %+v", s)
	}
	if s.Counters.Enqueued != total || s.Counters.Acked != total {
		t.Errorf("counters: enqueued %d acked %d, want %d each", s.Counters.Enqueued, s.Counters.Acked, total)
	}
	t.Logf("%d messages through %d producers / %d consumers in %d fsyncs (%.1f records per fsync)",
		total, producers, consumers, s.Log.Fsyncs, s.Log.RecordsPerFsync)
}

// With one consumer, FIFO ordering must hold strictly even while many
// producers are appending concurrently: whatever order the mutex grants
// sequence numbers in, delivery follows it.
func TestFIFOOrderingHoldsUnderConcurrentProducers(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO})

	const producers, perProd = 6, 40
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProd; i++ {
				if _, err := q.Enqueue([]byte(fmt.Sprintf("p%d-%d", p, i)), 0, 0, ""); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	var lastSeq uint64
	seen := 0
	for seen < producers*perProd {
		msgs, err := q.Dequeue(7, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if m.Seq <= lastSeq {
				t.Fatalf("FIFO violated: seq %d delivered after %d", m.Seq, lastSeq)
			}
			lastSeq = m.Seq
			seen++
			if err := q.Ack(m.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// The same, for LIFO: a single consumer draining a queue that is not being
// written to must see strictly decreasing sequence numbers.
func TestLIFOOrderingHoldsAfterConcurrentProducers(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: LIFO})

	const producers, perProd = 6, 40
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProd; i++ {
				if _, err := q.Enqueue([]byte(fmt.Sprintf("p%d-%d", p, i)), 0, 0, ""); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	lastSeq := ^uint64(0)
	seen := 0
	for seen < producers*perProd {
		msgs, err := q.Dequeue(7, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if m.Seq >= lastSeq {
				t.Fatalf("LIFO violated: seq %d delivered after %d", m.Seq, lastSeq)
			}
			lastSeq = m.Seq
			seen++
			if err := q.Ack(m.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// Concurrent dedup: many producers retrying the same logical enqueue must
// produce exactly one message.
func TestConcurrentDedupAdmitsExactlyOne(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO})

	const racers = 32
	ids := make([]string, racers)
	dups := make([]bool, racers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := q.Enqueue([]byte("same"), 0, 0, "one-key")
			if err != nil {
				t.Errorf("enqueue: %v", err)
				return
			}
			ids[i] = res.ID
			dups[i] = res.Duplicate
		}(i)
	}
	close(start)
	wg.Wait()

	if s := q.Stats(); s.Total != 1 {
		t.Fatalf("queue holds %d messages, want exactly 1", s.Total)
	}
	accepted := 0
	for i := range ids {
		if !dups[i] {
			accepted++
		}
		if ids[i] != ids[0] {
			t.Fatalf("racer %d got id %s, want the single winner %s", i, ids[i], ids[0])
		}
	}
	if accepted != 1 {
		t.Errorf("%d enqueues reported as new, want exactly 1", accepted)
	}
}
