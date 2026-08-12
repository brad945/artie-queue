package queue

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
	"time"
)

// observed is the part of a message's state that must survive a restart.
type observed struct {
	Seq       uint64
	Priority  int
	Attempts  int
	State     string
	VisibleAt time.Time
	Payload   string
}

func observe(q *Queue) map[string]observed {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[string]observed, len(q.byID))
	for id, m := range q.byID {
		out[id] = observed{
			Seq:       m.Seq,
			Priority:  m.Priority,
			Attempts:  m.Attempts,
			State:     m.state.String(),
			VisibleAt: m.VisibleAt,
			Payload:   string(m.Payload),
		}
	}
	return out
}

// TestRandomizedOperationsSurviveRestart drives a queue through a few hundred
// pseudo-random operations, restarts it, and checks the recovered state
// against what was there before — message by message, field by field.
//
// The targeted recovery tests each assert one property. This one asserts that
// no *combination* of operations produces a state the log cannot reproduce,
// which is a different and harder claim. Seeds are fixed so a failure is
// reproducible.
//
// Long delays and long visibility timeouts are deliberate: they keep promotion
// and lease expiry from firing mid-run, so a mismatch means a real recovery
// bug rather than a timing coincidence.
func TestRandomizedOperationsSurviveRestart(t *testing.T) {
	configs := []Config{
		{Ordering: FIFO},
		{Ordering: LIFO},
		{Ordering: FIFO, PriorityEnabled: true, MaxAttempts: 3},
		{Ordering: LIFO, PriorityEnabled: true, MaxAttempts: 2},
	}

	for _, seed := range []uint64{1, 7, 42, 1337, 90210} {
		for ci, base := range configs {
			name := fmt.Sprintf("seed%d/cfg%d", seed, ci)
			t.Run(name, func(t *testing.T) {
				rng := rand.New(rand.NewPCG(seed, uint64(ci)+1))
				cfg := base
				cfg.Name = "model"
				q, root := mustQueue(t, cfg)

				const ops = 300
				for i := 0; i < ops; i++ {
					switch n := rng.IntN(100); {
					case n < 50:
						delay := time.Duration(0)
						if rng.IntN(5) == 0 {
							delay = time.Hour // stays delayed for the whole run
						}
						if _, err := q.Enqueue(
							[]byte(fmt.Sprintf("m%03d", i)),
							rng.IntN(5), delay, "",
						); err != nil {
							t.Fatalf("op %d enqueue: %v", i, err)
						}
					case n < 75:
						if _, err := q.Dequeue(1+rng.IntN(3), time.Hour); err != nil {
							t.Fatalf("op %d dequeue: %v", i, err)
						}
					case n < 90:
						if id := randomLeased(q, rng); id != "" {
							if err := q.Ack(id); err != nil {
								t.Fatalf("op %d ack: %v", i, err)
							}
						}
					default:
						if id := randomLeased(q, rng); id != "" {
							if err := q.Nack(id); err != nil {
								t.Fatalf("op %d nack: %v", i, err)
							}
						}
					}
				}

				before := observe(q)
				beforeSeq := q.Stats().NextSeq
				if len(before) == 0 {
					t.Fatal("the run left nothing in the queue; the test would prove nothing")
				}

				q2 := reopen(t, q, root)
				after := observe(q2)

				if len(after) != len(before) {
					t.Fatalf("recovered %d messages, want %d", len(after), len(before))
				}
				if got := q2.Stats().NextSeq; got != beforeSeq {
					t.Errorf("next seq = %d after restart, want %d", got, beforeSeq)
				}

				for id, want := range before {
					got, ok := after[id]
					if !ok {
						t.Fatalf("message %s (seq %d, state %s) was lost", id, want.Seq, want.State)
					}
					if got.Seq != want.Seq || got.Priority != want.Priority ||
						got.Payload != want.Payload || got.Attempts != want.Attempts {
						t.Fatalf("message %s changed across restart:\n before %+v\n after  %+v", id, want, got)
					}
					// Leases are deliberately not durable: an in-flight message
					// comes back ready, exactly as its lease expiring would have
					// left it. Everything else keeps its state.
					wantState := want.State
					if wantState == "in_flight" {
						wantState = "ready"
					}
					if got.State != wantState {
						t.Fatalf("message %s state %s -> %s, want %s", id, want.State, got.State, wantState)
					}
				}

				// Delivery order after recovery must match what the comparator
				// says it should be, computed independently of the heap.
				assertDeliveryOrderMatchesModel(t, q2, cfg)
			})
		}
	}
}

// randomLeased picks an in-flight message id, or "" if there are none.
func randomLeased(q *Queue, rng *rand.Rand) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.inflight.Len() == 0 {
		return ""
	}
	return q.inflight.items[rng.IntN(q.inflight.Len())].ID
}

// assertDeliveryOrderMatchesModel drains the queue and compares the order
// against a plain sort of the same messages — a model of the comparator
// written independently of the heap that implements it.
func assertDeliveryOrderMatchesModel(t *testing.T, q *Queue, cfg Config) {
	t.Helper()

	type entry struct {
		id       string
		seq      uint64
		priority int
	}
	var model []entry
	q.mu.Lock()
	for _, m := range q.ready.items {
		model = append(model, entry{m.ID, m.Seq, m.Priority})
	}
	q.mu.Unlock()

	sort.SliceStable(model, func(i, j int) bool {
		a, b := model[i], model[j]
		if cfg.PriorityEnabled && a.priority != b.priority {
			return a.priority > b.priority
		}
		if cfg.Ordering == LIFO {
			return a.seq > b.seq
		}
		return a.seq < b.seq
	})

	var got []string
	for {
		msgs, err := q.Dequeue(1, time.Hour)
		if err != nil {
			t.Fatalf("draining: %v", err)
		}
		if len(msgs) == 0 {
			break
		}
		got = append(got, msgs[0].ID)
	}

	if len(got) != len(model) {
		t.Fatalf("drained %d messages, model expected %d", len(got), len(model))
	}
	for i := range model {
		if got[i] != model[i].id {
			t.Fatalf("delivery order diverges from the comparator at position %d:\n got   seq of %s\n want  seq %d priority %d",
				i, got[i], model[i].seq, model[i].priority)
		}
	}
}

// A queue whose log has failed must stop accepting work rather than accept
// messages it cannot persist. Failing closed is the point.
func TestWALFailureMakesTheQueueRefuseWrites(t *testing.T) {
	q, _ := mustQueue(t, Config{Ordering: FIFO})
	if _, err := q.Enqueue([]byte("before"), 0, 0, ""); err != nil {
		t.Fatal(err)
	}

	// Simulate what the log does to the queue when a write or fsync fails.
	// (That the log itself latches this on a real I/O error is covered by
	// TestWriteFailureIsStickyAndFatal in the wal package, which can close the
	// descriptor out from under it.)
	q.mu.Lock()
	q.failed = errors.New("simulated fsync failure: input/output error")
	q.mu.Unlock()

	_, err := q.Enqueue([]byte("after"), 0, 0, "")
	if err == nil {
		t.Fatal("enqueue succeeded after the log failed")
	}
	t.Logf("enqueue correctly refused: %v", err)

	// Sticky: the queue does not recover on its own, and says so.
	if _, err := q.Enqueue([]byte("again"), 0, 0, ""); err == nil {
		t.Error("a second enqueue was accepted after a fatal log error")
	}
	st := q.Stats()
	if st.Healthy {
		t.Error("stats still report the queue as healthy")
	}
	if st.Error == "" {
		t.Error("stats do not explain why the queue is unhealthy")
	}

	// Reads still work, so an operator can see what is stuck.
	if st.Total == 0 {
		t.Error("the message enqueued before the failure is no longer visible")
	}
}
