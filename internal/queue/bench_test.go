package queue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkEnqueue measures durable enqueues per second at increasing
// concurrency. Every message here is fsynced before its call returns, at every
// concurrency level — the guarantee does not change, only how many callers
// share each fsync.
//
// This is the number the README's group-commit claim rests on, so it is a
// benchmark rather than a paragraph:
//
//	go test ./internal/queue -bench Enqueue -benchtime 2000x
func BenchmarkEnqueue(b *testing.B) {
	for _, concurrency := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("producers-%d", concurrency), func(b *testing.B) {
			q, err := Create(b.TempDir(), Config{Name: "bench", Ordering: FIFO}, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer q.Close()

			payload := []byte(`{"task":"benchmark","n":0}`)
			var done atomic.Int64
			b.ResetTimer()

			var wg sync.WaitGroup
			per := b.N / concurrency
			extra := b.N % concurrency
			for c := 0; c < concurrency; c++ {
				n := per
				if c < extra {
					n++
				}
				wg.Add(1)
				go func(n int) {
					defer wg.Done()
					for i := 0; i < n; i++ {
						if _, err := q.Enqueue(payload, i%5, 0, ""); err != nil {
							b.Error(err)
							return
						}
						done.Add(1)
					}
				}(n)
			}
			wg.Wait()
			b.StopTimer()

			st := q.Stats()
			if st.Log.Fsyncs > 0 {
				b.ReportMetric(st.Log.RecordsPerFsync, "records/fsync")
				b.ReportMetric(float64(st.Log.AvgSyncUS), "µs/fsync")
			}
			if done.Load() != int64(b.N) {
				b.Fatalf("completed %d enqueues, expected %d", done.Load(), b.N)
			}
		})
	}
}

// BenchmarkDequeueAck measures the consume path: lease then acknowledge. The
// ack is durable, the dequeue is not, so this is roughly one fsync per message
// rather than two.
func BenchmarkDequeueAck(b *testing.B) {
	q, err := Create(b.TempDir(), Config{Name: "bench", Ordering: FIFO}, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()

	payload := []byte(`{"task":"benchmark"}`)
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(payload, 0, 0, ""); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs, err := q.Dequeue(1, time.Minute)
		if err != nil {
			b.Fatal(err)
		}
		if len(msgs) == 0 {
			b.Fatalf("queue drained early at %d of %d", i, b.N)
		}
		if err := q.Ack(msgs[0].ID); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkComparator isolates the ordering decision itself — the operation on
// the critical path of every heap push and pop, executed under the queue mutex.
func BenchmarkComparator(b *testing.B) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"fifo", Config{Ordering: FIFO}},
		{"lifo", Config{Ordering: LIFO}},
		{"priority-fifo", Config{Ordering: FIFO, PriorityEnabled: true}},
	}
	x := &Message{Seq: 100, Priority: 2, effPriority: 2}
	y := &Message{Seq: 200, Priority: 4, effPriority: 4}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			cfg := tc.cfg
			var sink bool
			for i := 0; i < b.N; i++ {
				sink = cfg.less(x, y)
			}
			if sink && false {
				b.Fatal("unreachable")
			}
		})
	}
}

// BenchmarkReplay measures startup: how long recovering a log of a given size
// takes, since that is the time a restart costs before the queue serves again.
func BenchmarkReplay(b *testing.B) {
	root := b.TempDir()
	q, err := Create(root, Config{Name: "bench", Ordering: FIFO, PriorityEnabled: true}, nil)
	if err != nil {
		b.Fatal(err)
	}
	const messages = 20000
	payload := []byte(`{"task":"benchmark","n":12345}`)
	// Build the log with concurrent producers: durability is identical, but
	// group commit means the fixture takes a second instead of a minute.
	var setup sync.WaitGroup
	for w := 0; w < 32; w++ {
		setup.Add(1)
		go func(w int) {
			defer setup.Done()
			for i := w; i < messages; i += 32 {
				if _, err := q.Enqueue(payload, i%5, 0, ""); err != nil {
					b.Error(err)
					return
				}
			}
		}(w)
	}
	setup.Wait()
	size := q.Stats().Log.SizeBytes
	if err := q.Close(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loaded, err := Load(root, "bench", nil)
		if err != nil {
			b.Fatal(err)
		}
		if loaded.Stats().Ready != messages {
			b.Fatalf("recovered %d messages, want %d", loaded.Stats().Ready, messages)
		}
		loaded.Close()
	}
	b.StopTimer()
	b.ReportMetric(float64(messages), "messages/op")
	b.ReportMetric(float64(size)/float64(messages), "bytes/message")
}
