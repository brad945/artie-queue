package demo

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// QueueDef is one of the composed queues the demo creates on startup.
//
// A queue's configuration is immutable — replay derives dead-lettering from
// max_attempts, so the same records must always produce the same state — which
// is why switching "mode" in the UI switches between pre-created queues rather
// than mutating one. Seeing all of them side by side is also the better demo:
// it is the same comparator, configured five ways.
type QueueDef struct {
	Name       string `json:"name"`
	Label      string `json:"label"`
	Ordering   string `json:"ordering"`
	Priority   bool   `json:"priority_enabled"`
	AgingMS    int    `json:"aging_interval_ms"`
	Comparator string `json:"comparator"`
}

// QueueDefs are the compositions the dashboard offers. Delay is per message,
// so every one of these is also its delayed variant.
var QueueDefs = []QueueDef{
	{"fifo", "FIFO", "fifo", false, 0, "seq ASC"},
	{"lifo", "LIFO", "lifo", false, 0, "seq DESC"},
	{"priority-fifo", "Priority → FIFO", "fifo", true, 0, "priority DESC, seq ASC"},
	{"priority-lifo", "Priority → LIFO", "lifo", true, 0, "priority DESC, seq DESC"},
	{"priority-fifo-aging", "Priority → FIFO + aging", "fifo", true, 800, "(priority + age/800ms) DESC, seq ASC"},
}

// taskNames give the job stream some texture. The queue neither knows nor
// cares what a payload means.
var taskNames = []string{
	"resize-image", "send-email", "reindex-doc", "charge-card",
	"transcode-video", "sync-webhook", "render-thumbnail", "export-report",
}

// WorkerView is one worker's state for the dashboard.
type WorkerView struct {
	ID        int    `json:"id"`
	State     string `json:"state"`
	Task      string `json:"task,omitempty"`
	MessageID string `json:"message_id,omitempty"`
	Priority  int    `json:"priority"`
	Attempts  int    `json:"attempts"`
	StartedMS int64  `json:"started_ms,omitempty"`
	TotalMS   int64  `json:"total_ms,omitempty"`
}

// Completion is a finished job, for the "done" column.
type Completion struct {
	Task     string `json:"task"`
	Priority int    `json:"priority"`
	Attempts int    `json:"attempts"`
	Worker   int    `json:"worker"`
	Outcome  string `json:"outcome"`
	Seq      uint64 `json:"seq"`
	AtMS     int64  `json:"at_ms"`
}

// Runner is the job runner: a worker pool plus the knobs the dashboard turns.
type Runner struct {
	client *Client
	events *ringWriter

	mu           sync.Mutex
	target       string
	desired      int
	failureRate  float64 // fraction of jobs that nack
	abandonRate  float64 // fraction that never ack, so the lease expires
	workMinMS    int
	workMaxMS    int
	visibilityMS int64
	workers      map[int]*WorkerView
	nextWorkerID int
	stops        map[int]chan struct{}
	wg           sync.WaitGroup

	done      int
	failed    int
	abandoned int
	submitted int
	recent    []Completion
}

// NewRunner returns a runner with sane demo defaults: short leases and short
// jobs, so redelivery and dead-lettering happen on a timescale you can watch.
func NewRunner(client *Client, events *ringWriter) *Runner {
	return &Runner{
		client:       client,
		events:       events,
		target:       QueueDefs[2].Name, // priority → FIFO: the most interesting default
		failureRate:  0.15,
		abandonRate:  0.05,
		workMinMS:    250,
		workMaxMS:    900,
		visibilityMS: 4000,
		workers:      make(map[int]*WorkerView),
		stops:        make(map[int]chan struct{}),
	}
}

// SetWorkers grows or shrinks the pool.
func (r *Runner) SetWorkers(n int) {
	if n < 0 {
		n = 0
	}
	if n > 32 {
		n = 32
	}
	r.mu.Lock()
	r.desired = n
	for len(r.stops) < n {
		id := r.nextWorkerID
		r.nextWorkerID++
		stop := make(chan struct{})
		r.stops[id] = stop
		r.workers[id] = &WorkerView{ID: id, State: "idle"}
		r.wg.Add(1)
		go r.workerLoop(id, stop)
	}
	for len(r.stops) > n {
		// Remove the highest id so the list stays stable in the UI.
		victim := -1
		for id := range r.stops {
			if id > victim {
				victim = id
			}
		}
		close(r.stops[victim])
		delete(r.stops, victim)
		delete(r.workers, victim)
	}
	r.mu.Unlock()
}

// Stop shuts the pool down.
func (r *Runner) Stop() {
	r.SetWorkers(0)
	r.wg.Wait()
}

// Config is the tunable state the dashboard shows and edits.
type Config struct {
	Target       string  `json:"target"`
	Workers      int     `json:"workers"`
	FailureRate  float64 `json:"failure_rate"`
	AbandonRate  float64 `json:"abandon_rate"`
	WorkMinMS    int     `json:"work_min_ms"`
	WorkMaxMS    int     `json:"work_max_ms"`
	VisibilityMS int64   `json:"visibility_ms"`
}

func (r *Runner) Config() Config {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Config{
		Target:       r.target,
		Workers:      r.desired,
		FailureRate:  r.failureRate,
		AbandonRate:  r.abandonRate,
		WorkMinMS:    r.workMinMS,
		WorkMaxMS:    r.workMaxMS,
		VisibilityMS: r.visibilityMS,
	}
}

// Update applies dashboard changes. Only non-nil fields are touched.
func (r *Runner) Update(target *string, workers *int, failure, abandon *float64, visibility *int64) {
	r.mu.Lock()
	if target != nil && *target != "" {
		if r.target != *target {
			r.events.add(fmt.Sprintf("switched consumers to queue %q", *target))
		}
		r.target = *target
	}
	if failure != nil {
		r.failureRate = clamp01(*failure)
	}
	if abandon != nil {
		r.abandonRate = clamp01(*abandon)
	}
	if visibility != nil && *visibility > 0 {
		r.visibilityMS = *visibility
	}
	r.mu.Unlock()

	if workers != nil {
		r.SetWorkers(*workers)
	}
}

func clamp01(f float64) float64 {
	if f < 0 {
		return 0
	}
	if f > 1 {
		return 1
	}
	return f
}

// Target is the queue the pool is currently consuming.
func (r *Runner) Target() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.target
}

// Submit enqueues n jobs. priority < 0 means "spread them across levels", which
// is what makes the ordering visible on the board.
func (r *Runner) Submit(queue string, n, priority int, delayMS int64, dedupID string) ([]EnqueueResult, error) {
	if n <= 0 {
		n = 1
	}
	if n > 500 {
		n = 500
	}
	out := make([]EnqueueResult, 0, n)
	for i := 0; i < n; i++ {
		p := priority
		if p < 0 {
			p = rand.IntN(5)
		}
		req := EnqueueRequest{
			Payload: map[string]any{
				"task": taskNames[rand.IntN(len(taskNames))],
				"n":    i + 1,
			},
			Priority: p,
			DelayMS:  delayMS,
			DedupID:  dedupID,
		}
		res, err := r.client.Enqueue(queue, req)
		if err != nil {
			return out, err
		}
		out = append(out, res)
	}
	r.mu.Lock()
	r.submitted += len(out)
	r.mu.Unlock()
	return out, nil
}

// Burst fires n enqueues from concurrent goroutines. This is the group-commit
// demo: with one writer the log does roughly one fsync per record, and with
// many the records-per-fsync number climbs while durability stays identical.
func (r *Runner) Burst(queue string, n, concurrency int) (int, time.Duration, error) {
	if n <= 0 {
		n = 200
	}
	if n > 5000 {
		n = 5000
	}
	if concurrency <= 0 {
		concurrency = 32
	}
	if concurrency > 128 {
		concurrency = 128
	}

	jobs := make(chan int, n)
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		ok    int
		first error
	)
	start := time.Now()
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				_, err := r.client.Enqueue(queue, EnqueueRequest{
					Payload:  map[string]any{"task": "burst", "n": i},
					Priority: rand.IntN(5),
				})
				mu.Lock()
				if err != nil && first == nil {
					first = err
				} else if err == nil {
					ok++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	r.mu.Lock()
	r.submitted += ok
	r.mu.Unlock()
	r.events.add(fmt.Sprintf("burst: %d durable enqueues from %d goroutines in %s", ok, concurrency, elapsed.Round(time.Millisecond)))
	return ok, elapsed, first
}

// workerLoop is one consumer: lease, work, then ack, nack, or walk away.
func (r *Runner) workerLoop(id int, stop chan struct{}) {
	defer r.wg.Done()
	for {
		select {
		case <-stop:
			return
		default:
		}

		r.mu.Lock()
		target := r.target
		vis := r.visibilityMS
		minMS, maxMS := r.workMinMS, r.workMaxMS
		failure, abandon := r.failureRate, r.abandonRate
		r.mu.Unlock()

		msgs, err := r.client.Dequeue(target, 1, vis)
		if err != nil {
			r.setWorker(id, &WorkerView{ID: id, State: "no server"})
			if sleepOrStop(stop, 400*time.Millisecond) {
				return
			}
			continue
		}
		if len(msgs) == 0 {
			r.setWorker(id, &WorkerView{ID: id, State: "idle"})
			if sleepOrStop(stop, 60*time.Millisecond) {
				return
			}
			continue
		}

		m := msgs[0]
		task := taskOf(m.Payload)
		dur := time.Duration(minMS+rand.IntN(max(1, maxMS-minMS))) * time.Millisecond
		r.setWorker(id, &WorkerView{
			ID: id, State: "working", Task: task, MessageID: m.ID,
			Priority: m.Priority, Attempts: m.Attempts,
			StartedMS: time.Now().UnixMilli(), TotalMS: dur.Milliseconds(),
		})
		if sleepOrStop(stop, dur) {
			return
		}

		roll := rand.Float64()
		switch {
		case roll < abandon:
			// Walk away without acking: the lease expires and the queue
			// redelivers. This is the crashed-consumer case.
			r.record(Completion{Task: task, Priority: m.Priority, Attempts: m.Attempts,
				Worker: id, Outcome: "abandoned", Seq: m.Seq, AtMS: time.Now().UnixMilli()})
			r.bump(&r.abandoned)
		case roll < abandon+failure:
			if err := r.client.Nack(target, m.ID); err == nil {
				r.record(Completion{Task: task, Priority: m.Priority, Attempts: m.Attempts,
					Worker: id, Outcome: "failed", Seq: m.Seq, AtMS: time.Now().UnixMilli()})
				r.bump(&r.failed)
			}
		default:
			if err := r.client.Ack(target, m.ID); err == nil {
				r.record(Completion{Task: task, Priority: m.Priority, Attempts: m.Attempts,
					Worker: id, Outcome: "done", Seq: m.Seq, AtMS: time.Now().UnixMilli()})
				r.bump(&r.done)
			}
		}
		r.setWorker(id, &WorkerView{ID: id, State: "idle"})
	}
}

func sleepOrStop(stop chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stop:
		return true
	case <-t.C:
		return false
	}
}

func taskOf(payload json.RawMessage) string {
	var p struct {
		Task string `json:"task"`
	}
	if err := json.Unmarshal(payload, &p); err == nil && p.Task != "" {
		return p.Task
	}
	return "job"
}

func (r *Runner) setWorker(id int, v *WorkerView) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.stops[id]; ok {
		r.workers[id] = v
	}
}

func (r *Runner) bump(counter *int) {
	r.mu.Lock()
	*counter++
	r.mu.Unlock()
}

func (r *Runner) record(c Completion) {
	r.mu.Lock()
	r.recent = append(r.recent, c)
	if len(r.recent) > 40 {
		r.recent = r.recent[len(r.recent)-40:]
	}
	r.mu.Unlock()
}

// RunnerState is the runner's slice of the dashboard snapshot.
type RunnerState struct {
	Config    Config       `json:"config"`
	Workers   []WorkerView `json:"workers"`
	Recent    []Completion `json:"recent"`
	Submitted int          `json:"submitted"`
	Done      int          `json:"done"`
	Failed    int          `json:"failed"`
	Abandoned int          `json:"abandoned"`
}

func (r *Runner) State() RunnerState {
	r.mu.Lock()
	defer r.mu.Unlock()
	workers := make([]WorkerView, 0, len(r.workers))
	for id := range r.stops {
		if v, ok := r.workers[id]; ok {
			workers = append(workers, *v)
		}
	}
	// Stable order so cards do not jump around between frames.
	for i := 1; i < len(workers); i++ {
		for j := i; j > 0 && workers[j].ID < workers[j-1].ID; j-- {
			workers[j], workers[j-1] = workers[j-1], workers[j]
		}
	}
	recent := make([]Completion, len(r.recent))
	copy(recent, r.recent)
	return RunnerState{
		Config: Config{
			Target: r.target, Workers: r.desired,
			FailureRate: r.failureRate, AbandonRate: r.abandonRate,
			WorkMinMS: r.workMinMS, WorkMaxMS: r.workMaxMS, VisibilityMS: r.visibilityMS,
		},
		Workers:   workers,
		Recent:    recent,
		Submitted: r.submitted,
		Done:      r.done,
		Failed:    r.failed,
		Abandoned: r.abandoned,
	}
}
