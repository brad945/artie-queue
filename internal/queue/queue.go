package queue

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/brad945/artie-queue/internal/wal"
)

var (
	// ErrNotFound means no message with that id exists in this queue.
	ErrNotFound = errors.New("message not found")
	// ErrNotInFlight means the message exists but is not currently leased, so
	// there is nothing to ack or nack.
	ErrNotInFlight = errors.New("message is not in flight")
	// ErrClosed means the queue has been shut down.
	ErrClosed = errors.New("queue is closed")
	// ErrIncompleteQueue means a queue directory holds a log with no records
	// at all — a create that was interrupted before it committed anything.
	// It is not corruption: there is no data, so there is nothing to lose.
	ErrIncompleteQueue = errors.New("queue was never fully created")
)

const (
	logFileName = "wal.log"
	// dedupPurgeInterval bounds how long an expired dedup entry occupies
	// memory. Purging is an O(n) scan of the dedup map, so it runs on a fixed
	// interval rather than on every timer tick.
	dedupPurgeInterval = 30 * time.Second
	// defaultCompactThreshold is the log size at which the background loop
	// snapshots live state into a fresh file.
	defaultCompactThreshold = 8 << 20
)

// Queue is one composable queue: a delayed heap, a ready heap ordered by the
// configured comparator, a set of in-flight leases, a dead-letter list, and
// the append-only log that all of it is rebuilt from.
//
// Locking: one mutex guards the whole index. At this scale that is the right
// call — the critical sections are heap operations and map writes measured in
// hundreds of nanoseconds, and the expensive part of the write path (fsync)
// deliberately happens outside the lock.
//
// The rule that makes that safe: *append to the log while holding the mutex,
// wait for durability after releasing it*. Appending under the lock keeps log
// order identical to state-transition order, which is what makes replay
// reproduce exactly the state the crash interrupted. Waiting outside the lock
// is what lets concurrent writers collect into one fsync.
type Queue struct {
	cfg  Config
	dir  string
	path string
	log  *wal.Log
	logf func(string, ...any)

	mu        sync.Mutex
	ready     readyHeap
	delayed   delayedHeap
	inflight  leaseHeap
	byID      map[string]*Message
	dlq       []*Message
	dedup     map[string]dedupEntry
	nextSeq   uint64
	failed    error
	lastAge   time.Time
	lastPurge time.Time
	counters  Counters
	closed    bool

	compactThreshold int64
	// compactFloor stops compaction from thrashing. A queue whose *live* state
	// is already larger than the threshold would otherwise re-compact on every
	// timer tick forever, rewriting the whole log each time and reclaiming
	// nothing. After each compaction the floor is set to twice the resulting
	// size, so the log has to genuinely double before it is worth rewriting
	// again — which bounds write amplification to a constant factor.
	compactFloor int64

	wake      chan struct{}
	done      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// Counters are lifetime totals for the stats endpoint.
type Counters struct {
	Enqueued     uint64 `json:"enqueued"`
	Dequeued     uint64 `json:"dequeued"`
	Acked        uint64 `json:"acked"`
	Nacked       uint64 `json:"nacked"`
	Expired      uint64 `json:"lease_expired"`
	DeadLettered uint64 `json:"dead_lettered"`
	Duplicates   uint64 `json:"duplicates_rejected"`
	Compactions  uint64 `json:"compactions"`
}

func newQueue(root string, logf func(string, ...any)) *Queue {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	q := &Queue{
		dir:              root,
		logf:             logf,
		byID:             make(map[string]*Message),
		dedup:            make(map[string]dedupEntry),
		nextSeq:          1,
		compactThreshold: defaultCompactThreshold,
		wake:             make(chan struct{}, 1),
		done:             make(chan struct{}),
	}
	q.ready.cfg = &q.cfg
	return q
}

// Create makes a new queue directory and writes its META record. It fails if
// the queue already exists.
func Create(root string, cfg Config, logf func(string, ...any)) (*Queue, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	dir := filepath.Join(root, cfg.Name)
	path := filepath.Join(dir, logFileName)

	// Decide "does this queue already exist?" by asking whether its log holds
	// any committed record — the same question Load asks — rather than whether
	// a file is present. A create interrupted part-way through writing its
	// META record leaves a non-empty file containing nothing committed; if
	// that counted as "exists", the name would be unusable forever while
	// startup simultaneously skipped it as incomplete.
	if _, err := os.Stat(path); err == nil {
		res, err := wal.Replay(path, func(wal.Record) error { return nil })
		if err != nil {
			// Corruption. Do not overwrite it — that would destroy whatever is
			// in there. Surface it so an operator can look.
			return nil, fmt.Errorf("queue %q already has a log that cannot be read: %w", cfg.Name, err)
		}
		if res.Records > 0 {
			return nil, fmt.Errorf("%w: %q", ErrQueueExists, cfg.Name)
		}
		// No committed records: the previous create never finished. Starting
		// over loses nothing, because nothing was ever acknowledged.
	}
	q := newQueue(dir, logf)
	q.cfg = cfg
	q.path = path

	// Opening at size 0 truncates any incomplete prefix left by the
	// interrupted create above.
	log, err := wal.Open(q.path, 0)
	if err != nil {
		return nil, err
	}
	q.log = log

	meta, err := encodeConfig(cfg)
	if err != nil {
		log.Close()
		return nil, err
	}
	batch, err := q.log.Append(wal.TypeMeta, meta)
	if err != nil {
		log.Close()
		return nil, err
	}
	if err := q.log.Wait(batch); err != nil {
		log.Close()
		return nil, err
	}
	q.start()
	return q, nil
}

// Load replays an existing queue's log and reopens it for appending.
//
// Any corruption reported by Replay is returned as-is: startup fails, loudly,
// naming the byte offset. A torn tail is truncated with a warning, because a
// record that was never fully written was also never acknowledged to a client.
func Load(root, name string, logf func(string, ...any)) (*Queue, error) {
	dir := filepath.Join(root, name)
	q := newQueue(dir, logf)
	q.path = filepath.Join(dir, logFileName)

	var sawMeta bool
	res, err := wal.Replay(q.path, func(rec wal.Record) error {
		if rec.Type == wal.TypeMeta {
			sawMeta = true
		} else if !sawMeta {
			return errors.New("first record is not META")
		}
		return q.apply(rec)
	})
	if err != nil {
		return nil, err
	}
	if !sawMeta {
		if res.Records == 0 {
			// The log exists but contains nothing: a crash between creating
			// the file and committing its META record. No record was ever
			// written, so nothing was ever acknowledged and there is nothing
			// to lose. Report it distinctly so startup can carry on without
			// this queue instead of refusing to boot at all — the alternative
			// is one interrupted create bricking every other queue on the box.
			return nil, fmt.Errorf("%w: %s", ErrIncompleteQueue, q.path)
		}
		return nil, fmt.Errorf("queue %q: log %s has records but no META record", name, q.path)
	}
	if res.Torn != nil {
		q.logf("WARN queue %q: torn record at end of log (offset %d, %d bytes discarded); "+
			"this is the expected artifact of a crash mid-write and no client was ever told it committed",
			name, res.Torn.Offset, res.Torn.Bytes)
	}
	q.logf("queue %q: replayed %d records (%d bytes); %d ready, %d delayed, %d dead-lettered, next seq %d",
		name, res.Records, res.ValidBytes, q.ready.Len(), q.delayed.Len(), len(q.dlq), q.nextSeq)

	log, err := wal.Open(q.path, res.ValidBytes)
	if err != nil {
		return nil, err
	}
	q.log = log
	q.start()
	return q, nil
}

func (q *Queue) start() {
	q.lastAge = time.Now()
	q.lastPurge = time.Now()
	q.wg.Add(1)
	go q.run()
}

// Config returns the queue's immutable configuration.
func (q *Queue) Config() Config { return q.cfg }

// SetCompactThreshold overrides the log size that triggers compaction. Used by
// the demo (to make compaction observable) and by tests.
func (q *Queue) SetCompactThreshold(n int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.compactThreshold = n
}

// ---------------------------------------------------------------------------
// State transitions.
//
// Every mutation of queue state goes through one of the apply* methods below,
// and they are the only place state changes. The live write path calls them
// directly; replay calls them via apply(Record). One implementation, two entry
// points — so a recovered queue cannot drift from a running one, because there
// is no second copy of the rules to drift from.
// ---------------------------------------------------------------------------

func (q *Queue) apply(rec wal.Record) error {
	switch rec.Type {
	case wal.TypeMeta:
		cfg, err := decodeConfig(rec.Payload)
		if err != nil {
			return err
		}
		if err := cfg.Validate(); err != nil {
			return err
		}
		q.cfg = cfg
		if cfg.NextSeq > q.nextSeq {
			q.nextSeq = cfg.NextSeq
		}
		return nil
	case wal.TypeEnqueue:
		m, err := decodeMessage(rec.Payload)
		if err != nil {
			return err
		}
		return q.applyEnqueue(m, time.Now())
	case wal.TypeAck:
		return q.applyAck(string(rec.Payload))
	case wal.TypeNack, wal.TypeLeaseExpire:
		_, err := q.applyAttempt(string(rec.Payload), time.Now())
		return err
	case wal.TypeDedup:
		e, err := decodeDedup(rec.Payload)
		if err != nil {
			return err
		}
		q.dedup[e.DedupID] = e
		return nil
	}
	return fmt.Errorf("queue: unhandled record type %s", rec.Type)
}

func (q *Queue) applyEnqueue(m *Message, now time.Time) error {
	if _, exists := q.byID[m.ID]; exists {
		return fmt.Errorf("duplicate message id %q", m.ID)
	}
	if m.Seq >= q.nextSeq {
		q.nextSeq = m.Seq + 1
	}
	q.byID[m.ID] = m
	if m.DedupID != "" {
		q.dedup[m.DedupID] = dedupEntry{
			DedupID:   m.DedupID,
			MessageID: m.ID,
			ExpiresAt: m.CreatedAt.Add(q.cfg.dedupWindow()),
		}
	}
	if m.state == stateDead {
		m.index = -1
		q.dlq = append(q.dlq, m)
		return nil
	}
	q.place(m, now)
	return nil
}

// place routes a live message to the delayed heap or the ready heap. This is
// the only gate delay needs: a delayed message is simply not in the structure
// that dequeue looks at.
func (q *Queue) place(m *Message, now time.Time) {
	if m.VisibleAt.After(now) {
		m.state = stateDelayed
		heap.Push(&q.delayed, m)
		return
	}
	m.state = stateReady
	m.effPriority = q.effectivePriority(m, now)
	heap.Push(&q.ready, m)
}

func (q *Queue) applyAck(id string) error {
	m, ok := q.byID[id]
	if !ok {
		// Reachable only from replay, and only if the log is internally
		// inconsistent: the live path checks existence before it writes an ACK.
		// Refuse rather than shrug.
		return fmt.Errorf("ack for unknown message %q", id)
	}
	q.detach(m)
	delete(q.byID, id)
	return nil
}

// applyAttempt handles both nack and lease expiry: they are the same state
// transition (a delivery that did not succeed), differing only in what caused
// them, which the record type already records. It reports whether the message
// crossed into the dead-letter queue.
//
// Counters are deliberately *not* touched here. These functions run during
// replay as well as on the live path, and a counter incremented in both places
// would mean different things depending on whether the log had been compacted
// since — a compacted snapshot restores dead-lettered messages as plain
// records, so replaying it would count zero dead-letterings for the same
// state. Counters live in the callers and mean "since this process started".
func (q *Queue) applyAttempt(id string, now time.Time) (deadLettered bool, err error) {
	m, ok := q.byID[id]
	if !ok {
		return false, fmt.Errorf("attempt record for unknown message %q", id)
	}
	q.detach(m)
	m.Attempts++
	if m.Attempts >= q.cfg.MaxAttempts {
		m.state = stateDead
		m.index = -1
		q.dlq = append(q.dlq, m)
		return true, nil
	}
	// Requeued immediately, keeping its original Seq so it returns to its
	// place in the ordering rather than to the back of the queue.
	m.VisibleAt = now
	q.place(m, now)
	return false, nil
}

// detach removes a message from whichever structure currently holds it.
func (q *Queue) detach(m *Message) {
	switch m.state {
	case stateReady:
		if m.index >= 0 && m.index < q.ready.Len() {
			heap.Remove(&q.ready, m.index)
		}
	case stateDelayed:
		if m.index >= 0 && m.index < q.delayed.Len() {
			heap.Remove(&q.delayed, m.index)
		}
	case stateInFlight:
		if m.index >= 0 && m.index < q.inflight.Len() {
			heap.Remove(&q.inflight, m.index)
		}
	case stateDead:
		for i, d := range q.dlq {
			if d == m {
				q.dlq = append(q.dlq[:i], q.dlq[i+1:]...)
				break
			}
		}
	}
	m.index = -1
}

func (q *Queue) effectivePriority(m *Message, now time.Time) int {
	iv := q.cfg.agingInterval()
	if iv <= 0 || !q.cfg.PriorityEnabled {
		return m.Priority
	}
	boost := int(now.Sub(m.CreatedAt) / iv)
	if boost > q.cfg.AgingMaxBoost {
		boost = q.cfg.AgingMaxBoost
	}
	if boost < 0 {
		boost = 0
	}
	return m.Priority + boost
}

// ---------------------------------------------------------------------------
// Public operations.
// ---------------------------------------------------------------------------

// EnqueueResult reports what happened to an enqueue.
type EnqueueResult struct {
	ID        string    `json:"id"`
	Seq       uint64    `json:"seq"`
	VisibleAt time.Time `json:"visible_at"`
	Duplicate bool      `json:"duplicate"`
}

// Enqueue appends a message and returns once it is durably on disk.
//
// A duplicate DedupID inside the dedup window is not an error: it returns the
// original message's id with Duplicate set. A producer retrying after a
// timeout wants idempotence, not a 409 to handle.
func (q *Queue) Enqueue(payload []byte, priority int, delay time.Duration, dedupID string) (EnqueueResult, error) {
	q.mu.Lock()
	if err := q.checkLocked(); err != nil {
		q.mu.Unlock()
		return EnqueueResult{}, err
	}
	now := time.Now()
	if dedupID != "" {
		if e, ok := q.dedup[dedupID]; ok && now.Before(e.ExpiresAt) {
			q.counters.Duplicates++
			q.mu.Unlock()
			return EnqueueResult{ID: e.MessageID, Duplicate: true}, nil
		}
	}
	m := &Message{
		ID:        newID(),
		DedupID:   dedupID,
		Seq:       q.nextSeq,
		Priority:  priority,
		VisibleAt: now.Add(delay),
		CreatedAt: now,
		Payload:   append([]byte(nil), payload...),
	}
	batch, err := q.log.Append(wal.TypeEnqueue, encodeMessage(m))
	if err != nil {
		q.failed = err
		q.mu.Unlock()
		return EnqueueResult{}, err
	}
	if err := q.applyEnqueue(m, now); err != nil {
		q.mu.Unlock()
		return EnqueueResult{}, err
	}
	q.counters.Enqueued++
	res := EnqueueResult{ID: m.ID, Seq: m.Seq, VisibleAt: m.VisibleAt}
	q.mu.Unlock()

	// Durability wait happens with the lock released: that is what allows
	// every concurrent enqueue to share one fsync.
	if err := q.waitDurable(batch); err != nil {
		return EnqueueResult{}, err
	}
	q.signal()
	return res, nil
}

// Dequeue leases up to max messages. Leased messages are invisible to other
// consumers until they are acked, nacked, or their visibility deadline passes.
//
// Dequeue writes nothing to the log. A lease is not durable state: if the
// process dies, every in-flight message correctly reappears as ready on
// restart, which is exactly what lease expiry would have done anyway. The
// consequence — an attempt count can under-count by one per server crash — is
// cheaper than an fsync on the read path.
func (q *Queue) Dequeue(max int, visibility time.Duration) ([]*Message, error) {
	if max <= 0 {
		max = 1
	}
	q.mu.Lock()
	if err := q.checkLocked(); err != nil {
		q.mu.Unlock()
		return nil, err
	}
	now := time.Now()
	// Promote anything already due. The background timer normally does this;
	// doing it here too closes the window between a deadline passing and the
	// timer goroutine being scheduled, so "visible at T" means dequeuable at T
	// rather than at T plus however long the scheduler took. It is an O(1)
	// check of the delayed heap's head, not a scan.
	q.promoteDue(now)

	if visibility <= 0 {
		visibility = q.cfg.visibilityTimeout()
	}
	out := make([]*Message, 0, max)
	for len(out) < max && q.ready.Len() > 0 {
		m := heap.Pop(&q.ready).(*Message)
		m.state = stateInFlight
		m.deadline = now.Add(visibility)
		heap.Push(&q.inflight, m)
		out = append(out, m.clone())
	}
	q.counters.Dequeued += uint64(len(out))
	q.mu.Unlock()

	if len(out) > 0 {
		q.signal() // a new lease deadline may be the next timer event
	}
	return out, nil
}

// Ack permanently deletes a leased message. Acking a dead-lettered message
// purges it from the DLQ.
func (q *Queue) Ack(id string) error {
	q.mu.Lock()
	if err := q.checkLocked(); err != nil {
		q.mu.Unlock()
		return err
	}
	m, ok := q.byID[id]
	if !ok {
		q.mu.Unlock()
		return ErrNotFound
	}
	if m.state != stateInFlight && m.state != stateDead {
		q.mu.Unlock()
		return ErrNotInFlight
	}
	batch, err := q.log.Append(wal.TypeAck, []byte(id))
	if err != nil {
		q.failed = err
		q.mu.Unlock()
		return err
	}
	if err := q.applyAck(id); err != nil {
		q.mu.Unlock()
		return err
	}
	q.counters.Acked++
	q.mu.Unlock()
	return q.waitDurable(batch)
}

// Nack returns a leased message immediately, incrementing its attempt count.
func (q *Queue) Nack(id string) error {
	q.mu.Lock()
	if err := q.checkLocked(); err != nil {
		q.mu.Unlock()
		return err
	}
	m, ok := q.byID[id]
	if !ok {
		q.mu.Unlock()
		return ErrNotFound
	}
	if m.state != stateInFlight {
		q.mu.Unlock()
		return ErrNotInFlight
	}
	batch, err := q.log.Append(wal.TypeNack, []byte(id))
	if err != nil {
		q.failed = err
		q.mu.Unlock()
		return err
	}
	dead, err := q.applyAttempt(id, time.Now())
	if err != nil {
		q.mu.Unlock()
		return err
	}
	q.counters.Nacked++
	if dead {
		q.counters.DeadLettered++
	}
	q.mu.Unlock()

	if err := q.waitDurable(batch); err != nil {
		return err
	}
	q.signal()
	return nil
}

// ---------------------------------------------------------------------------
// Background timer: promotion, lease expiry, aging, dedup purge, compaction.
// ---------------------------------------------------------------------------

func (q *Queue) run() {
	defer q.wg.Done()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		next := q.tick()

		d := time.Hour
		if !next.IsZero() {
			if d = time.Until(next); d < time.Millisecond {
				d = time.Millisecond
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(d)

		select {
		case <-q.wake:
		case <-timer.C:
		case <-q.done:
			return
		}
	}
}

// tick performs everything that has come due and returns the time of the next
// scheduled event, or the zero time if there is nothing pending. Nothing here
// polls: each deadline comes from the head of a heap.
func (q *Queue) tick() time.Time {
	q.mu.Lock()
	if q.failed != nil || q.closed {
		// A queue whose log has failed has nothing left to schedule. Without
		// this the expired-lease loop below would fail to append, break, and
		// leave an already-past deadline as the next wakeup — a 1ms spin for
		// as long as the process lives.
		q.mu.Unlock()
		return time.Time{}
	}
	now := time.Now()
	var maxBatch uint64

	q.promoteDue(now)

	// Expire leases whose deadline has passed. Each expiry is logged, because
	// unlike the lease itself the attempt count is durable state that decides
	// whether a message eventually dead-letters.
	for q.inflight.Len() > 0 && !q.inflight.items[0].deadline.After(now) {
		m := q.inflight.items[0]
		batch, err := q.log.Append(wal.TypeLeaseExpire, []byte(m.ID))
		if err != nil {
			q.failed = err
			break
		}
		if batch > maxBatch {
			maxBatch = batch
		}
		dead, err := q.applyAttempt(m.ID, now)
		if err != nil {
			q.failed = err
			break
		}
		q.counters.Expired++
		if dead {
			q.counters.DeadLettered++
		}
	}

	q.agePriorities(now)

	if now.Sub(q.lastPurge) >= dedupPurgeInterval {
		for k, e := range q.dedup {
			if !now.Before(e.ExpiresAt) {
				delete(q.dedup, k)
			}
		}
		q.lastPurge = now
	}

	size := q.log.Size()
	shouldCompact := q.failed == nil && !q.closed &&
		size >= q.compactThreshold && size >= q.compactFloor
	next := q.nextDeadlineLocked(now)
	q.mu.Unlock()

	if maxBatch > 0 {
		// Durability of expiry records, again outside the lock.
		_ = q.waitDurable(maxBatch)
	}
	if shouldCompact {
		if err := q.Compact(); err != nil {
			q.logf("WARN queue %q: compaction failed: %v", q.cfg.Name, err)
		}
	}
	return next
}

// promoteDue moves every message whose VisibleAt has arrived from the delayed
// heap to the ready heap. It looks only at the head of the delayed heap, so it
// costs O(1) when nothing is due.
func (q *Queue) promoteDue(now time.Time) {
	for q.delayed.Len() > 0 && !q.delayed.items[0].VisibleAt.After(now) {
		m := heap.Pop(&q.delayed).(*Message)
		m.state = stateReady
		m.effPriority = q.effectivePriority(m, now)
		heap.Push(&q.ready, m)
	}
}

// agePriorities implements the optional anti-starvation boost.
//
// A comparator that reads the clock would silently invalidate the heap: the
// ordering between two elements could change while they sit in it, and the
// heap invariant is only maintained at push and pop. So the boost is
// materialised into a stored field on a fixed tick and the heap is rebuilt in
// O(n) — the comparator itself stays a pure function of stored fields, and is
// therefore stable between ticks.
func (q *Queue) agePriorities(now time.Time) {
	iv := q.cfg.agingInterval()
	if iv <= 0 || q.ready.Len() == 0 || now.Sub(q.lastAge) < iv {
		return
	}
	q.lastAge = now
	changed := false
	for _, m := range q.ready.items {
		if eff := q.effectivePriority(m, now); eff != m.effPriority {
			m.effPriority = eff
			changed = true
		}
	}
	if changed {
		heap.Init(&q.ready)
	}
}

func (q *Queue) nextDeadlineLocked(now time.Time) time.Time {
	var next time.Time
	consider := func(t time.Time) {
		if t.IsZero() {
			return
		}
		if next.IsZero() || t.Before(next) {
			next = t
		}
	}
	if q.delayed.Len() > 0 {
		consider(q.delayed.items[0].VisibleAt)
	}
	if q.inflight.Len() > 0 {
		consider(q.inflight.items[0].deadline)
	}
	if iv := q.cfg.agingInterval(); iv > 0 && q.ready.Len() > 0 {
		consider(q.lastAge.Add(iv))
	}
	if len(q.dedup) > 0 {
		consider(q.lastPurge.Add(dedupPurgeInterval))
	}
	return next
}

func (q *Queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *Queue) checkLocked() error {
	if q.closed {
		return ErrClosed
	}
	return q.failed
}

func (q *Queue) waitDurable(batch uint64) error {
	if err := q.log.Wait(batch); err != nil {
		q.mu.Lock()
		if q.failed == nil {
			q.failed = err
			q.logf("FATAL queue %q: write-ahead log failure, queue is now read-only: %v", q.cfg.Name, err)
		}
		q.mu.Unlock()
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Compaction.
// ---------------------------------------------------------------------------

// Compact rewrites the log as a snapshot of live state: config, every message
// still in the system with its current attempt count, and any dedup entry
// whose message is already gone. Write to a temp file, fsync it, atomically
// rename it over the live log, fsync the directory, then swap the descriptor.
// A crash at any point in that sequence leaves either the whole old log or the
// whole new one.
//
// It runs under the queue mutex, so writes pause for its duration. That is the
// honest simple choice at this scale; the note in the README on where it
// breaks (and what an online snapshot would look like) is the interesting part.
func (q *Queue) Compact() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.checkLocked(); err != nil {
		return err
	}
	// Everything buffered must be on disk before we replace the file, or we
	// would drop records that a client has already been told are durable.
	if err := q.log.Sync(); err != nil {
		q.failed = err
		return err
	}

	before := q.log.Size()
	tmp := q.path + ".compact"
	w, err := wal.NewWriter(tmp)
	if err != nil {
		return err
	}

	cfg := q.cfg
	cfg.NextSeq = q.nextSeq // must survive: compacting an empty queue would
	// otherwise reset Seq and reorder future messages
	meta, err := encodeConfig(cfg)
	if err != nil {
		w.Abort()
		return err
	}
	if err := w.Append(wal.TypeMeta, meta); err != nil {
		w.Abort()
		return err
	}

	live := make([]*Message, 0, len(q.byID))
	for _, m := range q.byID {
		live = append(live, m)
	}
	// Deterministic output: same state always produces the same snapshot bytes.
	sort.Slice(live, func(i, j int) bool { return live[i].Seq < live[j].Seq })
	for _, m := range live {
		// In-flight messages are snapshotted as ordinary live messages. If we
		// crash now they come back ready, which is precisely what their lease
		// expiring would have done.
		if err := w.Append(wal.TypeEnqueue, encodeMessage(m)); err != nil {
			w.Abort()
			return err
		}
	}

	now := time.Now()
	orphans := make([]dedupEntry, 0)
	for _, e := range q.dedup {
		if !now.Before(e.ExpiresAt) {
			continue // window closed, let it go
		}
		if _, stillLive := q.byID[e.MessageID]; stillLive {
			continue // rebuilt from the message's own ENQUEUE record
		}
		orphans = append(orphans, e)
	}
	sort.Slice(orphans, func(i, j int) bool { return orphans[i].DedupID < orphans[j].DedupID })
	for _, e := range orphans {
		if err := w.Append(wal.TypeDedup, encodeDedup(e)); err != nil {
			w.Abort()
			return err
		}
	}

	size := w.Size()
	if err := w.CommitAs(q.path); err != nil {
		w.Abort()
		return err
	}
	if err := q.log.Reopen(size); err != nil {
		q.failed = err
		return err
	}
	// The log must double before another automatic compaction is worth doing.
	q.compactFloor = 2 * size
	q.counters.Compactions++
	q.logf("queue %q: compacted log %d -> %d bytes (%d live messages, %d dedup entries)",
		q.cfg.Name, before, size, len(live), len(orphans))
	return nil
}

// ---------------------------------------------------------------------------
// Inspection.
// ---------------------------------------------------------------------------

// Stats is the depth/in-flight/delayed/DLQ/oldest-age view.
type Stats struct {
	Name            string   `json:"name"`
	Ordering        Ordering `json:"ordering"`
	PriorityEnabled bool     `json:"priority_enabled"`
	MaxAttempts     int      `json:"max_attempts"`
	AgingIntervalMS int      `json:"aging_interval_ms"`

	Ready        int    `json:"ready"`
	Delayed      int    `json:"delayed"`
	InFlight     int    `json:"in_flight"`
	DeadLettered int    `json:"dlq"`
	Total        int    `json:"total"`
	OldestAgeMS  int64  `json:"oldest_age_ms"`
	NextSeq      uint64 `json:"next_seq"`

	Counters Counters  `json:"counters"`
	Log      LogStats  `json:"log"`
	Healthy  bool      `json:"healthy"`
	Error    string    `json:"error,omitempty"`
	Now      time.Time `json:"now"`
}

// LogStats exposes the write path, including how well group commit is working.
type LogStats struct {
	SizeBytes       int64   `json:"size_bytes"`
	Records         uint64  `json:"records"`
	Fsyncs          uint64  `json:"fsyncs"`
	RecordsPerFsync float64 `json:"records_per_fsync"`
	LastBatch       int     `json:"last_batch"`
	MaxBatch        int     `json:"max_batch"`
	LastSyncUS      int64   `json:"last_sync_us"`
	AvgSyncUS       int64   `json:"avg_sync_us"`
	PendingBytes    int     `json:"pending_bytes"`
}

// Stats returns a consistent snapshot.
//
// Oldest-message age is an O(n) scan of live messages. It is the one number
// here that is not O(1), and the fix if it ever mattered is a fourth heap
// keyed by CreatedAt; at the depths this queue is built for, a scan on a stats
// call is cheaper than maintaining that heap on every enqueue.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()

	s := Stats{
		Name:            q.cfg.Name,
		Ordering:        q.cfg.Ordering,
		PriorityEnabled: q.cfg.PriorityEnabled,
		MaxAttempts:     q.cfg.MaxAttempts,
		AgingIntervalMS: q.cfg.AgingIntervalMS,
		Ready:           q.ready.Len(),
		Delayed:         q.delayed.Len(),
		InFlight:        q.inflight.Len(),
		DeadLettered:    len(q.dlq),
		Total:           len(q.byID),
		NextSeq:         q.nextSeq,
		Counters:        q.counters,
		Healthy:         q.failed == nil && !q.closed,
		Now:             now,
	}
	if q.failed != nil {
		s.Error = q.failed.Error()
	}

	var oldest time.Time
	for _, m := range q.byID {
		if m.state == stateDead {
			continue
		}
		if oldest.IsZero() || m.CreatedAt.Before(oldest) {
			oldest = m.CreatedAt
		}
	}
	if !oldest.IsZero() {
		s.OldestAgeMS = now.Sub(oldest).Milliseconds()
	}

	ls := q.log.Stats()
	s.Log = LogStats{
		SizeBytes:    ls.SizeBytes,
		Records:      ls.Records,
		Fsyncs:       ls.Batches,
		LastBatch:    ls.LastBatch,
		MaxBatch:     ls.MaxBatch,
		LastSyncUS:   ls.LastSync.Microseconds(),
		PendingBytes: ls.PendingBytes,
	}
	if ls.Batches > 0 {
		s.Log.RecordsPerFsync = float64(ls.Records) / float64(ls.Batches)
		s.Log.AvgSyncUS = (ls.TotalSync / time.Duration(ls.Batches)).Microseconds()
	}
	return s
}

// View is a read-only projection of a message for inspection endpoints.
type View struct {
	ID        string          `json:"id"`
	DedupID   string          `json:"dedup_id,omitempty"`
	Seq       uint64          `json:"seq"`
	Priority  int             `json:"priority"`
	EffPrio   int             `json:"effective_priority"`
	Attempts  int             `json:"attempts"`
	State     string          `json:"state"`
	VisibleAt time.Time       `json:"visible_at"`
	CreatedAt time.Time       `json:"created_at"`
	Deadline  time.Time       `json:"deadline,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

func (m *Message) view() View {
	v := View{
		ID:        m.ID,
		DedupID:   m.DedupID,
		Seq:       m.Seq,
		Priority:  m.Priority,
		EffPrio:   m.effPriority,
		Attempts:  m.Attempts,
		State:     m.state.String(),
		VisibleAt: m.VisibleAt,
		CreatedAt: m.CreatedAt,
		Payload:   PayloadJSON(m.Payload),
	}
	if m.state == stateInFlight {
		v.Deadline = m.deadline
	}
	return v
}

// PayloadJSON renders a stored payload as a valid JSON value. Payloads are
// arbitrary bytes as far as the log is concerned; the HTTP layer only ever
// stores valid JSON, but a log written by something else (or by a future
// client) must not be able to make this server emit malformed JSON.
func PayloadJSON(b []byte) json.RawMessage {
	if len(b) > 0 && json.Valid(b) {
		return json.RawMessage(b)
	}
	quoted, err := json.Marshal(string(b))
	if err != nil {
		return json.RawMessage(`""`)
	}
	return json.RawMessage(quoted)
}

func (m *Message) clone() *Message {
	c := *m
	c.Payload = append([]byte(nil), m.Payload...)
	return &c
}

// Peek is a non-destructive look at the queue's contents, in the order the
// comparator would actually hand them out. It exists for the dashboard and for
// debugging; it takes no leases and changes no state.
func (q *Queue) Peek(limit int) map[string][]View {
	if limit <= 0 {
		limit = 25
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	ready := append([]*Message(nil), q.ready.items...)
	sort.SliceStable(ready, func(i, j int) bool { return q.cfg.less(ready[i], ready[j]) })

	delayed := append([]*Message(nil), q.delayed.items...)
	sort.SliceStable(delayed, func(i, j int) bool { return delayed[i].VisibleAt.Before(delayed[j].VisibleAt) })

	inflight := append([]*Message(nil), q.inflight.items...)
	sort.SliceStable(inflight, func(i, j int) bool { return inflight[i].deadline.Before(inflight[j].deadline) })

	dead := append([]*Message(nil), q.dlq...)
	sort.SliceStable(dead, func(i, j int) bool { return dead[i].Seq < dead[j].Seq })

	return map[string][]View{
		"ready":     viewsOf(ready, limit),
		"delayed":   viewsOf(delayed, limit),
		"in_flight": viewsOf(inflight, limit),
		"dlq":       viewsOf(dead, limit),
	}
}

// DLQ returns dead-lettered messages, oldest first.
func (q *Queue) DLQ(limit int) []View {
	if limit <= 0 {
		limit = 100
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	dead := append([]*Message(nil), q.dlq...)
	sort.SliceStable(dead, func(i, j int) bool { return dead[i].Seq < dead[j].Seq })
	return viewsOf(dead, limit)
}

func viewsOf(ms []*Message, limit int) []View {
	if len(ms) > limit {
		ms = ms[:limit]
	}
	out := make([]View, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.view())
	}
	return out
}

// Close stops the background loop and flushes the log.
func (q *Queue) Close() error {
	var err error
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		close(q.done)
		q.wg.Wait()
		err = q.log.Close()
	})
	return err
}
