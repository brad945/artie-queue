// Package queue implements the composable queue itself: one ordered structure
// with a configurable comparator, two heaps, and lease-based delivery on top
// of an append-only log.
package queue

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"time"
)

// Ordering selects the tie-break rule inside a priority level.
type Ordering string

const (
	FIFO Ordering = "fifo"
	LIFO Ordering = "lifo"
)

// state tracks which structure currently owns a message. A message is in
// exactly one of these at a time, which is why a single heap index field on
// Message is enough.
type state uint8

const (
	stateReady state = iota
	stateDelayed
	stateInFlight
	stateDead
)

func (s state) String() string {
	switch s {
	case stateReady:
		return "ready"
	case stateDelayed:
		return "delayed"
	case stateInFlight:
		return "in_flight"
	case stateDead:
		return "dead"
	}
	return "unknown"
}

// Message is a queued item. The first block is persisted; the second block is
// runtime bookkeeping rebuilt on replay.
type Message struct {
	ID        string    // server-generated, unique for the life of the queue
	DedupID   string    // optional, client-supplied, for idempotent enqueue
	Seq       uint64    // monotonic, assigned at enqueue, never reused
	Priority  int       // higher = more urgent
	VisibleAt time.Time // now + delay; ineligible until this passes
	Payload   []byte
	Attempts  int       // failed delivery attempts so far
	CreatedAt time.Time // used for the dedup window and oldest-message age

	state       state
	index       int       // position in whichever heap holds it
	effPriority int       // Priority plus any aging boost; see agePriorities
	deadline    time.Time // visibility deadline while in flight
}

// Deadline reports the visibility deadline of a leased message. Meaningful
// only on messages handed back by Dequeue.
func (m *Message) Deadline() time.Time { return m.deadline }

// Config is a queue's immutable configuration, written as the first record of
// its log and rewritten at the head of every compaction snapshot.
//
// It is immutable by design: replay derives dead-lettering from MaxAttempts,
// so if the config could change under a log the same records would replay to a
// different state.
type Config struct {
	Name            string   `json:"name"`
	Ordering        Ordering `json:"ordering"`
	PriorityEnabled bool     `json:"priority_enabled"`
	MaxAttempts     int      `json:"max_attempts"`

	DefaultVisibilityTimeoutMS int `json:"default_visibility_timeout_ms"`

	// AgingIntervalMS, when non-zero, raises a waiting message's effective
	// priority by one level per interval, capped at AgingMaxBoost. Off by
	// default: with it off, ordering is a strict total order, which is what
	// the ordering tests assert against.
	AgingIntervalMS int `json:"aging_interval_ms"`
	AgingMaxBoost   int `json:"aging_max_boost"`

	// DedupWindowMS is how long a DedupID is remembered after enqueue.
	DedupWindowMS int `json:"dedup_window_ms"`

	// NextSeq is only meaningful in a snapshot's META record: it carries the
	// sequence counter forward so that compacting an empty queue cannot reset
	// Seq and reorder future messages against ones already acknowledged.
	NextSeq uint64 `json:"next_seq,omitempty"`
}

// Defaults chosen to match the assignment: 30s visibility, 5 attempts.
const (
	DefaultVisibilityTimeout = 30 * time.Second
	DefaultMaxAttempts       = 5
	DefaultDedupWindow       = 5 * time.Minute
)

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)

// Validate normalises a config and rejects anything that would be unsafe or
// ambiguous. The name check is also a security boundary: queue names become
// directory names, so anything with a path separator or a leading dot is out.
func (c *Config) Validate() error {
	if !nameRE.MatchString(c.Name) {
		return fmt.Errorf("invalid queue name %q: must match %s", c.Name, nameRE)
	}
	switch c.Ordering {
	case "":
		c.Ordering = FIFO
	case FIFO, LIFO:
	default:
		return fmt.Errorf("invalid ordering %q: want %q or %q", c.Ordering, FIFO, LIFO)
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = DefaultMaxAttempts
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("max_attempts must be >= 1, got %d", c.MaxAttempts)
	}
	if c.DefaultVisibilityTimeoutMS == 0 {
		c.DefaultVisibilityTimeoutMS = int(DefaultVisibilityTimeout / time.Millisecond)
	}
	if c.DefaultVisibilityTimeoutMS < 0 {
		return fmt.Errorf("default_visibility_timeout_ms must be >= 0")
	}
	if c.DedupWindowMS == 0 {
		c.DedupWindowMS = int(DefaultDedupWindow / time.Millisecond)
	}
	if c.DedupWindowMS < 0 {
		return fmt.Errorf("dedup_window_ms must be >= 0")
	}
	if c.AgingIntervalMS < 0 {
		return fmt.Errorf("aging_interval_ms must be >= 0")
	}
	if c.AgingIntervalMS > 0 && c.AgingMaxBoost <= 0 {
		c.AgingMaxBoost = 10
	}
	if !c.PriorityEnabled && c.AgingIntervalMS > 0 {
		return fmt.Errorf("aging_interval_ms requires priority_enabled")
	}
	return nil
}

func (c *Config) visibilityTimeout() time.Duration {
	return time.Duration(c.DefaultVisibilityTimeoutMS) * time.Millisecond
}

func (c *Config) dedupWindow() time.Duration {
	return time.Duration(c.DedupWindowMS) * time.Millisecond
}

func (c *Config) agingInterval() time.Duration {
	return time.Duration(c.AgingIntervalMS) * time.Millisecond
}

// less is the single comparator that produces all six queue types.
//
//	priority off, FIFO -> Seq ascending          (plain FIFO)
//	priority off, LIFO -> Seq descending         (plain LIFO)
//	priority on,  FIFO -> priority, then Seq asc (priority-FIFO)
//	priority on,  LIFO -> priority, then Seq desc(priority-LIFO)
//
// and each of those becomes its delayed variant purely by virtue of a message
// not entering this heap until VisibleAt passes. Delay is not a fifth
// comparator; it is a gate in front of the same one.
//
// Priority is primary and the ordering mode only tie-breaks within a priority
// level. The alternative — mode primary, priority as tie-breaker — is
// degenerate: Seq is unique, so the tie the priority would break never occurs
// and the priority field would be dead configuration.
func (c *Config) less(a, b *Message) bool {
	if c.PriorityEnabled && a.effPriority != b.effPriority {
		return a.effPriority > b.effPriority
	}
	if c.Ordering == LIFO {
		return a.Seq > b.Seq
	}
	return a.Seq < b.Seq
}

// newID returns a random message id. Random rather than derived from Seq so
// that an id carries no ordering information a client might come to depend on.
func newID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("queue: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
