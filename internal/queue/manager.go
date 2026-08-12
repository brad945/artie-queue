package queue

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// ErrQueueExists distinguishes "that name is taken" from "that configuration
// is invalid", so the HTTP layer can answer 409 rather than 400.
var ErrQueueExists = errors.New("queue already exists")

// Manager owns every queue in a data directory. Each queue is a directory
// holding its own independent log, which keeps compaction local to one queue
// and means the per-queue mutex really is per-queue: two queues never contend.
type Manager struct {
	root string
	logf func(string, ...any)

	mu     sync.RWMutex
	queues map[string]*Queue
}

// OpenManager loads every queue found under root.
//
// If any queue's log is corrupt, this fails and the server does not start.
// Booting with one queue silently missing would be the same class of mistake
// as skipping a bad record.
func OpenManager(root string, logf func(string, ...any)) (*Manager, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	m := &Manager{root: root, logf: logf, queues: make(map[string]*Queue)}

	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if _, err := os.Stat(filepath.Join(root, name, logFileName)); err != nil {
			continue // not a queue directory
		}
		q, err := Load(root, name, logf)
		if err != nil {
			if errors.Is(err, ErrIncompleteQueue) {
				// Loud, but not fatal: this queue holds no data and never
				// finished being created. Refusing to boot over it would let
				// one interrupted create take down every healthy queue too.
				logf("WARN skipping queue %q: %v (create it again to use it; "+
					"no records were ever written to it)", name, err)
				continue
			}
			m.Close()
			return nil, fmt.Errorf("loading queue %q: %w", name, err)
		}
		m.queues[name] = q
	}
	if len(m.queues) > 0 {
		logf("recovered %d queue(s) from %s", len(m.queues), root)
	}
	return m, nil
}

// Create makes a new queue. Returns an error if the name is taken.
func (m *Manager) Create(cfg Config) (*Queue, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.queues[cfg.Name]; ok {
		return nil, fmt.Errorf("%w: %q", ErrQueueExists, cfg.Name)
	}
	q, err := Create(m.root, cfg, m.logf)
	if err != nil {
		return nil, err
	}
	m.queues[cfg.Name] = q
	m.logf("created queue %q (ordering=%s priority=%v max_attempts=%d visibility=%dms)",
		cfg.Name, cfg.Ordering, cfg.PriorityEnabled, cfg.MaxAttempts, cfg.DefaultVisibilityTimeoutMS)
	return q, nil
}

// Get returns a queue by name.
func (m *Manager) Get(name string) (*Queue, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.queues[name]
	return q, ok
}

// List returns stats for every queue, sorted by name.
func (m *Manager) List() []Stats {
	m.mu.RLock()
	qs := make([]*Queue, 0, len(m.queues))
	for _, q := range m.queues {
		qs = append(qs, q)
	}
	m.mu.RUnlock()

	out := make([]Stats, 0, len(qs))
	for _, q := range qs {
		out = append(out, q.Stats())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Names returns the queue names, sorted.
func (m *Manager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.queues))
	for n := range m.queues {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Close shuts every queue down, flushing each log.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for _, q := range m.queues {
		if err := q.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.queues = make(map[string]*Queue)
	return firstErr
}
