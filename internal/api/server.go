// Package api exposes the queue over HTTP using only net/http's own router.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/brad945/artie-queue/internal/queue"
)

// MaxPayloadBytes caps a single message body. Without a cap, one client can
// turn an unbounded allocation into a denial of service on every other client
// sharing the process.
const MaxPayloadBytes = 256 << 10

// Server wires the queue manager to HTTP handlers.
type Server struct {
	mgr  *queue.Manager
	logf func(string, ...any)
}

// New returns a Server over the given manager.
func New(mgr *queue.Manager, logf func(string, ...any)) *Server {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Server{mgr: mgr, logf: logf}
}

// Routes registers every endpoint. Go's ServeMux handles method and wildcard
// matching, so there is no third-party router here either.
func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queues", s.createQueue)
	mux.HandleFunc("GET /queues", s.listQueues)
	mux.HandleFunc("POST /queues/{name}/messages", s.enqueue)
	mux.HandleFunc("POST /queues/{name}/dequeue", s.dequeue)
	mux.HandleFunc("POST /queues/{name}/messages/{id}/ack", s.ack)
	mux.HandleFunc("POST /queues/{name}/messages/{id}/nack", s.nack)
	mux.HandleFunc("GET /queues/{name}/stats", s.stats)
	mux.HandleFunc("GET /queues/{name}/dlq", s.dlq)
	mux.HandleFunc("GET /queues/{name}/peek", s.peek)
	mux.HandleFunc("POST /queues/{name}/compact", s.compact)
	mux.HandleFunc("GET /healthz", s.healthz)
	return mux
}

// ---------------------------------------------------------------------------

type createQueueRequest struct {
	Name                       string `json:"name"`
	Ordering                   string `json:"ordering"`
	PriorityEnabled            bool   `json:"priority_enabled"`
	MaxAttempts                int    `json:"max_attempts"`
	DefaultVisibilityTimeoutMS int    `json:"default_visibility_timeout_ms"`
	AgingIntervalMS            int    `json:"aging_interval_ms"`
	AgingMaxBoost              int    `json:"aging_max_boost"`
	DedupWindowMS              int    `json:"dedup_window_ms"`
}

func (s *Server) createQueue(w http.ResponseWriter, r *http.Request) {
	var req createQueueRequest
	if !decode(w, r, &req) {
		return
	}
	cfg := queue.Config{
		Name:                       req.Name,
		Ordering:                   queue.Ordering(req.Ordering),
		PriorityEnabled:            req.PriorityEnabled,
		MaxAttempts:                req.MaxAttempts,
		DefaultVisibilityTimeoutMS: req.DefaultVisibilityTimeoutMS,
		AgingIntervalMS:            req.AgingIntervalMS,
		AgingMaxBoost:              req.AgingMaxBoost,
		DedupWindowMS:              req.DedupWindowMS,
	}
	q, err := s.mgr.Create(cfg)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusCreated, q.Stats())
}

func (s *Server) listQueues(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"queues": s.mgr.List()})
}

type enqueueRequest struct {
	Payload  json.RawMessage `json:"payload"`
	Priority int             `json:"priority"`
	DelayMS  int64           `json:"delay_ms"`
	DedupID  string          `json:"dedup_id"`
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queueFor(w, r)
	if !ok {
		return
	}
	var req enqueueRequest
	if !decode(w, r, &req) {
		return
	}
	if len(req.Payload) == 0 {
		writeError(w, http.StatusBadRequest, errors.New("payload is required"))
		return
	}
	if len(req.Payload) > MaxPayloadBytes {
		writeError(w, http.StatusRequestEntityTooLarge,
			fmt.Errorf("payload is %d bytes, limit is %d", len(req.Payload), MaxPayloadBytes))
		return
	}
	if req.DelayMS < 0 {
		writeError(w, http.StatusBadRequest, errors.New("delay_ms must be >= 0"))
		return
	}
	if len(req.DedupID) > 256 {
		writeError(w, http.StatusBadRequest, errors.New("dedup_id must be at most 256 bytes"))
		return
	}

	res, err := q.Enqueue(req.Payload, req.Priority, time.Duration(req.DelayMS)*time.Millisecond, req.DedupID)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	// A duplicate is 200 with the original id, not a 409: the producer that
	// retried after a timeout wants to learn the enqueue already happened, and
	// an error status would make every client write a special case for the
	// success path.
	status := http.StatusCreated
	if res.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, res)
}

type dequeueRequest struct {
	MaxMessages         int   `json:"max_messages"`
	VisibilityTimeoutMS int64 `json:"visibility_timeout_ms"`
}

func (s *Server) dequeue(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queueFor(w, r)
	if !ok {
		return
	}
	var req dequeueRequest
	if r.ContentLength != 0 && !decode(w, r, &req) {
		return
	}
	if req.MaxMessages <= 0 {
		req.MaxMessages = 1
	}
	if req.MaxMessages > 1000 {
		req.MaxMessages = 1000
	}
	if req.VisibilityTimeoutMS < 0 {
		writeError(w, http.StatusBadRequest, errors.New("visibility_timeout_ms must be >= 0"))
		return
	}

	msgs, err := q.Dequeue(req.MaxMessages, time.Duration(req.VisibilityTimeoutMS)*time.Millisecond)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	out := make([]leased, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, leased{
			ID:        m.ID,
			Seq:       m.Seq,
			Priority:  m.Priority,
			Attempts:  m.Attempts,
			CreatedAt: m.CreatedAt,
			Deadline:  m.Deadline(),
			Payload:   queue.PayloadJSON(m.Payload),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": out})
}

type leased struct {
	ID        string          `json:"id"`
	Seq       uint64          `json:"seq"`
	Priority  int             `json:"priority"`
	Attempts  int             `json:"attempts"`
	CreatedAt time.Time       `json:"created_at"`
	Deadline  time.Time       `json:"deadline"`
	Payload   json.RawMessage `json:"payload"`
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queueFor(w, r)
	if !ok {
		return
	}
	if err := q.Ack(r.PathValue("id")); err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"acked": r.PathValue("id")})
}

func (s *Server) nack(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queueFor(w, r)
	if !ok {
		return
	}
	if err := q.Nack(r.PathValue("id")); err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nacked": r.PathValue("id")})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queueFor(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, q.Stats())
}

func (s *Server) dlq(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queueFor(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": q.DLQ(intParam(r, "limit", 100))})
}

func (s *Server) peek(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queueFor(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, q.Peek(intParam(r, "limit", 25)))
}

func (s *Server) compact(w http.ResponseWriter, r *http.Request) {
	q, ok := s.queueFor(w, r)
	if !ok {
		return
	}
	before := q.Stats().Log.SizeBytes
	if err := q.Compact(); err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"before_bytes": before,
		"after_bytes":  q.Stats().Log.SizeBytes,
	})
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	healthy := true
	for _, st := range s.mgr.List() {
		if !st.Healthy {
			healthy = false
			break
		}
	}
	if !healthy {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "queues": s.mgr.Names()})
}

// ---------------------------------------------------------------------------

func (s *Server) queueFor(w http.ResponseWriter, r *http.Request) (*queue.Queue, bool) {
	name := r.PathValue("name")
	q, ok := s.mgr.Get(name)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("no such queue %q", name))
		return nil, false
	}
	return q, true
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, MaxPayloadBytes+4096))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON body: %w", err))
		return false
	}
	return true
}

func intParam(r *http.Request, key string, def int) int {
	if v := r.URL.Query().Get(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

// writeQueueError maps engine errors to status codes. A write-ahead log
// failure surfaces as 503: the queue has stopped accepting work on purpose,
// and a client retrying later is the correct response.
func writeQueueError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, queue.ErrNotInFlight):
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, queue.ErrClosed):
		writeError(w, http.StatusServiceUnavailable, err)
	default:
		writeError(w, http.StatusServiceUnavailable, err)
	}
}
