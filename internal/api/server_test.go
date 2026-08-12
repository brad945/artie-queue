package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/brad945/artie-queue/internal/queue"
)

type testAPI struct {
	*testing.T
	srv  *httptest.Server
	mgr  *queue.Manager
	root string
}

func newTestAPI(t *testing.T) *testAPI {
	t.Helper()
	root := t.TempDir()
	mgr, err := queue.OpenManager(root, t.Logf)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	srv := httptest.NewServer(New(mgr, t.Logf).Routes())
	t.Cleanup(func() {
		srv.Close()
		mgr.Close()
	})
	return &testAPI{T: t, srv: srv, mgr: mgr, root: root}
}

// do sends a request and decodes the JSON response into out (if non-nil).
func (a *testAPI) do(method, path string, body any, out any) int {
	a.Helper()
	var rdr *bytes.Reader
	if body == nil {
		rdr = bytes.NewReader(nil)
	} else if raw, ok := body.(string); ok {
		rdr = bytes.NewReader([]byte(raw))
	} else {
		b, err := json.Marshal(body)
		if err != nil {
			a.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, a.srv.URL+path, rdr)
	if err != nil {
		a.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.srv.Client().Do(req)
	if err != nil {
		a.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			a.Fatalf("decoding %s %s response: %v", method, path, err)
		}
	}
	return resp.StatusCode
}

func (a *testAPI) mustCreate(cfg map[string]any) {
	a.Helper()
	if code := a.do("POST", "/queues", cfg, nil); code != http.StatusCreated {
		a.Fatalf("create queue: status %d", code)
	}
}

// ---------------------------------------------------------------------------

func TestCreateQueueValidation(t *testing.T) {
	a := newTestAPI(t)

	cases := []struct {
		name string
		body map[string]any
		want int
	}{
		{"valid", map[string]any{"name": "jobs", "ordering": "fifo"}, http.StatusCreated},
		{"duplicate name", map[string]any{"name": "jobs", "ordering": "fifo"}, http.StatusConflict},
		{"defaults applied", map[string]any{"name": "minimal"}, http.StatusCreated},
		{"bad ordering", map[string]any{"name": "q1", "ordering": "random"}, http.StatusBadRequest},
		{"empty name", map[string]any{"name": "", "ordering": "fifo"}, http.StatusBadRequest},
		{"negative max_attempts", map[string]any{"name": "q2", "max_attempts": -1}, http.StatusBadRequest},
		{"aging without priority", map[string]any{"name": "q3", "aging_interval_ms": 100}, http.StatusBadRequest},
		{"unknown field", map[string]any{"name": "q4", "colour": "blue"}, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if code := a.do("POST", "/queues", tc.body, nil); code != tc.want {
				t.Errorf("status = %d, want %d", code, tc.want)
			}
		})
	}
}

// Queue names become directory names, so anything that could climb out of the
// data directory has to be rejected before it reaches the filesystem.
func TestQueueNameCannotEscapeDataDirectory(t *testing.T) {
	a := newTestAPI(t)

	for _, name := range []string{
		"../evil", "..", ".", "/etc/passwd", "a/b", `a\b`, ".hidden",
		strings.Repeat("x", 65), "sp ace", "semi;colon",
	} {
		t.Run(name, func(t *testing.T) {
			code := a.do("POST", "/queues", map[string]any{"name": name}, nil)
			if code != http.StatusBadRequest {
				t.Errorf("name %q was accepted with status %d", name, code)
			}
		})
	}

	// Nothing should have been written outside the data root.
	entries, err := os.ReadDir(a.root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("unexpected entry created in data dir: %s", e.Name())
	}
	if _, err := os.Stat(filepath.Join(a.root, "..", "evil")); err == nil {
		t.Fatal("a queue directory escaped the data root")
	}
}

func TestEnqueueValidation(t *testing.T) {
	a := newTestAPI(t)
	a.mustCreate(map[string]any{"name": "jobs"})

	cases := []struct {
		name string
		body any
		want int
	}{
		{"object payload", map[string]any{"payload": map[string]any{"task": "x"}}, http.StatusCreated},
		{"string payload", map[string]any{"payload": "hello"}, http.StatusCreated},
		{"number payload", map[string]any{"payload": 42}, http.StatusCreated},
		{"with priority and delay", map[string]any{"payload": "x", "priority": 3, "delay_ms": 50}, http.StatusCreated},
		{"missing payload", map[string]any{"priority": 1}, http.StatusBadRequest},
		{"negative delay", map[string]any{"payload": "x", "delay_ms": -5}, http.StatusBadRequest},
		{"malformed json", `{"payload": `, http.StatusBadRequest},
		{"unknown field", map[string]any{"payload": "x", "ttl": 10}, http.StatusBadRequest},
		{"oversize dedup id", map[string]any{"payload": "x", "dedup_id": strings.Repeat("d", 300)}, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if code := a.do("POST", "/queues/jobs/messages", tc.body, nil); code != tc.want {
				t.Errorf("status = %d, want %d", code, tc.want)
			}
		})
	}

	t.Run("oversize payload", func(t *testing.T) {
		big := map[string]any{"payload": strings.Repeat("z", MaxPayloadBytes+100)}
		code := a.do("POST", "/queues/jobs/messages", big, nil)
		if code != http.StatusRequestEntityTooLarge && code != http.StatusBadRequest {
			t.Errorf("status = %d, want 413 (or 400 from the read limit)", code)
		}
	})
}

func TestUnknownQueueIs404(t *testing.T) {
	a := newTestAPI(t)
	paths := []struct {
		method, path string
	}{
		{"POST", "/queues/nope/messages"},
		{"POST", "/queues/nope/dequeue"},
		{"POST", "/queues/nope/messages/abc/ack"},
		{"POST", "/queues/nope/messages/abc/nack"},
		{"GET", "/queues/nope/stats"},
		{"GET", "/queues/nope/dlq"},
		{"GET", "/queues/nope/peek"},
		{"POST", "/queues/nope/compact"},
	}
	for _, p := range paths {
		t.Run(p.method+" "+p.path, func(t *testing.T) {
			body := any(nil)
			if p.method == "POST" && strings.HasSuffix(p.path, "/messages") {
				body = map[string]any{"payload": "x"}
			}
			if code := a.do(p.method, p.path, body, nil); code != http.StatusNotFound {
				t.Errorf("status = %d, want 404", code)
			}
		})
	}
}

// The full lifecycle over HTTP, including the status codes a client has to
// branch on.
func TestLifecycleOverHTTP(t *testing.T) {
	a := newTestAPI(t)
	a.mustCreate(map[string]any{
		"name": "jobs", "ordering": "lifo", "priority_enabled": true, "max_attempts": 2,
	})

	var enq struct {
		ID        string `json:"id"`
		Seq       uint64 `json:"seq"`
		Duplicate bool   `json:"duplicate"`
	}
	if code := a.do("POST", "/queues/jobs/messages", map[string]any{"payload": map[string]any{"task": "a"}, "priority": 1}, &enq); code != http.StatusCreated {
		t.Fatalf("enqueue: %d", code)
	}
	if enq.ID == "" || enq.Seq != 1 || enq.Duplicate {
		t.Fatalf("unexpected enqueue result: %+v", enq)
	}

	// Dequeue with no body at all must work: one message, default visibility.
	var dq struct {
		Messages []struct {
			ID       string          `json:"id"`
			Attempts int             `json:"attempts"`
			Payload  json.RawMessage `json:"payload"`
		} `json:"messages"`
	}
	if code := a.do("POST", "/queues/jobs/dequeue", nil, &dq); code != http.StatusOK {
		t.Fatalf("bodyless dequeue: %d", code)
	}
	if len(dq.Messages) != 1 || dq.Messages[0].ID != enq.ID {
		t.Fatalf("dequeue returned %+v", dq.Messages)
	}
	if got := string(dq.Messages[0].Payload); got != `{"task":"a"}` {
		t.Errorf("payload round-trip = %s, want the exact bytes sent", got)
	}

	// Ack of a leased message succeeds; a second ack is a 404 because the
	// message no longer exists.
	if code := a.do("POST", "/queues/jobs/messages/"+enq.ID+"/ack", nil, nil); code != http.StatusOK {
		t.Errorf("ack: %d", code)
	}
	if code := a.do("POST", "/queues/jobs/messages/"+enq.ID+"/ack", nil, nil); code != http.StatusNotFound {
		t.Errorf("second ack: %d, want 404", code)
	}
}

func TestAckOfUnleasedMessageIs409(t *testing.T) {
	a := newTestAPI(t)
	a.mustCreate(map[string]any{"name": "jobs"})

	var enq struct {
		ID string `json:"id"`
	}
	a.do("POST", "/queues/jobs/messages", map[string]any{"payload": "x"}, &enq)

	// It is ready, not leased: there is nothing to acknowledge.
	if code := a.do("POST", "/queues/jobs/messages/"+enq.ID+"/ack", nil, nil); code != http.StatusConflict {
		t.Errorf("ack of a ready message = %d, want 409", code)
	}
	if code := a.do("POST", "/queues/jobs/messages/"+enq.ID+"/nack", nil, nil); code != http.StatusConflict {
		t.Errorf("nack of a ready message = %d, want 409", code)
	}
}

// A retried enqueue is a success with the original id, not an error the client
// has to special-case.
func TestDedupReturns200WithOriginalID(t *testing.T) {
	a := newTestAPI(t)
	a.mustCreate(map[string]any{"name": "jobs"})

	body := map[string]any{"payload": "charge", "dedup_id": "txn-1"}
	var first, second struct {
		ID        string `json:"id"`
		Duplicate bool   `json:"duplicate"`
	}
	if code := a.do("POST", "/queues/jobs/messages", body, &first); code != http.StatusCreated {
		t.Fatalf("first enqueue = %d, want 201", code)
	}
	if code := a.do("POST", "/queues/jobs/messages", body, &second); code != http.StatusOK {
		t.Fatalf("retried enqueue = %d, want 200", code)
	}
	if !second.Duplicate || second.ID != first.ID {
		t.Errorf("retry returned %+v, want duplicate=true id=%s", second, first.ID)
	}

	var st struct {
		Total    int `json:"total"`
		Counters struct {
			Duplicates uint64 `json:"duplicates_rejected"`
		} `json:"counters"`
	}
	a.do("GET", "/queues/jobs/stats", nil, &st)
	if st.Total != 1 || st.Counters.Duplicates != 1 {
		t.Errorf("stats = %+v, want 1 message and 1 rejected duplicate", st)
	}
}

func TestStatsAndPeekShape(t *testing.T) {
	a := newTestAPI(t)
	a.mustCreate(map[string]any{"name": "jobs", "priority_enabled": true, "max_attempts": 2})

	for i := 0; i < 5; i++ {
		a.do("POST", "/queues/jobs/messages", map[string]any{"payload": i, "priority": i}, nil)
	}
	a.do("POST", "/queues/jobs/messages", map[string]any{"payload": "later", "delay_ms": 60000}, nil)
	a.do("POST", "/queues/jobs/dequeue", map[string]any{"max_messages": 2, "visibility_timeout_ms": 60000}, nil)

	var st struct {
		Ready       int    `json:"ready"`
		Delayed     int    `json:"delayed"`
		InFlight    int    `json:"in_flight"`
		DLQ         int    `json:"dlq"`
		Total       int    `json:"total"`
		OldestAgeMS int64  `json:"oldest_age_ms"`
		NextSeq     uint64 `json:"next_seq"`
		Healthy     bool   `json:"healthy"`
		Log         struct {
			SizeBytes int64  `json:"size_bytes"`
			Records   uint64 `json:"records"`
			Fsyncs    uint64 `json:"fsyncs"`
		} `json:"log"`
	}
	if code := a.do("GET", "/queues/jobs/stats", nil, &st); code != http.StatusOK {
		t.Fatalf("stats: %d", code)
	}
	if st.Ready != 3 || st.Delayed != 1 || st.InFlight != 2 || st.Total != 6 {
		t.Errorf("stats = %+v, want ready 3 delayed 1 in-flight 2 total 6", st)
	}
	if !st.Healthy || st.NextSeq != 7 || st.Log.Records == 0 || st.Log.SizeBytes == 0 {
		t.Errorf("stats = %+v", st)
	}

	// Peek must be non-destructive and ordered by the queue's comparator.
	var peek map[string][]struct {
		ID       string `json:"id"`
		Priority int    `json:"priority"`
		State    string `json:"state"`
	}
	if code := a.do("GET", "/queues/jobs/peek?limit=50", nil, &peek); code != http.StatusOK {
		t.Fatalf("peek: %d", code)
	}
	for _, key := range []string{"ready", "delayed", "in_flight", "dlq"} {
		if _, ok := peek[key]; !ok {
			t.Errorf("peek response is missing the %q group", key)
		}
	}
	if len(peek["ready"]) != 3 || len(peek["delayed"]) != 1 || len(peek["in_flight"]) != 2 {
		t.Errorf("peek groups = ready %d delayed %d in-flight %d",
			len(peek["ready"]), len(peek["delayed"]), len(peek["in_flight"]))
	}
	for i := 1; i < len(peek["ready"]); i++ {
		if peek["ready"][i-1].Priority < peek["ready"][i].Priority {
			t.Errorf("peek ready is not in comparator order: %+v", peek["ready"])
			break
		}
	}
	// And it changed nothing.
	var after struct {
		Ready int `json:"ready"`
	}
	a.do("GET", "/queues/jobs/stats", nil, &after)
	if after.Ready != 3 {
		t.Errorf("peek consumed messages: ready went to %d", after.Ready)
	}
}

func TestDLQEndpoint(t *testing.T) {
	a := newTestAPI(t)
	a.mustCreate(map[string]any{"name": "jobs", "max_attempts": 2})

	var enq struct {
		ID string `json:"id"`
	}
	a.do("POST", "/queues/jobs/messages", map[string]any{"payload": "poison"}, &enq)

	for i := 0; i < 2; i++ {
		var dq struct {
			Messages []struct {
				ID string `json:"id"`
			} `json:"messages"`
		}
		a.do("POST", "/queues/jobs/dequeue", map[string]any{"max_messages": 1, "visibility_timeout_ms": 60000}, &dq)
		if len(dq.Messages) != 1 {
			t.Fatalf("attempt %d: nothing to dequeue", i+1)
		}
		a.do("POST", "/queues/jobs/messages/"+dq.Messages[0].ID+"/nack", nil, nil)
	}

	var dlq struct {
		Messages []struct {
			ID       string `json:"id"`
			Attempts int    `json:"attempts"`
			State    string `json:"state"`
		} `json:"messages"`
	}
	if code := a.do("GET", "/queues/jobs/dlq", nil, &dlq); code != http.StatusOK {
		t.Fatalf("dlq: %d", code)
	}
	if len(dlq.Messages) != 1 || dlq.Messages[0].ID != enq.ID || dlq.Messages[0].Attempts != 2 {
		t.Fatalf("dlq = %+v", dlq.Messages)
	}
	if dlq.Messages[0].State != "dead" {
		t.Errorf("state = %q, want dead", dlq.Messages[0].State)
	}
	// A dead-lettered message can be purged with an ack.
	if code := a.do("POST", "/queues/jobs/messages/"+enq.ID+"/ack", nil, nil); code != http.StatusOK {
		t.Errorf("purging a dead-lettered message: %d", code)
	}
	var st struct {
		DLQ int `json:"dlq"`
	}
	a.do("GET", "/queues/jobs/stats", nil, &st)
	if st.DLQ != 0 {
		t.Errorf("dlq depth after purge = %d, want 0", st.DLQ)
	}
}

func TestListQueuesAndHealth(t *testing.T) {
	a := newTestAPI(t)
	a.mustCreate(map[string]any{"name": "beta"})
	a.mustCreate(map[string]any{"name": "alpha", "ordering": "lifo"})

	var list struct {
		Queues []struct {
			Name     string `json:"name"`
			Ordering string `json:"ordering"`
		} `json:"queues"`
	}
	if code := a.do("GET", "/queues", nil, &list); code != http.StatusOK {
		t.Fatalf("list: %d", code)
	}
	if len(list.Queues) != 2 || list.Queues[0].Name != "alpha" || list.Queues[1].Name != "beta" {
		t.Fatalf("list = %+v, want alpha then beta", list.Queues)
	}
	if list.Queues[0].Ordering != "lifo" {
		t.Errorf("alpha ordering = %q", list.Queues[0].Ordering)
	}

	var health struct {
		OK     bool     `json:"ok"`
		Queues []string `json:"queues"`
	}
	if code := a.do("GET", "/healthz", nil, &health); code != http.StatusOK || !health.OK {
		t.Errorf("healthz = %d %+v", code, health)
	}
}

func TestCompactEndpointShrinksLog(t *testing.T) {
	a := newTestAPI(t)
	a.mustCreate(map[string]any{"name": "jobs"})

	for i := 0; i < 50; i++ {
		a.do("POST", "/queues/jobs/messages", map[string]any{"payload": fmt.Sprintf("m%d", i)}, nil)
	}
	var dq struct {
		Messages []struct {
			ID string `json:"id"`
		} `json:"messages"`
	}
	a.do("POST", "/queues/jobs/dequeue", map[string]any{"max_messages": 45, "visibility_timeout_ms": 60000}, &dq)
	for _, m := range dq.Messages {
		a.do("POST", "/queues/jobs/messages/"+m.ID+"/ack", nil, nil)
	}

	var res struct {
		Before int64 `json:"before_bytes"`
		After  int64 `json:"after_bytes"`
	}
	if code := a.do("POST", "/queues/jobs/compact", nil, &res); code != http.StatusOK {
		t.Fatalf("compact: %d", code)
	}
	if res.After >= res.Before {
		t.Errorf("compaction did not shrink the log: %d -> %d", res.Before, res.After)
	}
	var st struct {
		Total int `json:"total"`
	}
	a.do("GET", "/queues/jobs/stats", nil, &st)
	if st.Total != 5 {
		t.Errorf("live messages after compaction = %d, want 5", st.Total)
	}
}

// Methods the mux does not register must not fall through to a handler.
func TestWrongMethodIsRejected(t *testing.T) {
	a := newTestAPI(t)
	a.mustCreate(map[string]any{"name": "jobs"})

	for _, c := range []struct{ method, path string }{
		{"GET", "/queues/jobs/dequeue"},
		{"DELETE", "/queues"},
		{"PUT", "/queues/jobs/messages"},
	} {
		if code := a.do(c.method, c.path, nil, nil); code == http.StatusOK || code == http.StatusCreated {
			t.Errorf("%s %s was accepted with %d", c.method, c.path, code)
		}
	}
}
