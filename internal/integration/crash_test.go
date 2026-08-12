package integration

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The single most important test in the repo.
//
// Load the queue with concurrent producers and a consumer, SIGKILL the process
// mid-flight, restart it, and assert the two properties that matter:
//
//  1. every message the server answered 201 to is still there
//  2. every message the server answered 200 to an ack for is gone
//
// Property 1 is what "fsync before responding" buys. Property 2 is what makes
// the ack record meaningful. A queue that gets either wrong is not durable, no
// matter what its README says.
func TestKill9MidLoadLosesNothingAcknowledged(t *testing.T) {
	dir := t.TempDir()
	port := freePort(t)
	s := start(t, dir, port)
	s.mustCreateQueue(map[string]any{
		"name":             "jobs",
		"ordering":         "fifo",
		"priority_enabled": true,
		"max_attempts":     5,
	})

	// A killed process can die between committing and answering, so the client
	// ends up with three categories, not two. Only the definite ones can carry
	// strict assertions; the uncertain ones bound the totals. (This is also the
	// reason DedupID exists: a producer in exactly this position can retry
	// safely.)
	var (
		mu              sync.Mutex
		accepted        = map[string]bool{} // enqueue returned 201: must survive
		acked           = map[string]bool{} // ack returned 200: must be gone
		ackUnknown      = map[string]bool{} // ack sent, no response: either is legal
		enqueueUnknowns int                 // enqueue sent, no response: may or may not exist
	)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Producers: hammer enqueue until the server dies underneath them.
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
				var res enqueueResp
				code, err := s.post("/queues/jobs/messages", map[string]any{
					"payload":  fmt.Sprintf("producer-%d-item-%d", p, i),
					"priority": i % 4,
				}, &res)
				if err != nil {
					// The server died with this request in flight. It may or
					// may not have committed; we cannot assert either way.
					mu.Lock()
					enqueueUnknowns++
					mu.Unlock()
					return
				}
				if code == http.StatusCreated {
					mu.Lock()
					accepted[res.ID] = true
					mu.Unlock()
				}
			}
		}(p)
	}

	// Consumer: lease, ack, record. Long visibility so nothing expires during
	// the test and muddies the assertion.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			var dq dequeueResp
			code, err := s.post("/queues/jobs/dequeue", map[string]any{
				"max_messages":          8,
				"visibility_timeout_ms": 600000,
			}, &dq)
			if err != nil {
				return
			}
			if code != http.StatusOK || len(dq.Messages) == 0 {
				time.Sleep(2 * time.Millisecond)
				continue
			}
			for _, m := range dq.Messages {
				mu.Lock()
				ackUnknown[m.ID] = true // pending until we hear back
				mu.Unlock()

				code, err := s.post("/queues/jobs/messages/"+m.ID+"/ack", nil, nil)
				if err != nil {
					return // leave it in ackUnknown: outcome genuinely unknown
				}
				mu.Lock()
				delete(ackUnknown, m.ID)
				if code == http.StatusOK {
					acked[m.ID] = true
				}
				mu.Unlock()
			}
		}
	}()

	time.Sleep(600 * time.Millisecond)

	// No SIGTERM, no shutdown, no flush.
	s.kill()
	close(stop)
	wg.Wait()

	mu.Lock()
	acceptedIDs := make([]string, 0, len(accepted))
	for id := range accepted {
		acceptedIDs = append(acceptedIDs, id)
	}
	ackedCount := len(acked)
	unknownAcks := len(ackUnknown)
	unknownEnqueues := enqueueUnknowns
	mu.Unlock()

	if len(acceptedIDs) < 100 {
		t.Fatalf("only %d messages were accepted before the kill; the test needs real load to mean anything", len(acceptedIDs))
	}
	if ackedCount == 0 {
		t.Fatal("no messages were acked before the kill; the test would not prove anything about acks")
	}
	t.Logf("before kill: %d enqueues confirmed, %d acks confirmed, %d acks and %d enqueues left in doubt",
		len(acceptedIDs), ackedCount, unknownAcks, unknownEnqueues)

	// Restart against the same data directory.
	s2 := start(t, dir, port)
	defer s2.kill()

	survivors := s2.allIDs("jobs")

	var lost, resurrected int
	for _, id := range acceptedIDs {
		mu.Lock()
		wasAcked, inDoubt := acked[id], ackUnknown[id]
		mu.Unlock()
		_, present := survivors[id]
		switch {
		case wasAcked && present:
			// The server said the ack was durable and the message came back.
			// This is the unforgivable one.
			resurrected++
		case !wasAcked && !inDoubt && !present:
			// The server said the enqueue was durable and nothing has removed
			// it since. Losing this is losing acknowledged data.
			lost++
		}
	}
	if lost > 0 {
		t.Errorf("%d messages the server had confirmed (201) were lost across the crash", lost)
	}
	if resurrected > 0 {
		t.Errorf("%d acked messages came back from the dead after the crash", resurrected)
	}

	// In-flight leases are not durable state, by design: anything that was
	// leased but unacked must come back ready, not stuck in flight forever.
	st := s2.stats("jobs")
	if st.InFlight != 0 {
		t.Errorf("in-flight = %d after restart, want 0 (leases do not survive a crash)", st.InFlight)
	}
	// Totals are bounded rather than exact, and the width of the band is
	// exactly the number of requests that were in flight when the process
	// died: an unanswered ack may or may not have committed, and so may an
	// unanswered enqueue.
	lo := len(acceptedIDs) - ackedCount - unknownAcks
	hi := len(acceptedIDs) - ackedCount + unknownEnqueues
	if st.Total < lo || st.Total > hi {
		t.Errorf("recovered %d messages, want between %d and %d (%d confirmed - %d acked, ±%d in doubt)",
			st.Total, lo, hi, len(acceptedIDs), ackedCount, unknownAcks+unknownEnqueues)
	}
	t.Logf("after restart: %d messages recovered (bounds %d..%d), %d ready, log %d bytes",
		st.Total, lo, hi, st.Ready, st.Log.SizeBytes)
}

// Delayed and dead-lettered messages have to survive a crash with their state
// intact, not just ready ones.
func TestKill9PreservesDelayedAndDeadLettered(t *testing.T) {
	dir := t.TempDir()
	port := freePort(t)
	s := start(t, dir, port)
	s.mustCreateQueue(map[string]any{
		"name":                          "mixed",
		"ordering":                      "lifo",
		"priority_enabled":              true,
		"max_attempts":                  2,
		"default_visibility_timeout_ms": 50,
	})

	var delayed enqueueResp
	if code, err := s.post("/queues/mixed/messages", map[string]any{
		"payload":  "much-later",
		"delay_ms": 3600000,
	}, &delayed); err != nil || code != http.StatusCreated {
		t.Fatalf("enqueue delayed: %d %v", code, err)
	}

	var poison enqueueResp
	if code, err := s.post("/queues/mixed/messages", map[string]any{"payload": "poison"}, &poison); err != nil || code != http.StatusCreated {
		t.Fatalf("enqueue poison: %d %v", code, err)
	}
	// Fail it until it dead-letters.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if s.stats("mixed").DeadLettered == 1 {
			break
		}
		var dq dequeueResp
		s.post("/queues/mixed/dequeue", map[string]any{"max_messages": 5, "visibility_timeout_ms": 20}, &dq)
		for _, m := range dq.Messages {
			s.post("/queues/mixed/messages/"+m.ID+"/nack", nil, nil)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if st := s.stats("mixed"); st.DeadLettered != 1 || st.Delayed != 1 {
		t.Fatalf("setup failed: dlq %d delayed %d", st.DeadLettered, st.Delayed)
	}

	s.kill()
	s2 := start(t, dir, port)
	defer s2.kill()

	st := s2.stats("mixed")
	if st.Delayed != 1 {
		t.Errorf("delayed = %d after crash, want 1", st.Delayed)
	}
	if st.DeadLettered != 1 {
		t.Errorf("dlq = %d after crash, want 1", st.DeadLettered)
	}
	msgs := s2.allIDs("mixed")
	if m, ok := msgs[poison.ID]; !ok || m.State != "dead" {
		t.Errorf("poison message state after crash = %+v, want dead", m)
	}
	if m, ok := msgs[delayed.ID]; !ok || m.State != "delayed" {
		t.Errorf("delayed message state after crash = %+v, want delayed", m)
	}
}

// The corruption policy, end to end: a checksum mismatch in the middle of a
// log stops the server from starting, and says exactly where the damage is.
func TestServerRefusesToStartOnCorruptLog(t *testing.T) {
	dir := t.TempDir()
	port := freePort(t)
	s := start(t, dir, port)
	s.mustCreateQueue(map[string]any{"name": "q", "ordering": "fifo"})
	for i := 0; i < 20; i++ {
		if code, err := s.post("/queues/q/messages", map[string]any{"payload": i}, nil); err != nil || code != http.StatusCreated {
			t.Fatalf("enqueue: %d %v", code, err)
		}
	}
	s.kill()

	logPath := filepath.Join(dir, "q", "wal.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xff
	if err := os.WriteFile(logPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	// verify names the damage without starting the server.
	code, out := run(t, "verify", "-dir", dir)
	if code == 0 {
		t.Errorf("verify exited 0 on a corrupt log:\n%s", out)
	}
	if !contains(out, "checksum mismatch") {
		t.Errorf("verify output does not explain the problem:\n%s", out)
	}

	// And the server refuses outright.
	cmd := startRaw(t, dir, port)
	if cmd.code == 0 {
		t.Errorf("server started despite a corrupt log:\n%s", cmd.out)
	}
	if !contains(cmd.out, "REFUSING TO START") {
		t.Errorf("server did not say why it refused:\n%s", cmd.out)
	}
	offset := extractOffset(t, cmd.out)
	if offset < 0 {
		t.Fatalf("refusal message does not name a byte offset:\n%s", cmd.out)
	}

	// The operator escape hatch: an explicit, auditable truncation. Nothing
	// discards data on its own — a human names the offset.
	code, out = run(t, "repair", "-dir", dir, "-queue", "q", "-truncate-at", strconv.FormatInt(offset, 10))
	if code != 0 {
		t.Fatalf("repair failed: %s", out)
	}
	if !contains(out, "original saved as") {
		t.Errorf("repair did not keep a backup:\n%s", out)
	}
	if code, out := run(t, "verify", "-dir", dir); code != 0 {
		t.Fatalf("log still fails verification after repair:\n%s", out)
	}

	s3 := start(t, dir, port)
	defer s3.kill()
	if st := s3.stats("q"); !st.Healthy {
		t.Errorf("queue unhealthy after repair: %+v", st)
	} else {
		t.Logf("after repair the queue starts with %d messages recovered", st.Total)
	}
}

// A torn tail — the file ending inside a record — is the normal artifact of a
// crash mid-write, and must not stop the server.
func TestServerTruncatesTornTailAndStarts(t *testing.T) {
	dir := t.TempDir()
	port := freePort(t)
	s := start(t, dir, port)
	s.mustCreateQueue(map[string]any{"name": "q", "ordering": "fifo"})
	for i := 0; i < 10; i++ {
		s.post("/queues/q/messages", map[string]any{"payload": i}, nil)
	}
	before := s.stats("q").Total
	s.kill()

	// Simulate a write interrupted part-way: a valid log plus a fragment.
	logPath := filepath.Join(dir, "q", "wal.log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// A plausible header claiming a payload that is not there.
	if _, err := f.Write([]byte{0x40, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef, 0x02, 0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2 := start(t, dir, port)
	defer s2.kill()

	if !contains(s2.logs(), "torn record at end of log") {
		t.Errorf("server did not warn about the torn tail:\n%s", s2.logs())
	}
	if st := s2.stats("q"); st.Total != before {
		t.Errorf("recovered %d messages, want %d", st.Total, before)
	}
}

// --- helpers ---------------------------------------------------------------

type rawRun struct {
	code int
	out  string
}

// startRaw runs the server in the foreground and waits for it to exit. Used
// when we expect it to refuse to start.
func startRaw(t *testing.T, dir string, port int) rawRun {
	t.Helper()
	code, out := run(t, "-addr", fmt.Sprintf("127.0.0.1:%d", port), "-dir", dir)
	return rawRun{code: code, out: out}
}

// extractOffset pulls the byte offset out of a corruption message.
func extractOffset(t *testing.T, msg string) int64 {
	t.Helper()
	const marker = "at byte offset "
	i := strings.Index(msg, marker)
	if i < 0 {
		return -1
	}
	rest := msg[i+len(marker):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	n, err := strconv.ParseInt(rest[:end], 10, 64)
	if err != nil {
		return -1
	}
	return n
}
