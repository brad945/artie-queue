// Package integration drives the real server binary as a subprocess.
//
// The crash-recovery test is the reason this package exists: you cannot
// SIGKILL yourself and then assert on what survived, so the server has to be a
// separate process that we can actually kill without warning.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// binary builds cmd/artie-queue once per test run and returns its path.
func binary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "artie-queue-bin")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "artie-queue")
		cmd := exec.Command("go", "build", "-o", binPath, "../../cmd/artie-queue")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			buildErr = fmt.Errorf("building server: %v\n%s", err, stderr.String())
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

// syncBuffer collects subprocess output while the test also reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type server struct {
	t    *testing.T
	cmd  *exec.Cmd
	out  *syncBuffer
	url  string
	dir  string
	port int
	done chan error
}

// freePort asks the kernel for an unused port and immediately releases it.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// start launches the server and waits for it to answer /healthz.
func start(t *testing.T, dir string, port int) *server {
	t.Helper()
	s := &server{
		t:    t,
		out:  &syncBuffer{},
		dir:  dir,
		port: port,
		url:  fmt.Sprintf("http://127.0.0.1:%d", port),
		done: make(chan error, 1),
	}
	s.cmd = exec.Command(binary(t), "-addr", fmt.Sprintf("127.0.0.1:%d", port), "-dir", dir)
	s.cmd.Stdout = s.out
	s.cmd.Stderr = s.out
	if err := s.cmd.Start(); err != nil {
		t.Fatalf("starting server: %v", err)
	}
	go func() { s.done <- s.cmd.Wait() }()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.url + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return s
			}
		}
		select {
		case err := <-s.done:
			t.Fatalf("server exited during startup: %v\n%s", err, s.out.String())
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.kill()
	t.Fatalf("server never became healthy\n%s", s.out.String())
	return nil
}

// kill sends SIGKILL: no shutdown hook, no flush, no chance to clean up. The
// only thing standing between the queue and data loss is what is already on
// disk.
func (s *server) kill() {
	if s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
	}
}

func (s *server) logs() string { return s.out.String() }

// --- HTTP helpers ----------------------------------------------------------

func (s *server) post(path string, body any, out any) (int, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return 0, err
		}
	}
	resp, err := http.Post(s.url+path, "application/json", &buf)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

func (s *server) get(path string, out any) (int, error) {
	resp, err := http.Get(s.url + path)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

func (s *server) mustCreateQueue(cfg map[string]any) {
	s.t.Helper()
	code, err := s.post("/queues", cfg, nil)
	if err != nil {
		s.t.Fatalf("create queue: %v", err)
	}
	if code != http.StatusCreated {
		s.t.Fatalf("create queue: status %d\n%s", code, s.logs())
	}
}

type enqueueResp struct {
	ID        string `json:"id"`
	Seq       uint64 `json:"seq"`
	Duplicate bool   `json:"duplicate"`
}

type peekResp struct {
	Ready    []messageView `json:"ready"`
	Delayed  []messageView `json:"delayed"`
	InFlight []messageView `json:"in_flight"`
	DLQ      []messageView `json:"dlq"`
}

type messageView struct {
	ID       string          `json:"id"`
	Seq      uint64          `json:"seq"`
	Attempts int             `json:"attempts"`
	State    string          `json:"state"`
	Payload  json.RawMessage `json:"payload"`
}

// allIDs returns every message id the server currently knows about.
func (s *server) allIDs(queue string) map[string]messageView {
	s.t.Helper()
	var p peekResp
	code, err := s.get("/queues/"+queue+"/peek?limit=1000000", &p)
	if err != nil || code != http.StatusOK {
		s.t.Fatalf("peek: status %d err %v", code, err)
	}
	out := make(map[string]messageView)
	for _, group := range [][]messageView{p.Ready, p.Delayed, p.InFlight, p.DLQ} {
		for _, m := range group {
			out[m.ID] = m
		}
	}
	return out
}

type dequeueResp struct {
	Messages []struct {
		ID       string          `json:"id"`
		Attempts int             `json:"attempts"`
		Payload  json.RawMessage `json:"payload"`
	} `json:"messages"`
}

type statsResp struct {
	Ready        int `json:"ready"`
	Delayed      int `json:"delayed"`
	InFlight     int `json:"in_flight"`
	DeadLettered int `json:"dlq"`
	Total        int `json:"total"`
	Counters     struct {
		Enqueued uint64 `json:"enqueued"`
		Acked    uint64 `json:"acked"`
	} `json:"counters"`
	Log struct {
		SizeBytes       int64   `json:"size_bytes"`
		Records         uint64  `json:"records"`
		Fsyncs          uint64  `json:"fsyncs"`
		RecordsPerFsync float64 `json:"records_per_fsync"`
	} `json:"log"`
	Healthy bool `json:"healthy"`
}

func (s *server) stats(queue string) statsResp {
	s.t.Helper()
	var st statsResp
	code, err := s.get("/queues/"+queue+"/stats", &st)
	if err != nil || code != http.StatusOK {
		s.t.Fatalf("stats: status %d err %v", code, err)
	}
	return st
}

// run executes the built binary with the given args and returns exit code and
// combined output. Used for the verify and repair subcommands.
func run(t *testing.T, args ...string) (int, string) {
	t.Helper()
	cmd := exec.Command(binary(t), args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("running %v: %v", args, err)
	}
	return code, out.String()
}

func contains(hay, needle string) bool { return strings.Contains(hay, needle) }
