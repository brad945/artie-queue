package demo

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/brad945/artie-queue/internal/wal"
)

// Supervisor runs the queue server as a child process so the dashboard can
// kill it for real. A simulated crash would prove nothing; SIGKILL to a
// separate pid, with recovery read back over HTTP, proves the log.
type Supervisor struct {
	bin  string
	dir  string
	addr string

	mu       sync.Mutex
	cmd      *exec.Cmd
	running  bool
	lastExit string
	logs     *ringWriter
	client   *Client
}

// NewSupervisor prepares (but does not start) a managed queue server.
func NewSupervisor(bin, dir, addr string) *Supervisor {
	return &Supervisor{
		bin:    bin,
		dir:    dir,
		addr:   addr,
		logs:   newRingWriter(300),
		client: NewClient("http://" + addr),
	}
}

// Client returns a client pointed at the managed server.
func (s *Supervisor) Client() *Client { return s.client }

// DataDir is where the queue keeps its logs.
func (s *Supervisor) DataDir() string { return s.dir }

// Start launches the server and waits for it to report healthy. If the process
// exits instead — which is exactly what a corrupt log causes — Start returns
// the error along with whatever the server printed, so the dashboard can show
// the refusal verbatim.
func (s *Supervisor) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil
	}
	s.logs.add(fmt.Sprintf("--- starting %s -addr %s -dir %s ---", filepath.Base(s.bin), s.addr, s.dir))
	cmd := exec.Command(s.bin, "-addr", s.addr, "-dir", s.dir)
	cmd.Stdout = s.logs
	cmd.Stderr = s.logs
	if err := cmd.Start(); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("starting queue server: %w", err)
	}
	s.cmd = cmd
	s.running = true
	s.lastExit = ""
	s.mu.Unlock()

	exited := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		s.mu.Lock()
		s.running = false
		if err != nil {
			s.lastExit = err.Error()
		} else {
			s.lastExit = "exited cleanly"
		}
		s.mu.Unlock()
		exited <- err
	}()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			return fmt.Errorf("queue server refused to start: %s", s.lastLogLines(6))
		default:
		}
		if s.client.Healthy() {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	s.Kill()
	return fmt.Errorf("queue server did not become healthy within 10s")
}

// Kill sends SIGKILL. No graceful shutdown, no flush: whatever is on disk is
// all there is.
func (s *Supervisor) Kill() {
	s.mu.Lock()
	cmd := s.cmd
	s.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	s.logs.add("--- SIGKILL ---")
	_ = cmd.Process.Kill()
	for i := 0; i < 200; i++ {
		if !s.Running() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Running reports whether the child process is alive.
func (s *Supervisor) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// Logs returns the tail of the server's output.
func (s *Supervisor) Logs() []string { return s.logs.lines() }

func (s *Supervisor) lastLogLines(n int) string {
	lines := s.logs.lines()
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}

// logPath is the log file for one queue.
func (s *Supervisor) logPath(queue string) string {
	return filepath.Join(s.dir, queue, "wal.log")
}

// FlipByteMidLog corrupts a byte in the middle of a queue's log: a checksum
// mismatch that is not at the tail, which the server must refuse to start on.
func (s *Supervisor) FlipByteMidLog(queue string) (string, error) {
	if s.Running() {
		return "", fmt.Errorf("stop the server before corrupting its log")
	}
	path := s.logPath(queue)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if len(data) < 64 {
		return "", fmt.Errorf("log is only %d bytes; enqueue some messages first", len(data))
	}
	off := len(data) / 2
	old := data[off]
	data[off] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", err
	}
	return fmt.Sprintf("flipped byte at offset %d of %s (0x%02x -> 0x%02x)", off, path, old, data[off]), nil
}

// TruncateTailMidRecord simulates a crash during a write: the file ends inside
// a record. The server must truncate it, warn, and start.
func (s *Supervisor) TruncateTailMidRecord(queue string) (string, error) {
	if s.Running() {
		return "", fmt.Errorf("stop the server before corrupting its log")
	}
	path := s.logPath(queue)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	// A real, well-formed record — header checksum and all — with only the
	// first few payload bytes written. This is what an interrupted write
	// actually leaves behind, and the case the header checksum lets the reader
	// recognise as torn rather than corrupt.
	full := wal.Encode(nil, wal.TypeEnqueue, bytes.Repeat([]byte("x"), 64))
	fragment := full[:wal.HeaderSize+3]
	if _, err := f.Write(fragment); err != nil {
		return "", err
	}
	return fmt.Sprintf("appended a valid %d-byte record header at offset %d of %s, followed by only 3 of its 64 payload bytes",
		wal.HeaderSize, st.Size(), path), nil
}

// Verify runs the verify subcommand and returns its output and exit code.
func (s *Supervisor) Verify() (string, int) {
	cmd := exec.Command(s.bin, "verify", "-dir", s.dir)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	}
	return out.String(), code
}

// Repair truncates a queue's log at the offset verify reported, after saving a
// backup. This is the operator escape hatch, run explicitly — the server never
// discards data on its own.
func (s *Supervisor) Repair(queue string) (string, error) {
	if s.Running() {
		return "", fmt.Errorf("stop the server before repairing its log")
	}
	report, code := s.Verify()
	if code == 0 {
		return "", fmt.Errorf("verify reports the log is clean; nothing to repair")
	}
	offset, ok := parseOffset(report)
	if !ok {
		return "", fmt.Errorf("could not find a byte offset in verify output:\n%s", report)
	}
	cmd := exec.Command(s.bin, "repair", "-dir", s.dir, "-queue", queue,
		"-truncate-at", strconv.FormatInt(offset, 10))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("repair failed: %w", err)
	}
	s.logs.add("--- repair: " + strings.ReplaceAll(strings.TrimSpace(string(out)), "\n", " | ") + " ---")
	return string(out), nil
}

func parseOffset(report string) (int64, bool) {
	const marker = "at byte offset "
	i := strings.Index(report, marker)
	if i < 0 {
		return 0, false
	}
	rest := report[i+len(marker):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	n, err := strconv.ParseInt(rest[:end], 10, 64)
	return n, err == nil
}

// ringWriter keeps the last n lines written to it, for the log pane.
type ringWriter struct {
	mu   sync.Mutex
	buf  []string
	max  int
	part bytes.Buffer
}

func newRingWriter(max int) *ringWriter { return &ringWriter{max: max} }

func (w *ringWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.part.Write(p)
	for {
		line, err := w.part.ReadString('\n')
		if err == io.EOF {
			w.part.Reset()
			w.part.WriteString(line)
			break
		}
		w.appendLocked(strings.TrimRight(line, "\r\n"))
	}
	return len(p), nil
}

func (w *ringWriter) add(line string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.appendLocked(line)
}

func (w *ringWriter) appendLocked(line string) {
	if line == "" {
		return
	}
	w.buf = append(w.buf, line)
	if len(w.buf) > w.max {
		w.buf = w.buf[len(w.buf)-w.max:]
	}
}

func (w *ringWriter) lines() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, len(w.buf))
	copy(out, w.buf)
	return out
}
