package demo

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"time"
)

//go:embed web
var webFS embed.FS

// NewEvents returns the demo's own event log — a running narration of what the
// dashboard did, separate from the queue server's log.
func NewEvents() *Events { return newRingWriter(60) }

// Events is the demo-level activity log shown in the UI.
type Events = ringWriter

// Add appends a line to the event log.
func (w *ringWriter) Add(line string) { w.add(line) }

// Dashboard serves the demo UI and the controls behind it.
type Dashboard struct {
	sup     *Supervisor
	client  *Client
	runner  *Runner
	events  *ringWriter
	managed bool // false when attached to a server we did not start
}

// NewDashboard wires the pieces together.
func NewDashboard(sup *Supervisor, client *Client, runner *Runner, events *ringWriter, managed bool) *Dashboard {
	return &Dashboard{sup: sup, client: client, runner: runner, events: events, managed: managed}
}

// Routes returns the dashboard's mux.
func (d *Dashboard) Routes() *http.ServeMux {
	mux := http.NewServeMux()

	content, err := fs.Sub(webFS, "web")
	if err != nil {
		panic(err)
	}
	mux.Handle("GET /", http.FileServer(http.FS(content)))

	mux.HandleFunc("GET /api/stream", d.stream)
	mux.HandleFunc("GET /api/snapshot", d.snapshotHandler)
	mux.HandleFunc("POST /api/submit", d.submit)
	mux.HandleFunc("POST /api/config", d.config)
	mux.HandleFunc("POST /api/burst", d.burst)
	mux.HandleFunc("POST /api/compact", d.compact)
	mux.HandleFunc("POST /api/server/kill", d.killServer)
	mux.HandleFunc("POST /api/server/start", d.startServer)
	mux.HandleFunc("POST /api/lab/corrupt", d.corrupt)
	mux.HandleFunc("POST /api/lab/repair", d.repair)
	return mux
}

// --- snapshot ---------------------------------------------------------------

// ServerState describes the supervised queue process.
type ServerState struct {
	Managed bool     `json:"managed"`
	Running bool     `json:"running"`
	Logs    []string `json:"logs"`
}

// Snapshot is one frame of the dashboard.
type Snapshot struct {
	TimeMS  int64           `json:"time_ms"`
	Defs    []QueueDef      `json:"defs"`
	Target  string          `json:"target"`
	Queues  json.RawMessage `json:"queues"`
	Stats   json.RawMessage `json:"stats"`
	Peek    json.RawMessage `json:"peek"`
	Runner  RunnerState     `json:"runner"`
	Server  ServerState     `json:"server"`
	Events  []string        `json:"events"`
	Offline bool            `json:"offline"`
}

func (d *Dashboard) snapshot() Snapshot {
	target := d.runner.Target()
	snap := Snapshot{
		TimeMS: time.Now().UnixMilli(),
		Defs:   QueueDefs,
		Target: target,
		Runner: d.runner.State(),
		Server: ServerState{
			Managed: d.managed,
			Running: !d.managed || d.sup.Running(),
		},
		Events: d.events.lines(),
	}
	if d.managed {
		snap.Server.Logs = d.sup.Logs()
	}
	if !snap.Server.Running {
		snap.Offline = true
		return snap
	}
	if qs, err := d.client.ListQueues(); err == nil {
		snap.Queues = qs
	}
	if st, err := d.client.Stats(target); err == nil {
		snap.Stats = st
	} else {
		snap.Offline = true
	}
	if pk, err := d.client.Peek(target, 60); err == nil {
		snap.Peek = pk
	}
	return snap
}

func (d *Dashboard) snapshotHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, d.snapshot())
}

// stream pushes a snapshot every 250ms over server-sent events. SSE rather
// than polling so the board animates smoothly, and rather than websockets
// because it is one-directional and needs no dependencies.
func (d *Dashboard) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	enc := json.NewEncoder(w)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		fmt.Fprint(w, "data: ")
		if err := enc.Encode(d.snapshot()); err != nil {
			return
		}
		fmt.Fprint(w, "\n")
		flusher.Flush()

		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

// --- controls ---------------------------------------------------------------

func (d *Dashboard) submit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Queue    string `json:"queue"`
		Count    int    `json:"count"`
		Priority int    `json:"priority"`
		DelayMS  int64  `json:"delay_ms"`
		DedupID  string `json:"dedup_id"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if req.Queue == "" {
		req.Queue = d.runner.Target()
	}
	res, err := d.runner.Submit(req.Queue, req.Count, req.Priority, req.DelayMS, req.DedupID)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "submitted": len(res)})
		return
	}
	dupes := 0
	for _, x := range res {
		if x.Duplicate {
			dupes++
		}
	}
	switch {
	case dupes > 0:
		d.events.add(fmt.Sprintf("submitted %d job(s) to %s — %d rejected as duplicates of %s",
			len(res), req.Queue, dupes, res[0].ID))
	case req.DelayMS > 0:
		d.events.add(fmt.Sprintf("submitted %d job(s) to %s, visible in %dms", len(res), req.Queue, req.DelayMS))
	default:
		d.events.add(fmt.Sprintf("submitted %d job(s) to %s", len(res), req.Queue))
	}
	writeJSON(w, http.StatusOK, map[string]any{"submitted": len(res), "duplicates": dupes, "results": res})
}

func (d *Dashboard) config(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Target       *string  `json:"target"`
		Workers      *int     `json:"workers"`
		FailureRate  *float64 `json:"failure_rate"`
		AbandonRate  *float64 `json:"abandon_rate"`
		VisibilityMS *int64   `json:"visibility_ms"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	d.runner.Update(req.Target, req.Workers, req.FailureRate, req.AbandonRate, req.VisibilityMS)
	writeJSON(w, http.StatusOK, d.runner.Config())
}

func (d *Dashboard) burst(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Count       int `json:"count"`
		Concurrency int `json:"concurrency"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	target := d.runner.Target()
	n, elapsed, err := d.runner.Burst(target, req.Count, req.Concurrency)
	resp := map[string]any{
		"enqueued":   n,
		"elapsed_ms": elapsed.Milliseconds(),
	}
	if elapsed > 0 {
		resp["per_second"] = float64(n) / elapsed.Seconds()
	}
	if err != nil {
		resp["error"] = err.Error()
	}
	writeJSON(w, http.StatusOK, resp)
}

func (d *Dashboard) compact(w http.ResponseWriter, r *http.Request) {
	target := d.runner.Target()
	before, after, err := d.client.Compact(target)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	d.events.add(fmt.Sprintf("compacted %s: %s → %s on disk", target, humanBytes(before), humanBytes(after)))
	writeJSON(w, http.StatusOK, map[string]any{"before_bytes": before, "after_bytes": after})
}

func (d *Dashboard) killServer(w http.ResponseWriter, r *http.Request) {
	if !d.managed {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "this dashboard is attached to a server it does not own; restart it with -queue-bin to enable the crash lab"})
		return
	}
	// Snapshot what the queue held so the UI can show what recovery restored.
	var before json.RawMessage
	if st, err := d.client.Stats(d.runner.Target()); err == nil {
		before = st
	}
	d.sup.Kill()
	d.events.add("SIGKILL sent to the queue server — no shutdown hook, no flush")
	writeJSON(w, http.StatusOK, map[string]any{"killed": true, "stats_before": before})
}

func (d *Dashboard) startServer(w http.ResponseWriter, r *http.Request) {
	if !d.managed {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "server is not managed by this dashboard"})
		return
	}
	if err := d.sup.Start(); err != nil {
		d.events.add("queue server refused to start: " + err.Error())
		writeJSON(w, http.StatusOK, map[string]any{"started": false, "error": err.Error(), "logs": d.sup.Logs()})
		return
	}
	if err := d.EnsureQueues(); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"started": true, "error": err.Error()})
		return
	}
	d.events.add("queue server restarted and replayed its log")
	writeJSON(w, http.StatusOK, map[string]any{"started": true, "logs": d.sup.Logs()})
}

// corrupt is the corruption lab: damage the log on purpose, then try to start.
//
//	mid-log  -> a complete record with a bad checksum: startup must refuse
//	tail     -> the file ends inside a record: startup must truncate and warn
func (d *Dashboard) corrupt(w http.ResponseWriter, r *http.Request) {
	if !d.managed {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "corruption lab needs a managed server (-queue-bin)"})
		return
	}
	var req struct {
		Mode  string `json:"mode"`
		Queue string `json:"queue"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if req.Queue == "" {
		req.Queue = d.runner.Target()
	}

	wasRunning := d.sup.Running()
	if wasRunning {
		d.sup.Kill()
	}

	var (
		what string
		err  error
	)
	switch req.Mode {
	case "tail":
		what, err = d.sup.TruncateTailMidRecord(req.Queue)
	default:
		req.Mode = "midlog"
		what, err = d.sup.FlipByteMidLog(req.Queue)
	}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	d.events.add("corruption lab: " + what)

	verify, verifyCode := d.sup.Verify()
	startErr := d.sup.Start()
	resp := map[string]any{
		"mode":        req.Mode,
		"what":        what,
		"verify":      verify,
		"verify_code": verifyCode,
		"started":     startErr == nil,
		"logs":        d.sup.Logs(),
		"explanation": explain(req.Mode, startErr == nil),
	}
	if startErr != nil {
		resp["error"] = startErr.Error()
		d.events.add("queue server REFUSED to start — the log is damaged and it will not guess")
	} else {
		_ = d.EnsureQueues()
		d.events.add("queue server truncated the torn tail, warned, and started")
	}
	writeJSON(w, http.StatusOK, resp)
}

func explain(mode string, started bool) string {
	if mode == "tail" {
		if started {
			return "The file ended in the middle of a record. That cannot be anything but an interrupted write, and no client was ever told it committed — so the server truncated it, logged a warning, and carried on."
		}
		return "Unexpected: a torn tail should have been truncated, not refused."
	}
	if started {
		return "Unexpected: a checksum mismatch should have stopped startup."
	}
	return "A complete record failed its checksum. That is real corruption, not a crash artifact, so the server refuses to start and names the byte offset. Skipping the record would silently lose data nobody would ever notice."
}

func (d *Dashboard) repair(w http.ResponseWriter, r *http.Request) {
	if !d.managed {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "repair needs a managed server (-queue-bin)"})
		return
	}
	var req struct {
		Queue string `json:"queue"`
	}
	readJSON(w, r, &req)
	if req.Queue == "" {
		req.Queue = d.runner.Target()
	}
	if d.sup.Running() {
		d.sup.Kill()
	}
	out, err := d.sup.Repair(req.Queue)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"error": err.Error(), "output": out})
		return
	}
	d.events.add("operator ran repair: log truncated at the corrupt offset, original backed up")
	startErr := d.sup.Start()
	resp := map[string]any{"output": out, "started": startErr == nil, "logs": d.sup.Logs()}
	if startErr != nil {
		resp["error"] = startErr.Error()
	} else {
		_ = d.EnsureQueues()
	}
	writeJSON(w, http.StatusOK, resp)
}

// EnsureQueues creates every composition the dashboard offers. Safe to call
// repeatedly: an existing queue is not an error.
func (d *Dashboard) EnsureQueues() error {
	for _, def := range QueueDefs {
		cfg := QueueConfig{
			Name:                       def.Name,
			Ordering:                   def.Ordering,
			PriorityEnabled:            def.Priority,
			MaxAttempts:                4,
			DefaultVisibilityTimeoutMS: 4000,
			AgingIntervalMS:            def.AgingMS,
			AgingMaxBoost:              8,
			DedupWindowMS:              60000,
		}
		if err := d.client.CreateQueue(cfg); err != nil {
			return fmt.Errorf("creating queue %q: %w", def.Name, err)
		}
	}
	return nil
}

// --- helpers ---------------------------------------------------------------

func readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.ContentLength == 0 {
		return true
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
