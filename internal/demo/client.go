// Package demo is a job runner that drives the queue over its real HTTP API,
// plus a dashboard for watching it work.
//
// It deliberately talks to the queue the same way any other client would —
// over HTTP, in a separate process — rather than importing the engine
// directly. That is what makes the crash and corruption demos honest: the
// dashboard cannot fake recovery, because it does not own the state.
package demo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is a small typed client for the queue API.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// NewClient returns a client with timeouts suited to a local server.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
		HTTP:    &http.Client{Timeout: 10 * time.Second},
	}
}

type apiError struct {
	Status int
	Body   string
}

func (e *apiError) Error() string { return fmt.Sprintf("queue api: status %d: %s", e.Status, e.Body) }

func (c *Client) do(method, path string, body, out any) error {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequest(method, c.BaseURL+path, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return &apiError{Status: resp.StatusCode, Body: string(bytes.TrimSpace(raw))}
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

// QueueConfig mirrors the create-queue request body.
type QueueConfig struct {
	Name                       string `json:"name"`
	Ordering                   string `json:"ordering"`
	PriorityEnabled            bool   `json:"priority_enabled"`
	MaxAttempts                int    `json:"max_attempts,omitempty"`
	DefaultVisibilityTimeoutMS int    `json:"default_visibility_timeout_ms,omitempty"`
	AgingIntervalMS            int    `json:"aging_interval_ms,omitempty"`
	AgingMaxBoost              int    `json:"aging_max_boost,omitempty"`
	DedupWindowMS              int    `json:"dedup_window_ms,omitempty"`
}

// CreateQueue creates a queue, treating "already exists" as success so the
// demo can be restarted against an existing data directory.
func (c *Client) CreateQueue(cfg QueueConfig) error {
	err := c.do(http.MethodPost, "/queues", cfg, nil)
	var ae *apiError
	if err != nil && asAPIError(err, &ae) && ae.Status == http.StatusConflict {
		return nil
	}
	return err
}

func asAPIError(err error, dst **apiError) bool {
	ae, ok := err.(*apiError)
	if ok {
		*dst = ae
	}
	return ok
}

// EnqueueRequest is one enqueue.
type EnqueueRequest struct {
	Payload  any    `json:"payload"`
	Priority int    `json:"priority"`
	DelayMS  int64  `json:"delay_ms,omitempty"`
	DedupID  string `json:"dedup_id,omitempty"`
}

// EnqueueResult is what the server said about it.
type EnqueueResult struct {
	ID        string `json:"id"`
	Seq       uint64 `json:"seq"`
	Duplicate bool   `json:"duplicate"`
}

func (c *Client) Enqueue(queue string, req EnqueueRequest) (EnqueueResult, error) {
	var res EnqueueResult
	err := c.do(http.MethodPost, "/queues/"+queue+"/messages", req, &res)
	return res, err
}

// Leased is a message handed out by a dequeue.
type Leased struct {
	ID        string          `json:"id"`
	Seq       uint64          `json:"seq"`
	Priority  int             `json:"priority"`
	Attempts  int             `json:"attempts"`
	CreatedAt time.Time       `json:"created_at"`
	Deadline  time.Time       `json:"deadline"`
	Payload   json.RawMessage `json:"payload"`
}

func (c *Client) Dequeue(queue string, max int, visibilityMS int64) ([]Leased, error) {
	var out struct {
		Messages []Leased `json:"messages"`
	}
	err := c.do(http.MethodPost, "/queues/"+queue+"/dequeue", map[string]any{
		"max_messages":          max,
		"visibility_timeout_ms": visibilityMS,
	}, &out)
	return out.Messages, err
}

func (c *Client) Ack(queue, id string) error {
	return c.do(http.MethodPost, "/queues/"+queue+"/messages/"+id+"/ack", nil, nil)
}

func (c *Client) Nack(queue, id string) error {
	return c.do(http.MethodPost, "/queues/"+queue+"/messages/"+id+"/nack", nil, nil)
}

// Stats and Peek are forwarded to the dashboard as raw JSON: the UI reads the
// server's real response shape rather than a copy of it that could drift.
func (c *Client) Stats(queue string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.do(http.MethodGet, "/queues/"+queue+"/stats", nil, &raw)
	return raw, err
}

// ListQueues returns every queue with its stats, for the queue selector.
func (c *Client) ListQueues() (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.do(http.MethodGet, "/queues", nil, &raw)
	return raw, err
}

func (c *Client) Peek(queue string, limit int) (json.RawMessage, error) {
	var raw json.RawMessage
	err := c.do(http.MethodGet, fmt.Sprintf("/queues/%s/peek?limit=%d", queue, limit), nil, &raw)
	return raw, err
}

func (c *Client) Compact(queue string) (before, after int64, err error) {
	var out struct {
		Before int64 `json:"before_bytes"`
		After  int64 `json:"after_bytes"`
	}
	err = c.do(http.MethodPost, "/queues/"+queue+"/compact", nil, &out)
	return out.Before, out.After, err
}

// Healthy reports whether the server is up and every queue is accepting work.
func (c *Client) Healthy() bool {
	req, err := http.NewRequest(http.MethodGet, c.BaseURL+"/healthz", nil)
	if err != nil {
		return false
	}
	client := &http.Client{Timeout: 750 * time.Millisecond}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}
