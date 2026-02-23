package internal

import (
	"sync"
	"sync/atomic"
	"time"
)

// StreamMetrics tracks runtime statistics for a single stream or module.
// All counter operations are thread-safe via atomic primitives; the
// startTime/lastMessageTime fields are guarded by a small mutex.
type StreamMetrics struct {
	messagesIn  atomic.Int64
	messagesOut atomic.Int64
	errors      atomic.Int64

	mu              sync.Mutex
	startTime       time.Time
	lastMessageTime time.Time
}

// newStreamMetrics creates a StreamMetrics instance with the start time set
// to the current wall clock.
func newStreamMetrics() *StreamMetrics {
	m := &StreamMetrics{}
	m.startTime = time.Now()
	return m
}

// RecordMessageIn increments the inbound message counter and updates
// lastMessageTime.
func (m *StreamMetrics) RecordMessageIn() {
	m.messagesIn.Add(1)
	m.mu.Lock()
	m.lastMessageTime = time.Now()
	m.mu.Unlock()
}

// RecordMessageOut increments the outbound message counter and updates
// lastMessageTime.
func (m *StreamMetrics) RecordMessageOut() {
	m.messagesOut.Add(1)
	m.mu.Lock()
	m.lastMessageTime = time.Now()
	m.mu.Unlock()
}

// RecordError increments the error counter.
func (m *StreamMetrics) RecordError() {
	m.errors.Add(1)
}

// MessagesIn returns the total inbound message count.
func (m *StreamMetrics) MessagesIn() int64 {
	return m.messagesIn.Load()
}

// MessagesOut returns the total outbound message count.
func (m *StreamMetrics) MessagesOut() int64 {
	return m.messagesOut.Load()
}

// Errors returns the total error count.
func (m *StreamMetrics) Errors() int64 {
	return m.errors.Load()
}

// Uptime returns how long the stream has been running since Start was called.
func (m *StreamMetrics) Uptime() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startTime.IsZero() {
		return 0
	}
	return time.Since(m.startTime)
}

// LastMessageTime returns the time the last message was recorded.
// Returns the zero value if no messages have been recorded yet.
func (m *StreamMetrics) LastMessageTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastMessageTime
}

// MetricsSnapshot is a point-in-time copy of StreamMetrics that can be
// safely marshalled or logged without holding any locks.
type MetricsSnapshot struct {
	MessagesIn      int64         `json:"messages_in"`
	MessagesOut     int64         `json:"messages_out"`
	Errors          int64         `json:"errors"`
	Uptime          time.Duration `json:"uptime_ns"`
	LastMessageTime time.Time     `json:"last_message_time"`
}

// Snapshot returns a consistent point-in-time copy of the metrics.
func (m *StreamMetrics) Snapshot() MetricsSnapshot {
	m.mu.Lock()
	uptime := time.Duration(0)
	if !m.startTime.IsZero() {
		uptime = time.Since(m.startTime)
	}
	lastMsg := m.lastMessageTime
	m.mu.Unlock()

	return MetricsSnapshot{
		MessagesIn:      m.messagesIn.Load(),
		MessagesOut:     m.messagesOut.Load(),
		Errors:          m.errors.Load(),
		Uptime:          uptime,
		LastMessageTime: lastMsg,
	}
}
