package internal

import (
	"sync"
	"time"
)

// HealthStatus represents the health state of a stream or module.
type HealthStatus int

const (
	// HealthStatusHealthy means the stream is operating normally.
	HealthStatusHealthy HealthStatus = iota
	// HealthStatusDegraded means the stream has experienced some errors but is
	// still running.
	HealthStatusDegraded
	// HealthStatusUnhealthy means the stream has stopped or is not functioning.
	HealthStatusUnhealthy
)

// String returns a human-readable representation of HealthStatus.
func (s HealthStatus) String() string {
	switch s {
	case HealthStatusHealthy:
		return "healthy"
	case HealthStatusDegraded:
		return "degraded"
	default:
		return "unhealthy"
	}
}

// HealthReport is a point-in-time summary of a stream's health.
type HealthReport struct {
	Status          HealthStatus  `json:"status"`
	StatusText      string        `json:"status_text"`
	MessagesIn      int64         `json:"messages_in"`
	MessagesOut     int64         `json:"messages_out"`
	Errors          int64         `json:"errors"`
	Uptime          time.Duration `json:"uptime_ns"`
	LastMessageTime time.Time     `json:"last_message_time"`
}

// healthTracker derives health status from a StreamMetrics instance and
// additional runtime flags set by lifecycle methods.
type healthTracker struct {
	metrics *StreamMetrics

	mu      sync.Mutex
	running bool

	// errorThreshold is the number of errors after which status transitions
	// to degraded; defaults to 1.
	errorThreshold int64
}

// newHealthTracker creates a healthTracker backed by the given metrics.
func newHealthTracker(m *StreamMetrics) *healthTracker {
	return &healthTracker{
		metrics:        m,
		errorThreshold: 1,
	}
}

// SetRunning marks the stream as started (true) or stopped (false).
func (h *healthTracker) SetRunning(running bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.running = running
}

// Report returns the current HealthReport by examining metrics and state.
func (h *healthTracker) Report() HealthReport {
	h.mu.Lock()
	running := h.running
	h.mu.Unlock()

	snap := h.metrics.Snapshot()

	var status HealthStatus
	switch {
	case !running:
		status = HealthStatusUnhealthy
	case snap.Errors >= h.errorThreshold:
		status = HealthStatusDegraded
	default:
		status = HealthStatusHealthy
	}

	return HealthReport{
		Status:          status,
		StatusText:      status.String(),
		MessagesIn:      snap.MessagesIn,
		MessagesOut:     snap.MessagesOut,
		Errors:          snap.Errors,
		Uptime:          snap.Uptime,
		LastMessageTime: snap.LastMessageTime,
	}
}
