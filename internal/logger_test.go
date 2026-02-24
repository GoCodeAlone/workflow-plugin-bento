package internal

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// captureLogger returns a bentoLogger that writes JSON records to a buffer so
// tests can inspect the output.
func captureLogger(buf *bytes.Buffer, component, name string) *bentoLogger {
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelDebug, // capture everything
	})
	return &bentoLogger{
		logger: slog.New(handler).With(
			slog.String("component", component),
			slog.String("name", name),
		),
	}
}

// lastRecord decodes the last JSON line written to buf.
func lastRecord(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) == 0 {
		t.Fatal("no log records written")
	}
	var record map[string]any
	if err := json.Unmarshal(lines[len(lines)-1], &record); err != nil {
		t.Fatalf("unmarshal last log record: %v", err)
	}
	return record
}

// --- Logger tests ---

func TestLogStreamStart(t *testing.T) {
	var buf bytes.Buffer
	l := captureLogger(&buf, "bento.stream", "test-stream")
	l.LogStreamStart("kafka")

	rec := lastRecord(t, &buf)
	if rec["msg"] != "stream started" {
		t.Errorf("expected msg %q, got %q", "stream started", rec["msg"])
	}
	if rec["transport"] != "kafka" {
		t.Errorf("expected transport %q, got %q", "kafka", rec["transport"])
	}
	if rec["component"] != "bento.stream" {
		t.Errorf("expected component %q, got %q", "bento.stream", rec["component"])
	}
}

func TestLogStreamStop(t *testing.T) {
	var buf bytes.Buffer
	l := captureLogger(&buf, "bento.stream", "test-stream")
	l.LogStreamStop(42)

	rec := lastRecord(t, &buf)
	if rec["msg"] != "stream stopped" {
		t.Errorf("expected msg %q, got %q", "stream stopped", rec["msg"])
	}
	// JSON numbers decode as float64.
	if int64(rec["messages_processed"].(float64)) != 42 {
		t.Errorf("expected messages_processed 42, got %v", rec["messages_processed"])
	}
}

func TestLogStreamError(t *testing.T) {
	var buf bytes.Buffer
	l := captureLogger(&buf, "bento.stream", "test-stream")
	l.LogStreamError(errors.New("connection refused"))

	rec := lastRecord(t, &buf)
	if rec["msg"] != "stream error" {
		t.Errorf("expected msg %q, got %q", "stream error", rec["msg"])
	}
	if rec["level"] != "ERROR" {
		t.Errorf("expected level ERROR, got %v", rec["level"])
	}
}

func TestLogMessageProcessed(t *testing.T) {
	var buf bytes.Buffer
	l := captureLogger(&buf, "bento.input", "test-input")
	l.LogMessageProcessed("orders")

	rec := lastRecord(t, &buf)
	if rec["msg"] != "message processed" {
		t.Errorf("expected msg %q, got %q", "message processed", rec["msg"])
	}
	if rec["topic"] != "orders" {
		t.Errorf("expected topic %q, got %q", "orders", rec["topic"])
	}
}

func TestLogTopicEvent(t *testing.T) {
	var buf bytes.Buffer
	l := captureLogger(&buf, "bento.broker", "test-broker")
	l.LogTopicEvent("stream_created", "payments")

	rec := lastRecord(t, &buf)
	if rec["msg"] != "topic event" {
		t.Errorf("expected msg %q, got %q", "topic event", rec["msg"])
	}
	if rec["event"] != "stream_created" {
		t.Errorf("expected event %q, got %q", "stream_created", rec["event"])
	}
}

func TestLogProcessingStartComplete(t *testing.T) {
	var buf bytes.Buffer
	l := captureLogger(&buf, "step.bento", "my-step")
	l.LogProcessingStart("my-step")
	l.LogProcessingComplete("my-step")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("expected 2 log lines, got %d", len(lines))
	}
	var first, last map[string]any
	if err := json.Unmarshal(lines[0], &first); err != nil {
		t.Fatalf("unmarshal first: %v", err)
	}
	if err := json.Unmarshal(lines[1], &last); err != nil {
		t.Fatalf("unmarshal last: %v", err)
	}
	if first["msg"] != "processing started" {
		t.Errorf("expected %q, got %q", "processing started", first["msg"])
	}
	if last["msg"] != "processing complete" {
		t.Errorf("expected %q, got %q", "processing complete", last["msg"])
	}
}

func TestLogProcessingError(t *testing.T) {
	var buf bytes.Buffer
	l := captureLogger(&buf, "step.bento", "my-step")
	l.LogProcessingError("my-step", errors.New("bloblang parse failed"))

	rec := lastRecord(t, &buf)
	if rec["msg"] != "processing error" {
		t.Errorf("expected msg %q, got %q", "processing error", rec["msg"])
	}
	if rec["level"] != "ERROR" {
		t.Errorf("expected level ERROR, got %v", rec["level"])
	}
}

// --- Metrics tests ---

func TestMetricsRecordMessageIn(t *testing.T) {
	m := newStreamMetrics()
	m.RecordMessageIn()
	m.RecordMessageIn()

	if m.MessagesIn() != 2 {
		t.Errorf("expected MessagesIn=2, got %d", m.MessagesIn())
	}
	if m.MessagesOut() != 0 {
		t.Errorf("expected MessagesOut=0, got %d", m.MessagesOut())
	}
}

func TestMetricsRecordMessageOut(t *testing.T) {
	m := newStreamMetrics()
	m.RecordMessageOut()

	if m.MessagesOut() != 1 {
		t.Errorf("expected MessagesOut=1, got %d", m.MessagesOut())
	}
}

func TestMetricsRecordError(t *testing.T) {
	m := newStreamMetrics()
	m.RecordError()
	m.RecordError()

	if m.Errors() != 2 {
		t.Errorf("expected Errors=2, got %d", m.Errors())
	}
}

func TestMetricsUptime(t *testing.T) {
	m := newStreamMetrics()
	m.MarkStarted()
	time.Sleep(5 * time.Millisecond)
	uptime := m.Uptime()

	if uptime < 5*time.Millisecond {
		t.Errorf("expected uptime >= 5ms, got %v", uptime)
	}
}

func TestMetricsLastMessageTime(t *testing.T) {
	m := newStreamMetrics()
	if !m.LastMessageTime().IsZero() {
		t.Error("expected zero LastMessageTime before any messages")
	}

	before := time.Now()
	m.RecordMessageIn()
	after := time.Now()

	lmt := m.LastMessageTime()
	if lmt.Before(before) || lmt.After(after) {
		t.Errorf("LastMessageTime %v not in range [%v, %v]", lmt, before, after)
	}
}

func TestMetricsSnapshot(t *testing.T) {
	m := newStreamMetrics()
	m.MarkStarted()
	m.RecordMessageIn()
	m.RecordMessageIn()
	m.RecordMessageOut()
	m.RecordError()

	snap := m.Snapshot()
	if snap.MessagesIn != 2 {
		t.Errorf("expected MessagesIn=2, got %d", snap.MessagesIn)
	}
	if snap.MessagesOut != 1 {
		t.Errorf("expected MessagesOut=1, got %d", snap.MessagesOut)
	}
	if snap.Errors != 1 {
		t.Errorf("expected Errors=1, got %d", snap.Errors)
	}
	if snap.Uptime <= 0 {
		t.Errorf("expected positive Uptime, got %v", snap.Uptime)
	}
	if snap.LastMessageTime.IsZero() {
		t.Error("expected non-zero LastMessageTime")
	}
}

func TestMetricsConcurrentUpdates(t *testing.T) {
	m := newStreamMetrics()
	const goroutines = 50
	const msgsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < msgsPerGoroutine; j++ {
				m.RecordMessageIn()
				m.RecordMessageOut()
				m.RecordError()
			}
		}()
	}
	wg.Wait()

	expected := int64(goroutines * msgsPerGoroutine)
	if m.MessagesIn() != expected {
		t.Errorf("MessagesIn: expected %d, got %d", expected, m.MessagesIn())
	}
	if m.MessagesOut() != expected {
		t.Errorf("MessagesOut: expected %d, got %d", expected, m.MessagesOut())
	}
	if m.Errors() != expected {
		t.Errorf("Errors: expected %d, got %d", expected, m.Errors())
	}
}

// --- Health tests ---

func TestHealthStatusString(t *testing.T) {
	cases := []struct {
		status HealthStatus
		want   string
	}{
		{HealthStatusHealthy, "healthy"},
		{HealthStatusDegraded, "degraded"},
		{HealthStatusUnhealthy, "unhealthy"},
	}
	for _, c := range cases {
		if got := c.status.String(); got != c.want {
			t.Errorf("HealthStatus(%d).String() = %q, want %q", c.status, got, c.want)
		}
	}
}

func TestHealthTracker_InitiallyUnhealthy(t *testing.T) {
	m := newStreamMetrics()
	h := newHealthTracker(m)

	report := h.Report()
	if report.Status != HealthStatusUnhealthy {
		t.Errorf("expected Unhealthy before SetRunning(true), got %v", report.Status)
	}
}

func TestHealthTracker_HealthyWhenRunning(t *testing.T) {
	m := newStreamMetrics()
	h := newHealthTracker(m)
	h.SetRunning(true)

	report := h.Report()
	if report.Status != HealthStatusHealthy {
		t.Errorf("expected Healthy after SetRunning(true) with no errors, got %v", report.Status)
	}
}

func TestHealthTracker_DegradedOnError(t *testing.T) {
	m := newStreamMetrics()
	h := newHealthTracker(m)
	h.SetRunning(true)
	m.RecordError()

	report := h.Report()
	if report.Status != HealthStatusDegraded {
		t.Errorf("expected Degraded after error, got %v", report.Status)
	}
	if report.Errors != 1 {
		t.Errorf("expected Errors=1 in report, got %d", report.Errors)
	}
}

func TestHealthTracker_UnhealthyAfterStop(t *testing.T) {
	m := newStreamMetrics()
	h := newHealthTracker(m)
	h.SetRunning(true)
	h.SetRunning(false)

	report := h.Report()
	if report.Status != HealthStatusUnhealthy {
		t.Errorf("expected Unhealthy after stop, got %v", report.Status)
	}
}

func TestHealthTracker_ReportContainsMetrics(t *testing.T) {
	m := newStreamMetrics()
	h := newHealthTracker(m)
	h.SetRunning(true)
	m.RecordMessageIn()
	m.RecordMessageOut()

	report := h.Report()
	if report.MessagesIn != 1 {
		t.Errorf("expected MessagesIn=1, got %d", report.MessagesIn)
	}
	if report.MessagesOut != 1 {
		t.Errorf("expected MessagesOut=1, got %d", report.MessagesOut)
	}
	if report.StatusText != "healthy" {
		t.Errorf("expected status_text=healthy, got %q", report.StatusText)
	}
}

func TestHealthTracker_ConcurrentAccess(t *testing.T) {
	m := newStreamMetrics()
	h := newHealthTracker(m)

	var wg sync.WaitGroup
	const goroutines = 20
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			h.SetRunning(i%2 == 0)
			m.RecordMessageIn()
			_ = h.Report()
		}()
	}
	wg.Wait() // should not race
}
