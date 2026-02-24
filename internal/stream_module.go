package internal

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/warpstreamlabs/bento/v4/public/service"
)

// streamStatus represents the current state of a stream.
type streamStatus string

const (
	streamStarting streamStatus = "starting"
	streamRunning  streamStatus = "running"
	streamStopped  streamStatus = "stopped"
	streamErrored  streamStatus = "errored"
)

// streamModule wraps a complete Bento stream (input → pipeline → output) as a ModuleInstance.
type streamModule struct {
	name   string
	config map[string]any
	stream *service.Stream
	cancel context.CancelFunc
	done   chan struct{}
	status streamStatus
	mu     sync.RWMutex
}

func newStreamModule(name string, config map[string]any) (*streamModule, error) {
	return &streamModule{
		name:   name,
		config: config,
		done:   make(chan struct{}),
		status: streamStopped,
	}, nil
}

// Status returns the current stream status.
func (m *streamModule) Status() streamStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status
}

func (m *streamModule) setStatus(status streamStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = status
}

// Init validates the configuration.
func (m *streamModule) Init() error {
	if len(m.config) == 0 {
		return fmt.Errorf("bento.stream %q: configuration is empty", m.name)
	}
	return nil
}

// Start builds and runs the Bento stream.
func (m *streamModule) Start(ctx context.Context) error {
	m.setStatus(streamStarting)
	slog.Info("starting bento stream", "module", m.name)

	yamlStr, err := configToYAML(m.config)
	if err != nil {
		m.setStatus(streamErrored)
		return fmt.Errorf("bento.stream %q: %w", m.name, err)
	}

	builder := service.NewStreamBuilder()
	if err := builder.SetYAML(yamlStr); err != nil {
		m.setStatus(streamErrored)
		return fmt.Errorf("bento.stream %q: set yaml: %w", m.name, err)
	}

	stream, err := builder.Build()
	if err != nil {
		m.setStatus(streamErrored)
		return fmt.Errorf("bento.stream %q: build stream: %w", m.name, err)
	}
	m.stream = stream

	runCtx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	m.setStatus(streamRunning)
	slog.Info("bento stream running", "module", m.name)

	go func() {
		defer close(m.done)
		if err := stream.Run(runCtx); err != nil && runCtx.Err() == nil {
			m.setStatus(streamErrored)
			slog.Error("bento stream failed", "error", err, "module", m.name)
		} else {
			m.setStatus(streamStopped)
		}
	}()

	return nil
}

// Stop halts the running stream and waits for the goroutine to exit.
func (m *streamModule) Stop(ctx context.Context) error {
	slog.Info("stopping bento stream", "module", m.name)

	if m.stream != nil {
		if err := m.stream.Stop(ctx); err != nil {
			m.setStatus(streamErrored)
			slog.Error("error stopping bento stream", "error", err, "module", m.name)
			return fmt.Errorf("bento.stream %q: stop: %w", m.name, err)
		}
	}
	if m.cancel != nil {
		m.cancel()
	}
	select {
	case <-m.done:
		m.setStatus(streamStopped)
		slog.Info("bento stream stopped", "module", m.name)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
