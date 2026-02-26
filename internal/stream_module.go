package internal

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/warpstreamlabs/bento/v4/public/service"
)

// streamModule wraps a complete Bento stream (input → pipeline → output) as a ModuleInstance.
type streamModule struct {
	name    string
	config  map[string]any
	stream  *service.Stream
	cancel  context.CancelFunc
	done    chan struct{}
	log     *bentoLogger
	metrics *StreamMetrics
	health  *healthTracker
}

func newStreamModule(name string, config map[string]any) (*streamModule, error) {
	metrics := newStreamMetrics()
	return &streamModule{
		name:    name,
		config:  config,
		done:    make(chan struct{}),
		log:     newLogger("bento.stream", name),
		metrics: metrics,
		health:  newHealthTracker(metrics),
	}, nil
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
	yamlStr, err := configToYAML(m.config)
	if err != nil {
		return fmt.Errorf("bento.stream %q: %w", m.name, err)
	}

	builder := service.NewStreamBuilder()
	if err := builder.SetYAML(yamlStr); err != nil {
		return fmt.Errorf("bento.stream %q: set yaml: %w", m.name, err)
	}

	stream, err := builder.Build()
	if err != nil {
		return fmt.Errorf("bento.stream %q: build stream: %w", m.name, err)
	}
	m.stream = stream

	runCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	m.metrics.MarkStarted()
	m.log.LogStreamStart("bento",
		slog.Int("config_keys", len(m.config)),
	)

	go func() {
		defer close(m.done)
		m.health.SetRunning(true)
		if runErr := stream.Run(runCtx); runCtx.Err() == nil {
			m.health.SetRunning(false)
			if runErr != nil {
				m.metrics.RecordError()
				m.log.LogStreamError(runErr)
			}
		}
	}()

	return nil
}

// Stop halts the running stream and waits for the goroutine to exit.
func (m *streamModule) Stop(ctx context.Context) error {
	if m.stream != nil {
		if err := m.stream.Stop(ctx); err != nil {
			m.metrics.RecordError()
			m.log.LogStreamError(err, slog.String("phase", "stop"))
			return fmt.Errorf("bento.stream %q: stop: %w", m.name, err)
		}
	}
	if m.cancel != nil {
		m.cancel()
	}
	select {
	case <-m.done:
	case <-ctx.Done():
		return ctx.Err()
	}

	m.health.SetRunning(false)
	m.metrics.MarkStopped()
	snap := m.metrics.Snapshot()
	// Stream modules don't intercept individual messages, so messages_processed is 0.
	m.log.LogStreamStop(0,
		slog.Duration("uptime", snap.Uptime),
		slog.Int64("errors", snap.Errors),
	)

	return nil
}

// Health returns the current health report for this stream.
func (m *streamModule) Health() HealthReport {
	return m.health.Report()
}
