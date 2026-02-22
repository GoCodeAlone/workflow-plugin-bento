package internal

import (
	"context"
	"fmt"

	"github.com/warpstreamlabs/bento/v4/public/service"
)

// streamModule wraps a complete Bento stream (input → pipeline → output) as a ModuleInstance.
type streamModule struct {
	name   string
	config map[string]any
	stream *service.Stream
	cancel context.CancelFunc
	done   chan struct{}
}

func newStreamModule(name string, config map[string]any) (*streamModule, error) {
	return &streamModule{
		name:   name,
		config: config,
		done:   make(chan struct{}),
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

	runCtx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	go func() {
		defer close(m.done)
		if err := stream.Run(runCtx); err != nil && runCtx.Err() == nil {
			// Log error but don't panic — stream may have been stopped intentionally.
			_ = err
		}
	}()

	return nil
}

// Stop halts the running stream and waits for the goroutine to exit.
func (m *streamModule) Stop(ctx context.Context) error {
	if m.stream != nil {
		if err := m.stream.Stop(ctx); err != nil {
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
	return nil
}
