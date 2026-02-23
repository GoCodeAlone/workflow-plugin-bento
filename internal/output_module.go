package internal

import (
	"context"
	"fmt"
	"log/slog"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"github.com/warpstreamlabs/bento/v4/public/service"
)

// outputModule wraps a Bento output. It subscribes to a host EventBus topic and
// forwards incoming messages into the Bento output.
type outputModule struct {
	name         string
	config       map[string]any
	sourceTopic  string
	sourceBroker string
	subscriber   sdk.MessageSubscriber
	stream       *service.Stream
	producerFn   service.MessageHandlerFunc
	cancel       context.CancelFunc
	done         chan struct{}
}

// SetMessagePublisher satisfies the MessageAwareModule interface.
// outputModule does not publish, but must implement the full interface.
func (m *outputModule) SetMessagePublisher(_ sdk.MessagePublisher) {}

// SetMessageSubscriber satisfies the MessageAwareModule interface.
func (m *outputModule) SetMessageSubscriber(sub sdk.MessageSubscriber) {
	m.subscriber = sub
}

func newOutputModule(name string, config map[string]any) (*outputModule, error) {
	return &outputModule{
		name:   name,
		config: config,
		done:   make(chan struct{}),
	}, nil
}

// Init extracts source_topic (required) and source_broker (optional) from config.
func (m *outputModule) Init() error {
	topic, ok := m.config["source_topic"].(string)
	if !ok || topic == "" {
		return fmt.Errorf("bento.output %q: source_topic is required", m.name)
	}
	m.sourceTopic = topic

	if broker, ok := m.config["source_broker"].(string); ok {
		m.sourceBroker = broker
	}

	// Ensure an output config is present.
	if _, ok := m.config["output"]; !ok {
		return fmt.Errorf("bento.output %q: output configuration is required", m.name)
	}
	return nil
}

// Start builds the Bento output stream, registers a producer func, and
// subscribes to the host EventBus topic. Incoming messages are fed into Bento
// for delivery.
func (m *outputModule) Start(ctx context.Context) error {
	if m.subscriber == nil {
		return fmt.Errorf("bento.output %q: no MessageSubscriber set; ensure the host injects one", m.name)
	}

	// Build output YAML from the "output" key of the config.
	outputCfg, ok := m.config["output"].(map[string]any)
	if !ok {
		return fmt.Errorf("bento.output %q: output config must be a map", m.name)
	}
	outputYAML, err := configToYAML(outputCfg)
	if err != nil {
		return fmt.Errorf("bento.output %q: %w", m.name, err)
	}

	builder := service.NewStreamBuilder()
	builder.DisableLinting()
	if err := builder.AddOutputYAML(outputYAML); err != nil {
		return fmt.Errorf("bento.output %q: add output yaml: %w", m.name, err)
	}

	producerFn, err := builder.AddProducerFunc()
	if err != nil {
		return fmt.Errorf("bento.output %q: add producer func: %w", m.name, err)
	}
	m.producerFn = producerFn

	stream, err := builder.Build()
	if err != nil {
		return fmt.Errorf("bento.output %q: build stream: %w", m.name, err)
	}
	m.stream = stream

	runCtx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	go func() {
		defer close(m.done)
		if err := stream.Run(runCtx); err != nil && runCtx.Err() == nil {
			slog.Error("bento output stream runtime error", "name", m.name, "error", err)
		}
	}()

	// Subscribe to the host EventBus topic. When messages arrive, forward them
	// to the Bento producer.
	producerFnRef := m.producerFn
	if err := m.subscriber.Subscribe(m.sourceTopic, func(payload []byte, metadata map[string]string) error {
		msg := service.NewMessage(payload)
		for k, v := range metadata {
			msg.MetaSet(k, v)
		}
		return producerFnRef(runCtx, msg)
	}); err != nil {
		return fmt.Errorf("bento.output %q: subscribe to topic %q: %w", m.name, m.sourceTopic, err)
	}

	return nil
}

// Stop unsubscribes, stops the stream, and waits for the goroutine to exit.
func (m *outputModule) Stop(ctx context.Context) error {
	if m.subscriber != nil {
		_ = m.subscriber.Unsubscribe(m.sourceTopic)
	}
	if m.stream != nil {
		if err := m.stream.Stop(ctx); err != nil {
			return fmt.Errorf("bento.output %q: stop: %w", m.name, err)
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
