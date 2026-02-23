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

	slog.Info("starting bento output", "module", m.name, "source_topic", m.sourceTopic)

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

	moduleName := m.name
	producerFnRef := m.producerFn

	// Subscribe before launching the goroutine so that if Subscribe fails we
	// have not yet started the stream and there is no goroutine to clean up.
	if err := m.subscriber.Subscribe(m.sourceTopic, func(payload []byte, metadata map[string]string) error {
		slog.Debug("sending message to bento output", "module", moduleName, "topic", m.sourceTopic, "size", len(payload))

		msg := service.NewMessage(payload)
		for k, v := range metadata {
			msg.MetaSet(k, v)
		}
		sendErr := producerFnRef(runCtx, msg)
		if sendErr != nil {
			slog.Error("failed to send message to bento output", "error", sendErr, "module", moduleName)
		}
		return sendErr
	}); err != nil {
		cancel()
		return fmt.Errorf("bento.output %q: subscribe to topic %q: %w", m.name, m.sourceTopic, err)
	}

	go func() {
		defer close(m.done)
		if err := stream.Run(runCtx); err != nil && runCtx.Err() == nil {
			slog.Error("bento output stream failed", "error", err, "module", moduleName)
		}
	}()

	slog.Info("bento output running", "module", m.name)

	return nil
}

// Stop unsubscribes, stops the stream, and waits for the goroutine to exit.
func (m *outputModule) Stop(ctx context.Context) error {
	slog.Info("stopping bento output", "module", m.name)

	if m.subscriber != nil {
		if err := m.subscriber.Unsubscribe(m.sourceTopic); err != nil {
			slog.Error("error unsubscribing from source topic", "error", err, "module", m.name, "topic", m.sourceTopic)
		}
	}
	if m.stream != nil {
		if err := m.stream.Stop(ctx); err != nil {
			slog.Error("error stopping bento output", "error", err, "module", m.name)
			return fmt.Errorf("bento.output %q: stop: %w", m.name, err)
		}
	}
	if m.cancel != nil {
		m.cancel()
	}
	select {
	case <-m.done:
		slog.Info("bento output stopped", "module", m.name)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
