package internal

import (
	"context"
	"fmt"
	"log/slog"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"github.com/warpstreamlabs/bento/v4/public/service"
)

// inputModule wraps a Bento input. Messages received from the external source
// are forwarded to the host EventBus via MessagePublisher.
type inputModule struct {
	name         string
	config       map[string]any
	targetTopic  string
	targetBroker string
	publisher    sdk.MessagePublisher
	stream       *service.Stream
	cancel       context.CancelFunc
	done         chan struct{}
}

// SetMessagePublisher satisfies the MessageAwareModule interface.
func (m *inputModule) SetMessagePublisher(pub sdk.MessagePublisher) {
	m.publisher = pub
}

// SetMessageSubscriber satisfies the MessageAwareModule interface.
// inputModule does not need to subscribe, but must implement the full interface.
func (m *inputModule) SetMessageSubscriber(_ sdk.MessageSubscriber) {}

func newInputModule(name string, config map[string]any) (*inputModule, error) {
	return &inputModule{
		name:   name,
		config: config,
		done:   make(chan struct{}),
	}, nil
}

// Init extracts target_topic (required) and target_broker (optional) from config.
func (m *inputModule) Init() error {
	topic, ok := m.config["target_topic"].(string)
	if !ok || topic == "" {
		return fmt.Errorf("bento.input %q: target_topic is required", m.name)
	}
	m.targetTopic = topic

	if broker, ok := m.config["target_broker"].(string); ok {
		m.targetBroker = broker
	}

	// Ensure an input config is present.
	if _, ok := m.config["input"]; !ok {
		return fmt.Errorf("bento.input %q: input configuration is required", m.name)
	}
	return nil
}

// Start builds and runs the Bento input stream. Each message received is
// published to the host EventBus topic.
func (m *inputModule) Start(ctx context.Context) error {
	if m.publisher == nil {
		return fmt.Errorf("bento.input %q: no MessagePublisher set; ensure the host injects one", m.name)
	}

	slog.Info("starting bento input", "module", m.name, "target_topic", m.targetTopic)

	// Build input YAML from the "input" key of the config.
	inputCfg, ok := m.config["input"].(map[string]any)
	if !ok {
		return fmt.Errorf("bento.input %q: input config must be a map", m.name)
	}
	inputYAML, err := configToYAML(inputCfg)
	if err != nil {
		return fmt.Errorf("bento.input %q: %w", m.name, err)
	}

	builder := service.NewStreamBuilder()
	builder.DisableLinting()
	if err := builder.AddInputYAML(inputYAML); err != nil {
		return fmt.Errorf("bento.input %q: add input yaml: %w", m.name, err)
	}

	topic := m.targetTopic
	pub := m.publisher
	moduleName := m.name

	if err := builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		payload, msgErr := msg.AsBytes()
		if msgErr != nil {
			slog.Error("failed to read message bytes", "error", msgErr, "module", moduleName)
			return fmt.Errorf("read message bytes: %w", msgErr)
		}

		// Collect metadata into a string map.
		meta := map[string]string{}
		_ = msg.MetaWalkMut(func(key string, value any) error {
			if s, ok := value.(string); ok {
				meta[key] = s
			} else {
				meta[key] = fmt.Sprintf("%v", value)
			}
			return nil
		})

		slog.Debug("forwarding message to host eventbus", "module", moduleName, "topic", topic, "size", len(payload))

		_, pubErr := pub.Publish(topic, payload, meta)
		if pubErr != nil {
			slog.Error("failed to publish message", "error", pubErr, "module", moduleName, "topic", topic)
		}
		return pubErr
	}); err != nil {
		return fmt.Errorf("bento.input %q: add consumer func: %w", m.name, err)
	}

	stream, err := builder.Build()
	if err != nil {
		return fmt.Errorf("bento.input %q: build stream: %w", m.name, err)
	}
	m.stream = stream

	runCtx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	slog.Info("bento input running", "module", m.name)

	go func() {
		defer close(m.done)
		if err := stream.Run(runCtx); err != nil && runCtx.Err() == nil {
			slog.Error("bento input stream failed", "error", err, "module", moduleName)
		}
	}()

	return nil
}

// Stop halts the stream and waits for the goroutine to exit.
func (m *inputModule) Stop(ctx context.Context) error {
	slog.Info("stopping bento input", "module", m.name)

	if m.stream != nil {
		if err := m.stream.Stop(ctx); err != nil {
			slog.Error("error stopping bento input", "error", err, "module", m.name)
			return fmt.Errorf("bento.input %q: stop: %w", m.name, err)
		}
	}
	if m.cancel != nil {
		m.cancel()
	}
	select {
	case <-m.done:
		slog.Info("bento input stopped", "module", m.name)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
