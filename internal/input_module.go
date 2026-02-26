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
	log          *bentoLogger
	metrics      *StreamMetrics
	health       *healthTracker
}

// SetMessagePublisher satisfies the MessageAwareModule interface.
func (m *inputModule) SetMessagePublisher(pub sdk.MessagePublisher) {
	m.publisher = pub
}

// SetMessageSubscriber satisfies the MessageAwareModule interface.
// inputModule does not need to subscribe, but must implement the full interface.
func (m *inputModule) SetMessageSubscriber(_ sdk.MessageSubscriber) {}

func newInputModule(name string, config map[string]any) (*inputModule, error) {
	metrics := newStreamMetrics()
	return &inputModule{
		name:    name,
		config:  config,
		done:    make(chan struct{}),
		log:     newLogger("bento.input", name),
		metrics: metrics,
		health:  newHealthTracker(metrics),
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

// inputTransportType extracts the top-level key from the input config map,
// which represents the actual Bento input type (e.g. "kafka", "generate").
// Falls back to "bento.input" if the type cannot be determined.
func inputTransportType(inputCfg map[string]any) string {
	for k := range inputCfg {
		return k
	}
	return "bento.input"
}

// Start builds and runs the Bento input stream. Each message received is
// published to the host EventBus topic.
func (m *inputModule) Start(ctx context.Context) error {
	if m.publisher == nil {
		return fmt.Errorf("bento.input %q: no MessagePublisher set; ensure the host injects one", m.name)
	}

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
	metrics := m.metrics
	log := m.log

	if err := builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		payload, msgErr := msg.AsBytes()
		if msgErr != nil {
			metrics.RecordError()
			log.LogStreamError(msgErr, slog.String("phase", "read_bytes"))
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

		_, pubErr := pub.Publish(topic, payload, meta)
		if pubErr != nil {
			metrics.RecordError()
			log.LogStreamError(pubErr, slog.String("phase", "publish"))
			return pubErr
		}

		metrics.RecordMessageOut()
		log.LogMessageProcessed(topic)
		return nil
	}); err != nil {
		return fmt.Errorf("bento.input %q: add consumer func: %w", m.name, err)
	}

	stream, err := builder.Build()
	if err != nil {
		return fmt.Errorf("bento.input %q: build stream: %w", m.name, err)
	}
	m.stream = stream

	runCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	m.metrics.MarkStarted()
	// Extract the actual input transport type (e.g. "kafka", "generate") for
	// more informative log output instead of the generic module type string.
	transport := inputTransportType(inputCfg)
	m.log.LogStreamStart(transport,
		slog.String("target_topic", m.targetTopic),
		slog.String("target_broker", m.targetBroker),
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

// Stop halts the stream and waits for the goroutine to exit.
func (m *inputModule) Stop(ctx context.Context) error {
	if m.stream != nil {
		if err := m.stream.Stop(ctx); err != nil {
			m.metrics.RecordError()
			m.log.LogStreamError(err, slog.String("phase", "stop"))
			return fmt.Errorf("bento.input %q: stop: %w", m.name, err)
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
	m.log.LogStreamStop(snap.MessagesOut,
		slog.String("target_topic", m.targetTopic),
		slog.Duration("uptime", snap.Uptime),
		slog.Int64("errors", snap.Errors),
	)

	return nil
}

// Health returns the current health report for this input module.
func (m *inputModule) Health() HealthReport {
	return m.health.Report()
}
