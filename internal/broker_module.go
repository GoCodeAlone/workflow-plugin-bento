package internal

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"github.com/warpstreamlabs/bento/v4/public/service"
)

// brokerModule presents a MessageBroker interface via Bento transport.
// Streams are created on demand, one per topic.
type brokerModule struct {
	name            string
	config          map[string]any
	transport       string
	transportConfig map[string]any
	publisher       sdk.MessagePublisher
	subscriber      sdk.MessageSubscriber
	streams         map[string]*service.Stream
	mu              sync.RWMutex
	log             *bentoLogger
	metrics         *StreamMetrics
	health          *healthTracker
}

// SetMessagePublisher satisfies the MessageAwareModule interface.
func (m *brokerModule) SetMessagePublisher(pub sdk.MessagePublisher) {
	m.publisher = pub
}

// SetMessageSubscriber satisfies the MessageAwareModule interface.
func (m *brokerModule) SetMessageSubscriber(sub sdk.MessageSubscriber) {
	m.subscriber = sub
}

func newBrokerModule(name string, config map[string]any) (*brokerModule, error) {
	metrics := newStreamMetrics()
	return &brokerModule{
		name:    name,
		config:  config,
		streams: make(map[string]*service.Stream),
		log:     newLogger("bento.broker", name),
		metrics: metrics,
		health:  newHealthTracker(metrics),
	}, nil
}

// Init extracts transport and transport_config from the module config.
func (m *brokerModule) Init() error {
	transport, _ := m.config["transport"].(string)
	if transport == "" {
		transport = "memory"
	}
	m.transport = transport

	if tc, ok := m.config["transport_config"].(map[string]any); ok {
		m.transportConfig = tc
	} else {
		m.transportConfig = map[string]any{}
	}

	return nil
}

// Start is a no-op; individual per-topic streams are created on demand.
func (m *brokerModule) Start(_ context.Context) error {
	m.health.SetRunning(true)
	m.log.LogStreamStart(m.transport)
	return nil
}

// Stop shuts down all managed streams.
func (m *brokerModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	slog.Info("stopping bento broker", "module", m.name, "topics", len(m.streams))

	var firstErr error
	for topic, stream := range m.streams {
		if err := stream.Stop(ctx); err != nil && firstErr == nil {
			m.metrics.RecordError()
			m.log.LogStreamError(err, slog.String("topic", topic))
			firstErr = fmt.Errorf("bento.broker %q: stop stream for topic %q: %w", m.name, topic, err)
		}
		slog.Info("broker stream stopped", "module", m.name, "topic", topic)
	}
	m.streams = make(map[string]*service.Stream)

	m.health.SetRunning(false)
	snap := m.metrics.Snapshot()
	m.log.LogStreamStop(snap.MessagesIn+snap.MessagesOut,
		slog.String("transport", m.transport),
		slog.Duration("uptime", snap.Uptime),
		slog.Int64("errors", snap.Errors),
	)

	return firstErr
}

// ensureStream returns (creating if necessary) a running stream for topic.
// This is used internally when the broker needs a dedicated in-process pipe.
//
//nolint:unused // Reserved for future use by broker consumers.
func (m *brokerModule) ensureStream(ctx context.Context, topic string) (*service.Stream, error) {
	m.mu.RLock()
	if s, ok := m.streams[topic]; ok {
		m.mu.RUnlock()
		return s, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock.
	if s, ok := m.streams[topic]; ok {
		return s, nil
	}

	slog.Info("creating broker stream", "module", m.name, "topic", topic, "transport", m.transport)

	// Build a simple in-memory stream that holds messages for this topic.
	// The actual transport is configured via transportConfig / transport.
	builder := service.NewStreamBuilder()
	builder.DisableLinting()

	// Compose the input config from transport settings.
	inputCfg := map[string]any{m.transport: m.transportConfig}
	inputYAML, err := configToYAML(inputCfg)
	if err != nil {
		return nil, fmt.Errorf("build input yaml for topic %q: %w", topic, err)
	}
	if err := builder.AddInputYAML(inputYAML); err != nil {
		return nil, fmt.Errorf("add input yaml for topic %q: %w", topic, err)
	}

	pub := m.publisher
	metrics := m.metrics
	log := m.log

	if pub != nil {
		if err := builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
			payload, msgErr := msg.AsBytes()
			if msgErr != nil {
				metrics.RecordError()
				log.LogStreamError(msgErr, slog.String("topic", topic))
				return msgErr
			}
			meta := map[string]string{}
			_ = msg.MetaWalkMut(func(k string, v any) error {
				meta[k] = fmt.Sprintf("%v", v)
				return nil
			})

			slog.Debug("broker forwarding message", "module", moduleName, "topic", topic, "size", len(payload))

			_, pubErr := pub.Publish(topic, payload, meta)
			if pubErr != nil {
				metrics.RecordError()
				log.LogStreamError(pubErr, slog.String("phase", "publish"), slog.String("topic", topic))
				return pubErr
			}
			metrics.RecordMessageOut()
			log.LogMessageProcessed(topic)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("add consumer for topic %q: %w", topic, err)
		}
	}

	stream, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("build stream for topic %q: %w", topic, err)
	}

	m.log.LogTopicEvent("stream_created", topic,
		slog.String("transport", m.transport),
	)

	go func() {
		if runErr := stream.Run(ctx); runErr != nil && ctx.Err() == nil {
			metrics.RecordError()
			log.LogStreamError(runErr, slog.String("topic", topic))
		}
	}()

	m.streams[topic] = stream
	slog.Info("broker stream created", "module", m.name, "topic", topic)
	return stream, nil
}

// Health returns the current health report for this broker module.
func (m *brokerModule) Health() HealthReport {
	return m.health.Report()
}
