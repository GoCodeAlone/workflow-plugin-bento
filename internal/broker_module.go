package internal

import (
	"context"
	"fmt"
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
	return &brokerModule{
		name:    name,
		config:  config,
		streams: make(map[string]*service.Stream),
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
	return nil
}

// Stop shuts down all managed streams.
func (m *brokerModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for topic, stream := range m.streams {
		if err := stream.Stop(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("bento.broker %q: stop stream for topic %q: %w", m.name, topic, err)
		}
	}
	m.streams = make(map[string]*service.Stream)
	return firstErr
}

// ensureStream returns (creating if necessary) a running stream for topic.
// This is used internally when the broker needs a dedicated in-process pipe.
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
	if pub != nil {
		if err := builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
			payload, msgErr := msg.AsBytes()
			if msgErr != nil {
				return msgErr
			}
			meta := map[string]string{}
			_ = msg.MetaWalkMut(func(k string, v any) error {
				meta[k] = fmt.Sprintf("%v", v)
				return nil
			})
			_, pubErr := pub.Publish(topic, payload, meta)
			return pubErr
		}); err != nil {
			return nil, fmt.Errorf("add consumer for topic %q: %w", topic, err)
		}
	}

	stream, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("build stream for topic %q: %w", topic, err)
	}

	go func() {
		if err := stream.Run(ctx); err != nil && ctx.Err() == nil {
			_ = err
		}
	}()

	m.streams[topic] = stream
	return stream, nil
}
